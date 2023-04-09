package com.shvatov.redis.ipc.registrar

import com.fasterxml.jackson.databind.ObjectMapper
import com.shvatov.redis.ipc.annotation.listener.ChannelName
import com.shvatov.redis.ipc.annotation.listener.Listener
import com.shvatov.redis.ipc.annotation.listener.Payload
import com.shvatov.redis.ipc.config.RedisIPCAutoConfiguration.RedisCommonConfiguration.Companion.REDIS_IPC_OBJECT_MAPPER_BEAN
import com.shvatov.redis.ipc.config.RedisIPCAutoConfiguration.RedisCommonConfiguration.Companion.REDIS_IPC_SCHEDULER_BEAN
import com.shvatov.redis.ipc.config.RedisIPCAutoConfiguration.RedisListenerConfiguration.Companion.REDIS_IPC_MESSAGE_LISTENER_CONTAINER_BEAN
import net.bytebuddy.implementation.InvocationHandlerAdapter
import net.bytebuddy.matcher.ElementMatchers.isOverriddenFrom
import org.reactivestreams.Subscription
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanInitializationException
import org.springframework.beans.factory.DisposableBean
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.config.BeanDefinition
import org.springframework.beans.factory.support.BeanDefinitionRegistry
import org.springframework.beans.factory.support.GenericBeanDefinition
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.core.env.Environment
import org.springframework.core.type.AnnotationMetadata
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.PatternTopic
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair.fromSerializer
import org.springframework.data.redis.serializer.RedisSerializer.byteArray
import org.springframework.data.redis.serializer.RedisSerializer.string
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler
import reactor.util.retry.RetrySpec
import java.lang.reflect.Method
import java.nio.charset.Charset
import java.time.Duration
import java.util.Collections
import java.util.concurrent.atomic.AtomicReference

internal class RedisListenerBeanDefinitionRegistrar(
    environment: Environment
) : AbstractRedisBeanDefinitionRegistrar(environment) {

    override fun registerBeanDefinitions(metadata: AnnotationMetadata, registry: BeanDefinitionRegistry) {
        doRegisterRedisListeners(registry)
    }

    private fun doRegisterRedisListeners(registry: BeanDefinitionRegistry) {
        val processedListenerClasses = Collections.synchronizedSet(HashSet<Class<*>>())
        reflections.getMethodsAnnotatedWith(Listener::class.java).forEach { method ->
            val listenerClass = method.declaringClass
            if (!processedListenerClasses.add(listenerClass)) {
                throw BeanInitializationException(
                    "Found multiple listeners declared in the same class " +
                            listenerClass.simpleName
                )
            }
            doRegisterRedisListener(method, registry)
        }
    }

    private fun doRegisterRedisListener(method: Method, registry: BeanDefinitionRegistry) {
        log.trace(
            "Registering Redis listener based on the method {}#{}",
            method.declaringClass.simpleName, method.name
        )

        val config = resolveListenerConfig(method)
        val listenerProxyClass = createListenerProxyClass(config)
        registerListenerProxyBean(registry, listenerProxyClass, config)
    }

    private fun createListenerProxyClass(config: RedisListenerConfig): Class<*> {
        val helper = RedisListenerHelper(config)
        return byteBuddy.subclass(config.listenerClass)
            .implement(RedisListenerProxy::class.java)
            .method(isOverriddenFrom(ApplicationContextAware::class.java))
            .intercept(InvocationHandlerAdapter.of { obj, _, args -> helper.init(obj, args[0] as ApplicationContext) })
            .method(isOverriddenFrom(InitializingBean::class.java))
            .intercept(InvocationHandlerAdapter.of { _, _, _ -> helper.subscribe() })
            .method(isOverriddenFrom(DisposableBean::class.java))
            .intercept(InvocationHandlerAdapter.of { _, _, _ -> helper.destroy() })
            .make()
            .load(this::class.java.classLoader)
            .loaded
    }

    private fun resolveListenerConfig(method: Method): RedisListenerConfig {
        val annotation = method.getAnnotation(Listener::class.java)
        return with(annotation) {
            RedisListenerConfig(
                listenerMethod = method,
                listenerClass = method.declaringClass,
                payloadClass = method.payloadClass,
                channelNameArgumentIsFirst = method.isChannelNameFirstArgument,
                channels = channels.resolvedFromEnvironment(),
                channelPatterns = channelPatterns.resolvedFromEnvironment(),
                retries = actualRetriesNumber,
                retriesBackOffDuration = actualRetriesBackoffDuration,
                bufferSize = actualBufferSize,
                bufferingDuration = actualBufferingDuration,
            ).apply { validate() }
        }
    }

    private fun RedisListenerConfig.validate() {
        if (channels.isEmpty() && channelPatterns.isEmpty()) {
            throw BeanInitializationException("\"channels\" or \"channelPatterns\" must be not empty")
        }

        if (bufferSize > 0) {
            val payloadHolderClass = listenerMethod.parameters
                .first { it.isAnnotationPresent(Payload::class.java) }
                .type
            if (!payloadHolderClass.isAssignableFrom(List::class.java)) {
                throw BeanInitializationException(
                    "Type of the parameter annotated with " +
                            "@Payload must be List if buffering is enabled"
                )
            }
        }
    }

    private fun registerListenerProxyBean(
        registry: BeanDefinitionRegistry,
        listenerProxyClass: Class<*>,
        config: RedisListenerConfig
    ) {
        val beanDefinition = GenericBeanDefinition()
            .apply {
                setBeanClass(listenerProxyClass)
                setDependsOn(
                    REDIS_IPC_MESSAGE_LISTENER_CONTAINER_BEAN,
                    REDIS_IPC_OBJECT_MAPPER_BEAN,
                    REDIS_IPC_SCHEDULER_BEAN
                )
                scope = BeanDefinition.SCOPE_SINGLETON
            }
        registry.registerBeanDefinition(IPC_LISTENER_PREFIX + config.listenerClass.simpleName, beanDefinition)
    }

    private val Method.payloadClass: Class<*>
        get() {
            val payloadArgument = parameters.firstOrNull { it.isAnnotationPresent(Payload::class.java) }
                ?: throw BeanInitializationException(
                    "One of the arguments of the listener " +
                            "method must be annotated with @Payload"
                )
            return payloadArgument.getAnnotation(Payload::class.java).payloadClass
                .takeIf { it != Unit::class }
                ?.javaObjectType
                ?: payloadArgument.type
        }

    private val Method.isChannelNameFirstArgument: Boolean
        get() {
            val (index, _) = parameters.withIndex()
                .firstOrNull { (_, param) -> param.isAnnotationPresent(ChannelName::class.java) }
                ?: throw BeanInitializationException(
                    "One of the arguments of the listener " +
                            "method must be annotated with @ChannelName"
                )
            return index == 0
        }

    private fun Array<String>.resolvedFromEnvironment() =
        map { environment.getProperty(it, it) }
            .filter { it.isNotBlank() }

    private val Listener.actualRetriesNumber: Int
        get() = if (retriesExpression.isNotBlank()) {
            environment.getRequiredProperty(retriesExpression, Int::class.java)
        } else retries

    private val Listener.actualRetriesBackoffDuration: Duration?
        get() = if (retriesBackoffDurationExpression.isNotBlank()) {
            environment.getRequiredProperty(retriesBackoffDurationExpression, Duration::class.java)
        } else if (retriesBackoffDuration > 0) {
            Duration.of(
                retriesBackoffDuration.toLong(),
                retriesBackoffDurationUnit.toChronoUnit()
            )
        } else null

    private val Listener.actualBufferSize: Int
        get() = if (bufferSizeExpression.isNotBlank()) {
            environment.getRequiredProperty(bufferSizeExpression, Int::class.java)
        } else bufferSize

    private val Listener.actualBufferingDuration: Duration?
        get() = if (bufferingDurationExpression.isNotBlank()) {
            environment.getRequiredProperty(bufferSizeExpression, Duration::class.java)
        } else if (bufferingDuration > 0) {
            Duration.of(
                bufferingDuration.toLong(),
                bufferingDurationUnit.toChronoUnit()
            )
        } else null

    internal interface RedisListenerProxy : ApplicationContextAware, InitializingBean, DisposableBean

    private class RedisListenerHelper(private val config: RedisListenerConfig) {

        private lateinit var listenerContainer: ReactiveRedisMessageListenerContainer
        private lateinit var objectMapper: ObjectMapper
        private lateinit var disposable: Disposable
        private lateinit var originalListener: Any
        private lateinit var scheduler: Scheduler

        private val subscription = AtomicReference<Subscription?>(null)

        fun init(originalListener: Any, applicationContext: ApplicationContext) {
            log.trace("Initializing RedisListenerHelper for {}", config.listenerClass.simpleName)

            this.originalListener = originalListener

            listenerContainer = applicationContext.getBean(
                REDIS_IPC_MESSAGE_LISTENER_CONTAINER_BEAN,
                ReactiveRedisMessageListenerContainer::class.java
            )

            objectMapper = applicationContext.getBean(REDIS_IPC_OBJECT_MAPPER_BEAN, ObjectMapper::class.java)

            scheduler = applicationContext.getBean(REDIS_IPC_SCHEDULER_BEAN, Scheduler::class.java)
        }

        fun subscribe() {
            val topics = config.channels.map { ChannelTopic(it) } +
                    config.channelPatterns.map { PatternTopic(it) }

            val listenerClassName = config.listenerClass.simpleName
            val listenerMethodName = config.listenerMethod.name

            log.trace("Subscribing to topics {} in {}", topics, listenerClassName)

            val listener = listenerContainer.receive(topics, fromSerializer(string()), fromSerializer(byteArray()))
                .publishOn(scheduler)
                .doOnNext { log.trace("Received message {} in {}", it, listenerClassName) }
                .map {
                    val payload = when {
                        config.payloadClass.isAssignableFrom(ByteArray::class.java) -> it.message

                        config.payloadClass.isAssignableFrom(String::class.java) ->
                            String(it.message, Charset.defaultCharset())

                        else -> objectMapper.readValue(it.message, config.payloadClass)
                    }
                    RedisMessage(it.channel, payload)
                }

            var extendedListener = if (config.bufferSize > 1) {
                val bufferedListener = if (config.bufferingDuration != null) {
                    listener.bufferTimeout(config.bufferSize, config.bufferingDuration)
                } else listener.buffer(config.bufferSize)

                bufferedListener.flatMapIterable {
                    it.groupBy { (channelName, _) -> channelName }
                        .map { (channelName, messages) ->
                            RedisMessages(
                                channelName,
                                messages.map { (_, payload) -> payload })
                        }
                }
            } else listener

            extendedListener = extendedListener.doOnNext {
                log.trace("Deserialized message(s) {} in {}", it, listenerClassName)
            }

            extendedListener = extendedListener.flatMap {
                val arguments = when (it) {
                    is RedisMessage -> if (config.channelNameArgumentIsFirst) {
                        arrayOf(it.channelName, it.payload)
                    } else arrayOf(it.payload, it.channelName)

                    is RedisMessages -> if (config.channelNameArgumentIsFirst) {
                        arrayOf(it.channelName, it.payloads)
                    } else arrayOf(it.payloads, it.channelName)

                    else -> throw UnsupportedOperationException("Unknown type of payload " + it::class.java.simpleName)
                }

                val processor = Flux.just(it)
                    .publishOn(scheduler)
                    .doOnNext {
                        log.trace(
                            "Executing {}#{} with the following arguments: {}",
                            listenerClassName, listenerMethodName, arguments
                        )
                        config.listenerMethod.invoke(originalListener, *arguments)
                    }
                    .doOnError { exception ->
                        log.error(
                            "Execution of {}#{} with the following arguments has failed: {}",
                            listenerClassName, listenerMethodName, arguments, exception
                        )
                    }

                if (config.retries > 0) {
                    if (config.retriesBackOffDuration != null) {
                        processor.retryWhen(
                            RetrySpec.backoff(
                                config.retries.toLong(),
                                config.retriesBackOffDuration
                            ).doBeforeRetry {
                                log.warn(
                                    "Retrying execution of {}#{} with the following arguments: {}",
                                    listenerClassName, listenerMethodName, arguments
                                )
                            }
                        )
                    } else processor.retryWhen(RetrySpec.max(config.retries.toLong()))
                } else processor
            }

            disposable = extendedListener
                .doOnError { log.error("Unhandled exception during message processing", it) }
                .doOnSubscribe {
                    log.trace("Subscribing to topics {} in {}", topics, listenerClassName)
                    subscription.set(it)
                }
                .subscribe()
        }

        fun destroy() {
            log.trace("Destroying RedisListenerHelper for {}", config.listenerClass.simpleName)
            subscription.get()!!.cancel()
            disposable.dispose()
        }

        private data class RedisMessage(val channelName: String, val payload: Any)

        private data class RedisMessages(val channelName: String, val payloads: List<Any>)

        private companion object {
            val log: Logger = LoggerFactory.getLogger(RedisListenerHelper::class.java)
        }
    }

    private data class RedisListenerConfig(
        val listenerMethod: Method,
        val listenerClass: Class<*>,
        val payloadClass: Class<*>,
        val channelNameArgumentIsFirst: Boolean,
        val channelPatterns: List<String>,
        val channels: List<String>,
        val retries: Int,
        val retriesBackOffDuration: Duration?,
        val bufferSize: Int,
        val bufferingDuration: Duration?,
    )

}
