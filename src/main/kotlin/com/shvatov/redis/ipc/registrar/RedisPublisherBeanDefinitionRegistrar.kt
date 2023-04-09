package com.shvatov.redis.ipc.registrar

import com.fasterxml.jackson.databind.ObjectMapper
import com.shvatov.redis.ipc.annotation.publisher.CorrelationId
import com.shvatov.redis.ipc.annotation.publisher.Publish
import com.shvatov.redis.ipc.annotation.publisher.Publisher
import com.shvatov.redis.ipc.annotation.publisher.PublisherId
import com.shvatov.redis.ipc.config.RedisIPCAutoConfiguration.RedisCommonConfiguration
import com.shvatov.redis.ipc.config.RedisIPCAutoConfiguration.RedisListenerConfiguration
import net.bytebuddy.implementation.InvocationHandlerAdapter
import net.bytebuddy.matcher.ElementMatchers
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanInitializationException
import org.springframework.beans.factory.config.BeanDefinition
import org.springframework.beans.factory.support.BeanDefinitionRegistry
import org.springframework.beans.factory.support.GenericBeanDefinition
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.core.env.Environment
import org.springframework.core.type.AnnotationMetadata
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair.fromSerializer
import org.springframework.data.redis.serializer.RedisSerializer
import org.springframework.util.ReflectionUtils
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.util.retry.Retry
import java.lang.reflect.Field
import java.lang.reflect.Method
import java.lang.reflect.ParameterizedType
import java.time.Duration

// todo:
// - additional validation
// - verbose logging
// - refactoring
internal class RedisPublisherBeanDefinitionRegistrar(
    environment: Environment
) : AbstractRedisBeanDefinitionRegistrar(environment) {

    override fun registerBeanDefinitions(metadata: AnnotationMetadata, registry: BeanDefinitionRegistry) {
        doRegisterRedisPublishers(registry)
    }

    private fun doRegisterRedisPublishers(registry: BeanDefinitionRegistry) {
        reflections.getTypesAnnotatedWith(Publisher::class.java).forEach { publisherClass ->
            doRegisterRedisPublisher(registry, publisherClass)
        }
    }

    private fun doRegisterRedisPublisher(registry: BeanDefinitionRegistry, publisherClass: Class<*>) {
        val publisherProxyClass = createPublisherProxyClass(publisherClass)
        registerPublisherProxyBean(registry, publisherProxyClass, publisherClass)
    }

    private fun createPublisherProxyClass(publisherClass: Class<*>): Class<*> {
        var publisherClassBuilder = byteBuddy.subclass(publisherClass);
        publisherClassBuilder = publisherClassBuilder.implement(RedisPublisherProxy::class.java)

        val helpers = mutableListOf<RedisPublisherHelper>()
        publisherClass.publisherMethods.forEach { method ->
            val config = resolveListenerConfig(method)
            val helper = RedisPublisherHelper(config)
            publisherClassBuilder = publisherClassBuilder
                .method(ElementMatchers.`is`(method))
                .intercept(InvocationHandlerAdapter.of { _, _, args -> helper.publish(args) })
            helpers.add(helper)
        }

        publisherClassBuilder = publisherClassBuilder
            .method(ElementMatchers.isOverriddenFrom(ApplicationContextAware::class.java))
            .intercept(InvocationHandlerAdapter.of { _, _, args ->
                helpers.forEach { helper ->
                    helper.init(args[0] as ApplicationContext)
                }
            })

        return publisherClassBuilder.make()
            .load(this::class.java.classLoader)
            .loaded
    }

    private fun resolveListenerConfig(method: Method): RedisPublisherConfig {
        val annotation = method.getAnnotation(Publish::class.java)
        return with(annotation) {
            RedisPublisherConfig(
                publishMethod = method,
                publishRequestToChannel = publishRequestToChannel
                    .takeIf { it.isNotBlank() }
                    ?: channel,
                receiveResponseFromChannel = receiveResponseFromChannel.takeIf { it.isNotBlank() },
                awaitAtLeastOneReceiver = awaitAtLeastOneReceiver,
                retries = actualRetriesNumber,
                retriesBackoffDuration = actualRetriesBackoffDuration,
                responseAwaitTimeout = actualResponseAwaitTimeout
            ).apply { validate() }
        }
    }

    private fun RedisPublisherConfig.validate() {
        if (publishRequestToChannel.isBlank()) {
            throw BeanInitializationException("\"channel\" or \"publishRequestToChannel\" must be not empty")
        }

        if (!(responsePublisherClass == Mono::class.java || responsePublisherClass == Flux::class.java)) {
            throw BeanInitializationException(
                "Return type of the publisher method must be either Mono or Flux, got "
                        + responsePublisherClass.simpleName
            )
        }

        if (awaitAtLeastOneReceiver && retries <= 0) {
            throw BeanInitializationException(
                "If \"awaitAtLeastOneReceiver\" = true, then number of retries must be >= 0"
            )
        }

        if (!(publishMethodReturnsUnit
                    || publishMethodReturnsReceiversNumber
                    || publishMethodReturnsSingleResponse
                    || publishMethodReturnsMultipleResponses)) {
            throw BeanInitializationException(
                "Return type of the publisher method must be either Mono<Unit>, Mono<Long>, Mono/Flux<T>, got "
                        + responsePublisherClass.simpleName
            )
        }

        if (publishMethodReturnsSingleResponse || publishMethodReturnsMultipleResponses) {
            requireNotNull(requestCorrelationIdField)
            requireNotNull(requestPublisherIdField)
            requireNotNull(responseCorrelationIdField)
            requireNotNull(responsePublisherIdField)
            requireNotNull(responseAwaitTimeout)

            if (publishRequestToChannel == receiveResponseFromChannel) {
                throw BeanInitializationException("Cannot send to the same channel, from which response is expected")
            }
        }
    }

    private fun registerPublisherProxyBean(
        registry: BeanDefinitionRegistry,
        publisherProxyClass: Class<*>,
        publisherClass: Class<*>
    ) {
        val beanDefinition = GenericBeanDefinition()
            .apply {
                setBeanClass(publisherProxyClass)
                setDependsOn(
                    RedisListenerConfiguration.REDIS_IPC_MESSAGE_LISTENER_CONTAINER_BEAN,
                    RedisCommonConfiguration.REDIS_IPC_OBJECT_MAPPER_BEAN,
                    RedisCommonConfiguration.REDIS_IPC_SCHEDULER_BEAN
                )
                scope = BeanDefinition.SCOPE_SINGLETON
            }
        registry.registerBeanDefinition(IPC_LISTENER_PREFIX + publisherClass.simpleName, beanDefinition)
    }

    private val Class<*>.publisherMethods: Collection<Method>
        get() = methods.filter { it.isAnnotationPresent(Publish::class.java) }

    private val Publish.actualRetriesNumber: Int
        get() = if (retriesExpression.isNotBlank()) {
            environment.getRequiredProperty(retriesExpression, Int::class.java)
        } else retries

    private val Publish.actualRetriesBackoffDuration: Duration?
        get() = if (retriesBackoffDurationExpression.isNotBlank()) {
            environment.getRequiredProperty(retriesBackoffDurationExpression, Duration::class.java)
        } else if (retriesBackoffDuration > 0) {
            Duration.of(
                retriesBackoffDuration.toLong(),
                retriesBackoffDurationUnit.toChronoUnit()
            )
        } else null

    private val Publish.actualResponseAwaitTimeout: Duration?
        get() = if (responseAwaitTimeoutExpression.isNotBlank()) {
            environment.getRequiredProperty(responseAwaitTimeoutExpression, Duration::class.java)
        } else if (responseAwaitTimeout > 0) {
            Duration.of(
                responseAwaitTimeout.toLong(),
                responseAwaitTimeoutUnit.toChronoUnit()
            )
        } else null

    internal interface RedisPublisherProxy : ApplicationContextAware

    private class RedisPublisherHelper(private val config: RedisPublisherConfig) {
        private lateinit var listenerContainer: ReactiveRedisMessageListenerContainer
        private lateinit var template: ReactiveRedisTemplate<String, ByteArray>
        private lateinit var objectMapper: ObjectMapper
        private lateinit var scheduler: Scheduler

        fun init(applicationContext: ApplicationContext) {
            log.trace(
                "Initializing RedisPublisherHelper for {}#{}",
                config.publishMethod.declaringClass.simpleName,
                config.publishMethod
            )

            listenerContainer = applicationContext.getBean(
                RedisListenerConfiguration.REDIS_IPC_MESSAGE_LISTENER_CONTAINER_BEAN,
                ReactiveRedisMessageListenerContainer::class.java
            )

            @Suppress("UNCHECKED_CAST")
            template = applicationContext.getBean(
                RedisCommonConfiguration.REDIS_IPC_TEMPLATE_BEAN,
                ReactiveRedisTemplate::class.java
            ) as ReactiveRedisTemplate<String, ByteArray>

            objectMapper = applicationContext.getBean(
                RedisCommonConfiguration.REDIS_IPC_OBJECT_MAPPER_BEAN,
                ObjectMapper::class.java
            )

            scheduler = applicationContext.getBean(
                RedisCommonConfiguration.REDIS_IPC_SCHEDULER_BEAN,
                Scheduler::class.java
            )
        }

        fun publish(args: Array<out Any>): Any {
            val anyPayload = args[0]
            val payloadBytes: ByteArray = if (anyPayload is ByteArray) {
                anyPayload
            } else {
                objectMapper.writeValueAsBytes(anyPayload)
            }

            return with(config) {
                when {
                    publishMethodReturnsUnit -> publishAsync(payloadBytes).then()
                    publishMethodReturnsReceiversNumber -> publishAsync(payloadBytes)
                    publishMethodReturnsSingleResponse -> publishSync(anyPayload, payloadBytes).next()
                    publishMethodReturnsMultipleResponses -> publishSync(anyPayload, payloadBytes)
                    else -> throw UnsupportedOperationException("Unsupported publisher operation!")
                }
            }
        }

        private fun publishAsync(payloadBytes: ByteArray): Mono<Long> {
            return template.convertAndSend(config.publishRequestToChannel, payloadBytes)
                .map { receivers ->
                    if (!config.awaitAtLeastOneReceiver || receivers > 0L) {
                        return@map receivers
                    }
                    throw IllegalStateException("Expected at least one receiver, got none")
                }
                .let { mono ->
                    if (config.retries <= 0) {
                        return@let mono
                    }

                    return@let mono.retryWhen(
                        if (config.retriesBackoffDuration != null) {
                            Retry.backoff(config.retries.toLong(), config.retriesBackoffDuration)
                        } else {
                            Retry.maxInARow(config.retries.toLong())
                        }
                    )
                }
        }

        private fun publishSync(anyPayload: Any, payloadBytes: ByteArray): Flux<out Any> {
            ReflectionUtils.makeAccessible(config.requestCorrelationIdField!!)
            ReflectionUtils.makeAccessible(config.requestPublisherIdField!!)
            ReflectionUtils.makeAccessible(config.responseCorrelationIdField!!)
            ReflectionUtils.makeAccessible(config.responsePublisherIdField!!)

            val rqCorrelationId = ReflectionUtils.getField(config.requestCorrelationIdField, anyPayload)
            val rqPublisherId = ReflectionUtils.getField(config.requestPublisherIdField, anyPayload)

            val publisher = publishAsync(payloadBytes)
            val listener = listenerContainer.receive(
                listOf(ChannelTopic.of(config.receiveResponseFromChannel!!)),
                fromSerializer(RedisSerializer.string()),
                fromSerializer(RedisSerializer.byteArray())
            )
                .publishOn(scheduler)
                .map { objectMapper.readValue(it.message, config.responseClass) }
                .filter { response ->
                    val rsCorrelationId = ReflectionUtils.getField(config.responseCorrelationIdField, response)
                    val rsPublisherId = ReflectionUtils.getField(config.responsePublisherIdField, response)

                    rqCorrelationId == rsCorrelationId
                            && rqPublisherId != rsPublisherId
                }
                .timeout(config.responseAwaitTimeout!!)
            return publisher.flatMapMany { listener }
        }

        private companion object {
            val log: Logger = LoggerFactory.getLogger(RedisPublisherHelper::class.java)
        }
    }

    private data class RedisPublisherConfig(
        val publishMethod: Method,
        val publishRequestToChannel: String,
        val receiveResponseFromChannel: String?,
        val awaitAtLeastOneReceiver: Boolean,
        val retries: Int,
        val retriesBackoffDuration: Duration?,
        val responseAwaitTimeout: Duration?,

        val publishMethodReturnType: ParameterizedType =
            publishMethod.genericReturnType as ParameterizedType,

        val requestClass: Class<*> = publishMethod.parameters.first().type,

        val requestCorrelationIdField: Field? = requestClass.declaredFields
            .firstOrNull { it.isAnnotationPresent(CorrelationId::class.java) },
        val requestPublisherIdField: Field? = requestClass.declaredFields
            .firstOrNull { it.isAnnotationPresent(PublisherId::class.java) },

        val responseClass: Class<*> = publishMethodReturnType.actualTypeArguments[0] as Class<*>,
        val responsePublisherClass: Class<*> = publishMethod.returnType,

        val responseCorrelationIdField: Field? = responseClass.declaredFields
            .firstOrNull { it.isAnnotationPresent(CorrelationId::class.java) },
        val responsePublisherIdField: Field? = responseClass.declaredFields
            .firstOrNull { it.isAnnotationPresent(PublisherId::class.java) },

        val publishMethodReturnsUnit: Boolean =
            responseClass == Unit::class.java
                    || responseClass == Void::class.java,

        val publishMethodReturnsReceiversNumber: Boolean =
            responseClass == Long::class.javaObjectType,

        val publishMethodReturnsSingleResponse: Boolean =
            !publishMethodReturnsUnit
                    && !publishMethodReturnsReceiversNumber
                    && publishMethod.returnType == Mono::class.java,

        val publishMethodReturnsMultipleResponses: Boolean =
            !publishMethodReturnsUnit
                    && !publishMethodReturnsReceiversNumber
                    && publishMethod.returnType == Flux::class.java,
    )
}
