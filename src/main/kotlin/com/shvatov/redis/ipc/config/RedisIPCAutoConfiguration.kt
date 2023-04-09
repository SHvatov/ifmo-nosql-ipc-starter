package com.shvatov.redis.ipc.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.shvatov.redis.ipc.config.RedisIPCAutoConfiguration.RedisCommonConfiguration
import com.shvatov.redis.ipc.config.RedisIPCAutoConfiguration.RedisListenerConfiguration
import com.shvatov.redis.ipc.config.RedisIPCAutoConfiguration.RedisPublisherConfiguration
import com.shvatov.redis.ipc.registrar.RedisListenerBeanDefinitionRegistrar
import com.shvatov.redis.ipc.registrar.RedisPublisherBeanDefinitionRegistrar
import com.shvatov.redis.ipc.registrar.RedisTopicsBeanDefinitionRegistrar
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.context.annotation.Import
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisOperations
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.RedisSerializer
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import reactor.core.scheduler.Schedulers

/**
 * @see [org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration]
 * @see [org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration]
 */
@Configuration("redis.ipc.auto-configuration")
@Import(
    value = [
        RedisCommonConfiguration::class,
        RedisListenerConfiguration::class,
        RedisPublisherConfiguration::class,
    ]
)
@AutoConfiguration(after = [RedisReactiveAutoConfiguration::class])
internal class RedisIPCAutoConfiguration {

    @Configuration("redis.ipc.common-configuration")
    @Import(RedisTopicsBeanDefinitionRegistrar::class)
    class RedisCommonConfiguration {

        @Bean(REDIS_IPC_TEMPLATE_BEAN)
        fun reactiveRedisIpcTemplate(
            reactiveRedisConnectionFactory: ReactiveRedisConnectionFactory
        ): ReactiveRedisOperations<String, ByteArray> {
            val serializationContext = RedisSerializationContext.newSerializationContext<String, ByteArray>()
                .key(RedisSerializer.string())
                .hashKey(RedisSerializer.string())
                .value(RedisSerializer.byteArray())
                .hashValue(RedisSerializer.byteArray())
                .build()
            return ReactiveRedisTemplate(reactiveRedisConnectionFactory, serializationContext)
        }

        @Bean(REDIS_IPC_SCHEDULER_BEAN)
        fun reactiveRedisIpcScheduler(
            @Value("\${spring.data.redis.ipc.scheduler.max-pool-size:25}") maxPoolSize: Int,
            @Value("\${spring.data.redis.ipc.scheduler.core-pool-size:1}") corePoolSize: Int,
            @Value("\${spring.data.redis.ipc.scheduler.queue-capacity:10000}") queueCapacity: Int,
            @Value("\${spring.data.redis.ipc.scheduler.keep-alive-seconds:60}") keepAliveSeconds: Int,
        ) = Schedulers.fromExecutor(
            ThreadPoolTaskExecutor().apply {
                setThreadNamePrefix(IPC_THREAD_POOL_PREFIX)
                setKeepAliveSeconds(keepAliveSeconds)
                setQueueCapacity(queueCapacity)
                setCorePoolSize(corePoolSize)
                setMaxPoolSize(maxPoolSize)
            }.also {
                it.initialize()
            }
        )

        @Bean(REDIS_IPC_OBJECT_MAPPER_BEAN)
        fun redisIpcObjectMapper() = ObjectMapper()

        internal companion object {
            private const val IPC_THREAD_POOL_PREFIX = "redis-ipc"

            const val REDIS_IPC_TEMPLATE_BEAN = "reactiveRedisIpcTemplate"
            const val REDIS_IPC_SCHEDULER_BEAN = "reactiveRedisIpcScheduler"
            const val REDIS_IPC_OBJECT_MAPPER_BEAN = "redisIpcObjectMapper"
        }
    }

    @DependsOn("redis.ipc.common-configuration")
    @Configuration("redis.ipc.consumer-configuration")
    @Import(RedisListenerBeanDefinitionRegistrar::class)
    class RedisListenerConfiguration {

        @Bean(REDIS_IPC_MESSAGE_LISTENER_CONTAINER_BEAN)
        fun reactiveRedisIpcMessageListenerContainer(
            reactiveRedisConnectionFactory: ReactiveRedisConnectionFactory
        ) = ReactiveRedisMessageListenerContainer(reactiveRedisConnectionFactory)

        internal companion object {
            const val REDIS_IPC_MESSAGE_LISTENER_CONTAINER_BEAN = "reactiveRedisIpcMessageListenerContainer"
        }
    }

    @DependsOn("redis.ipc.common-configuration")
    @Configuration("redis.ipc.publisher-configuration")
    @Import(RedisPublisherBeanDefinitionRegistrar::class)
    class RedisPublisherConfiguration

}
