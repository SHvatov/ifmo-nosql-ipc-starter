package com.shvatov.redis.ipc.extension

import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import java.util.concurrent.ConcurrentHashMap

class RedisExtension : BeforeAllCallback, AfterAllCallback {

    private val redisToTest = ConcurrentHashMap<String, GenericContainer<*>>()

    @Synchronized
    override fun beforeAll(context: ExtensionContext) {
        redisToTest.computeIfAbsent(context.uniqueId) {
            prepareRedis().apply {
                start()
                System.setProperty(REDIS_HOST_PROPERTY, host)
                System.setProperty(REDIS_PORT_PROPERTY, getMappedPort(6379).toString())
            }
        }
    }

    @Synchronized
    override fun afterAll(context: ExtensionContext) {
        redisToTest.getValue(context.uniqueId)
            .apply {
                stop()
                System.clearProperty(REDIS_HOST_PROPERTY)
                System.clearProperty(REDIS_PORT_PROPERTY)
            }
    }

    private companion object {

        private const val REDIS_HOST_PROPERTY = "spring.data.redis.host"
        private const val REDIS_PORT_PROPERTY = "spring.data.redis.port"

        private const val REDIS_LATEST_IMAGE = "redis:latest"
        private const val REDIS_DEFAULT_PORT = 6379

        fun prepareRedis(): GenericContainer<*> =
            GenericContainer(DockerImageName.parse(REDIS_LATEST_IMAGE))
                .withExposedPorts(REDIS_DEFAULT_PORT)
    }

}
