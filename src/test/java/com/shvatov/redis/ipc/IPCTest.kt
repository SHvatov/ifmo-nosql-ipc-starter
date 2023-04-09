package com.shvatov.redis.ipc

import com.shvatov.redis.ipc.IPCTest.TestRedisListenerConfiguration
import com.shvatov.redis.ipc.config.RedisIPCAutoConfiguration
import com.shvatov.redis.ipc.config.RedisIPCAutoConfiguration.RedisCommonConfiguration.Companion.REDIS_IPC_SCHEDULER_BEAN
import com.shvatov.redis.ipc.dto.TestMessage
import com.shvatov.redis.ipc.extension.RedisExtension
import com.shvatov.redis.ipc.publisher.TestRedisPublisher
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration
import org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit.jupiter.SpringExtension
import reactor.core.scheduler.Scheduler
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicReference

@SpringBootTest(classes = [TestRedisListenerConfiguration::class])
@ExtendWith(SpringExtension::class)
@ExtendWith(RedisExtension::class)
@TestPropertySource(locations = ["classpath:application-test.properties"])
class IPCTest {

    @field:Autowired
    @field:Qualifier(REDIS_IPC_SCHEDULER_BEAN)
    private lateinit var ipcScheduler: Scheduler

    @field:Autowired
    private lateinit var publisher: TestRedisPublisher

    @SpringBootApplication
    @Import(
        value = [
            RedisIPCAutoConfiguration::class,
            RedisAutoConfiguration::class,
            RedisReactiveAutoConfiguration::class
        ]
    )
    class TestRedisListenerConfiguration {

    }

    @Test
    @Timeout(value = 20, unit = SECONDS)
    fun redisHasStartedTest() {
        val response = AtomicReference<TestMessage?>(null)

        val request = TestMessage(payload = "Request")
        publisher.publishAndAwaitSingleResponse(request)
            .doOnNext { response.set(it) }
            .block()

        assertNotNull(response.get())
        assertEquals(request.correlationId, response.get()!!.correlationId)
        assertEquals("Response", response.get()!!.payload)
    }

}
