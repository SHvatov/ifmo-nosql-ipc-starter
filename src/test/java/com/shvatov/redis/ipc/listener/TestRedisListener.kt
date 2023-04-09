package com.shvatov.redis.ipc.listener

import com.shvatov.redis.ipc.annotation.listener.ChannelName
import com.shvatov.redis.ipc.annotation.listener.Listener
import com.shvatov.redis.ipc.annotation.listener.Payload
import com.shvatov.redis.ipc.dto.TestMessage
import com.shvatov.redis.ipc.publisher.TestRedisPublisher
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import java.util.concurrent.TimeUnit.MILLISECONDS


open class TestRedisListener {

    @set:Autowired
    lateinit var publisher: TestRedisPublisher

    @Listener(
        channels = ["test_channel_in"],
        bufferSize = 10,
        bufferingDuration = 500,
        bufferingDurationUnit = MILLISECONDS,
        retries = 5
    )
    fun onMessage(
        @ChannelName channel: String,
        @Payload(payloadClass = TestMessage::class) message: List<TestMessage>
    ) {
        logger.error("Processing following message from channel {}: {}", channel, message)

        val firstMessage = message.first()
        publisher.publish(firstMessage.copy(publisherId = UUID.randomUUID(), payload = "Response"))
            .block()
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(TestRedisListener::class.java)
    }
}
