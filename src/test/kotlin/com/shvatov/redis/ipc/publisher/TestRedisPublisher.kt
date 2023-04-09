package com.shvatov.redis.ipc.publisher

import com.shvatov.redis.ipc.annotation.publisher.Publish
import com.shvatov.redis.ipc.annotation.publisher.Publisher
import com.shvatov.redis.ipc.dto.TestMessage
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.concurrent.TimeUnit


@Publisher
interface TestRedisPublisher {

    @Publish(
        publishRequestToChannel = "test_channel_in",
        receiveResponseFromChannel = "test_channel_out",
        awaitAtLeastOneReceiver = true,
        retries = 5,
        retriesBackoffDuration = 5,
        retriesBackoffDurationUnit = TimeUnit.SECONDS,
        responseAwaitTimeout = 10,
        responseAwaitTimeoutUnit = TimeUnit.SECONDS
    )
    fun publishAndAwaitSingleResponse(message: TestMessage): Mono<TestMessage>

    @Publish(
        publishRequestToChannel = "test_channel_in",
        receiveResponseFromChannel = "test_channel_out",
        awaitAtLeastOneReceiver = true,
        retries = 5,
        retriesBackoffDuration = 5,
        retriesBackoffDurationUnit = TimeUnit.SECONDS,
        responseAwaitTimeout = 10,
        responseAwaitTimeoutUnit = TimeUnit.SECONDS
    )
    fun publishAndAwaitMultipleResponses(message: TestMessage): Flux<TestMessage>

    @Publish(
        publishRequestToChannel = "test_channel",
        awaitAtLeastOneReceiver = true,
        retries = 5,
        retriesBackoffDuration = 5,
        retriesBackoffDurationUnit = TimeUnit.SECONDS,
        responseAwaitTimeout = 10,
        responseAwaitTimeoutUnit = TimeUnit.SECONDS
    )
    fun publishAndGetReceivers(message: TestMessage): Mono<Long>

    @Publish(
        publishRequestToChannel = "test_channel_out",
        awaitAtLeastOneReceiver = true,
        retries = 5,
        retriesBackoffDuration = 5,
        retriesBackoffDurationUnit = TimeUnit.SECONDS,
        responseAwaitTimeout = 10,
        responseAwaitTimeoutUnit = TimeUnit.SECONDS
    )
    fun publish(message: TestMessage): Mono<Void>

}
