package com.shvatov.redis.ipc.annotation.publisher

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Publish(
    // channels
    val channel: String = "",
    val publishRequestToChannel: String = "",
    val receiveResponseFromChannel: String = "",

    // awaiting receivers
    val awaitAtLeastOneReceiver: Boolean = false,

    // max retries to resend the message until at least one listener consumes message
    val retries: Int = -1,
    val retriesExpression: String = "",
    val retriesBackoffDuration: Int = -1,
    val retriesBackoffDurationUnit: TimeUnit = SECONDS,
    val retriesBackoffDurationExpression: String = "",

    // awaiting response
    val responseAwaitTimeout: Int = -1,
    val responseAwaitTimeoutUnit: TimeUnit = SECONDS,
    val responseAwaitTimeoutExpression: String = ""
)
