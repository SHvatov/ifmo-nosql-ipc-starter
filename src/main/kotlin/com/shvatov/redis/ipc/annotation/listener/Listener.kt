package com.shvatov.redis.ipc.annotation.listener

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Listener(
    // channels
    val channelPatterns: Array<String> = [],
    val channels: Array<String> = [],

    // retries - number
    val retries: Int = -1,
    val retriesExpression: String = "",

    // retries - backoff
    val retriesBackoffDuration: Int = -1,
    val retriesBackoffDurationUnit: TimeUnit = SECONDS,
    val retriesBackoffDurationExpression: String = "",

    // buffering - by size
    val bufferSize: Int = -1,
    val bufferSizeExpression: String = "",

    // buffering - by duration
    val bufferingDuration: Int = -1,
    val bufferingDurationUnit: TimeUnit = SECONDS,
    val bufferingDurationExpression: String = "",
)
