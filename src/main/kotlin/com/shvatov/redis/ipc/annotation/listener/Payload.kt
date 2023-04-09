package com.shvatov.redis.ipc.annotation.listener

import kotlin.reflect.KClass

@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class Payload(val payloadClass: KClass<out Any> = Unit::class)
