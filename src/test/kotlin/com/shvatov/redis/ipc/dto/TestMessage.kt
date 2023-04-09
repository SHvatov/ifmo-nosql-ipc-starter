package com.shvatov.redis.ipc.dto

import com.shvatov.redis.ipc.annotation.publisher.CorrelationId
import com.shvatov.redis.ipc.annotation.publisher.PublisherId
import java.util.UUID

data class TestMessage(
    @field:CorrelationId val correlationId: UUID = UUID.randomUUID(),
    @field:PublisherId val publisherId: UUID = UUID.randomUUID(),
    val payload: String? = null,
)
