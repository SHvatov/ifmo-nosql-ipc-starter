package com.shvatov.redis.ipc.registrar

import net.bytebuddy.ByteBuddy
import org.reflections.Reflections
import org.reflections.scanners.MethodAnnotationsScanner
import org.reflections.util.ConfigurationBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar
import org.springframework.core.env.Environment

internal abstract class AbstractRedisBeanDefinitionRegistrar(
    protected val environment: Environment
) : ImportBeanDefinitionRegistrar {

    protected val reflections: Reflections = Reflections(
        ConfigurationBuilder()
            .forPackages(
                *environment
                    .getRequiredProperty(PACKAGES_TO_SCAN_PROPERTY)
                    .split(",")
                    .filter { it.isNotBlank() }
                    .toTypedArray()
            )
            .addScanners(MethodAnnotationsScanner())
    )

    protected companion object {
        const val PACKAGES_TO_SCAN_PROPERTY = "spring.data.redis.ipc.packages-to-scan"
        const val IPC_LISTENER_PREFIX = "ipc."

        val byteBuddy: ByteBuddy = ByteBuddy()
        val log: Logger = LoggerFactory.getLogger(AbstractRedisBeanDefinitionRegistrar::class.java)
    }

}
