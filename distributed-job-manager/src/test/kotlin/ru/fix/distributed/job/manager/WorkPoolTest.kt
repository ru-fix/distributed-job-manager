package ru.fix.distributed.job.manager

import io.kotest.assertions.throwables.shouldThrow
import org.junit.jupiter.api.Test

class WorkPoolTest {
    @Test
    fun `WorkPool with correct symbols can be created`() {
        WorkPool(setOf("foo-is_a56"))
    }

    @Test
    fun `WorkPool with incorrect symbols can not be created`() {
        shouldThrow<Exception> { WorkPool(setOf("withCyrillicSymbol-ы")) }
        shouldThrow<Exception> { WorkPool(setOf("with:colon")) }
        shouldThrow<Exception> { WorkPool(setOf("with/slash")) }
        shouldThrow<Exception> { WorkPool(setOf("with\\backslash")) }
        shouldThrow<Exception> { WorkPool(setOf(".withDot")) }
        shouldThrow<Exception> { WorkPool(setOf(" withSpacePrefix")) }
        shouldThrow<Exception> { WorkPool(setOf("with space")) }
    }

}