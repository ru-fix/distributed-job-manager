package ru.fix.distributed.job.manager

import io.kotest.assertions.throwables.shouldThrow
import org.junit.jupiter.api.Test

class JobIdTest {
    @Test
    fun `JobId with correct symbols can be created`() {
        JobId("foo-is_a56")
    }

    @Test
    fun `JobId with incorrect symbols can not be created`() {
        shouldThrow<Exception> { JobId("withCyrillicSymbol-ы") }
        shouldThrow<Exception> { JobId("with:colon") }
        shouldThrow<Exception> { JobId("with/slash") }
        shouldThrow<Exception> { JobId("with\\backslash") }
        shouldThrow<Exception> { JobId(".withDot") }
        shouldThrow<Exception> { JobId(" withSpacePrefix") }
        shouldThrow<Exception> { JobId("with space") }
    }
}