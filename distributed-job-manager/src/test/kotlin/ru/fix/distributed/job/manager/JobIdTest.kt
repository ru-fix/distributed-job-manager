package ru.fix.distributed.job.manager

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
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

    @Test
    fun `JobId equality based on String id equality`(){
        JobId("foo").equals(JobId("foo")).shouldBeTrue()
        JobId("foo").equals(JobId("bar")).shouldBeFalse()
    }


}