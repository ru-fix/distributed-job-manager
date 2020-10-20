package ru.fix.distributed.job.manager

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.IdentityValidator.IdentityType.JobId

import ru.fix.distributed.job.manager.IdentityValidator.validate

class IdentityValidatorTest {
    @Test
    fun `Identity with correct symbols can be created`() {
        validate(JobId, "foo-is_a5.6")
    }

    @Test
    fun `Identity with incorrect symbols can not be created`() {
        shouldThrow<Exception> { validate(JobId, "withCyrillicSymbol-ы") }
        shouldThrow<Exception> { validate(JobId, "with:colon") }
        shouldThrow<Exception> { validate(JobId, "with/slash") }
        shouldThrow<Exception> { validate(JobId, "with\\backslash") }
        shouldThrow<Exception> { validate(JobId, " withSpacePrefix") }
        shouldThrow<Exception> { validate(JobId, "with space") }
    }

    @Test
    fun `JobId equality based on String id equality`() {
        JobId("foo").equals(JobId("foo")).shouldBeTrue()
        JobId("foo").equals(JobId("bar")).shouldBeFalse()
    }


}