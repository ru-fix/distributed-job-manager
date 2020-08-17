package ru.fix.distributed.job.manager.model

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.JobId

internal class WorkItemTest {
    private val jobId = JobId("job-A")

    @Test
    fun `WorkItem with correct symbols can be created`() {
        WorkItem("foo-is_a56", jobId)
        WorkItem(".withDot", jobId)
    }

    @Test
    fun `WorkItem with incorrect symbols can not be created`() {
        shouldThrow<AssertionError> { WorkItem("withCyrillicSymbol-ы", jobId) }
        shouldThrow<AssertionError> { WorkItem("with:colon", jobId) }
        shouldThrow<AssertionError> { WorkItem("with/slash", jobId) }
        shouldThrow<AssertionError> { WorkItem("with\\backslash", jobId) }
        shouldThrow<AssertionError> { WorkItem(" withSpacePrefix", jobId) }
        shouldThrow<AssertionError> { WorkItem("with space", jobId) }
        shouldThrow<AssertionError> { WorkItem("with]", jobId) }
    }

    @Test
    fun `WorkItem equality based on String id and JobId equality`() {
        WorkItem("foo", jobId) shouldBe WorkItem("foo", jobId)
        WorkItem("foo", jobId) shouldNotBe WorkItem("bar", jobId)
        WorkItem("foo", jobId) shouldNotBe WorkItem("foo", JobId("job-B"))
    }
}