package ru.fix.distributed.job.manager

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.aggregating.profiler.Profiler
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.zookeeper.testing.ZKTestingServer
import java.util.concurrent.atomic.AtomicInteger

open class DjmTestSuite {

    companion object {
        private val lastNodeId = AtomicInteger(1)
        fun generateNodeId() = lastNodeId.incrementAndGet().toString()

        private val lastRootId = AtomicInteger(1)
        fun generateRootPath() = "root/${lastRootId.incrementAndGet()}"
    }

    lateinit var server: ZKTestingServer

    @BeforeEach
    fun beforeEach() {
        server = ZKTestingServer().start()
    }

    @AfterEach
    fun afterEach() {
        server.client
    }

    fun createDJM(job: DistributedJob, profiler: Profiler = NoopProfiler()) =
            createDJM(listOf(job), profiler)

    fun createDJM(jobs: List<DistributedJob>, profiler: Profiler = NoopProfiler()) =
            DistributedJobManager(
                    server.client,
                    jobs,
                    profiler,
                    DistributedJobManagerSettings(
                            nodeId = generateNodeId(),
                            rootPath = generateRootPath(),
                            timeToWaitTermination = DynamicProperty.of(10000)
                    ))

}

