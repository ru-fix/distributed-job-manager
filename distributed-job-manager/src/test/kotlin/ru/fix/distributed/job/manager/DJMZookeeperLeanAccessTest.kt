package ru.fix.distributed.job.manager

import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.CuratorCache
import org.apache.curator.framework.recipes.cache.CuratorCacheListener
import org.apache.logging.log4j.kotlin.Logging
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.lang.Thread.sleep
import java.util.concurrent.atomic.AtomicBoolean

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Execution(ExecutionMode.CONCURRENT)
class DJMZookeeperLeanAccessTest : DJMTestSuite() {
    companion object : Logging

    @Test
    fun `when djm cluster is calm only lock nodes are updated in zookeeper`() {
        val jobWithFrequentButConstantWorkPool = object : DistributedJob {
            override val jobId = JobId("jobWithFrequentButConstantWorkPool")
            override fun getSchedule() = DynamicProperty.of(Schedule.withDelay(1000))
            override fun run(context: DistributedJobContext) {}
            override fun getWorkPool() = WorkPool.of((1..10).map { "item-$it" }.toSet())
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 100
        }
        repeat(5) {
            createDJM(jobWithFrequentButConstantWorkPool)
        }

        val zkLocksChanged = AtomicBoolean()
        val zkOtherPathsChaged = AtomicBoolean()

        val zkListener = CuratorCache.build(server.client, djmZkRootPath).apply { start() }
        zkListener.listenable().addListener(CuratorCacheListener { type: CuratorCacheListener.Type,
                                                                   oldData: ChildData?,
                                                                   _: ChildData? ->
            if (oldData != null) {
                val path = oldData.path
                        .removePrefix("/")
                        .substringAfter("/")

                if (path.startsWith("locks/")) {
                    logger.info("Lock path changed: $path, type: $type")
                    zkLocksChanged.set(true)
                } else {
                    logger.info("Other path changed: $path, type: $type")
                    zkOtherPathsChaged.set(true)
                }
            }
        })
        sleep(1000)
        zkLocksChanged.set(false)
        zkOtherPathsChaged.set(false)

        logger.info("Start of calm period")
        sleep(3000)
        logger.info("End of calm period")
        zkLocksChanged.get().shouldBeTrue()
        zkOtherPathsChaged.get().shouldBeFalse()

        closeAllDjms()
    }
}