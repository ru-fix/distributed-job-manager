package ru.fix.distributed.job.manager

import org.apache.curator.framework.CuratorFramework
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.zookeeper.testing.ZKTestingServer
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.util.*


internal abstract class AbstractJobManagerTest {

    companion object {
        const val JOB_MANAGER_ZK_ROOT_PATH = "/djm/job-manager-test"

        @JvmField
        val paths = ZkPathsManager(JOB_MANAGER_ZK_ROOT_PATH)
    }


    lateinit var zkTestingServer: ZKTestingServer

    @BeforeEach
    fun setUp() {
        zkTestingServer = ZKTestingServer()
        zkTestingServer.start()
    }

    @AfterEach
    fun tearDown() {
        zkTestingServer.close()
    }

    fun printDjmZkTree(): String = printZkTree(JOB_MANAGER_ZK_ROOT_PATH)

    fun printZkTree(path: String): String = ZkTreePrinter(zkTestingServer.client).print(path)

    fun defaultZkClient(): CuratorFramework {
        return zkTestingServer.createClient(60000, 15000)
    }

    @JvmOverloads
    fun createNewJobManager(
            jobs: Collection<DistributedJob>,
            curatorFramework: CuratorFramework = defaultZkClient(),
            nodeId: String = UUID.randomUUID().toString(),
            strategy: AssignmentStrategy = AssignmentStrategies.DEFAULT,
            workPoolCleanPeriod: DynamicProperty<Long> = DynamicProperty.of(1000L)
    ): DistributedJobManager {
        return DistributedJobManager(
                curatorFramework,
                jobs,
                NoopProfiler(),
                DistributedJobManagerSettings(
                        nodeId,
                        JOB_MANAGER_ZK_ROOT_PATH,
                        strategy,
                        DynamicProperty.of(180000L),
                        workPoolCleanPeriod
                )
        )
    }

}