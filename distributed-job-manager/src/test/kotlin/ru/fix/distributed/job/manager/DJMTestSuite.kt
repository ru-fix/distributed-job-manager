package ru.fix.distributed.job.manager

import org.apache.curator.framework.CuratorFramework
import org.apache.logging.log4j.kotlin.Logging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.netcrusher.tcp.TcpCrusher
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.aggregating.profiler.Profiler
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings
import ru.fix.distributed.job.manager.model.JobDisableConfig
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.zookeeper.lock.PersistentExpiringLockManagerConfig
import ru.fix.zookeeper.testing.ZKTestingServer
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
abstract class DJMTestSuite {

    companion object : Logging {
        private val lastNodeId = AtomicInteger(1)
        fun generateNodeId() = lastNodeId.incrementAndGet().toString()

        private val lastRootId = AtomicInteger(1)
        fun generateDjmRootPath() = "/${lastRootId.incrementAndGet()}"
    }

    lateinit var server: ZKTestingServer
    lateinit var djmZkRootPath: String
    lateinit var djmZkPathsManager: ZkPathsManager

    @BeforeEach
    fun beforeEach() {
        server = ZKTestingServer().start()
        djmZkRootPath = generateDjmRootPath()
        djmZkPathsManager = ZkPathsManager(djmZkRootPath)
    }

    @AfterEach
    fun afterEach() {
        closeAllDjms()
        server.close()
    }

    class DjmZkConnector(val curator: CuratorFramework, val tcpCrusher: TcpCrusher) {
        val isOpen: Boolean get() = tcpCrusher.isOpen

        fun close() {
            curator.close()
            tcpCrusher.close()
        }

        fun disconnect() {
            tcpCrusher.close()
        }

        fun connect() {
            tcpCrusher.open()
        }
    }

    private val djmConnections = ConcurrentHashMap<DistributedJobManager, DjmZkConnector>()

    fun createDJM(job: DistributedJob,
                  profiler: Profiler = NoopProfiler(),
                  assignmentStrategy: AssignmentStrategy = AssignmentStrategies.DEFAULT) =
            createDJM(listOf(job), profiler, assignmentStrategy)

    fun createDJM(jobs: List<DistributedJob>,
                  profiler: Profiler = NoopProfiler(),
                  assignmentStrategy: AssignmentStrategy = AssignmentStrategies.DEFAULT,
                  nodeId: String = generateNodeId(),
                  settings: DynamicProperty<DistributedJobManagerSettings> = DynamicProperty.of(
                          DistributedJobManagerSettings(
                                  timeToWaitTermination = Duration.ofSeconds(1),
                                  workPoolCleanPeriod = Duration.ofSeconds(1),
                                  lockManagerConfig = PersistentExpiringLockManagerConfig(
                                          lockAcquirePeriod = Duration.ofSeconds(15),
                                          expirationPeriod = Duration.ofSeconds(5),
                                          lockCheckAndProlongInterval = Duration.ofSeconds(5)
                                  ),
                                  jobDisableConfig = JobDisableConfig())
                  )
    ): DistributedJobManager {

        val tcpCrusher = server.openProxyTcpCrusher()
        val curator = server.createZkProxyClient(tcpCrusher)
        try {
            val djm = DistributedJobManager(
                    curator,
                    nodeId,
                    djmZkRootPath,
                    assignmentStrategy,
                    jobs,
                    profiler,
                    settings)
            djmConnections[djm] = DjmZkConnector(curator, tcpCrusher)
            return djm
        } catch (exc: Exception) {
            curator.close()
            tcpCrusher.close()
            throw exc
        }
    }

    fun disconnectDjm(djm: DistributedJobManager) {
        djmConnections[djm]!!.disconnect()
    }

    fun connectDjm(djm: DistributedJobManager) {
        djmConnections[djm]!!.connect()
    }

    fun isConnectedDjm(djm: DistributedJobManager): Boolean {
        return djmConnections[djm]!!.isOpen
    }

    fun closeDjm(djm: DistributedJobManager) {
        val connector = djmConnections.remove(djm)!!
        djm.close()
        connector.close()
    }

    fun closeAllDjms() {
        djmConnections.keys().toList().forEach(this::closeDjm)
    }

    val djms: List<DistributedJobManager> get() = djmConnections.keys().toList()

    fun printDjmZkTree(): String = printZkTree(djmZkRootPath)

    fun printZkTree(path: String): String = ZkTreePrinter(server.client).print(path, true)
}

