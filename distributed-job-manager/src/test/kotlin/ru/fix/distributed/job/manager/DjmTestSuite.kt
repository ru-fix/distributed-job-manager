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
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.zookeeper.lock.PersistentExpiringLockManagerConfig
import ru.fix.zookeeper.testing.ZKTestingServer
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
open class DjmTestSuite {

    companion object: Logging {
        private val lastNodeId = AtomicInteger(1)
        fun generateNodeId() = lastNodeId.incrementAndGet().toString()

        private val lastRootId = AtomicInteger(1)
        fun generateDjmRootPath() = "/${lastRootId.incrementAndGet()}"
    }

    lateinit var server: ZKTestingServer
    lateinit var djmZkRootPath: String

    @BeforeEach
    fun beforeEach() {
        server = ZKTestingServer().start()
        djmZkRootPath = generateDjmRootPath()
    }

    @AfterEach
    fun afterEach() {
        server.client.close()
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
                  assignmentStrategy: AssignmentStrategy = AssignmentStrategies.DEFAULT): DistributedJobManager {
        val tcpCrusher = server.openProxyTcpCrusher()
        val curator = server.createZkProxyClient(tcpCrusher)

        val djm = DistributedJobManager(
                curator,
                jobs,
                profiler,
                DistributedJobManagerSettings(
                        nodeId = generateNodeId(),
                        rootPath = djmZkRootPath,
                        assignmentStrategy = assignmentStrategy,
                        timeToWaitTermination = DynamicProperty.of(10000),
                        lockManagerConfig = DynamicProperty.of(PersistentExpiringLockManagerConfig(
                                lockAcquirePeriod = Duration.ofSeconds(15),
                                expirationPeriod = Duration.ofSeconds(5),
                                lockCheckAndProlongInterval = Duration.ofSeconds(5)
                        ))
                ))
        djmConnections[djm] = DjmZkConnector(curator, tcpCrusher)
        return djm
    }

    fun disconnectDjm(djm: DistributedJobManager){
        djmConnections[djm]!!.disconnect()
    }

    fun connectDjm(djm: DistributedJobManager){
        djmConnections[djm]!!.connect()
    }

    fun isConnectedDjm(djm: DistributedJobManager): Boolean {
        return djmConnections[djm]!!.isOpen
    }

    fun closeDjm(djm: DistributedJobManager){
        val connection = djmConnections.remove(djm)!!
        djm.close()
        connection.close()
    }

    fun closeAllDjms(){
        djmConnections.keys().toList().forEach (this::closeDjm)
    }

    val djms: List<DistributedJobManager> get() = djmConnections.keys().toList()
}

