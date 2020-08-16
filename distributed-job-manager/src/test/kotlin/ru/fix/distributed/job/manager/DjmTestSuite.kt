package ru.fix.distributed.job.manager

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

    companion object {
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
        closeAllDjms()
        server.close()
    }

    private val djmCrushers = ConcurrentHashMap<DistributedJobManager, TcpCrusher>()

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
        djmCrushers[djm] = tcpCrusher
        return djm
    }

    fun disconnectDjm(djm: DistributedJobManager){
        djmCrushers[djm]!!.close()
    }

    fun connectDjm(djm: DistributedJobManager){
        djmCrushers[djm]!!.open()
    }

    fun isConnectedDjm(djm: DistributedJobManager): Boolean {
        return djmCrushers[djm]!!.isOpen
    }

    fun closeDjm(djm: DistributedJobManager){
        djm.close()
        djmCrushers.compute(djm){_, crusher ->
            crusher!!.close()
            null
        }
    }

    fun closeAllDjms(){
        djmCrushers.keys().toList().forEach (this::closeDjm)
    }

    val djms: List<DistributedJobManager> get() = djmCrushers.keys().toList()
}

