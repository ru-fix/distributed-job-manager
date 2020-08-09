package ru.fix.distributed.job.manager

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.netcrusher.tcp.TcpCrusher
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.aggregating.profiler.Profiler
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.zookeeper.lock.PersistentExpiringLockManagerConfig
import ru.fix.zookeeper.testing.ZKTestingServer
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

open class DjmTestSuite {

    companion object {
        private val lastNodeId = AtomicInteger(1)
        fun generateNodeId() = lastNodeId.incrementAndGet().toString()

        private val lastRootId = AtomicInteger(1)
        fun generateDjmRootPath() = "root/${lastRootId.incrementAndGet()}"
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

    private val djmCrushers = ConcurrentHashMap<DistributedJobManager, TcpCrusher>()

    fun createDJM(job: DistributedJob,
                  profiler: Profiler = NoopProfiler(),
                  rootPath: String = generateDjmRootPath()) =
            createDJM(listOf(job), profiler, rootPath)

    fun createDJM(jobs: List<DistributedJob>,
                  profiler: Profiler = NoopProfiler(),
                  rootPath: String = generateDjmRootPath()): DistributedJobManager {
        val tcpCrusher = server.openProxyTcpCrusher()
        val curator = server.createZkProxyClient(tcpCrusher)

        val djm = DistributedJobManager(
                curator,
                jobs,
                profiler,
                DistributedJobManagerSettings(
                        nodeId = generateNodeId(),
                        rootPath = rootPath,
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

    fun closeDjm(djm: DistributedJobManager){
        djm.close()
        djmCrushers.compute(djm){_, crusher ->
            crusher!!.close()
            null
        }
    }
}

