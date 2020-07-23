package ru.fix.distributed.job.manager

import org.junit.jupiter.api.Test
import org.netcrusher.core.reactor.NioReactor
import org.netcrusher.tcp.TcpCrusher
import org.netcrusher.tcp.TcpCrusherBuilder
import ru.fix.stdlib.socket.SocketChecker


internal class DjmConnectionProblemsTest : AbstractJobManagerTest() {
    private val reactor: NioReactor = NioReactor()

    @Test
    fun `when djm1 reconnects to zk then workItem are distributed between djm1 and djm2`() {
        val proxyPort = SocketChecker.getAvailableRandomPort()
        val proxyCurator = zkTestingServer.createClient("localhost:${proxyPort}", 1000, 1000)
        val normalCurator = defaultZkClient()
        val crusherForWorker1 = tcpCrusher(proxyPort)

        val jobInstanceOnWorker1 = StubbedMultiJob(1, setOf("1", "2", "3", "4"))
        val jobInstanceOnWorker2 = StubbedMultiJob(1, setOf("1", "2", "3", "4"))
        val djm1 = createNewJobManager(jobs = setOf(jobInstanceOnWorker1), curatorFramework = proxyCurator)
        val djm2 = createNewJobManager(jobs = setOf(jobInstanceOnWorker2), curatorFramework = normalCurator)

        awaitSingleJobIsDistributedBetweenWorkers(15, jobInstanceOnWorker1, jobInstanceOnWorker2)
        crusherForWorker1.close()
        awaitSingleJobIsDistributedBetweenWorkers(30, jobInstanceOnWorker2)
        crusherForWorker1.open()
        awaitSingleJobIsDistributedBetweenWorkers(30, jobInstanceOnWorker1, jobInstanceOnWorker2)

        djm1.close()
        djm2.close()
        proxyCurator.close()
        normalCurator.close()
        crusherForWorker1.close()
    }

    private fun tcpCrusher(proxyPort: Int): TcpCrusher =
            TcpCrusherBuilder.builder()
                    .withReactor(reactor)
                    .withBindAddress("localhost", proxyPort)
                    .withConnectAddress("localhost", zkTestingServer.port)
                    .buildAndOpen()

}

