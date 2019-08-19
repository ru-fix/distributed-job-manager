package ru.fix.distributed.job.manager;

import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.distributed.job.manager.util.ZkTreePrinter;
import ru.fix.zookeeper.testing.ZKTestingServer;

import java.util.Set;

/**
 * @author Ayrat Zulkarnyaev
 */
@SuppressWarnings("ALL")
public class AbstractJobManagerTest {

    static final String JOB_MANAGER_ZK_ROOT_PATH = "/djm/job-manager-test";
    private static final Logger log = LoggerFactory.getLogger(AbstractJobManagerTest.class);

    public ZKTestingServer zkTestingServer;

    @BeforeEach
    public void setUp() throws Exception {
        zkTestingServer = new ZKTestingServer();
        zkTestingServer.start();
    }

    JobManagerPaths paths = new JobManagerPaths(JOB_MANAGER_ZK_ROOT_PATH);

    String printZkTree(String path) {
        return new ZkTreePrinter(zkTestingServer.getClient()).print(path);
    }

    public static class WorkItemMonitor {
        public void check(Set<String> args) {
        }
    }

}
