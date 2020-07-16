package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.zookeeper.testing.ZKTestingServer;
import ru.fix.zookeeper.utils.ZkTreePrinter;

import java.util.Set;

/**
 * @author Ayrat Zulkarnyaev
 */
@SuppressWarnings("ALL")
public class AbstractJobManagerTest {

    public static final String JOB_MANAGER_ZK_ROOT_PATH = "/djm/job-manager-test";
    private static final Logger log = LoggerFactory.getLogger(AbstractJobManagerTest.class);

    public ZKTestingServer zkTestingServer;
    ZkPathsManager paths = new ZkPathsManager(JOB_MANAGER_ZK_ROOT_PATH);

    @BeforeEach
    public void setUp() throws Exception {
        zkTestingServer = new ZKTestingServer();
        zkTestingServer.start();
    }

    @AfterEach
    void tearDown() {
        zkTestingServer.close();
    }

    String printDjmZkTree() {
        return printZkTree(JOB_MANAGER_ZK_ROOT_PATH);
    }

    String printZkTree(String path) {
        return new ZkTreePrinter(zkTestingServer.getClient()).print(path);
    }

    CuratorFramework defaultZkClient() {
        return zkTestingServer.createClient(60000, 15000);
    }

    public static class WorkItemMonitor {
        public void check(Set<String> args) {
        }
    }

}
