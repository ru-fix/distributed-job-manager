package ru.fix.cpapsm.commons.distributed.job.manager;

import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.commons.zkconfig.ZKTestingServer;
import ru.fix.cpapsm.commons.distributed.job.manager.util.ZkTreePrinter;

import java.util.Set;

/**
 * @author Ayrat Zulkarnyaev
 */
@SuppressWarnings("ALL")
public class AbstractJobManagerTest {

    static final String JOB_MANAGER_ZK_ROOT_PATH = "/cpapsm/job-manager-test";
    private static final Logger log = LoggerFactory.getLogger(AbstractJobManagerTest.class);

    @Rule
    public ZKTestingServer zkTestingServer = new ZKTestingServer();

    JobManagerPaths paths = new JobManagerPaths(JOB_MANAGER_ZK_ROOT_PATH);

    String printZkTree(String path) {
        return new ZkTreePrinter(zkTestingServer.getClient()).print(path);
    }

    public static class WorkItemMonitor {
        public void check(Set<String> args) {
        }
    }

}
