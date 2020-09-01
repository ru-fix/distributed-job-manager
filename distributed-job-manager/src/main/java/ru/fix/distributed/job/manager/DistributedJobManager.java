package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.PrefixedProfiler;
import ru.fix.aggregating.profiler.ProfiledCall;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings;
import ru.fix.distributed.job.manager.model.JobDescriptor;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * <p>
 * How to use: <br>
 * Create single instance of {@link DistributedJobManager} for each server (JVM instances).
 * In {@link DistributedJobManager#DistributedJobManager(
 * CuratorFramework, String, String, AssignmentStrategy, Collection, Profiler, DistributedJobManagerSettings)}
 * register list of jobs that could run on this server (JVM instance).
 * {@link DistributedJobManager} will balance workload between available servers for you.
 * </p>
 * <p>
 * Each node instance register as worker and provide Executor to run Jobs. <br>
 * One of node will be selected as leader and became Manager.
 * Manager controls job assignment.
 * Node with Manager also starts local Worker so Node can work as worker and as a manager.
 * <br>
 * Every worker provides unique id and register as child node at /workers <br>
 * Every worker register available jobs classes that it can run in /workers/worker-id/available/job-id and /work-pool/job-id <br>
 * Avery worker listen to /workers/id/assigned. New schedulable job assignments will be added there by Manager. <br>
 * When new assigned job appears, worker acquire lock /jobs/job-id.lock and start launching it with given
 * delay.
 * When job disappears from worker /assigned path, worker stop executing job and release job lock.
 * </p>
 * <p>
 * ZK node tree managed by {@link DistributedJobManager} described in {@link ZkPathsManager}
 * <pre>
 */
public class DistributedJobManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DistributedJobManager.class);

    private final Worker worker;
    private final Manager manager;
    private final String nodeId;
    private final String rootPath;
    private final Profiler djmProfiler;
    private final DynamicProperty<DistributedJobManagerSettings> settings;

    /**
     *
     * @param curatorFramework client to access ZooKeeper
     * @param nodeId unique identifies DJM instance within DJM cluster. Providing non unique identifies
     *               could lead to undetermined behaviour of DJM cluster.
     * @param rootPath path within ZooKeeper where DJM will store it's state.
     *                 All DJM instances within same cluster should share same rootPath.
     * @param assignmentStrategy Specifies how to distribute job work items between DJM nodes,
     *                           use {@link ru.fix.distributed.job.manager.strategy.AssignmentStrategies#DEFAULT}
     * @param userDefinedJobs Job instances to launch within DJM cluster
     * @param profiler records DJM metrics
     * @param settings DJM configuration
     */
    public DistributedJobManager(
            CuratorFramework curatorFramework,
            String nodeId,
            String rootPath,
            AssignmentStrategy assignmentStrategy,
            Collection<DistributedJob> userDefinedJobs,
            Profiler profiler,
            DynamicProperty<DistributedJobManagerSettings> settings) throws Exception {


        this.nodeId = nodeId;
        this.rootPath = rootPath;
        this.settings = settings;
        logger.info("Starting DJM with id {} at {}", nodeId, rootPath);

        this.djmProfiler = new PrefixedProfiler(
                profiler,
                ProfilerMetrics.DJM_PREFIX,
                Collections.singletonMap("djmNodeId", nodeId)
        );

        try(ProfiledCall initProfiledCall = djmProfiler.profiledCall(ProfilerMetrics.DJM_INIT).start()) {
            ZkPathsManager paths = new ZkPathsManager(rootPath);
            paths.initPaths(curatorFramework);

            Collection<JobDescriptor> jobs = userDefinedJobs.stream()
                    .map(JobDescriptor::new).collect(Collectors.toList());

            this.manager = new Manager(
                    curatorFramework,
                    nodeId,
                    paths,
                    assignmentStrategy,
                    djmProfiler,
                    settings);

            this.worker = new Worker(
                    curatorFramework,
                    nodeId,
                    paths,
                    jobs,
                    djmProfiler,
                    settings);

            this.manager.start();
            this.worker.start();

            initProfiledCall.stop();
        }
        logger.info("DJM with id {} at {} started.", nodeId, rootPath);
    }

    @Override
    public void close() throws Exception {
        logger.info("Closing DJM with id {} at {}", nodeId, rootPath);
        long closingStart = System.currentTimeMillis();

        djmProfiler.profiledCall(ProfilerMetrics.DJM_CLOSE).profileThrowable(() -> {
            worker.close();
            manager.close();
        });

        logger.info("DJM closed. Closing took {} ms.",
                System.currentTimeMillis() - closingStart);
    }
}
