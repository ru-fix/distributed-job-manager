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
 * Schedules and launches {@link DistributedJob} instances.
 * Balance work load {@link WorkPool} between {@link DistributedJobManager} instances within cluster.
 * </p>
 * <p>
 * Create single instance of {@link DistributedJobManager} within each application instances.
 * {@link DistributedJobManager#DistributedJobManager(CuratorFramework, String, String, AssignmentStrategy, Collection, Profiler, DynamicProperty)}
 * All DJM instances should have same rootPath and different nodeId.
 * Provide list of jobs that could run within this application instance/servers (JVM instance).
 * {@link DistributedJobManager} will balance workload between available applications/servers for you.
 * </p>
 * <p>
 * Each DJM node instance behave as a Worker and provides threads to launch jobs {@link DistributedJob}. <br>
 * One of DJM node instances within cluster will be selected as a leader and behave as a Manager.  <br>
 * Manager controls job assignment for workers within cluster. <br>
 * So within cluster of DJM nodes all nodes will work as a Workers and one node will work as a Manager and also as a Worker.  <br>
 * Every worker provides unique id and register a child node at /workers zookeeper path <br>
 * Every worker registers available jobs that it can run in /workers/worker-id/available/job-id and /work-pool/job-id  zookeeper paths <br>
 * Avery worker listens to /workers/worker-id/assigned subtree.  <br>
 * New schedulable job assignments will be added to this subtree by Manager. <br>
 * </p>
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
