package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.PrefixedProfiler;
import ru.fix.aggregating.profiler.ProfiledCall;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings;
import ru.fix.distributed.job.manager.model.JobDescriptor;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * <p>
 * How to use: <br>
 * Create single instance of {@link DistributedJobManager} for each server (JVM instances).
 * In {@link DistributedJobManager#DistributedJobManager(
 *CuratorFramework, Collection, Profiler, DistributedJobManagerSettings)}
 * register list
 * of jobs that could be run on this server (JVM instance). {@link DistributedJobManager} will balance workload between
 * available servers for you.
 * </p>
 * <p>
 * Each node instance register as worker and provide resources to run Jobs. <br>
 * One of node will be selected as leader and became Manager. Manager controls job assignment. Node with Manager also
 * starts local Worker so Node can work as worker and as a manager.
 * <br>
 * Every worker provide unique id and register as child node at /workers <br>
 * Every worker register available jobs classes that it can run in /workers/worker-id/available/job-id and /work-pool/job-id <br>
 * All workers should register same SchedulableJobs.
 * Avery worker listen to /workers/id/assigned. New schedulable job will be added there by Manager. <br>
 * When new assigned job appears, worker acquire lock /jobs/job-id.lock and start launching it with given
 * delay.
 * When job disappears from worker /assigned path, worker stop executing job and release job lock.
 * </p>
 * <p>
 * ZK node tree managed by {@link DistributedJobManager} described in {@link ZkPathsManager}
 * <pre>
 *
 * @author Kamil Asfandiyarov
 */
public class DistributedJobManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DistributedJobManager.class);

    private final Worker worker;
    private final Manager manager;
    private final String nodeId;
    private final Profiler djmProfiler;

    public DistributedJobManager(CuratorFramework curatorFramework,
                                 Collection<DistributedJob> userDefinedJobs,
                                 Profiler profiler,
                                 DistributedJobManagerSettings settings) throws Exception {
        this.nodeId = settings.getNodeId();
        log.info("Starting DJM with id {}", nodeId);

        this.djmProfiler = new PrefixedProfiler(
                profiler,
                ProfilerMetrics.DJM_PREFIX,
                Collections.singletonMap("djmNodeId", nodeId)
        );

        try(ProfiledCall initProfiledCall = djmProfiler.profiledCall(ProfilerMetrics.DJM_INIT).start()) {
            ZkPathsManager paths = new ZkPathsManager(settings.getRootPath());
            paths.initPaths(curatorFramework);

            Collection<JobDescriptor> jobs = userDefinedJobs.stream()
                    .map(JobDescriptor::new).collect(Collectors.toList());

            this.manager = new Manager(curatorFramework, djmProfiler, settings);
            this.worker = new Worker(
                    curatorFramework,
                    jobs,
                    djmProfiler,
                    settings);

            this.manager.start();
            this.worker.start();

            initProfiledCall.stop();
        }
    }

    @Override
    public void close() throws Exception {
        log.info("Closing DJM with id {}", nodeId);

        djmProfiler.profiledCall(ProfilerMetrics.DJM_CLOSE).profileThrowable(() -> {
            worker.close();
            manager.close();
        });
    }
}
