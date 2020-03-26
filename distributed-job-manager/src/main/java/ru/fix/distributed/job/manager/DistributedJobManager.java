package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.PrefixedProfiler;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy;
import ru.fix.distributed.job.manager.util.DistributedJobSettings;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.Collection;

/**
 * <p>
 * How to use: <br>
 * Create single instance of {@link DistributedJobManager} for each server (JVM instances).
 * In {@link DistributedJobManager#DistributedJobManager(
 * String, CuratorFramework, String, Collection, AssignmentStrategy, Profiler, DynamicProperty)}
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
    private String nodeId;
    private DynamicProperty<DistributedJobSettings> jobsEnabled;
    private DistributedJobManagerSettings configs;

    private static class Timespan {
        long startTimestamp;
        long stopTimestamp;

        public Timespan start() {
            startTimestamp = System.currentTimeMillis();
            return this;
        }

        public Timespan stop() {
            stopTimestamp = System.currentTimeMillis();
            return this;
        }

        public long getTimespan() {
            return stopTimestamp - startTimestamp;
        }
    }

    public DistributedJobManager(CuratorFramework curatorFramework,
                                 Collection<DistributedJob> distributedJobs,
                                 Profiler profiler,
                                 DistributedJobManagerSettings settings) throws Exception {

        final Timespan djmInitTimespan = new Timespan().start();

        log.trace("Starting DistributedJobManager with settings {}", settings);
        initPaths(curatorFramework, settings.getRootPath());

        final Timespan managerInitTimespan = new Timespan().start();
        this.manager = new Manager(curatorFramework, profiler, settings);
        managerInitTimespan.stop();

        final Timespan workerInitTimespan = new Timespan().start();

        this.nodeId = settings.getNodeId();

        this.jobsEnabled = settings.component5();
        this.configs = settings;
        this.worker = new Worker(
                curatorFramework,
                distributedJobs,
                new PrefixedProfiler(profiler, "djm."),
                settings,
                jobsEnabled);

        workerInitTimespan.stop();


        final Timespan managerStartTimespan = new Timespan().start();
        this.manager.start();
        managerStartTimespan.stop();

        final Timespan workerStartTimespan = new Timespan().start();
        this.worker.start();

        workerStartTimespan.stop();

        djmInitTimespan.stop();

        log.info("DJM initialized in {}ms, Manager init in {}ms, started in {}ms, Worker init in {}ms, started in {}ms",
                djmInitTimespan.getTimespan(),
                managerInitTimespan.getTimespan(),
                managerStartTimespan.getTimespan(),
                workerInitTimespan.getTimespan(),
                workerStartTimespan.getTimespan());
    }

    private static void initPaths(CuratorFramework curatorFramework, String rootPath) throws Exception {
        ZkPathsManager paths = new ZkPathsManager(rootPath);
        createIfNeeded(curatorFramework, paths.allWorkers());
        createIfNeeded(curatorFramework, paths.aliveWorkers());
        createIfNeeded(curatorFramework, paths.workerVersion());
        createIfNeeded(curatorFramework, paths.assignmentVersion());
        createIfNeeded(curatorFramework, paths.locks());
        createIfNeeded(curatorFramework, paths.availableWorkPool());
        createIfNeeded(curatorFramework, paths.availableWorkPoolVersion());
    }
    private static void createIfNeeded(CuratorFramework curatorFramework, String path) throws Exception {
        if (curatorFramework.checkExists().forPath(path) == null) {
            curatorFramework.create().creatingParentsIfNeeded().forPath(path);
        }
    }

    @Override
    public void close() throws Exception {
        log.info("Closing DJM with worker id {}", nodeId);

        Timespan djmClosing = new Timespan().start();

        Timespan workerClosing = new Timespan().start();
        worker.close();
        workerClosing.stop();

        Timespan managerClosing = new Timespan().start();
        manager.close();
        managerClosing.stop();

        djmClosing.stop();

        log.info("DJM closed in {}ms, Worker {} closed in {}ms, Manager closed in {}ms",
                djmClosing.getTimespan(),
                nodeId,
                workerClosing.getTimespan(),
                managerClosing.getTimespan());
    }

    public DistributedJobManagerSettings getSettings(){
        return configs;
    }
}
