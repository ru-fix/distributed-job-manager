package ru.fix.distributed.job.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/**
 * @author Ayrat Zulkarnyaev
 */
public class JobContext implements DistributedJobContext {

    private static final Logger log = LoggerFactory.getLogger(JobContext.class);
    private final List<ShutdownListener> shutdownListeners = new CopyOnWriteArrayList<>();
    private final String jobId;
    private final Set<String> workShare;
    /**
     * Nullable
     */
    private final Supplier<Boolean> shutdownChecker;
    private volatile boolean shutdownFlag;

    public JobContext(String jobId,
                      Set<String> workShare) {
        this.jobId = jobId;
        this.workShare = workShare;
        this.shutdownChecker = null;
    }

    /**
     * Constructor can be used for test purposes in DistributedJob implementations
     */
    public JobContext(String jobId, Set<String> workShare, Supplier<Boolean> shutdownChecker) {
        this.jobId = jobId;
        this.workShare = workShare;
        this.shutdownChecker = shutdownChecker;
    }

    @Override
    public Set<String> getWorkShare() {
        return workShare;
    }

    @Override
    public boolean isNeedToShutdown() {
        return shutdownFlag
                || shutdownChecker != null && shutdownChecker.get();
    }

    @Override
    public void addShutdownListener(ShutdownListener listener) {
        shutdownListeners.add(listener);
        if (shutdownFlag) {
            listener.onShutdown();
        }
    }

    @Override
    public void removeShutdownListener(ShutdownListener o) {
        shutdownListeners.remove(o);
    }

    public void shutdown() {
        shutdownFlag = true;
        shutdownListeners.forEach(shutdownListener -> {
            try {
                shutdownListener.onShutdown();
            } catch (Exception exc) {
                log.error("Failed to call shutdown listener on job: {}", jobId, exc);
            }
        });
    }
}
