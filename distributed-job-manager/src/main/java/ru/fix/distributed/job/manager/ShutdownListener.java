package ru.fix.distributed.job.manager;

@FunctionalInterface
public interface ShutdownListener {

    void onShutdown();
}
