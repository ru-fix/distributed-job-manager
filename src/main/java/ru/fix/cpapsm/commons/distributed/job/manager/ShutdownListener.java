package ru.fix.cpapsm.commons.distributed.job.manager;

@FunctionalInterface
public interface ShutdownListener {

    void onShutdown();
}
