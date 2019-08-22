package ru.fix.distributed.job.manager.exception;

/**
 * @author Ayrat Zulkarnyaev
 */
public class WorkItemMutexException extends Exception {

    public WorkItemMutexException(String message) {
        super(message);
    }

    public WorkItemMutexException(String message, Throwable cause) {
        super(message, cause);
    }

    public WorkItemMutexException(Throwable cause) {
        super(cause);
    }
}
