package ru.fix.cpapsm.commons.distributed.job.manager.exception;

/**
 * @author Kamil Asfandiyarov
 */
public class NodeManagerException extends Exception {

    public NodeManagerException() {
        super();
    }

    public NodeManagerException(String message) {
        super(message);
    }

    public NodeManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public NodeManagerException(Throwable cause) {
        super(cause);
    }

    public NodeManagerException(String message, Throwable cause, boolean enableSuppression, boolean
            writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
