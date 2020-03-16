package ru.fix.distributed.job.manager.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.distributed.job.manager.WorkPool;
import ru.fix.distributed.job.manager.model.JobDescriptor;

/**
 * @author a.petrov
 */
public class WorkPoolUtils {

    private static final Logger log = LoggerFactory.getLogger(WorkPoolUtils.class);

    private static final int WORK_POOL_ITEM_MAX_LENGTH = 255;

    /**
     * Check WorkPool items for restrictions.
     * <p>
     * Method check for:
     * - standard ZooKeeper naming restrictions
     * - maximum length
     *
     * @param job      Descriptor of job, which owns WorkPool.
     * @param workPool WorkPool to check.
     */
    public static void checkWorkPoolItemsRestrictions(JobDescriptor job, WorkPool workPool) {
        workPool.getItems()
                .forEach(workPoolItem -> checkWorkPoolItemRestriction(job.getJobId(), workPoolItem));
    }

    private static void checkWorkPoolItemRestriction(String jobId, String workPoolItem) {
        checkZkRestrictions(jobId, workPoolItem);
        if (workPoolItem != null) {
            checkMaxLength(jobId, workPoolItem);
        }
    }

    /**
     * Check WorkPool item for maximum length.
     * <p>
     * It uses {@link WorkPoolUtils#WORK_POOL_ITEM_MAX_LENGTH}
     * as maximum length constraint.
     *
     * @param jobId        Job ID, which owns WorkPool. Used for error logging.
     * @param workPoolItem WorkPool item to check.
     */
    private static void checkMaxLength(String jobId, String workPoolItem) {
        if (workPoolItem.length() > WORK_POOL_ITEM_MAX_LENGTH) {
            log.error("WorkPool item \"{}\" of job \"{}\" is too long. Maximal length is {}, actual is {}.",
                    workPoolItem, jobId, WORK_POOL_ITEM_MAX_LENGTH, workPoolItem.length());
        }
    }

    /**
     * Check WorkPool item for standard ZooKeeper naming restrictions.
     * <p>
     * See Data Model section in ZooKeeper Programmer's Guide for details.
     *
     * @param jobId        Job ID, which owns WorkPool. Used for error logging.
     * @param workPoolItem WorkPool item to check.
     */
    private static void checkZkRestrictions(String jobId, String workPoolItem) {
        if (workPoolItem == null) {
            log.error("WorkPool item can't be null. Job \"{}\".", jobId);
        } else if (workPoolItem.length() == 0) {
            log.error("WorkPool item length must be > 0. Job \"{}\".", jobId);
        } else if (".".equals(workPoolItem) || "..".equals(workPoolItem)) {
            log.error("WorkPool item \"{}\" of job \"{}\" seems like relative path part.",
                    workPoolItem, jobId);
        } else {
            char[] workPoolItemAsChars = workPoolItem.toCharArray();
            for (int i = 0; i < workPoolItemAsChars.length; ++i) {
                char ch = workPoolItemAsChars[i];
                if (ch == 0) {
                    log.error("WorkPool item \"{}\" of job \"{}\" contains null character @{}.",
                            workPoolItem, jobId, i);
                } else if (ch == '/') {
                    log.error("WorkPool item \"{}\" of job \"{}\" contains / character @{}.",
                            workPoolItem, jobId, i);
                } else if (ch > '\u0000' && ch < '\u001f'
                        || ch > '\u007f' && ch < '\u009F'
                        || ch > '\ud800' && ch < '\uf8ff'
                        || ch > '\ufff0' && ch < '\uffff') {
                    log.error("WorkPool item \"{}\" of job \"{}\" contains restricted character \"{}\" @{}.",
                            workPoolItem, jobId, charToStringRepresentation(ch), i);
                }
            }
        }
    }

    private static String charToStringRepresentation(char ch) {
        return "\\u" + Integer.toHexString(ch);
    }
}
