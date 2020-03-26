package ru.fix.distributed.job.manager.assertion;


import org.junit.jupiter.api.Assertions;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class RetryAssert {

    private static final int DEFAULT_ITERATION_PERIOD = 200;
    private static final int DEFAULT_TIMEOUT = 15_000;

    // Equals assertion

    public static <T> void assertEquals(T expectedValue, ValueSupplier<T> actualValueSupplier) throws Exception {
        assertEquals(null, expectedValue, actualValueSupplier);
    }

    public static <T> void assertEquals(String message, T expectedValue,
                                        ValueSupplier<T> actualValueSupplier) throws Exception {
        assertEquals(message, expectedValue, actualValueSupplier, DEFAULT_ITERATION_PERIOD, DEFAULT_TIMEOUT);
    }

    public static <T> void assertEquals(String message, T expectedValue, ValueSupplier<T> actualValueSupplier,
                                        long timeout) throws Exception {
        assertEquals(message, expectedValue, actualValueSupplier, DEFAULT_ITERATION_PERIOD, timeout);
    }

    public static <T> void assertEquals(String message,
                                        T expectedValue,
                                        ValueSupplier<T> actualValueSupplier,
                                        long iterationPeriod,
                                        long timeout) throws Exception {
        T value = waitForCondition(
                v -> Objects.equals(expectedValue, v), actualValueSupplier, iterationPeriod, timeout
        );
        Assertions.assertEquals(expectedValue, value, message);
    }

    // Null assertion

    public static <T> void assertNull(ValueSupplier<T> valueSupplier) throws Exception {
        assertNull(null, valueSupplier);
    }

    public static <T> void assertNull(String message, ValueSupplier<T> valueSupplier) throws Exception {
        assertNull(message, valueSupplier, DEFAULT_ITERATION_PERIOD, DEFAULT_TIMEOUT);
    }

    public static <T> void assertNull(String message, ValueSupplier<T> valueSupplier, long timeout) throws Exception {
        assertNull(message, valueSupplier, DEFAULT_ITERATION_PERIOD, timeout);
    }

    public static <T> void assertNull(String message,
                                      ValueSupplier<T> valueSupplier,
                                      long iterationPeriod,
                                      long timeout) throws Exception {
        T value = waitForCondition(Objects::isNull, valueSupplier, iterationPeriod, timeout);
        Assertions.assertNull(value, message);
    }

    // Not null assertion

    public static <T> void assertNotNull(ValueSupplier<T> valueSupplier) throws Exception {
        assertNotNull(null, valueSupplier);
    }

    public static <T> void assertNotNull(String message, ValueSupplier<T> valueSupplier) throws Exception {
        assertNotNull(message, valueSupplier, DEFAULT_ITERATION_PERIOD, DEFAULT_TIMEOUT);
    }

    public static <T> void assertNotNull(String message,
                                         ValueSupplier<T> valueSupplier,
                                         long timeout) throws Exception {
        assertNotNull(message, valueSupplier, DEFAULT_ITERATION_PERIOD, timeout);
    }

    public static <T> void assertNotNull(String message,
                                         ValueSupplier<T> valueSupplier,
                                         long iterationPeriod,
                                         long timeout) throws Exception {
        T value = waitForCondition(Objects::nonNull, valueSupplier, iterationPeriod, timeout);
        Assertions.assertNotNull(value, message);
    }

    // True assertion

    public static void assertTrue(ValueSupplier<Boolean> valueSupplier) throws Exception {
        assertTrue(() -> null, valueSupplier);
    }

    public static void assertTrue(Supplier<String> messageSupplier,
                                  ValueSupplier<Boolean> valueSupplier) throws Exception {
        assertTrue(messageSupplier, valueSupplier, DEFAULT_ITERATION_PERIOD, DEFAULT_TIMEOUT);
    }

    public static void assertTrue(String message, ValueSupplier<Boolean> valueSupplier) throws Exception {
        assertTrue(() -> message, valueSupplier, DEFAULT_ITERATION_PERIOD, DEFAULT_TIMEOUT);
    }

    public static void assertTrue(String message, ValueSupplier<Boolean> valueSupplier, long timeout) throws Exception {
        assertTrue(() -> message, valueSupplier, DEFAULT_ITERATION_PERIOD, timeout);
    }

    public static void assertTrue(Supplier<String> messageSupplier,
                                  ValueSupplier<Boolean> valueSupplier,
                                  long timeout) throws Exception {
        assertTrue(messageSupplier, valueSupplier, DEFAULT_ITERATION_PERIOD, timeout);
    }

    public static void assertTrue(Supplier<String> messageSupplier,
                                  ValueSupplier<Boolean> valueSupplier,
                                  long iterationPeriod,
                                  long timeout) throws Exception {
        Boolean value = waitForCondition(v -> v, valueSupplier, iterationPeriod, timeout);
        Assertions.assertTrue(value, messageSupplier.get());
    }


    public static void repeatableExecuteUntilSuccess(Runnable execute, long timeout) throws Exception {
        long started = System.currentTimeMillis();
        while (true) {
            try {
                execute.run();
                break;
            } catch (AssertionError e) {
                if (System.currentTimeMillis() - started > timeout) {
                    throw e;
                } else {
                    Thread.sleep(timeout / 10);
                }
            }
        }
    }

    /**
     * Checks a result of condition defined within incoming lambda expression.
     *
     * @param conditionChecker a lambda expression with condition logic to execute
     * @param iterationPeriod  a wait period between check condition attempts
     * @param timeout          a total time for check codition attempts
     * @return true - if condition succeeded, false - if timeout has expired
     * @throws Exception
     */
    private static <T> T waitForCondition(ConditionChecker<T> conditionChecker, ValueSupplier<T> valueSupplier,
                                          long iterationPeriod, long timeout) throws Exception {
        long startTime = System.currentTimeMillis();
        T value;
        boolean shouldSleep;
        boolean conditionResult;

        do {
            value = valueSupplier.get();
            conditionResult = conditionChecker.check(value);
            shouldSleep = !conditionResult && System.currentTimeMillis() < startTime + timeout;

            if (shouldSleep) {
                TimeUnit.MILLISECONDS.sleep(iterationPeriod);
            }
        } while (shouldSleep);

        return value;
    }

    /**
     * An interface for a value supply.
     *
     * @param <T> type of value
     */
    @FunctionalInterface
    public interface ValueSupplier<T> {
        T get() throws Exception;
    }

    /**
     * An interface for condition checks for specific argument.
     *
     * @param <T>
     */
    @FunctionalInterface
    public interface ConditionChecker<T> {
        boolean check(T value) throws Exception;
    }
}
