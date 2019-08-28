package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.fix.aggregating.profiler.AggregatingProfiler;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy;
import ru.fix.distributed.job.manager.strategy.ClassifiedAssignmentStrategy;
import ru.fix.distributed.job.manager.strategy.DefaultAssignmentStrategy;
import ru.fix.distributed.job.manager.strategy.DefaultAssignmentStrategyTest;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.zookeeper.testing.ZKTestingServer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class DistributedJobManagerTest {
    private ZKTestingServer testingServer;

    @BeforeEach
    public void setUp() throws Exception {
        testingServer = new ZKTestingServer();
    }

    @Test
    public void testRun() throws Exception {
        assertTrue(true);
    }

}