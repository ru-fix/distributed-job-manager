package ru.fix.distributed.job.manager.cache

import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.CuratorCache
import org.apache.curator.framework.recipes.cache.CuratorCacheListener
import org.apache.curator.utils.ZKPaths
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.AbstractJobManagerTest
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

internal class CuratorCacheTest : AbstractJobManagerTest() {

    @Test
    fun `not all nodes are cached, because initialized event ignored`() {
        val path = ZKPaths.makePath(JOB_MANAGER_ZK_ROOT_PATH, "cache")
        val eventsCount = 3000
        val createEventsTriggered = AtomicInteger()
        val changeEventsTriggered = AtomicInteger()

        zkTestingServer.client.create().creatingParentsIfNeeded().forPath(path)
        (1..eventsCount).forEach {
            zkTestingServer.client.create().forPath(ZKPaths.makePath(path, it.toString()))
        }
        val workPooledCache = CuratorCache.build(zkTestingServer.client, path)
        val allChangedEventsReachedSemaphore = Semaphore(0)
        val curatorCacheListener = CuratorCacheListener { type, _, _ ->
            when (type) {
                CuratorCacheListener.Type.NODE_CHANGED -> {
                    if (changeEventsTriggered.incrementAndGet() == eventsCount) {
                        allChangedEventsReachedSemaphore.release()
                    }
                }
                CuratorCacheListener.Type.NODE_CREATED -> {
                    createEventsTriggered.incrementAndGet()
                }
                CuratorCacheListener.Type.NODE_DELETED -> {
                }
            }
        }

        workPooledCache.listenable().addListener(curatorCacheListener)
        workPooledCache.start()

        (1..eventsCount).forEach {
            zkTestingServer.client.setData().forPath(ZKPaths.makePath(path, it.toString()), byteArrayOf())
        }
        assertEquals(eventsCount + 1, createEventsTriggered.get())
        allChangedEventsReachedSemaphore.tryAcquire(2, TimeUnit.SECONDS)
        assertNotEquals(eventsCount, changeEventsTriggered.get())
    }

    @Test
    fun `all nodes are cached when initialized event handled`() {
        val path = ZKPaths.makePath(JOB_MANAGER_ZK_ROOT_PATH, "cache1")
        val eventsCount = 3000
        val createEventsTriggered = AtomicInteger()
        val changeEventsTriggered = AtomicInteger()

        zkTestingServer.client.create().creatingParentsIfNeeded().forPath(path)
        (1..eventsCount).forEach {
            zkTestingServer.client.create().forPath(ZKPaths.makePath(path, it.toString()))
        }
        val workPooledCache = CuratorCache.build(zkTestingServer.client, path)
        val initSemaphore = Semaphore(0)
        val allChangedEventsReachedSemaphore = Semaphore(0)

        val curatorCacheListener = object : CuratorCacheListener {
            override fun event(type: CuratorCacheListener.Type?, oldData: ChildData?, data: ChildData?) {
                when (type) {
                    CuratorCacheListener.Type.NODE_CHANGED -> {
                        if (changeEventsTriggered.incrementAndGet() == eventsCount) {
                            allChangedEventsReachedSemaphore.release()
                        }
                    }
                    CuratorCacheListener.Type.NODE_CREATED -> {
                        createEventsTriggered.incrementAndGet()
                    }
                    CuratorCacheListener.Type.NODE_DELETED -> {
                    }
                }
            }

            override fun initialized() {
                initSemaphore.release()
            }
        }

        workPooledCache.listenable().addListener(curatorCacheListener)
        workPooledCache.start()
        initSemaphore.acquire()

        (1..eventsCount).forEach {
            zkTestingServer.client.setData().forPath(ZKPaths.makePath(path, it.toString()), byteArrayOf())
        }
        assertEquals(eventsCount + 1, createEventsTriggered.get())
        allChangedEventsReachedSemaphore.tryAcquire(2, TimeUnit.SECONDS)
        assertEquals(eventsCount, changeEventsTriggered.get())
    }

}