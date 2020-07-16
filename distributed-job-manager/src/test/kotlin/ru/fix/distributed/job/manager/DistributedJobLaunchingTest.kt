package ru.fix.distributed.job.manager

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

class DistributedJobLaunchingTest {

    @Test
    @Disabled("TODO")
    fun `job restarted with delay`() {

    }
    @Test
    @Disabled("TODO")
    fun `job restarted with rate`() {

    }
    @Test
    @Disabled("TODO")
    fun `job restarted by schedule after failure`() {

    }

    @Test
    @Disabled("TODO")
    fun `work pool single thread strategy passes several WorkItems to single job run`() {

    }

    @Test
    @Disabled("TODO")
    fun `work pool thread per workItem strategy passes single WorkItem to job run and run all work items in parallel`() {

    }

    @Test
    @Disabled("TODO")
    fun `custom work pool running strategy split work items between job launches`() {

    }

    @Test
    @Disabled("TODO")
    fun `during disconnect of DJM1, DJM2 does not steal WorkItem that currently under work by DJM1`(){

    }

    @Test
    @Disabled("TODO")
    fun `when DJM3 disconnects, WorkItems rebalanced between DJM1 and DJM2`(){
    }

    @Test
    @Disabled("TODO")
    fun `when DJM3 shutdowns, WorkItems rebalanced between DJM1 and DJM2`(){
    }

    @Test
    @Disabled("TODO")
    fun `series of random DJMs disconnects, shutdowns, launches, WorkPool changes and restarts does not affect correct WorkItem launching and schedulling`(){
        TODO("same workItem running only by single thread within the cluster")
        TODO("all workItem runs by schedule as expected with small disturbances")

    }

    @Test
    @Disabled("TODO")
    fun `series of DJMs restarts one by one does not affect correct WorkItem launching and schedulling`(){
    }

    @Test
    @Disabled("TODO")
    fun `series of DJMs restarts two by two does not affect correct WorkItem launching and schedulling`(){
    }

    @Test
    @Disabled("TODO")
    fun `series of DJMs restarts three by three does not affect correct WorkItem launching and schedulling`(){
    }

    @Test
    @Disabled("TODO")
    fun `Change in Job WorkPool triggers rebalance`(){

    }

    @Test
    @Disabled("TODO")
    fun `If WorkPool and number of DJMs does not change, no rebalance is triggered `(){

    }

    @Test
    @Disabled("TODO")
    fun `DJM shutdown triggers rebalance in cluster `(){

    }


    @Test
    @Disabled("TODO")
    fun `DJM set of available jobs changes triggers rebalance in cluster `(){

    }

    @Test
    @Disabled("TODO")
    fun `DJM follows assignment strategy`(){

    }

    @Test
    @Disabled("TODO")
    fun `Assignment strategy that assign same workItem to different workers rise an exception`(){
    }

    @Test
    @Disabled("TODO")
    fun `DJM does not allow two jobs with same ID`(){
    }

    @Test
    @Disabled("TODO")
    fun `DJM does not allow incorrect symbols in WorkPool`(){
    }
}