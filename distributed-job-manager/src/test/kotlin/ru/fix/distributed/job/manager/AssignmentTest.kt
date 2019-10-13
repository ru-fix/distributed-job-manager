package ru.fix.distributed.job.manager

import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId

class AssignmentTest {

    class WorkerScope(private val state: AssignmentState){
        operator fun String.invoke(builder: JobScope.()->Unit){
            builder(JobScope(state, this))
        }
    }

    class JobScope(private val state: AssignmentState,
                   private val worker:String){
        operator fun String.invoke(vararg items: String){
            for(item in items) {
                state.addWorkItem(WorkerId(worker), WorkItem(item, JobId(this)))
            }
        }
    }

    fun assignmentState(builder: WorkerScope.()->Unit) =
            AssignmentState().apply {
                builder(WorkerScope(this))
            }


    @Test
    fun `assignment of `() {
        val current = assignmentState {
            "w1"{
                "j1"(
                        "wi1",
                        "wi2",
                        "wi3"
                )
            }
            "w2"{
                "j1"(
                        "wi4",
                        "wi5"

                )
                "j2"(
                        "wi1",
                        "wi2"

                )
            }

        }

        println(current)

    }
}