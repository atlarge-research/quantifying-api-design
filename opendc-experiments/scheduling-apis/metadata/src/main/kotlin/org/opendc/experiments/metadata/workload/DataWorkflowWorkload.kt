package org.opendc.experiments.metadata.workload

import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import org.opendc.experiments.metadata.storage.Object
import org.opendc.experiments.metadata.storage.ObjectRequester
import org.opendc.experiments.metadata.storage.StorageService
import org.opendc.experiments.metadata.trace.loader.Task
import org.opendc.experiments.metadata.trace.loader.Workflow
import org.opendc.simulator.compute.SimMachineContext
import org.opendc.simulator.compute.workload.SimWorkload
import org.opendc.simulator.core.SimulationCoroutineScope
import org.opendc.simulator.flow.FlowConnection
import org.opendc.simulator.flow.FlowSource

class DataWorkflowWorkload(
    val scope : SimulationCoroutineScope,
    val workflow: Workflow,
    val metadataApi: Boolean,
    val storage: StorageService,
) : SimWorkload {
    val jobs : MutableList<Job> = mutableListOf()
    val mutex = Mutex()
    var tasks : MutableList<Task> = mutableListOf()
    var running : Int = 0

    override fun onStart(ctx: SimMachineContext) {
        tasks.addAll(workflow.tasks)

        for (core in 1..workflow.cpuCount) {
            scope.launch { runTask(ctx, core) }
            running++
        }

        for (cpu in ctx.cpus){
            cpu.startConsumer(Consumer())
        }
    }

    private suspend fun runTask(ctx: SimMachineContext, idx: Int){
        while (true){
            mutex.lock()
            if (tasks.isEmpty()){  // if empty then stop
                running--
                if (running <= 0){  // if last task, close context
                    ctx.close()
                }

                mutex.unlock()
                return
            }

            val task = getNextTask()
            mutex.unlock()

            val request = StorageRequest()
            request.mutex.lock()
            storage.RequestObject(task.objectID, request)
            request.mutex.lock()

            delay(task.runtime)
        }
    }

    private  fun getNextTask() : Task{
        if (metadataApi){
            tasks = (tasks.sortedBy { storage.GetDownloadEstimate(it.objectID) }).toMutableList()
            return tasks.removeFirst()
        }else {
            tasks = tasks.shuffled().toMutableList()
            return tasks.removeFirst()
        }
    }


    class StorageRequest() : ObjectRequester{
        val mutex = Mutex()

        override suspend fun Receive(obj: Object?) {
            mutex.unlock()
        }

    }

    override fun onStop(ctx: SimMachineContext) {
        for (job in jobs){
            job.cancel()
        }
    }

    override fun toString(): String = "DataWorkflowWorkload(workflowId=${workflow.id},taskNum=${workflow.tasks.size},cpuCount=${workflow.cpuCount})"

    class Consumer : FlowSource{
        override fun onPull(conn: FlowConnection, now: Long): Long {
            return Long.MAX_VALUE
        }
    }
}



