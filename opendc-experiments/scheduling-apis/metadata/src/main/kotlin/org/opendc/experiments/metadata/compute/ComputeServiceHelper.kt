package org.opendc.experiments.metadata.compute

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import mu.KotlinLogging
import org.opendc.compute.api.Server
import org.opendc.compute.api.ServerState
import org.opendc.compute.api.ServerWatcher
import org.opendc.compute.service.ComputeService
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.compute.simulator.SimHost
import org.opendc.compute.workload.telemetry.TelemetryManager
import org.opendc.compute.workload.topology.HostSpec
import org.opendc.compute.workload.topology.Topology
import org.opendc.experiments.metadata.storage.StorageService
import org.opendc.experiments.metadata.trace.loader.Workflow
import org.opendc.experiments.metadata.workload.DataWorkflowWorkload
import org.opendc.simulator.core.SimulationCoroutineScope
import org.opendc.simulator.flow.FlowEngine
import java.time.Duration
import kotlin.math.max

class ComputeServiceHelper(
    private val scope: SimulationCoroutineScope,
    private val telemetry: TelemetryManager,
    scheduler: ComputeScheduler,
    schedulingQuantum: Duration = Duration.ofSeconds(5),
    private val topology: Topology,
    private val metadataApi: Boolean,
    private val storage : StorageService,
) : AutoCloseable {
    private val logger = KotlinLogging.logger {}

    public var hosts: MutableList<SimHost> = mutableListOf()

    /**
     * The [ComputeService] that has been configured by the manager.
     */
    public val service: ComputeService

    /**
     * The [FlowEngine] to simulate the hosts.
     */
    private val _engine = FlowEngine(scope.coroutineContext, scope.clock)

    /**
     * The hosts that belong to this class.
     */
    private val _hosts = mutableSetOf<SimHost>()

    init {
        val service = createService(scheduler, schedulingQuantum)
        this.service = service
    }

    /**
     * Converge a simulation of the [ComputeService] by replaying the workload trace given by [trace].
     */
    public suspend fun run(trace: List<Workflow>, seed: Long, submitImmediately: Boolean = false) {
        val client = service.newClient()

        // Create new image for the virtual machine
        val image = client.newImage("pod-image")

        logger.debug {"setting objects..."}
        setObjects(trace)
        logger.debug {"objects set"}
        storage.Run()

        try {
            coroutineScope {

                // Start the fault injector
                var offset = Long.MIN_VALUE

                var i = -1
                for (entry in trace.sortedBy { it.startTime }) {

                    val now = scope.clock.millis()
                    val start = entry.startTime.toEpochMilli()

                    if (offset < 0) {
                        offset = start - now
                    }

                    if (!submitImmediately) {
                        delay(max(0, (start - offset) - now))
                    }

                    launch {
                        val cpuCapacity = 1.0


                        val workload = DataWorkflowWorkload(scope, entry, metadataApi, storage)

                        val server = client.newServer(
                            entry.id,
                            image,
                            client.newFlavor(
                                entry.id,
                                entry.cpuCount,
                                1,
                                meta = mapOf("cpu-capacity" to cpuCapacity)
                            ),
                            meta = mapOf("workload" to workload, "cpu-capacity" to cpuCapacity)
                        )

                        val w = Waiter()
                        w.mutex.lock()
                        server.watch(w)
                        // Wait for the server reach its end time
                        w.mutex.lock()
                        // Delete the server after reaching the end-time of the virtual machine
                        server.delete()
                    }
                }
            }
            yield()
        } finally {
            client.close()
            storage.Close()
        }
    }


    fun setObjects(workflows : List<Workflow>){
        val seen = mutableSetOf<String>()
        for (workflow in workflows){
            for (task in workflow.tasks){
                if (seen.contains(task.objectID)){
                    continue
                }

                storage.SetIfNew(task.objectID, task.objectSize)

                seen.add(task.objectID)
            }
        }
    }

    public fun apply(topology: Topology, optimize: Boolean = false) {
        val hosts = topology.resolve()
        for (spec in hosts) {
            registerHost(spec, optimize)
        }
    }

    /**
     * Register a host for this simulation.
     *
     * @param spec The definition of the host.
     * @param optimize Merge the CPU resources of the host into a single CPU resource.
     * @return The [SimHost] that has been constructed by the runner.
     */
    public fun registerHost(spec: HostSpec, optimize: Boolean = false): SimHost {
        val meterProvider = telemetry.createMeterProvider(spec)
        val host = SimHost(
            spec.uid,
            spec.name,
            spec.model,
            spec.meta,
            scope.coroutineContext,
            _engine,
            meterProvider,
            spec.hypervisor,
            powerDriver = spec.powerDriver,
            optimize = optimize
        )
        hosts.add(host)

        require(_hosts.add(host)) { "Host with uid ${spec.uid} already exists" }
        service.addHost(host)

        return host
    }

    override fun close() {
        service.close()

        for (host in _hosts) {
            host.close()
        }

        _hosts.clear()
    }

    /**
     * Construct a [ComputeService] instance.
     */
    private fun createService(nodeScheduler: ComputeScheduler, schedulingQuantum: Duration): ComputeService {
        val meterProvider = telemetry.createMeterProvider(nodeScheduler)
        return ComputeService(scope.coroutineContext, scope.clock, meterProvider, nodeScheduler, schedulingQuantum)
    }
}


class Waiter : ServerWatcher {
    val mutex = Mutex()

    public override fun onStateChanged(server: Server, newState: ServerState) {
        when (newState) {
            ServerState.TERMINATED -> {
                mutex.unlock()
            }

            ServerState.ERROR -> {
                mutex.unlock()
            }

            else -> {}
        }
    }
}

class K8sException(message: String) : Exception(message)
