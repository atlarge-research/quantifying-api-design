package org.opendc.experiments.overprovision.k8s

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import org.opendc.compute.api.ComputeClient
import org.opendc.compute.api.Server
import org.opendc.compute.api.ServerState
import org.opendc.compute.api.ServerWatcher
import org.opendc.compute.service.ComputeService
import org.opendc.compute.service.internal.ClientServer
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.compute.simulator.SimHost
import org.opendc.compute.workload.VirtualMachine
import org.opendc.compute.workload.telemetry.TelemetryManager
import org.opendc.compute.workload.topology.HostSpec
import org.opendc.compute.workload.topology.Topology
import org.opendc.simulator.compute.kernel.interference.VmInterferenceModel
import org.opendc.simulator.compute.workload.SimRuntimeWorkload
import org.opendc.simulator.compute.workload.SimTraceWorkload
import org.opendc.simulator.flow.FlowEngine
import java.time.Clock
import java.time.Duration
import kotlin.coroutines.CoroutineContext
import kotlin.math.max
import kotlin.random.Random

class K8sComputeService(private val client: ComputeClient,
                        private val engine: FlowEngine,
                        private val context: CoroutineContext,
                        private val clock: Clock,
                        private val telemetry: TelemetryManager,
                        private val interferenceModel: VmInterferenceModel? = null,
                        scheduler: ComputeScheduler,
                        schedulingQuantum: Duration = Duration.ofMinutes(5)) {
    private val id: Int

    private val k8s: ComputeService

    private val servers = ArrayList<Server>()

    init {
        id = Random.nextInt(0, 10_000)
        k8s = createService(scheduler, schedulingQuantum)
    }

    public suspend fun run(trace: List<VirtualMachine>, submitImmediately: Boolean = false){
        val k8sClient = k8s.newClient()

        // Create new image for the virtual machine
        val image = k8sClient.newImage("pod-k8s-image-$id")
        try {
            coroutineScope {
                var offset = Long.MIN_VALUE
                val traces = trace.sortedBy { it.startTime }
                for (entry in traces) {
                    val now = clock.millis()
                    val start = entry.startTime.toEpochMilli()

                    if (offset < 0) {
                        offset = start - now
                    }

                    // Make sure the trace entries are ordered by submission time
                    assert(start - offset >= 0) { "Invalid trace order" }

                    if (!submitImmediately) {
                        delay(max(0, (start - offset) - now))
                    }

                    launch {
                        val workload = SimRuntimeWorkload((entry.stopTime.toEpochMilli()-entry.startTime.toEpochMilli()), 1.0, "k8s-"+entry.name)

                        val server = k8sClient.newServer(
                            "k8s-pod-"+entry.name,
                            image,
                            k8sClient.newFlavor(
                                entry.name,
                                entry.cpuCount,
                                entry.memCapacity,
                                meta = if (entry.cpuCapacity > 0.0) mapOf("cpu-capacity" to entry.cpuCapacity) else emptyMap()
                            ),
                            meta = mapOf("workload" to workload)
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
            k8sClient.close()
            client.close()
            for (server in servers){
                server.delete()
            }
        }
    }

    public suspend fun apply(topology: Topology, optimize: Boolean = false) {
        val hosts = topology.resolve()
        for (spec in hosts) {
            registerHost(spec, optimize)
        }
    }

    public suspend fun registerHost(spec: HostSpec, optimize: Boolean = false): K8sNode {
        val meterProvider = telemetry.createMeterProvider(spec)
        val host = K8sNode(
            spec.uid,
            "k8s-" + spec.name,
            spec.model,
            spec.meta,
            context,
            engine,
            meterProvider,
            spec.hypervisor,
            powerDriver = spec.powerDriver,
            interferenceDomain = interferenceModel?.newDomain(),
            optimize = optimize
        )

        val image = client.newImage("k8s-image")
        val server = client.newServer(
            spec.name,
            image,
            client.newFlavor(
                spec.name,
                spec.model.cpus.size,
                spec.model.memory.sumOf { it.size },
                meta = emptyMap()
            ),
            meta = mapOf("workload" to host)
        )
        servers.add(server)
        if (server is ClientServer) host.server = server
        k8s.addHost(host)

        return host
    }

    private fun createService(scheduler: ComputeScheduler, schedulingQuantum: Duration): ComputeService {
        val meterProvider = telemetry.createMeterProvider(scheduler)
        return ComputeService(context, clock, meterProvider, scheduler, schedulingQuantum)
    }
}


class Waiter() : ServerWatcher {
    public val mutex = Mutex()

    public override fun onStateChanged(server: Server, newState: ServerState) {
        when(newState){
            ServerState.DELETED -> {
                mutex.unlock()
            }
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
