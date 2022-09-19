package org.opendc.experiments.oversubscription.k8s

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import org.opendc.compute.api.ComputeClient
import org.opendc.compute.api.Server
import org.opendc.compute.api.ServerState
import org.opendc.compute.api.ServerWatcher
import org.opendc.compute.service.ComputeService
import org.opendc.compute.service.driver.K8sNode
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.compute.simulator.SimHost
import org.opendc.compute.workload.VirtualMachine
import org.opendc.compute.workload.telemetry.TelemetryManager
import org.opendc.compute.workload.topology.HostSpec
import org.opendc.compute.workload.topology.Topology
import org.opendc.simulator.compute.workload.SimRuntimeWorkload
import org.opendc.simulator.flow.FlowEngine
import java.time.Clock
import java.time.Duration
import java.util.Random
import kotlin.coroutines.CoroutineContext
import kotlin.math.max

class K8sComputeServiceHelper(private val context: CoroutineContext,
                              private val clock: Clock,
                              private val telemetry: TelemetryManager,
                              nodeScheduler: ComputeScheduler,
                              podScheduler: ComputeScheduler,
                              schedulingQuantum: Duration = Duration.ofMinutes(5),
                              private val k8sTopology: Topology,
                              private val oversubscription: Float,
) : AutoCloseable {
    public var hosts: MutableList<SimHost> = mutableListOf()
    /**
     * The [ComputeService] that has been configured by the manager.
     */
    public val service: K8sComputeService

    /**
     * The [FlowEngine] to simulate the hosts.
     */
    private val _engine = FlowEngine(context, clock)

    /**
     * The hosts that belong to this class.
     */
    private val _hosts = mutableSetOf<SimHost>()

    init {
        val service = createService(nodeScheduler, podScheduler, schedulingQuantum)
        this.service = service
    }

    /**
     * Converge a simulation of the [ComputeService] by replaying the workload trace given by [trace].
     */
    public suspend fun run(trace: List<VirtualMachine>, seed: Long, submitImmediately: Boolean = false) {
        val client = service.newClient()

        assignClustersToTrace(k8sTopology, trace)
        scheduleNodesToHosts(client, k8sTopology, oversubscription)

        // Create new image for the virtual machine
        val image = client.newImage("pod-image")

        try {
            coroutineScope {
                // Start the fault injector
                var offset = Long.MIN_VALUE

                for (entry in trace.sortedBy { it.startTime }) {

                    val now = clock.millis()
                    val start = entry.startTime.toEpochMilli()

                    if (offset < 0) {
                        offset = start - now
                    }

                    if (!submitImmediately) {
                        delay(max(0, (start - offset) - now))
                    }

                    launch {
                        val workload = SimRuntimeWorkload((entry.stopTime.toEpochMilli()-entry.startTime.toEpochMilli()), 1.0, name = entry.name)

                        val server = client.newServer(
                            entry.name,
                            image,
                            client.newFlavor(
                                entry.name,
                                entry.cpuCount,
                                entry.memCapacity,
                                meta = mapOf("cpu-capacity" to entry.cpuCapacity)
                            ),
                            meta = mapOf("workload" to workload, "cluster" to entry.cluster)
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
        }
    }



    public fun apply(topology: Topology, optimize: Boolean = false) {
        val hosts = topology.resolve()
        for (spec in hosts) {
            registerHost(spec, optimize)
        }
    }

    public fun assignClustersToTrace(topology: Topology, trace: List<VirtualMachine>) {
        var i = 0
        val trace = trace.shuffled(Random(0))
        val nodes = topology.resolve()
        val totalCPUS = nodes.sumOf { it.model.cpus.size }
        val clusters = nodes.distinctBy { it.cluster }.map { it.cluster }
        for (cluster in clusters){
            val cpus = nodes.filter { it.cluster == cluster }.sumOf { it.model.cpus.size }
            val amount = (trace.size * (cpus.toFloat()/totalCPUS.toFloat())).toInt()
            for (j in i..i+amount-1){
                trace[j].cluster = cluster
            }
            i += amount
        }
        for (j in i..trace.size-1){
            trace[j].cluster = clusters[clusters.size-1]
        }
    }

    public suspend fun scheduleNodesToHosts(client: ComputeClient, topology: Topology, oversubscription: Float){
        val nodes = mutableListOf<K8sNode>()
        val image = client.newImage("node-image")

        val random = Random(0)
        val vms = topology.resolve()

        for (vm in vms){
            hosts.shuffle(random)
                    val node = K8sNode(
                        cluster = vm.cluster,
                        uid = vm.uid,
                        name = vm.name,
                        image = image,
                        cpuCount = vm.model.cpus.size,
                        availableCpuCount = vm.model.cpus.size,
                        availableMemory = vm.model.memory.sumOf { it.size },
                        pods = mutableListOf()
                    )
            nodes.add(node)
            }

            service.scheduleK8sNodes(nodes, oversubscription)
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
            context,
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
    private fun createService(nodeScheduler: ComputeScheduler, podScheduler: ComputeScheduler, schedulingQuantum: Duration): K8sComputeService {
        val meterProvider = telemetry.createMeterProvider(podScheduler)
        return K8sComputeService(context, clock, meterProvider, nodeScheduler=nodeScheduler,podScheduler= podScheduler, schedulingQuantum)
    }
}


class Waiter() : ServerWatcher {
    val mutex = Mutex()

    public override fun onStateChanged(server: Server, newState: ServerState) {
        when(newState){
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
