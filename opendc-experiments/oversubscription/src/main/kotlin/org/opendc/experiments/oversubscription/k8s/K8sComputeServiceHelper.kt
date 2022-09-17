package org.opendc.experiments.oversubscription.k8s

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import org.opendc.compute.api.Server
import org.opendc.compute.api.ServerState
import org.opendc.compute.api.ServerWatcher
import org.opendc.compute.service.ComputeService
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
                        scheduler: ComputeScheduler,
                        schedulingQuantum: Duration = Duration.ofMinutes(5),
                        private val k8sTopology: Topology,
                        private val oversubscription: Float,
) : AutoCloseable {
    public var hosts: MutableList<SimHost> = mutableListOf()
    /**
     * The [ComputeService] that has been configured by the manager.
     */
    public val service: ComputeService

    /**
     * The [FlowEngine] to simulate the hosts.
     */
    private val _engine = FlowEngine(context, clock)

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
    public suspend fun run(trace: List<VirtualMachine>, seed: Long, submitImmediately: Boolean = false) {
        assignClustersToTrace(k8sTopology, trace)
        assignClustersToHosts(k8sTopology, oversubscription)

        val client = service.newClient()

        // Create new image for the virtual machine
        val image = client.newImage("vm-image")

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
                                meta = if (entry.cpuCapacity > 0.0) mapOf("cpu-capacity" to entry.cpuCapacity, "cluster" to entry.cluster) else mapOf("cluster" to entry.cluster)
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

    public fun assignClustersToHosts(topology: Topology, oversubscription: Float){
        for (host in hosts){
            host.partitionsTotalRemaining = ((host.partitionsTotalRemaining * oversubscription).toInt())
        }

        val random = Random(0)
        val nodes = topology.resolve()
        for (node in nodes){
            hosts.shuffle(random)
            var assigned = false
            for (host in hosts){
                if (node.model.cpus.size<=host.partitionsTotalRemaining){
                    host.partitionsTotalRemaining -= node.model.cpus.size
                    if (!host.partitions.contains(node.cluster)) {
                        host.partitions[node.cluster] = mutableListOf(node.model.cpus.size)
                        host.partitionsUsed[node.cluster] = mutableListOf(0)
                    } else{
                        host.partitions[node.cluster]!!.add(node.model.cpus.size)
                        host.partitionsUsed[node.cluster]!!.add(0)
                    }
                    assigned = true
                    break
                }
            }
            if (!assigned){
                throw K8sException("unable to assign cluster")
            }
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
    private fun createService(scheduler: ComputeScheduler, schedulingQuantum: Duration): ComputeService {
        val meterProvider = telemetry.createMeterProvider(scheduler)
        return K8sComputeService(context, clock, meterProvider, scheduler, schedulingQuantum)
    }
}


class Waiter() : ServerWatcher {
    public val mutex = Mutex()

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
