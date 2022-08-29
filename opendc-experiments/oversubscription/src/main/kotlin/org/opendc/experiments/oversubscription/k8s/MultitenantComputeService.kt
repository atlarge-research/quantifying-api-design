package org.opendc.experiments.overprovision.k8s

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import org.opendc.compute.service.ComputeService
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.compute.workload.VirtualMachine
import org.opendc.compute.workload.telemetry.TelemetryManager
import org.opendc.simulator.compute.kernel.interference.VmInterferenceModel
import java.time.Clock
import java.time.Duration
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.launch
import org.opendc.compute.simulator.SimHost
import org.opendc.compute.workload.FailureModel
import org.opendc.compute.workload.createComputeScheduler
import org.opendc.compute.workload.export.parquet.ParquetComputeMetricExporter
import org.opendc.compute.workload.telemetry.SdkTelemetryManager
import org.opendc.compute.workload.topology.HostSpec
import org.opendc.compute.workload.topology.Topology
import org.opendc.experiments.capelin.topology.clusterTopology
import org.opendc.simulator.core.runBlockingSimulation
import org.opendc.simulator.flow.FlowEngine
import org.opendc.telemetry.sdk.metrics.export.CoroutineMetricReader
import java.io.File
import java.util.*

class MultitenantComputeService(
    private val scope: CoroutineScope,
    private val context: CoroutineContext,
    private val clock: Clock,
    private val telemetry: TelemetryManager,
    scheduler: ComputeScheduler,
    private val failureModel: FailureModel? = null,
    private val interferenceModel: VmInterferenceModel? = null,
    schedulingQuantum: Duration = Duration.ofMinutes(5),
    val allocationPolicy: String = "random",
    private val trace : String,
    private val tenantAmount: Int
    ) {
    /**
     * The [ComputeService] that has been configured by the manager.
     */
    private val service: ComputeService

    /**
     * The [FlowEngine] to simulate the hosts.
     */
    private val _engine = FlowEngine(context, clock)
    /**
     * The hosts that belong to this class.
     */
    private val _hosts = mutableSetOf<SimHost>()

    private val topology = clusterTopology(File("src/main/resources/topology", "${trace}-$tenantAmount-k8s.txt"))

    init {
        this.service = createService(scheduler, schedulingQuantum)
    }

    public suspend fun run(traces: List<VirtualMachine>, seed: Long, submitImmediately: Boolean = false) {
        val client = service.newClient()

        coroutineScope {
            for (subTraces in traces.chunked(traces.size/tenantAmount)) {
                val k8s = K8sComputeService(client, _engine, context, clock, telemetry,
                    null, createK8sScheduler(allocationPolicy, Random(seed), service))
                k8s.apply(topology)
                launch {
                    k8s.run(subTraces)
                }
            }
        }
    }

    public fun apply(topology: Topology, optimize: Boolean = false) {
        val hosts = topology.resolve()
        for (spec in hosts) {
            registerHost(spec, optimize)
        }
    }

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
            interferenceDomain = interferenceModel?.newDomain(),
            optimize = optimize
        )

        require(_hosts.add(host)) { "Host with uid ${spec.uid} already exists" }
        service.addHost(host)

        return host
    }

    fun close() {
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
        return ComputeService(context, clock, meterProvider, scheduler, schedulingQuantum)
    }
}
