package org.opendc.experiments.reservations

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import org.opendc.compute.api.Server
import org.opendc.compute.api.ServerState
import org.opendc.compute.api.ServerWatcher
import org.opendc.compute.service.ComputeService
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.compute.simulator.SimHost
import org.opendc.compute.workload.FailureModel
import org.opendc.compute.workload.VirtualMachine
import org.opendc.compute.workload.telemetry.TelemetryManager
import org.opendc.compute.workload.topology.HostSpec
import org.opendc.compute.workload.topology.Topology
import org.opendc.simulator.compute.kernel.interference.VmInterferenceModel
import org.opendc.simulator.compute.workload.SimRuntimeWorkload
import org.opendc.simulator.flow.FlowEngine
import java.time.Clock
import java.time.Duration
import kotlin.coroutines.CoroutineContext
import kotlin.math.max

class ReservationComputeService(
    private val context: CoroutineContext,
    private val clock: Clock,
    private val telemetry: TelemetryManager,
    scheduler: ComputeScheduler,
    private val failureModel: FailureModel? = null,
    private val interferenceModel: VmInterferenceModel? = null,
    schedulingQuantum: Duration = Duration.ofMinutes(5)
) : AutoCloseable {
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
    public suspend fun run(trace: List<VirtualMachine>, seed: Long, reservationRatio: Float, submitImmediately: Boolean = false) {
        val random = java.util.Random(seed)
        val injector = failureModel?.createInjector(context, clock, service, random)
        val client = service.newClient()

        // Create new image for the virtual machine
        val image = client.newImage("vm-image")

        try {
            coroutineScope {
                // Start the fault injector
                injector?.start()

                var offset = Long.MIN_VALUE

                val randTrace = trace.sortedBy { it.startTime }
                val reservationI = (randTrace.size*reservationRatio).toInt()
                var reservation = emptyList<VirtualMachine>()
                var online = emptyList<VirtualMachine>()
                if (0<reservationI){
                    reservation = reservationOrderStopTimeRange(randTrace.slice(0..reservationI-1))
                }
                if (reservationI<randTrace.size){
                    online  = randTrace.slice(reservationI..randTrace.size-1).sortedBy { it.startTime }
                }

                var r = 0
                var o = 0
                var entry : VirtualMachine
                while (r<reservation.size || o <online.size) {
                    if (r >= reservation.size || (o<online.size && online[o].startTime < reservation[r].startTime)){
                        entry = online[o]
                        o++
                    } else{
                        entry = reservation[r]
                        r++
                    }

                    val now = clock.millis()
                    val start = entry.startTime.toEpochMilli()

                    if (offset < 0) {
                        offset = start - now
                    }

                    if (!submitImmediately) {
                        delay(max(0, (start - offset) - now))
                    }

                    launch {
                        val workload = SimRuntimeWorkload((entry.stopTime.toEpochMilli()-entry.startTime.toEpochMilli()), 1.0)

                        val server = client.newServer(
                            entry.name,
                            image,
                            client.newFlavor(
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
            injector?.close()
            client.close()
        }
    }

    fun reservationOrderStopTimeRange(vms:List<VirtualMachine>) : List<VirtualMachine>{
        val reservation = mutableListOf<VirtualMachine>()

        val sts = vms.map { it.startTime }.sorted()
        var avg = 0L
        for (i in 0..sts.size) {
            if (i < sts.size-1) {
                avg += sts[i+1].minusMillis(sts[i].toEpochMilli()).epochSecond
            }
        }
        avg /= sts.size-1

        val sorted = vms.sortedBy { it.startTime }
        val start = sorted[0].startTime.minusSeconds(1L)
        val stop = sorted[sorted.size-1].stopTime.plusSeconds(1L)
        var t0 = start

        while( t0.isBefore(stop)){
            val t1 = t0.plusSeconds(avg)
            var fVms = vms.filter { it.startTime.isAfter(t0) && it.startTime.isBefore(t1) }
            fVms = fVms.sortedBy { it.stopTime }
            reservation.addAll(fVms)
            t0 = t1
        }
        return reservation
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
        return ComputeService(context, clock, meterProvider, scheduler, schedulingQuantum)
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
