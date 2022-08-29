package org.opendc.experiments.overprovision.k8s

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.metrics.MeterProvider
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement
import io.opentelemetry.api.metrics.ObservableLongMeasurement
import kotlinx.coroutines.*
import mu.KotlinLogging
import org.opendc.compute.api.Flavor
import org.opendc.compute.api.Server
import org.opendc.compute.api.ServerState
import org.opendc.compute.service.internal.ClientServer
import org.opendc.compute.service.driver.Host
import org.opendc.compute.service.driver.HostListener
import org.opendc.compute.service.driver.HostModel
import org.opendc.compute.service.driver.HostState
import org.opendc.compute.simulator.SimHost
import org.opendc.compute.simulator.SimMetaWorkloadMapper
import org.opendc.compute.simulator.SimWorkloadMapper
import org.opendc.simulator.compute.SimBareMetalMachine
import org.opendc.simulator.compute.SimMachineContext
import org.opendc.simulator.compute.kernel.SimHypervisor
import org.opendc.simulator.compute.kernel.SimHypervisorProvider
import org.opendc.simulator.compute.kernel.cpufreq.PerformanceScalingGovernor
import org.opendc.simulator.compute.kernel.cpufreq.ScalingGovernor
import org.opendc.simulator.compute.kernel.interference.VmInterferenceDomain
import org.opendc.simulator.compute.model.MachineModel
import org.opendc.simulator.compute.model.MemoryUnit
import org.opendc.simulator.compute.power.ConstantPowerModel
import org.opendc.simulator.compute.power.PowerDriver
import org.opendc.simulator.compute.power.SimplePowerDriver
import org.opendc.simulator.compute.workload.SimWorkload
import org.opendc.simulator.flow.FlowEngine
import java.util.*
import kotlin.coroutines.CoroutineContext
import io.opentelemetry.api.common.AttributesBuilder
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes
import org.opendc.simulator.compute.kernel.SimVirtualMachine
import org.opendc.simulator.compute.runWorkload
import java.time.Clock

class K8sNode(
    override val uid: UUID,
    override val name: String,
    model: MachineModel,
    override val meta: Map<String, Any>,
    context: CoroutineContext,
    engine: FlowEngine,
    meterProvider: MeterProvider,
    hypervisorProvider: SimHypervisorProvider,
    scalingGovernor: ScalingGovernor = PerformanceScalingGovernor(),
    powerDriver: PowerDriver = SimplePowerDriver(ConstantPowerModel(0.0)),
    private val mapper: SimWorkloadMapper = SimMetaWorkloadMapper(),
    private val interferenceDomain: VmInterferenceDomain? = null,
    private val optimize: Boolean = false
) : SimWorkload, Host, AutoCloseable {

    public var server: ClientServer? = null
    /**
     * The [CoroutineScope] of the host bounded by the lifecycle of the host.
     */
    private val scope: CoroutineScope = CoroutineScope(context + Job())

    /**
     * The clock instance used by the host.
     */
    private val clock = engine.clock

    /**
     * The logger instance of this server.
     */
    private val logger = KotlinLogging.logger {}

    /**
     * The [Meter] to track metrics of the simulated host.
     */
    private val meter = meterProvider.get("org.opendc.compute.simulator")

    /**
     * The event listeners registered with this host.
     */
    private val listeners = mutableListOf<HostListener>()

    /**
     * The machine to run on.
     */
    public val machine: SimBareMetalMachine = SimBareMetalMachine(engine, model.optimize(), powerDriver)

    /**
     * The hypervisor to run multiple workloads.
     */
    public val hypervisor: SimHypervisor = hypervisorProvider
        .create(engine, scalingGovernor = null, interferenceDomain = null)

    /**
     * The virtual machines running on the hypervisor.
     */
    private val guests = HashMap<Server, Guest>()
    private val _guests = mutableListOf<Guest>()

    override val state: HostState
        get() = _state
    private var _state: HostState = HostState.UP
        set(value) {
            if (value != field) {
                listeners.forEach { it.onStateChanged(this, value) }
            }
            field = value
        }

    override val model: HostModel = HostModel(model.cpus.sumOf { it.frequency }, model.cpus.size, model.memory.sumOf { it.size })

    /**
     * The [GuestListener] that listens for guest events.
     */
    private val guestListener = object : GuestListener {
        override fun onStart(guest: Guest) {
            listeners.forEach { it.onStateChanged(this@K8sNode, guest.server, guest.state) }
        }

        override fun onStop(guest: Guest) {
            listeners.forEach { it.onStateChanged(this@K8sNode, guest.server, guest.state) }
        }
    }

    init {
        meter.upDownCounterBuilder("system.guests")
            .setDescription("Number of guests on this host")
            .setUnit("1")
            .buildWithCallback(::collectGuests)
        meter.gaugeBuilder("system.cpu.limit")
            .setDescription("Amount of CPU resources available to the host")
            .buildWithCallback(::collectCpuLimit)
        meter.gaugeBuilder("system.cpu.demand")
            .setDescription("Amount of CPU resources the guests would use if there were no CPU contention or CPU limits")
            .setUnit("MHz")
            .buildWithCallback { result -> result.record(hypervisor.cpuDemand) }
        meter.gaugeBuilder("system.cpu.usage")
            .setDescription("Amount of CPU resources used by the host")
            .setUnit("MHz")
            .buildWithCallback { result -> result.record(hypervisor.cpuUsage) }
        meter.gaugeBuilder("system.cpu.utilization")
            .setDescription("Utilization of the CPU resources of the host")
            .setUnit("%")
            .buildWithCallback {result -> result.record(
                hypervisor.cpuUsage / _cpuLimit
            )}
        meter.counterBuilder("system.cpu.time")
            .setDescription("Amount of CPU time spent by the host")
            .setUnit("s")
            .buildWithCallback(::collectCpuTime)
        meter.gaugeBuilder("system.power.usage")
            .setDescription("Power usage of the host ")
            .setUnit("W")
            .buildWithCallback { result -> result.record(machine.powerUsage) }
        meter.counterBuilder("system.power.total")
            .setDescription("Amount of energy used by the CPU")
            .setUnit("J")
            .ofDoubles()
            .buildWithCallback { result -> result.record(machine.energyUsage) }
        meter.counterBuilder("system.time")
            .setDescription("The uptime of the host")
            .setUnit("s")
            .buildWithCallback(::collectUptime)
        meter.gaugeBuilder("system.time.boot")
            .setDescription("The boot time of the host")
            .setUnit("1")
            .ofLongs()
            .buildWithCallback(::collectBootTime)
    }


    override fun canFit(server: Server): Boolean {
        val sufficientMemory = model.memoryCapacity >= server.flavor.memorySize
        val enoughCpus = model.cpuCount >= server.flavor.cpuCount
        val canFit = hypervisor.canFit(server.flavor.toMachineModel())

        return sufficientMemory && enoughCpus && canFit
    }

    override suspend fun spawn(server: Server, start: Boolean) {
        val guest = guests.computeIfAbsent(server) { key ->
            require(canFit(key)) { "Server does not fit" }

            val machine = hypervisor.newMachine(key.flavor.toMachineModel(), key.name)
            val newGuest = Guest(
                scope.coroutineContext,
                clock,
                this,
                hypervisor,
                mapper,
                guestListener,
                server,
                machine
            )

            _guests.add(newGuest)
            newGuest
        }

        if (start) {
            guest.start()
        }
    }

    override fun contains(server: Server): Boolean {
        return server in guests
    }

    override suspend fun start(server: Server) {
        val guest = requireNotNull(guests[server]) { "Unknown server ${server.uid} at host $uid" }
        guest.start()
    }

    override suspend fun stop(server: Server) {
        val guest = requireNotNull(guests[server]) { "Unknown server ${server.uid} at host $uid" }
        guest.stop()
    }

    override suspend fun delete(server: Server) {
        val guest = guests[server] ?: return
        guest.delete()
    }

    override fun addListener(listener: HostListener) {
        listeners.add(listener)
    }

    override fun removeListener(listener: HostListener) {
        listeners.remove(listener)
    }

    override fun close() {
        reset()
        scope.cancel()
        machine.cancel()
    }

    override fun hashCode(): Int = uid.hashCode()



    override fun equals(other: Any?): Boolean {
        return other is K8sNode && uid == other.uid
    }

    override fun toString(): String = "K8sNode[uid=$uid,name=$name,model=$model]"

    public suspend fun fail() {
        reset()

        for (guest in _guests) {
            guest.fail()
        }
    }

    public suspend fun recover() {
        updateUptime()

        // Wait for the hypervisor to launch before recovering the guests
        yield()

        for (guest in _guests) {
            guest.recover()
        }
    }

    /**
     * The [Job] that represents the machine running the hypervisor.
     */
    private var _ctx: SimMachineContext? = null

    /**
     * Reset the machine.
     */
    private fun reset() {
        updateUptime()

        // Stop the hypervisor
        _ctx?.close()
        _state = HostState.DOWN
    }

    /**
     * Convert flavor to machine model.
     */
    private fun Flavor.toMachineModel(): MachineModel {
        val originalCpu = machine.model.cpus[0]
        val cpuCapacity = (this.meta["cpu-capacity"] as? Double ?: Double.MAX_VALUE).coerceAtMost(originalCpu.frequency)
        val processingNode = originalCpu.node.copy(coreCount = cpuCount)
        val processingUnits = (0 until cpuCount).map { originalCpu.copy(id = it, node = processingNode, frequency = cpuCapacity) }
        val memoryUnits = listOf(MemoryUnit("Generic", "Generic", 3200.0, memorySize))

        return MachineModel(processingUnits, memoryUnits).optimize()
    }

    /**
     * Optimize the [MachineModel] for simulation.
     */
    private fun MachineModel.optimize(): MachineModel {
        if (!optimize) {
            return this
        }

        val originalCpu = cpus[0]
        val freq = cpus.sumOf { it.frequency }
        val processingNode = originalCpu.node.copy(coreCount = 1)
        val processingUnits = listOf(originalCpu.copy(frequency = freq, node = processingNode))

        val memorySize = memory.sumOf { it.size }
        val memoryUnits = listOf(MemoryUnit("Generic", "Generic", 3200.0, memorySize))

        return MachineModel(processingUnits, memoryUnits)
    }

    private val STATE_KEY = AttributeKey.stringKey("state")

    private val terminatedState = Attributes.of(STATE_KEY, "terminated")
    private val runningState = Attributes.of(STATE_KEY, "running")
    private val errorState = Attributes.of(STATE_KEY, "error")
    private val invalidState = Attributes.of(STATE_KEY, "invalid")

    /**
     * Helper function to collect the guest counts on this host.
     */
    private fun collectGuests(result: ObservableLongMeasurement) {
        var terminated = 0L
        var running = 0L
        var error = 0L
        var invalid = 0L

        val guests = _guests.listIterator()
        for (guest in guests) {
            when (guest.state) {
                ServerState.TERMINATED -> terminated++
                ServerState.RUNNING -> running++
                ServerState.ERROR -> error++
                ServerState.DELETED -> {
                    // Remove guests that have been deleted
                    this.guests.remove(guest.server)
                    guests.remove()
                }
                else -> invalid++
            }
        }

        result.record(terminated, terminatedState)
        result.record(running, runningState)
        result.record(error, errorState)
        result.record(invalid, invalidState)
    }

    private val _cpuLimit = machine.model.cpus.sumOf { it.frequency }

    /**
     * Helper function to collect the CPU limits of a machine.
     */
    private fun collectCpuLimit(result: ObservableDoubleMeasurement) {
        result.record(_cpuLimit)

        val guests = _guests
        for (i in guests.indices) {
            guests[i].collectCpuLimit(result)
        }
    }

    private val _activeState = Attributes.of(STATE_KEY, "active")
    private val _stealState = Attributes.of(STATE_KEY, "steal")
    private val _lostState = Attributes.of(STATE_KEY, "lost")
    private val _idleState = Attributes.of(STATE_KEY, "idle")

    /**
     * Helper function to track the CPU time of a machine.
     */
    private fun collectCpuTime(result: ObservableLongMeasurement) {
        val counters = hypervisor.counters
        counters.flush()

        result.record(counters.cpuActiveTime / 1000L, _activeState)
        result.record(counters.cpuIdleTime / 1000L, _idleState)
        result.record(counters.cpuStealTime / 1000L, _stealState)
        result.record(counters.cpuLostTime / 1000L, _lostState)

        val guests = _guests
        for (i in guests.indices) {
            guests[i].collectCpuTime(result)
        }
    }

    private var _lastReport = clock.millis()

    /**
     * Helper function to track the uptime of a machine.
     */
    private fun updateUptime() {
        val now = clock.millis()
        val duration = now - _lastReport
        _lastReport = now

        if (_state == HostState.UP) {
            _uptime += duration
        } else if (_state == HostState.DOWN && scope.isActive) {
            // Only increment downtime if the machine is in a failure state
            _downtime += duration
        }

        val guests = _guests
        for (i in guests.indices) {
            guests[i].updateUptime(duration)
        }
    }

    private var _uptime = 0L
    private var _downtime = 0L
    private val _upState = Attributes.of(STATE_KEY, "up")
    private val _downState = Attributes.of(STATE_KEY, "down")

    /**
     * Helper function to track the uptime of a machine.
     */
    private fun collectUptime(result: ObservableLongMeasurement) {
        updateUptime()

        result.record(_uptime, _upState)
        result.record(_downtime, _downState)

        val guests = _guests
        for (i in guests.indices) {
            guests[i].collectUptime(result)
        }
    }

    private var _bootTime = Long.MIN_VALUE

    /**
     * Helper function to track the boot time of a machine.
     */
    private fun collectBootTime(result: ObservableLongMeasurement) {
        if (_bootTime != Long.MIN_VALUE) {
            result.record(_bootTime)
        }

        val guests = _guests
        for (i in guests.indices) {
            guests[i].collectBootTime(result)
        }
    }

    override fun onStart(ctx: SimMachineContext) {
        hypervisor.onStart(ctx)
    }

    override fun onStop(ctx: SimMachineContext) {
        hypervisor.onStop(ctx)
    }
}




/**
 * A virtual machine instance that is managed by a [SimHost].
 */
public class Guest(
    context: CoroutineContext,
    private val clock: Clock,
    public val host: K8sNode,
    private val hypervisor: SimHypervisor,
    private val mapper: SimWorkloadMapper,
    private val listener: GuestListener,
    public val server: Server,
    public val machine: SimVirtualMachine
) {
    /**
     * The [CoroutineScope] of the guest.
     */
    private val scope: CoroutineScope = CoroutineScope(context + Job())

    /**
     * The logger instance of this guest.
     */
    private val logger = KotlinLogging.logger {}

    /**
     * The state of the [Guest].
     *
     * [ServerState.PROVISIONING] is an invalid value for a guest, since it applies before the host is selected for
     * a server.
     */
    public var state: ServerState = ServerState.TERMINATED

    /**
     * The attributes of the guest.
     */
    public val attributes: Attributes = GuestAttributes(this)

    /**
     * Start the guest.
     */
    public suspend fun start() {
        when (state) {
            ServerState.TERMINATED, ServerState.ERROR -> {
                logger.info { "User requested to start server ${server.uid}" }
                doStart()
            }
            ServerState.RUNNING -> return
            ServerState.DELETED -> {
                logger.warn { "User tried to start deleted server" }
                throw IllegalArgumentException("Server is deleted")
            }
            else -> assert(false) { "Invalid state transition" }
        }
    }

    /**
     * Stop the guest.
     */
    public suspend fun stop() {
        when (state) {
            ServerState.RUNNING -> doStop(ServerState.TERMINATED)
            ServerState.ERROR -> doRecover()
            ServerState.TERMINATED, ServerState.DELETED -> return
            else -> assert(false) { "Invalid state transition" }
        }
    }

    /**
     * Delete the guest.
     *
     * This operation will stop the guest if it is running on the host and remove all resources associated with the
     * guest.
     */
    public suspend fun delete() {
        stop()

        state = ServerState.DELETED
        hypervisor.removeMachine(machine)
        scope.cancel()
    }

    /**
     * Fail the guest if it is active.
     *
     * This operation forcibly stops the guest and puts the server into an error state.
     */
    public suspend fun fail() {
        if (state != ServerState.RUNNING) {
            return
        }

        doStop(ServerState.ERROR)
    }

    /**
     * Recover the guest if it is in an error state.
     */
    public suspend fun recover() {
        if (state != ServerState.ERROR) {
            return
        }

        doStart()
    }

    /**
     * The [Job] representing the current active virtual machine instance or `null` if no virtual machine is active.
     */
    private var job: Job? = null

    /**
     * Launch the guest on the simulated
     */
    private suspend fun doStart() {
        assert(job == null) { "Concurrent job running" }
        val workload = mapper.createWorkload(server)

        val job = scope.launch { runMachine(workload) }
        this.job = job

        state = ServerState.RUNNING
        onStart()

        job.invokeOnCompletion { cause ->
            this.job = null
            onStop(if (cause != null && cause !is CancellationException) ServerState.ERROR else ServerState.TERMINATED)
        }
    }

    /**
     * Attempt to stop the server and put it into [target] state.
     */
    private suspend fun doStop(target: ServerState) {
        assert(job != null) { "Invalid job state" }
        val job = job ?: return
        job.cancel()
        job.join()

        state = target
    }

    /**
     * Attempt to recover from an error state.
     */
    private fun doRecover() {
        state = ServerState.TERMINATED
    }

    /**
     * Converge the process that models the virtual machine lifecycle as a coroutine.
     */
    private suspend fun runMachine(workload: SimWorkload) {
        delay(1) // TODO Introduce model for boot time
        machine.runWorkload(workload, mapOf("driver" to host, "server" to server))
    }

    /**
     * This method is invoked when the guest was started on the host and has booted into a running state.
     */
    private fun onStart() {
        _bootTime = clock.millis()
        state = ServerState.RUNNING
        listener.onStart(this)
    }

    /**
     * This method is invoked when the guest stopped.
     */
    private fun onStop(target: ServerState) {
        state = target
        listener.onStop(this)
    }

    private val STATE_KEY = AttributeKey.stringKey("state")

    private var _uptime = 0L
    private var _downtime = 0L
    private val _upState = attributes.toBuilder()
        .put(STATE_KEY, "up")
        .build()
    private val _downState = attributes.toBuilder()
        .put(STATE_KEY, "down")
        .build()

    /**
     * Helper function to track the uptime and downtime of the guest.
     */
    public fun updateUptime(duration: Long) {
        if (state == ServerState.RUNNING) {
            _uptime += duration
        } else if (state == ServerState.ERROR) {
            _downtime += duration
        }
    }

    /**
     * Helper function to track the uptime of the guest.
     */
    public fun collectUptime(result: ObservableLongMeasurement) {
        result.record(_uptime, _upState)
        result.record(_downtime, _downState)
    }

    private var _bootTime = Long.MIN_VALUE

    /**
     * Helper function to track the boot time of the guest.
     */
    public fun collectBootTime(result: ObservableLongMeasurement) {
        if (_bootTime != Long.MIN_VALUE) {
            result.record(_bootTime, attributes)
        }
    }

    private val _activeState = attributes.toBuilder()
        .put(STATE_KEY, "active")
        .build()
    private val _stealState = attributes.toBuilder()
        .put(STATE_KEY, "steal")
        .build()
    private val _lostState = attributes.toBuilder()
        .put(STATE_KEY, "lost")
        .build()
    private val _idleState = attributes.toBuilder()
        .put(STATE_KEY, "idle")
        .build()

    /**
     * Helper function to track the CPU time of a machine.
     */
    public fun collectCpuTime(result: ObservableLongMeasurement) {
        val counters = machine.counters
        counters.flush()

        result.record(counters.cpuActiveTime / 1000, _activeState)
        result.record(counters.cpuIdleTime / 1000, _idleState)
        result.record(counters.cpuStealTime / 1000, _stealState)
        result.record(counters.cpuLostTime / 1000, _lostState)
    }

    private val _cpuLimit = machine.model.cpus.sumOf { it.frequency }

    /**
     * Helper function to collect the CPU limits of a machine.
     */
    public fun collectCpuLimit(result: ObservableDoubleMeasurement) {
        result.record(_cpuLimit, attributes)
    }

    /**
     * An optimized [Attributes] implementation.
     */
    private class GuestAttributes(private val uid: String, private val attributes: Attributes) : Attributes by attributes {
        /**
         * Construct a [GuestAttributes] instance from a [Guest].
         */
        constructor(guest: Guest) : this(
            guest.server.uid.toString(),
            Attributes.builder()
                .put(ResourceAttributes.HOST_NAME, guest.server.name)
                .put(ResourceAttributes.HOST_ID, guest.server.uid.toString())
                .put(ResourceAttributes.HOST_TYPE, guest.server.flavor.name)
                .put(AttributeKey.longKey("host.num_cpus"), guest.server.flavor.cpuCount.toLong())
                .put(AttributeKey.longKey("host.mem_capacity"), guest.server.flavor.memorySize)
                .put(AttributeKey.stringArrayKey("host.labels"), guest.server.labels.map { (k, v) -> "$k:$v" })
                .put(ResourceAttributes.HOST_ARCH, ResourceAttributes.HostArchValues.AMD64)
                .put(ResourceAttributes.HOST_IMAGE_NAME, guest.server.image.name)
                .put(ResourceAttributes.HOST_IMAGE_ID, guest.server.image.uid.toString())
                .build()
        )

        override fun <T : Any?> get(key: AttributeKey<T>): T? {
            // Optimize access to the HOST_ID key which is accessed quite often
            if (key == ResourceAttributes.HOST_ID) {
                @Suppress("UNCHECKED_CAST")
                return uid as T?
            }
            return attributes.get(key)
        }

        override fun toBuilder(): AttributesBuilder {
            val delegate = attributes.toBuilder()
            return object : AttributesBuilder {

                override fun putAll(attributes: Attributes): AttributesBuilder {
                    delegate.putAll(attributes)
                    return this
                }

                override fun <T : Any?> put(key: AttributeKey<Long>, value: Int): AttributesBuilder {
                    delegate.put<T>(key, value)
                    return this
                }

                override fun <T : Any?> put(key: AttributeKey<T>, value: T): AttributesBuilder {
                    delegate.put(key, value)
                    return this
                }

                override fun build(): Attributes = GuestAttributes(uid, delegate.build())
            }
        }

        override fun equals(other: Any?): Boolean = attributes == other

        // Cache hash code
        private val _hash = attributes.hashCode()

        override fun hashCode(): Int = _hash
    }
}


/**
 * Helper interface to listen for [Guest] events.
 */
interface GuestListener {
    /**
     * This method is invoked when the guest machine is running.
     */
    fun onStart(guest: Guest)

    /**
     * This method is invoked when the guest machine is stopped.
     */
    fun onStop(guest: Guest)
}
