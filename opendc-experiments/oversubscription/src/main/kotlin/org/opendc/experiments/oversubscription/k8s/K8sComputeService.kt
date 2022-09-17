package org.opendc.experiments.oversubscription.k8s

import org.opendc.compute.service.internal.*

/*
 * Copyright (c) 2021 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import org.opendc.compute.service.internal.SchedulingRequest
import org.opendc.compute.service.internal.InternalServer
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.metrics.MeterProvider
import io.opentelemetry.api.metrics.ObservableLongMeasurement
import kotlinx.coroutines.*
import mu.KotlinLogging
import org.opendc.common.util.Pacer
import org.opendc.compute.api.*
import org.opendc.compute.service.ComputeService
import org.opendc.compute.service.driver.Host
import org.opendc.compute.service.driver.HostListener
import org.opendc.compute.service.driver.HostState
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.simulator.compute.workload.SimRuntimeWorkload
import java.time.Clock
import java.time.Duration
import java.util.*
import kotlin.coroutines.CoroutineContext
import kotlin.math.abs
import kotlin.math.max

/**
 * Internal implementation of the OpenDC Compute service.
 *
 * @param context The [CoroutineContext] to use in the service.
 * @param clock The clock instance to use.
 * @param meterProvider The [MeterProvider] for creating a [Meter] for the service.
 * @param scheduler The scheduler implementation to use.
 * @param schedulingQuantum The interval between scheduling cycles.
 */
internal class K8sComputeService(
    private val context: CoroutineContext,
    private val clock: Clock,
    meterProvider: MeterProvider,
    private val scheduler: ComputeScheduler,
    schedulingQuantum: Duration
) : ComputeService, HostListener {
    /**
     * A mapping from host to servers.
     */
    private val hostToServers = mutableMapOf<HostView, MutableList<InternalServer>>()

    /**
     * The [CoroutineScope] of the service bounded by the lifecycle of the service.
     */
    private val scope = CoroutineScope(context + Job())

    /**
     * The logger instance of this server.
     */
    private val logger = KotlinLogging.logger {}

    /**
     * The [Meter] to track metrics of the [ComputeService].
     */
    private val meter = meterProvider.get("org.opendc.compute.service")

    /**
     * The [Random] instance used to generate unique identifiers for the objects.
     */
    private val random = Random(0)

    /**
     * A mapping from host to host view.
     */
    private val hostToView = mutableMapOf<Host, HostView>()

    /**
     * The available hypervisors.
     */
    private val availableHosts: MutableSet<HostView> = mutableSetOf()

    /**
     * The servers that should be launched by the service.
     */
    private var queue: Deque<SchedulingRequest> = ArrayDeque()

    /**
     * The active servers in the system.
     */
    private val activeServers: MutableMap<Server, Host> = mutableMapOf()

    /**
     * The registered flavors for this compute service.
     */
    internal val flavors = mutableMapOf<UUID, InternalFlavor>()

    /**
     * The registered images for this compute service.
     */
    internal val images = mutableMapOf<UUID, InternalImage>()

    /**
     * The registered servers for this compute service.
     */
    private val servers = mutableMapOf<UUID, InternalServer>()

    private var maxCores = 0
    private var maxMemory = 0L

    /**
     * The number of scheduling attempts.
     */
    private val _schedulingAttempts = meter.counterBuilder("scheduler.attempts")
        .setDescription("Number of scheduling attempts")
        .setUnit("1")
        .build()
    private val _schedulingAttemptsSuccessAttr = Attributes.of(AttributeKey.stringKey("result"), "success")
    private val _schedulingAttemptsFailureAttr = Attributes.of(AttributeKey.stringKey("result"), "failure")
    private val _schedulingAttemptsErrorAttr = Attributes.of(AttributeKey.stringKey("result"), "error")

    /**
     * The response time of the service.
     */
    private val _schedulingLatency = meter.histogramBuilder("scheduler.latency")
        .setDescription("End to end latency for a server to be scheduled (in multiple attempts)")
        .ofLongs()
        .setUnit("ms")
        .build()

    /**
     * The number of servers that are pending.
     */
    private val _servers = meter.upDownCounterBuilder("scheduler.servers")
        .setDescription("Number of servers managed by the scheduler")
        .setUnit("1")
        .build()
    private val _serversPendingAttr = Attributes.of(AttributeKey.stringKey("state"), "pending")
    private val _serversActiveAttr = Attributes.of(AttributeKey.stringKey("state"), "active")

    /**
     * The [Pacer] to use for scheduling the scheduler cycles.
     */
    private val pacer = Pacer(scope.coroutineContext, clock, schedulingQuantum.toMillis(), ::doSchedule)

    override val hosts: Set<Host>
        get() = hostToView.keys

    override val hostCount: Int
        get() = hostToView.size

    init {
        val upState = Attributes.of(AttributeKey.stringKey("state"), "up")
        val downState = Attributes.of(AttributeKey.stringKey("state"), "down")

        meter.upDownCounterBuilder("scheduler.hosts")
            .setDescription("Number of hosts registered with the scheduler")
            .setUnit("1")
            .buildWithCallback { result ->
                val total = hostCount
                val available = availableHosts.size.toLong()

                result.record(available, upState)
                result.record(total - available, downState)
            }

        meter.gaugeBuilder("system.time.provision")
            .setDescription("The most recent timestamp where the server entered a provisioned state")
            .setUnit("1")
            .ofLongs()
            .buildWithCallback(::collectProvisionTime)
    }

    override fun newClient(): ComputeClient {
        check(scope.isActive) { "Service is already closed" }
        return object : ComputeClient {
            private var isClosed: Boolean = false

            override suspend fun queryFlavors(): List<Flavor> {
                check(!isClosed) { "Client is already closed" }

                return flavors.values.map { ClientFlavor(it) }
            }

            override suspend fun findFlavor(id: UUID): Flavor? {
                check(!isClosed) { "Client is already closed" }

                return flavors[id]?.let { ClientFlavor(it) }
            }

            override suspend fun newFlavor(
                name: String,
                cpuCount: Int,
                memorySize: Long,
                labels: Map<String, String>,
                meta: Map<String, Any>
            ): Flavor {
                check(!isClosed) { "Client is already closed" }

                val uid = UUID(clock.millis(), random.nextLong())
                val flavor = InternalFlavor(
                    this@K8sComputeService,
                    uid,
                    name,
                    cpuCount,
                    memorySize,
                    labels,
                    meta
                )

                flavors[uid] = flavor

                return ClientFlavor(flavor)
            }

            override suspend fun queryImages(): List<Image> {
                check(!isClosed) { "Client is already closed" }

                return images.values.map { ClientImage(it) }
            }

            override suspend fun findImage(id: UUID): Image? {
                check(!isClosed) { "Client is already closed" }

                return images[id]?.let { ClientImage(it) }
            }

            override suspend fun newImage(name: String, labels: Map<String, String>, meta: Map<String, Any>): Image {
                check(!isClosed) { "Client is already closed" }

                val uid = UUID(clock.millis(), random.nextLong())
                val image = InternalImage(this@K8sComputeService, uid, name, labels, meta)

                images[uid] = image

                return ClientImage(image)
            }

            override suspend fun newServer(
                name: String,
                image: Image,
                flavor: Flavor,
                labels: Map<String, String>,
                meta: Map<String, Any>,
                start: Boolean
            ): Server {
                check(!isClosed) { "Client is closed" }

                val uid = UUID(clock.millis(), random.nextLong())
                val server = InternalServer(
                    this@K8sComputeService,
                    uid,
                    name,
                    requireNotNull(flavors[flavor.uid]) { "Unknown flavor" },
                    requireNotNull(images[image.uid]) { "Unknown image" },
                    labels.toMutableMap(),
                    meta.toMutableMap()
                )

                servers[uid] = server

                if (start) {
                    server.start()
                }

                return ClientServer(server)
            }

            override suspend fun findServer(id: UUID): Server? {
                check(!isClosed) { "Client is already closed" }

                return servers[id]?.let { ClientServer(it) }
            }

            override suspend fun queryServers(): List<Server> {
                check(!isClosed) { "Client is already closed" }

                return servers.values.map { ClientServer(it) }
            }

            override fun close() {
                isClosed = true
            }

            override fun toString(): String = "ComputeClient"
        }
    }

    override fun addHost(host: Host) {
        // Check if host is already known
        if (host in hostToView) {
            return
        }

        val hv = HostView(host)
        maxCores = max(maxCores, host.model.cpuCount)
        maxMemory = max(maxMemory, host.model.memoryCapacity)
        hostToView[host] = hv

        if (host.state == HostState.UP) {
            availableHosts += hv
        }

        scheduler.addHost(hv)
        host.addListener(this)
    }

    override fun removeHost(host: Host) {
        val view = hostToView.remove(host)
        if (view != null) {
            availableHosts.remove(view)
            scheduler.removeHost(view)
            host.removeListener(this)
        }
    }

    override fun close() {
        scope.cancel()
    }

    override fun schedule(server: InternalServer): org.opendc.compute.service.internal.SchedulingRequest {
        logger.debug { "Enqueueing server ${server.uid} to be assigned to host." }
        val now = clock.millis()
        val request = SchedulingRequest(server, now)

        server.lastProvisioningTimestamp = now
        queue.add(request)
        _servers.add(1, _serversPendingAttr)
        requestSchedulingCycle()
        return request
    }

    internal fun delete(flavor: InternalFlavor) {
        flavors.remove(flavor.uid)
    }

    internal fun delete(image: InternalImage) {
        images.remove(image.uid)
    }

    internal fun delete(server: InternalServer) {
        servers.remove(server.uid)
    }

    /**
     * Indicate that a new scheduling cycle is needed due to a change to the service's state.
     */
    private fun requestSchedulingCycle() {
        // Bail out in case the queue is empty.
        if (queue.isEmpty()) {
            return
        }

        pacer.enqueue()
    }

    /**
     * Run a single scheduling iteration.
     */
    private fun doSchedule(now: Long) {
        val newQueue: Deque<SchedulingRequest> = ArrayDeque()
        while (queue.isNotEmpty()) {
            val request = queue.poll()

            if (request.isCancelled) {
                _servers.add(-1, _serversPendingAttr)
                continue
            }

            val server = request.server
            val hv = scheduler.select(request.server)
            if (hv == null || !hv.host.canFit(server)) {
                logger.trace { "Server $server selected for scheduling but no capacity available for it at the moment" }

                if (server.flavor.memorySize > maxMemory || server.flavor.cpuCount > maxCores) {
                    // Remove the incoming image
                    _servers.add(-1, _serversPendingAttr)
                    _schedulingAttempts.add(1, _schedulingAttemptsFailureAttr)

                    logger.warn { "Failed to spawn $server: does not fit [${clock.instant()}]" }

                    server.state = ServerState.TERMINATED
                    continue
                } else {
                    newQueue.add(request)
                    continue
                }
            }

            // Remove request from pendings
            _servers.add(-1, _serversPendingAttr)
            _schedulingLatency.record(now - request.submitTime, server.attributes)


            val oversubscription = calculateOversubscription(hv, server)
            if (oversubscription > 0) {
                tryToMigrate(hv, oversubscription)
            }

            logger.info { "Assigned server $server to host ${hv.host}." }

            assignServer(hv, server)
        }
        queue = newQueue
    }

    private fun assignServer(hv: HostView, server: InternalServer) {
        val host = hv.host
        // Speculatively update the hypervisor view information to prevent other images in the queue from
        // deciding on stale values.
        hv.instanceCount++
        hv.provisionedCores += server.flavor.cpuCount
        hv.availableMemory -= server.flavor.memorySize // XXX Temporary hack
        addHostMapping(server, hv)
        addK8sPartition(server, host)

        scope.launch {
            try {
                server.host = host
                host.spawn(server)
                activeServers[server] = host

                _servers.add(1, _serversActiveAttr)
                _schedulingAttempts.add(1, _schedulingAttemptsSuccessAttr)
            } catch (e: Throwable) {
                logger.error(e) { "Failed to deploy VM" }

                hv.instanceCount--
                hv.provisionedCores -= server.flavor.cpuCount
                hv.availableMemory += server.flavor.memorySize
                deleteHostMapping(server, hv)
                freeK8sPartition(server, host)

                _schedulingAttempts.add(1, _schedulingAttemptsErrorAttr)
            }
        }
    }

    override fun onStateChanged(host: Host, newState: HostState) {
        when (newState) {
            HostState.UP -> {
                logger.debug { "[${clock.instant()}] Host ${host.uid} state changed: $newState" }

                val hv = hostToView[host]
                if (hv != null) {
                    // Corner case for when the hypervisor already exists
                    availableHosts += hv
                }

                // Re-schedule on the new machine
                requestSchedulingCycle()
            }

            HostState.DOWN -> {
                logger.debug { "[${clock.instant()}] Host ${host.uid} state changed: $newState" }

                val hv = hostToView[host] ?: return
                availableHosts -= hv

                requestSchedulingCycle()
            }
        }
    }

    fun calculateOversubscription(hv: HostView, newServer: InternalServer? = null): Int {
        val total = hv.host.model.cpuCount
        var used = if (newServer != null) getRealCpuCount(newServer) else 0
        for (server in hostToServers.getOrDefault(hv, mutableListOf())) {
            used += getRealCpuCount(server)
        }
        return used - total
    }

    fun getRealCpuCount(server: InternalServer): Int {
        return (server.flavor.cpuCount * getUtilization(server)).toInt()
    }

    fun getUtilization(server: InternalServer): Double {
        val workload: SimRuntimeWorkload = server.flavor.meta["workload"] as SimRuntimeWorkload
        return workload.utilization
    }

    fun tryToMigrate(hv: HostView, cpuCount: Int) {
        var remaining = cpuCount
        val migrations = mutableListOf<Pair<InternalServer, Host>>()
        for (server in hostToServers.getOrDefault(hv, mutableListOf()).sortedBy { cpuCount - getRealCpuCount(it) }) {
            val cluster = server.flavor.meta.getOrDefault("cluster", "") as String
            val candidateHosts = hosts.filter {
                it.partitions.contains(cluster) &&
                    (it.partitions[cluster]!! - it.partitionsUsed[cluster]!!) < server.flavor.cpuCount &&
                    calculateOversubscription(it, server) <= 0
            }.sortedBy { -(it.partitions[cluster]!! - it.partitionsUsed[cluster]!!) }
            if (candidateHosts.size > 0) {
                migrations.add(Pair(server, candidateHosts[0]))
                remaining -= getRealCpuCount(server)
                if (remaining < 0) {
                    break
                }
            }
        }

        for (migration in migrations) {
            migration.first.host = migration.second
            val workload = migration.first.flavor.meta["workload"] as SimRuntimeWorkload
            migration.first.flavor.meta["workload"] = SimRuntimeWorkload(0, utilization = workload.utilization, sources = workload.getSources())

            migration.first.stop()
        }
        assignServer(hostToView[migration.second]!!, migration.first)
    }

    fun addHostMapping(server: InternalServer, hv: HostView) {
        val servers = hostToServers.getOrDefault(hv, mutableListOf())
        servers.add(server)
        hostToServers[hv] = servers
    }

    fun deleteHostMapping(server: InternalServer, hv: HostView) {
        hostToServers[hv]?.remove(server)
    }


    fun addK8sPartition(server: Server, host: Host) {
        val cluster: String = server.flavor.meta["cluster"] as String
        if (cluster == "") {
            return
        }
        val used = host.partitionsUsed[cluster]
        if (used != null) {
            host.partitionsUsed[cluster] = used + server.flavor.cpuCount
        }
    }

    fun freeK8sPartition(server: Server, host: Host) {
        val cluster: String = server.flavor.meta["cluster"] as String
        if (cluster == "") {
            return
        }
        val used = host.partitionsUsed[cluster]
        if (used != null) {
            host.partitionsUsed[cluster] = used - server.flavor.cpuCount
        }
    }

    override fun onStateChanged(host: Host, server: Server, newState: ServerState) {
        require(server is InternalServer) { "Invalid server type passed to service" }

        if (server.host != host) {
            // This can happen when a server is rescheduled and started on another machine, while being deleted from
            // the old machine.
            return
        }

        server.state = newState

        if (newState == ServerState.TERMINATED || newState == ServerState.DELETED) {
            logger.info { "[${clock.instant()}] Server ${server.uid} ${server.name} ${server.flavor} finished." }

            if (activeServers.remove(server) != null) {
                _servers.add(-1, _serversActiveAttr)
            }

            val hv = hostToView[host]
            if (hv != null) {
                hv.provisionedCores -= server.flavor.cpuCount
                hv.instanceCount--
                hv.availableMemory += server.flavor.memorySize
                deleteHostMapping(server, hv)
                freeK8sPartition(server, hv.host)
            } else {
                logger.error { "Unknown host $host" }
            }

            // Try to reschedule if needed
            requestSchedulingCycle()
        }
    }

    /**
     * Collect the timestamp when each server entered its provisioning state most recently.
     */
    private fun collectProvisionTime(result: ObservableLongMeasurement) {
        for ((_, server) in servers) {
            result.record(server.lastProvisioningTimestamp, server.attributes)
        }
    }
}
