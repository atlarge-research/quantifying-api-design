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
import org.opendc.compute.service.driver.K8sNode
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.simulator.compute.workload.SimRuntimeWorkload
import java.time.Clock
import java.time.Duration
import java.util.*
import kotlin.coroutines.CoroutineContext
import kotlin.math.max

/**
 * Internal implementation of the OpenDC Compute service.
 *
 * @param context The [CoroutineContext] to use in the service.
 * @param clock The clock instance to use.
 * @param meterProvider The [MeterProvider] for creating a [Meter] for the service.
 * @param podScheduler The scheduler implementation to use.
 * @param schedulingQuantum The interval between scheduling cycles.
 */
class K8sComputeService(
    private val context: CoroutineContext,
    private val clock: Clock,
    meterProvider: MeterProvider,
    private val nodeScheduler: ComputeScheduler,
    private val podScheduler: ComputeScheduler,
    schedulingQuantum: Duration,
    private val oversubscriptionApi: Boolean = false,
    private val migration: Boolean = false,
) : ComputeService, HostListener {
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

        podScheduler.addHost(hv)
        nodeScheduler.addHost(hv)
        host.addListener(this)
    }

    override fun removeHost(host: Host) {
        val view = hostToView.remove(host)
        if (view != null) {
            availableHosts.remove(view)
            podScheduler.removeHost(view)
            nodeScheduler.removeHost(view)
            host.removeListener(this)
        }
    }

    override fun close() {
        scope.cancel()
    }

    override fun schedule(server: InternalServer): SchedulingRequest {
        logger.debug { "Enqueueing server ${server.uid} to be assigned to host." }
        val now = clock.millis()
        val request = SchedulingRequest(server, now)

        server.lastProvisioningTimestamp = now
        queue.add(request)
        _servers.add(1, _serversPendingAttr)
        requestSchedulingCycle()
        return request
    }

    override fun delete(flavor: Flavor) {
        flavors.remove(flavor.uid)
    }

    override fun delete(image: Image) {
        images.remove(image.uid)
    }

    override fun delete(server: Server) {
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
            val hv = podScheduler.select(request.server)
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
                val migrated = tryToMigrate(hv, oversubscription, server)
                logger.info { "Migrated $migrated CPUs." }
            }

            assignPod(hv, server)
            logger.info { "Assigned pod $server to host ${hv.host}." }
        }
        queue = newQueue
    }


    private fun assignPod(hv: HostView, pod: InternalServer, node: K8sNode? = null) {
        val host = hv.host
        // Speculatively update the hypervisor view information to prevent other images in the queue from
        // deciding on stale values.
        hv.instanceCount++
        hv.provisionedCores += pod.flavor.cpuCount
        hv.availableMemory -= pod.flavor.memorySize // XXX Temporary hack
        addK8sPod(pod, hv, node)

        runBlocking {
            try {
                pod.host = host
                host.spawn(pod)
                activeServers[pod] = host

                _servers.add(1, _serversActiveAttr)
                _schedulingAttempts.add(1, _schedulingAttemptsSuccessAttr)
                logger.info { "Assigned server $pod to host ${hv.host}." }
            } catch (e: Throwable) {
                logger.error(e) { "Failed to deploy VM" }

                hv.instanceCount--
                hv.provisionedCores -= pod.flavor.cpuCount
                hv.availableMemory += pod.flavor.memorySize
                removeK8sPod(pod, hv, node)

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
        var used = if (newServer != null) getPodActiveCpuCount(newServer) else 0

        for (cluster in hv.host.k8sNodes.values) {
            for (node in cluster) {
                for (pod in node.pods) {
                    used += getPodActiveCpuCount(pod)
                }
            }
        }

        return used - total
    }

    fun getPodActiveCpuCount(server: InternalServer): Int {
        return (server.flavor.cpuCount * getUtilization(server)).toInt()
    }

    fun getUtilization(server: InternalServer): Double {
        val workload: SimRuntimeWorkload = server.meta["workload"] as SimRuntimeWorkload
        return workload.utilization
    }

    fun tryToMigrate(hv: HostView, cpuCount: Int, newPod: InternalServer): Int {
        var migrated = 0

        if (oversubscriptionApi) {
            migrated += tryToMigrateK8sPods(hv, cpuCount - migrated, newPod)
        }

        if (migration && migrated < cpuCount) {
            migrated += tryToMigrateNodes(hv, cpuCount - migrated, newPod)
        }
        return migrated
    }

    fun tryToMigrateK8sPods(hv: HostView, cpuCount: Int, newPod: InternalServer): Int {
        val cluster = newPod.meta["cluster"]!! as String
        var migrated = 0

        // get nodes sorted by cpu usage and check if pods from there can be migrated
        for (node in hv.host.k8sNodes[cluster]!!.sortedBy { cpuCount - getNodeActiveCpuCount(it) }) {
            val pods = node.pods.toList()
            for (pod in pods) {
                migrated += tryToMigrateK8sPod(hv, pod, node, cluster)
                if (cpuCount <= migrated) {
                    return migrated
                }
            }
        }

        return migrated
    }

    fun tryToMigrateK8sPod(from: HostView, pod: InternalServer, node: K8sNode, cluster: String): Int {
        val candidateHosts = hosts.filter {
            it != from.host &&
            K8sFilter().test(hostToView[it]!!, pod) && K8sVCpuFilter().test(hostToView[it]!!, pod) &&
            calculateOversubscription(hostToView[it]!!, pod) <= 0
        }.sortedBy { calculateOversubscription(hostToView[it]!!, pod) }

        if (0 < candidateHosts.size) {
            val to = hostToView[candidateHosts[0]]!!
            migratePod(from, to, pod)
            return getPodActiveCpuCount(pod)
        } else {
            return 0
        }
    }

    fun tryToMigrateNodes(hv: HostView, cpuCount: Int, newPod: InternalServer): Int {
        var migrated = 0
        // get k8s VMs and for every vm sort by size (descending)
        var nodes = hv.host.k8sNodes.values.flatten()
        nodes = nodes.sortedBy { cpuCount - getNodeActiveCpuCount(it) }
        for (node in nodes) {
            // check if migrating this node the pod cannot be assigned in the selected host
            if (!canBeAssignedWithout(newPod, hv, node)) {
                continue
            }
            // try to migrate k8s node
            migrated += tryToMigrateK8sNode(hv, node)
            if (cpuCount <= migrated) {
                break
            }
        }
        return migrated
    }

    fun getNodeActiveCpuCount(node: K8sNode): Int {
        var cpuCount = 0
        for (pod in node.pods) {
            cpuCount += getPodActiveCpuCount(pod)
        }
        return cpuCount
    }

    fun tryToMigrateK8sNode(from: HostView, node: K8sNode): Int {
        val nodeServer = nodeToServer(node)
        val to = nodeScheduler.select(nodeServer)

        if (to != null) {
            to.host.addK8sNode(node)
            val pods = node.pods.toList()  // in order to avoid concurrent modifications
            for (pod in pods) {
                migratePod(from, to, pod, node)
            }
            from.host.removeK8sNode(node)
            return getNodeActiveCpuCount(node)
        } else {
            return 0
        }
    }

    fun nodeToServer(node: K8sNode): Server {
        var cpuCount = 0
        var memorySize = 0L
        var cpuCapacity = 0.0

        for (pod in node.pods) {
            cpuCount += pod.flavor.cpuCount
            memorySize += pod.flavor.memorySize
            cpuCapacity += pod.flavor.meta["cpu-capacity"] as Double
        }

        cpuCapacity /= node.pods.size  // cpuCapacity is the average of all pods

        val flavor = InternalFlavor(
            this@K8sComputeService,
            node.uid,
            node.name,
            cpuCount,
            memorySize,
            labels = emptyMap(),
            meta = mapOf("cpu-capacity" to cpuCapacity, "cluster" to node.cluster)
        )

        val image = images[node.image.uid]!!

        return InternalServer(
            this@K8sComputeService,
            node.uid,
            node.name,
            flavor,
            image,
            flavor.labels.toMutableMap(),
            flavor.meta.toMutableMap()
        )
    }

    fun migratePod(from: HostView, to: HostView, pod: InternalServer, node: K8sNode? = null) {
        // extract remaining workload
        val workload = (pod.meta["workload"] as SimRuntimeWorkload)
        val sources = workload.getSources()
        val amount = workload.getRemainingAmountMean()
        val utilization = workload.utilization

        // delete from current host
        pod.host = to.host  // necessary for OnStateChanged call, otherwise removes all the metadata of the server
        runBlocking {
            from.host.delete(pod)
        }
        removeK8sPod(pod, from, null)

        // add to new host
        // no need to set duration in workload, remaining sources already contain that information
        // if sources size is 0, it means it has not started yet
        pod.meta["workload"] = if (sources.size > 0) SimRuntimeWorkload(0L, utilization = utilization, sources = sources, clock = workload.clock) else workload
        assignPod(to, pod, node)
        logger.info { "Migrated pod $pod to host ${to.host}." }
    }

    fun addK8sPod(pod: InternalServer, hv: HostView, node: K8sNode?) {
        var node = node
        val cluster = pod.meta["cluster"]!!

        if (node == null) {
            val nodes = hv.host.k8sNodes[cluster]!!.sortedBy { -it.availableCpuCount }  // TODO: decide how to choose node, must in the same as K8sPodScheduler
            for (candidate in nodes) {
                if (pod.flavor.cpuCount <= candidate.availableCpuCount) {  // TODO: check memory?
                    node = candidate
                    break
                }
            }
        }

        if (node == null) {
            throw Exception("failed to add K8s pod to node")
        }
        assert(hv.host.k8sNodes[node.cluster]!!.contains(node)) {
            "K8s node not in host"
        }
        node.availableCpuCount -= pod.flavor.cpuCount
        node.availableMemory -= pod.flavor.memorySize
        node.pods.add(pod)
    }

    fun removeK8sPod(pod: InternalServer, hv: HostView, node: K8sNode?) {
        var node = node
        val cluster = pod.meta["cluster"]!!

        if (node == null) {
            for (candidate in hv.host.k8sNodes[cluster]!!) {
                if (candidate.pods.contains(pod)) {
                    node = candidate
                    break
                }
            }
        }

        if (node == null) {
            throw Exception("failed to remove K8s pod to node")
        }

        node.availableCpuCount += pod.flavor.cpuCount
        node.availableMemory += pod.flavor.memorySize
        node.pods.remove(pod)
    }

    fun canBeAssignedWithout(pod: InternalServer, hv: HostView, remove: K8sNode): Boolean {
        for (node in hv.host.k8sNodes.getOrDefault(pod.meta["cluster"]!!, mutableListOf())) {
            if (node == remove) {
                continue
            }
            if (pod.flavor.cpuCount <= node.availableCpuCount) {  // TODO: check memory?
                return true
            }
        }

        return false
    }

    public suspend fun scheduleK8sNodes(nodes: List<K8sNode>, oversubscription: Float) {
        var remainingCpuCount: MutableMap<Host, Int> = mutableMapOf()

        for (host in hosts) {
            remainingCpuCount[host] = (host.model.cpuCount * oversubscription).toInt()
        }

        val hostList = hosts.toMutableList()
        val random = Random(0)

        for (node in nodes) {
            hostList.shuffle(random)
            var assigned = false
            for (host in hosts) {
                val remaingCpuCount = remainingCpuCount[host]!!
                if (node.cpuCount <= remaingCpuCount) {
                    remainingCpuCount[host] = remaingCpuCount - node.cpuCount
                    host.addK8sNode(node)
                    assigned = true
                    break
                }
            }
            if (!assigned) {
                throw K8sException("unable to assign cluster")
            }
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
                removeK8sPod(server, hv, null)
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
