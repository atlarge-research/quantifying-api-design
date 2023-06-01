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
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement
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
import org.opendc.compute.service.scheduler.filters.ComputeFilter
import org.opendc.compute.service.scheduler.filters.VCpuFilter
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
    private val oversubscription: Double = 1.0,
    private val migration: Boolean = false,
) : ComputeService, HostListener {
    private val migrations = mutableListOf<Migration>()
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

    private val clusters = mutableMapOf<String, Int>()
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
    private val _schedulingAttemptsRetryAttr = Attributes.of(AttributeKey.stringKey("result"), "retry")
    private val _schedulingAttemptsFailureAttr = Attributes.of(AttributeKey.stringKey("result"), "failure")
    private val _schedulingAttemptsErrorAttr = Attributes.of(AttributeKey.stringKey("result"), "error")

    private val _migrationAttempts = meter.counterBuilder("migration.attempts")
        .setDescription("Number of migration attempts")
        .setUnit("1")
        .build()
    private val _migrationAttemptsSuccessAttr = Attributes.of(AttributeKey.stringKey("result"), "success")
    private val _migrationAttemptsFailureAttr = Attributes.of(AttributeKey.stringKey("result"), "failure")

    private val _migrations = meter.gaugeBuilder("scheduler.migrations")
        .setDescription("Migrations")
        .setUnit("1")
        .buildWithCallback(::collectMigrations)

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

    override fun schedule(pod: InternalServer): SchedulingRequest {
        logger.debug { "[${clock.instant()}] Enqueueing pod $pod to be assigned to host." }
        val now = clock.millis()
        val request = SchedulingRequest(pod, now)

        pod.lastProvisioningTimestamp = now
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
                logger.warn { "Request for pod ${request.server} is canceled" }
                _servers.add(-1, _serversPendingAttr)
                continue
            }

            val pod = request.server
            val hv = podScheduler.select(request.server)
            if (hv == null || !hv.host.canFit(pod)) {
                logger.trace { "Pod $pod selected for scheduling but no capacity available for it at the moment" }

                if (pod.flavor.memorySize > maxMemory || pod.flavor.cpuCount > maxCores) {
                    // Remove the incoming image
                    _servers.add(-1, _serversPendingAttr)
                    _schedulingAttempts.add(1, _schedulingAttemptsFailureAttr)

                    logger.warn { "Failed to spawn $pod: does not fit [${clock.instant()}]" }

                    pod.state = ServerState.TERMINATED
                    continue
                } else {
                    _schedulingAttempts.add(1, _schedulingAttemptsRetryAttr)
                    newQueue.add(request)
                    continue
                }
            }

            // Remove request from pendings
            _servers.add(-1, _serversPendingAttr)
            _schedulingLatency.record(now - request.submitTime, pod.attributes)

            val oversubscription = calculateOversubscription(hv, pod)
            if (oversubscription > 0) {
                val migrated = tryToMigrate(hv, oversubscription, pod)
                if (oversubscription <= migrated) {
                    _migrationAttempts.add(1, _migrationAttemptsSuccessAttr)
                    logger.info { "Oversubscription avoided in $hv - migrated $migrated CPUs out of $oversubscription." }
                } else {
                    _migrationAttempts.add(1, _migrationAttemptsFailureAttr)
                    logger.info { "Oversubscription not avoided $hv - migrated $migrated CPUs out of $oversubscription." }
                }
            }

            assignPod(hv, pod)
            logger.info { "[${clock.instant()}] Assigned pod $pod to host $hv." }
        }
        queue = newQueue
    }

    private fun assignPod(hv: HostView, pod: InternalServer, node: K8sNode? = null) {
        val host = hv.host
        // Speculatively update the hypervisor view information to prevent other images in the queue from
        // deciding on stale values.
        addK8sPod(pod, hv, node)

        runBlocking {
            try {
                pod.host = host
                host.spawn(pod)
                activeServers[pod] = host

                _servers.add(1, _serversActiveAttr)
                _schedulingAttempts.add(1, _schedulingAttemptsSuccessAttr)
            } catch (e: Throwable) {
                logger.error(e) { "Failed to deploy Pod $pod" }

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

    fun calculateOversubscription(hv: HostView, newPod: InternalServer? = null): Double {
        val total = hv.host.model.cpuCount
        val totalCap = hv.host.model.cpuCapacity

        var used = if (newPod == null) 0 else newPod.flavor.cpuCount
        var usedCap = if (newPod == null) 0.0 else newPod.meta["cpu-capacity"] as Double

        for (node in hv.host.k8sNodes.values.flatten()){
            for (pod in node.pods){
                used += pod.flavor.cpuCount
                usedCap += pod.meta["cpu-capacity"] as Double
            }
        }
        return usedCap - totalCap
    }

    fun tryToMigrate(hv: HostView, cpuCap: Double, newPod: InternalServer): Double {
        var migrated : Double = 0.0

        if (migration && oversubscriptionApi) {
            migrated += tryToMigratePods(hv, cpuCap, newPod)
        }

        if (migration && migrated < cpuCap) {
            migrated += tryToMigrateNodes(hv, cpuCap - migrated, newPod)
        }

        return migrated
    }

    fun tryToMigratePods(hv: HostView, cpuCapacity: Double, newPod: InternalServer): Double {
        var migrated = 0.0

        // get nodes sorted by cpu usage and check if pods from there can be migrated
        val allPods = mutableListOf<InternalServer>()
        for (node in hv.host.k8sNodes.values.flatten()){
            allPods.addAll(node.pods)
        }

        val migration = Migration(mutableListOf(), cpuCapacity)
        val cpuCounts = allPods.map { it.flavor.cpuCount }.distinct()
        for (cpuCount in cpuCounts.sorted()){
            for (pod in allPods.filter { it.flavor.cpuCount == cpuCount }.sortedBy { it.flavor.memorySize }){
                val migratedPodCapacity = tryToMigratePod(hv, pod)
                if (migratedPodCapacity>0){
                    migration.pods.add(pod)
                }
                migrated += migratedPodCapacity
                if (cpuCapacity <= migrated) {
                    break
                }
            }
        }


        migrations.add(migration)
        return migrated
    }

    fun tryToMigratePod(from: HostView, pod: InternalServer): Double {
        val candidateHosts = hosts.
        map { hostToView[it]!! }.
        filter {
            it != from &&
            K8sFilter().test(it, pod) && K8sVCpuFilter().test(it, pod) &&
            K8sSameNodeFilter(from).test(it, pod) && calculateOversubscription(it,pod) <= 0
        }.
        sortedBy { it.host.model.cpuCount - it.provisionedCores }

        if (candidateHosts.isNotEmpty()) {
            val to = candidateHosts.first()
            migratePod(from, to, pod)
            logger.info { "[${clock.instant()}]  Pod migration - pod $pod to host $to." }
            return pod.meta["cpu-capacity"] as Double
        } else {
            return 0.0
        }
    }

    fun tryToMigrateNodes(hv: HostView, cpuCapacity: Double, newPod: InternalServer): Double {
        var migrated = 0.0
        // get k8s VMs and for every vm sort by size (descending)
        var nodes = hv.host.k8sNodes.values.flatten()
        nodes = nodes.sortedBy { it.cpuCount }

        val migration = Migration(mutableListOf(), cpuCapacity)
        for (node in nodes) {
            // check if migrating this node the pod cannot be assigned in the selected host
            if (!canBeAssignedWithout(newPod, hv, node) || getActiveCpuCapacity(node)==0.0) {
                continue
            }
            // try to migrate k8s node
            val migratedNodeCapacity = tryToMigrateNode(hv, node)
            if (migratedNodeCapacity>0){
                migration.pods.addAll(node.pods)
            }
            migrated += migratedNodeCapacity
            if (cpuCapacity <= migrated) {
                break
            }
        }

        migrations.add(migration)
        return migrated
    }

    fun tryToMigrateNode(from: HostView, node: K8sNode): Double {
        val nodeServer = nodeToServer(node)
        val candidateHosts = hosts.
        map { hostToView[it]!! }.
        filter {
            it != from &&
            ComputeFilter().test(it, nodeServer)
            && VCpuFilter(oversubscription).test(it, nodeServer)
            && calculateOversubscription(it, nodeServer)<=0
        }.sortedBy { it.host.model.cpuCount - it.provisionedCores }

        if (candidateHosts.isNotEmpty()) {
            val capacity = getActiveCpuCapacity(node)
            val to = candidateHosts.first()
            addK8sNode(node, to)
            val pods = node.pods.toList()  // in order to avoid concurrent modifications
            for (pod in pods) {
                migratePod(from, to, pod, node)
                logger.info { "[${clock.instant()}] Node migration - pod $pod to host $to." }
            }
            removeK8sNode(node, from)
            logger.info { "Node migration - node $node to host $to." }
            return capacity
        } else {
            return 0.0
        }
    }

    fun getActiveCpuCapacity(node : K8sNode) : Double{
        var activeCpuCap = 0.0
        for (pod in node.pods){
            activeCpuCap += pod.meta["cpu-capacity"] as Double
        }
        return  activeCpuCap
    }

    fun nodeToServer(node: K8sNode): InternalServer {
        val flavor = InternalFlavor(
            this@K8sComputeService,
            node.uid,
            node.name,
            node.cpuCount,
            node.memory,
            labels = emptyMap(),
            meta = mapOf("cpu-capacity" to node.cpuCapacity, "cluster" to node.cluster)
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
        // delete from current host
        val newWorkload = generateWorkloadAfterMigration(pod)
        pod.host = null  // necessary for OnStateChanged call, otherwise removes all the metadata of the server
        runBlocking {
            from.host.delete(pod)
        }
        removeK8sPod(pod, from, null)

        // add to new host
        // no need to set duration in workload, remaining sources already contain that information
        // if sources size is 0, it means it has not started yet
        pod.meta["workload"] = newWorkload
        assignPod(to, pod, node)
    }


    fun generateWorkloadAfterMigration(pod: InternalServer) : SimRuntimeWorkload{
        val workload = (pod.meta["workload"] as SimRuntimeWorkload)
        val sources = workload.getSources()

        // add migration penalty 1min/4Gb
        val penalty = Duration.ofMinutes(pod.flavor.memorySize/4_000).toMillis()
        if (sources.isEmpty()){
            workload.duration += penalty
        } else {
            for (source in sources){
                source.remainingAmount += (workload.cpuCapacity/workload.cpuCount * workload.utilization) * penalty
            }
        }


        return if (sources.size > 0) SimRuntimeWorkload(
            workload.duration,
            utilization = workload.utilization,
            workload.cpuCount,
            workload.cpuCapacity,
            sources = sources,
            clock = workload.clock
        )
        else workload
    }
    fun addK8sPod(pod: InternalServer, hv: HostView, node: K8sNode?) {
        var node = node
        val cluster = pod.meta["cluster"]!!

        if (node == null) {
            val nodes =
                hv.host.k8sNodes[cluster]!!.sortedByDescending { it.availableCpuCount }  // TODO: decide how to choose node, must in the same as K8sPodScheduler
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
        clusters[node.cluster] = clusters[node.cluster]!! - pod.flavor.cpuCount
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
            throw Exception("failed to remove K8s pod $pod from node $node")
        }

        node.availableCpuCount += pod.flavor.cpuCount
        node.availableMemory += pod.flavor.memorySize
        node.pods.remove(pod)
        clusters[node.cluster] = clusters[node.cluster]!! + pod.flavor.cpuCount
    }

    fun addK8sNode(node: K8sNode, hv: HostView) {
        hv.host.addK8sNode(node)
        hv.instanceCount++
        hv.provisionedCores += node.cpuCount
        hv.availableMemory -= node.memory

        if (!clusters.contains(node.cluster)){
            clusters[node.cluster] = 0
        }

        clusters[node.cluster] = clusters[node.cluster]!! + node.cpuCount
    }

    fun removeK8sNode(node: K8sNode, hv: HostView) {
        hv.host.removeK8sNode(node)
        hv.instanceCount--
        hv.provisionedCores -= node.cpuCount
        hv.availableMemory += node.memory

        clusters[node.cluster] = clusters[node.cluster]!! - node.cpuCount
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
        val random = Random(0)

        for (node in nodes.shuffled(random).sortedByDescending { it.cpuCount }) {
            val hv = nodeScheduler.select(nodeToServer(node))
            if (hv != null) {
                addK8sNode(node, hv)
            } else {
                throw K8sException("unable to assign cluster to Node $node")
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
            logger.info { "[${clock.instant()}] Pod $server ${server.flavor} finished." }

            if (activeServers.remove(server) != null) {
                _servers.add(-1, _serversActiveAttr)
            }

            val hv = hostToView[host]
            if (hv != null) {
                removeK8sPod(server, hv, null)
            } else {
                logger.error { "Unknown host $host" }
            }

            // Try to reschedule if needed
            requestSchedulingCycle()
        }
    }

    fun updateTimes(hv :HostView){
        for (nodes in hv.host.k8sNodes.values.flatten()){
            for (pod in nodes.pods){
                (pod.meta["workload"] as SimRuntimeWorkload).forceSourceUpdate()
            }
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

    data class Migration(val pods: MutableList<InternalServer>, val oversubscription: Double)
    private fun collectMigrations(result: ObservableDoubleMeasurement) {
        for (migration in migrations) {
            if (migration.pods.size <= 0){
                continue
            }

            var podsNames = ""
            var improvement = 0.0
            var penalty = 0L

            for (pod in migration.pods){
                podsNames += "-${pod.name}"
                improvement += pod.meta["cpu-capacity"] as Double
                penalty += Duration.ofMinutes(pod.flavor.memorySize/4_000).toMillis()
            }

            result.record(improvement, Attributes.builder()
                .put(AttributeKey.longKey("migration.pods"), migration.pods.size.toLong())
                .put(AttributeKey.doubleKey("migration.oversubscription"), oversubscription)
                .put(AttributeKey.longKey("migration.penalty"), penalty)
                .build())
        }
        migrations.clear()
    }
}
