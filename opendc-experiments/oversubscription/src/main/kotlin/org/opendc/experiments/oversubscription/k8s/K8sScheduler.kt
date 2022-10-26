package org.opendc.experiments.oversubscription.k8s

import org.opendc.compute.api.Server
import org.opendc.compute.service.internal.HostView
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.compute.service.scheduler.FilterScheduler
import org.opendc.compute.service.scheduler.ReplayScheduler
import org.opendc.compute.service.scheduler.filters.*
import org.opendc.compute.service.scheduler.weights.*
import java.util.Random

public fun createK8sNodeScheduler(name: String, seeder: Random, placements: Map<String, String> = emptyMap(), cpuAllocationRatio: Double = 16.0, ramAllocationRatio: Double = 1.5): ComputeScheduler {
    return when (name) {
        "mem" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), K8sSameVMFilter()),
            weighers = listOf(RamWeigher(multiplier = 1.0))
        )
        "mem-inv" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), K8sSameVMFilter()),
            weighers = listOf(RamWeigher(multiplier = -1.0))
        )
        "core-mem" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), K8sSameVMFilter()),
            weighers = listOf(CoreRamWeigher(multiplier = 1.0))
        )
        "core-mem-inv" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), K8sSameVMFilter()),
            weighers = listOf(CoreRamWeigher(multiplier = -1.0))
        )
        "active-servers" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), K8sSameVMFilter()),
            weighers = listOf(InstanceCountWeigher(multiplier = -1.0))
        )
        "active-servers-inv" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), K8sSameVMFilter()),
            weighers = listOf(InstanceCountWeigher(multiplier = 1.0))
        )
        "provisioned-cores" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), K8sSameVMFilter()),
            weighers = listOf(VCpuWeigher(cpuAllocationRatio, multiplier = 1.0))
        )
        "provisioned-cores-inv" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), K8sSameVMFilter()),
            weighers = listOf(VCpuWeigher(cpuAllocationRatio, multiplier = -1.0))
        )
        "random" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), K8sSameVMFilter()),
            weighers = emptyList(),
            subsetSize = Int.MAX_VALUE,
            random = Random(seeder.nextLong())
        )
        "replay" -> ReplayScheduler(placements)
        else -> throw IllegalArgumentException("Unknown policy $name")
    }
}


/**
 * Create a [ComputeScheduler] for the experiment.
 */
public fun createK8sPodScheduler(name: String, seeder: Random, placements: Map<String, String> = emptyMap(), cpuAllocationRatio: Double = 16.0, ramAllocationRatio: Double = 1.5): ComputeScheduler {
    return when (name) {
        "mem" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter()),
            weighers = listOf(RamWeigher(multiplier = 1.0))
        )
        "mem-inv" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter()),
            weighers = listOf(RamWeigher(multiplier = -1.0))
        )
        "core-mem" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter()),
            weighers = listOf(CoreRamWeigher(multiplier = 1.0))
        )
        "core-mem-inv" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter()),
            weighers = listOf(CoreRamWeigher(multiplier = -1.0))
        )
        "active-servers" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter()),
            weighers = listOf(InstanceCountWeigher(multiplier = -1.0))
        )
        "provisioned-cores" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter()), // TODO: RamFilter(ramAllocationRatio))
            weighers = listOf(K8sVCpuWeigherMax(cpuAllocationRatio, multiplier = 1.0))
        )
        "provisioned-cores-inv" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter()), // TODO: RamFilter(ramAllocationRatio))
            weighers = listOf(K8sVCpuWeigher(cpuAllocationRatio, multiplier = -1.0))
        )
        "random" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter()),
            weighers = emptyList(),
            subsetSize = Int.MAX_VALUE,
            random = Random(seeder.nextLong())
        )
        "replay" -> ReplayScheduler(placements)
        else -> throw IllegalArgumentException("Unknown policy $name")
    }
}

public class K8sFilter : HostFilter {
    override fun test(host: HostView, server: Server): Boolean {
        return host.host.k8sNodes.contains(server.meta["cluster"])
    }

    override fun toString(): String = "K8sFilter"
}

public class K8sSameVMFilter() : HostFilter {
    override fun test(host: HostView, server: Server): Boolean {
        if (!host.host.k8sNodes.contains(server.meta["cluster"])){
            return true
        }

        for (node in host.host.k8sNodes[server.meta["cluster"]]!!){
            if (node.name == server.name){
                return false
            }
        }
        return true
    }

    override fun toString(): String = "K8sFilter"
}

public class K8sSameNodeFilter(val node: HostView) : HostFilter {
    override fun test(host: HostView, server: Server): Boolean {
        return host.host.k8sNodes.contains(server.meta["cluster"]) && node != host
    }

    override fun toString(): String = "K8sFilter"
}



public class K8sVCpuFilter : HostFilter {
    override fun test(host: HostView, server: Server): Boolean {
        val requested = server.flavor.cpuCount

        val cluster = server.meta["cluster"]!!
        if (!host.host.k8sNodes.contains(cluster)){
            return false
        }

        val nodes = host.host.k8sNodes[cluster]!!

        for (node in nodes){
            if (requested <= node.availableCpuCount){
                return true
            }
        }

        return false
    }
}

public class K8sVCpuWeigher(private val allocationRatio: Double, override val multiplier: Double = 1.0) : HostWeigher {
    init {
        require(allocationRatio > 0.0) { "Allocation ratio must be greater than zero" }
    }

    override fun getWeight(host: HostView, server: Server): Double {
        val cluster = server.meta["cluster"]!!

        if (!host.host.k8sNodes.contains(cluster)){
            return 0.0
        }

        val nodes = host.host.k8sNodes[cluster]!!

        for (node in nodes.sortedBy { it.cpuCount }){
            if (node.availableCpuCount >= server.flavor.cpuCount){
                return node.availableCpuCount.toDouble()
            }
        }

        return Double.MAX_VALUE
    }

    override fun toString(): String = "VCpuWeigher"
}


public class K8sVCpuWeigherMax(private val allocationRatio: Double, override val multiplier: Double = 1.0) : HostWeigher {
    init {
        require(allocationRatio > 0.0) { "Allocation ratio must be greater than zero" }
    }

    override fun getWeight(host: HostView, server: Server): Double {
        val cluster = server.meta["cluster"]!!

        if (!host.host.k8sNodes.contains(cluster)){
            return 0.0
        }

        val nodes = host.host.k8sNodes[cluster]!!

        for (node in nodes.sortedBy { -it.cpuCount }){
            if (node.availableCpuCount >= server.flavor.cpuCount){
                return node.availableCpuCount.toDouble()
            }
        }

        return Double.MAX_VALUE
    }

    override fun toString(): String = "VCpuWeigher"
}

