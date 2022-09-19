package org.opendc.experiments.oversubscription.k8s

import org.opendc.compute.api.Server
import org.opendc.compute.service.internal.HostView
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.compute.service.scheduler.FilterScheduler
import org.opendc.compute.service.scheduler.ReplayScheduler
import org.opendc.compute.service.scheduler.filters.ComputeFilter
import org.opendc.compute.service.scheduler.filters.HostFilter
import org.opendc.compute.service.scheduler.filters.RamFilter
import org.opendc.compute.service.scheduler.filters.VCpuFilter
import org.opendc.compute.service.scheduler.weights.*
import java.util.Random

public fun createK8sNodeScheduler(name: String, seeder: Random, placements: Map<String, String> = emptyMap(), cpuAllocationRatio: Double = 16.0, ramAllocationRatio: Double = 1.5): ComputeScheduler {
    return when (name) {
        "mem" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(RamWeigher(multiplier = 1.0))
        )
        "mem-inv" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(RamWeigher(multiplier = -1.0))
        )
        "core-mem" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(CoreRamWeigher(multiplier = 1.0))
        )
        "core-mem-inv" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(CoreRamWeigher(multiplier = -1.0))
        )
        "active-servers" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(InstanceCountWeigher(multiplier = -1.0))
        )
        "active-servers-inv" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(InstanceCountWeigher(multiplier = 1.0))
        )
        "provisioned-cores" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(VCpuWeigher(cpuAllocationRatio, multiplier = 1.0))
        )
        "provisioned-cores-inv" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(VCpuWeigher(cpuAllocationRatio, multiplier = -1.0))
        )
        "random" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
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
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(RamWeigher(multiplier = 1.0))
        )
        "mem-inv" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(RamWeigher(multiplier = -1.0))
        )
        "core-mem" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(CoreRamWeigher(multiplier = 1.0))
        )
        "core-mem-inv" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(CoreRamWeigher(multiplier = -1.0))
        )
        "active-servers" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(InstanceCountWeigher(multiplier = -1.0))
        )
        "oversubscription" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(K8sOverprovisionWeigher(cpuAllocationRatio, multiplier = 1.5), K8sVCpuWeigher(cpuAllocationRatio, multiplier = 1.0))
        )
        "regular" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(K8sVCpuWeigher(cpuAllocationRatio, multiplier = 1.0))
        )
        "random" -> FilterScheduler(
            filters = listOf(K8sFilter(), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
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

public class K8sVCpuFilter(private val allocationRatio: Double) : HostFilter {
    override fun test(host: HostView, server: Server): Boolean {
        val requested = server.flavor.cpuCount

        val cluster = server.meta["cluster"]!!
        if (!host.host.k8sNodes.contains(cluster)){
            return false
        }

        val nodes = host.host.k8sNodes[cluster]!!

        for (node in nodes){
            if (node.availableCpuCount-requested >= 0){
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
        var maxAvailable = 0

        for (node in nodes){
            if (node.availableCpuCount > maxAvailable){
                maxAvailable = node.availableCpuCount
            }
        }

        return maxAvailable.toDouble()
    }

    override fun toString(): String = "VCpuWeigher"
}

public class K8sOverprovisionWeigher(private val allocationRatio: Double, override val multiplier: Double = 1.0) : HostWeigher {
    init {
        require(allocationRatio > 0.0) { "Allocation ratio must be greater than zero" }
    }

    override fun getWeight(host: HostView, server: Server): Double {
        val overprovision = host.host.model.cpuCount- host.provisionedCores
        if (overprovision>0){
            return 1.0
        } else{
            return overprovision.toDouble()
        }
    }

    override fun toString(): String = "VCpuWeigher"
}
