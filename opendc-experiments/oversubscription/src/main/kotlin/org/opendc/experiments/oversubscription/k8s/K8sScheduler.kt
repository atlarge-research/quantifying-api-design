package org.opendc.experiments.oversubscription.k8s

import org.opendc.compute.api.Server
import org.opendc.compute.service.ComputeService
import org.opendc.compute.service.driver.HostState
import org.opendc.compute.service.internal.HostView
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.compute.service.scheduler.FilterScheduler
import org.opendc.compute.service.scheduler.ReplayScheduler
import org.opendc.compute.service.scheduler.filters.ComputeFilter
import org.opendc.compute.service.scheduler.filters.HostFilter
import org.opendc.compute.service.scheduler.filters.RamFilter
import org.opendc.compute.service.scheduler.filters.VCpuFilter
import org.opendc.compute.service.scheduler.weights.*
import org.opendc.compute.simulator.SimHost
import java.util.Random
import kotlin.math.abs

/**
 * Create a [ComputeScheduler] for the experiment.
 */
public fun createComputeSchedulerK8s(name: String, seeder: Random, placements: Map<String, String> = emptyMap(), cpuAllocationRatio: Double = 16.0, ramAllocationRatio: Double = 1.5, tenantAmount: Int = 1): ComputeScheduler {
    return when (name) {
        "mem" -> FilterScheduler(
            filters = listOf(K8sFilter(tenantAmount), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(RamWeigher(multiplier = 1.0))
        )
        "mem-inv" -> FilterScheduler(
            filters = listOf(K8sFilter(tenantAmount), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(RamWeigher(multiplier = -1.0))
        )
        "core-mem" -> FilterScheduler(
            filters = listOf(K8sFilter(tenantAmount), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(CoreRamWeigher(multiplier = 1.0))
        )
        "core-mem-inv" -> FilterScheduler(
            filters = listOf(K8sFilter(tenantAmount), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(CoreRamWeigher(multiplier = -1.0))
        )
        "active-servers" -> FilterScheduler(
            filters = listOf(K8sFilter(tenantAmount), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(InstanceCountWeigher(multiplier = -1.0))
        )
        "oversubscription" -> FilterScheduler(
            filters = listOf(K8sFilter(tenantAmount), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(K8sOverprovisionWeigher(cpuAllocationRatio, multiplier = 1.5), K8sVCpuWeigher(cpuAllocationRatio, multiplier = 1.0))
        )
        "regular" -> FilterScheduler(
            filters = listOf(K8sFilter(tenantAmount), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(K8sVCpuWeigher(cpuAllocationRatio, multiplier = 1.0))
        )
        "random" -> FilterScheduler(
            filters = listOf(K8sFilter(tenantAmount), ComputeFilter(), K8sVCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = emptyList(),
            subsetSize = Int.MAX_VALUE,
            random = Random(seeder.nextLong())
        )
        "replay" -> ReplayScheduler(placements)
        else -> throw IllegalArgumentException("Unknown policy $name")
    }
}

public class K8sFilter(val tenantAmount: Int) : HostFilter {
    override fun test(host: HostView, server: Server): Boolean {
        return (host.host as SimHost).partitions.contains(server.flavor.meta["cluster"])
    }

    override fun toString(): String = "K8sFilter"
}

public class K8sVCpuFilter(private val allocationRatio: Double) : HostFilter {
    override fun test(host: HostView, server: Server): Boolean {
        val requested = server.flavor.cpuCount
        if (!host.host.partitions.contains(server.flavor.meta["cluster"])){
            return false
        }

        val total = host.host.partitions[server.flavor.meta["cluster"]]!!
        val used = host.host.partitionsUsed[server.flavor.meta["cluster"]]!!

        var i = 0
        while (i<total.size){
            val free = total.get(i) - used.get(i)
            if (free >= requested){
                return true
            }
            i++
        }

        return false
    }
}


public class K8sVCpuWeigher(private val allocationRatio: Double, override val multiplier: Double = 1.0) : HostWeigher {

    init {
        require(allocationRatio > 0.0) { "Allocation ratio must be greater than zero" }
    }

    override fun getWeight(host: HostView, server: Server): Double {
        if (!host.host.partitions.contains(server.flavor.meta["cluster"])){
            return 0.0
        }

        var maxFree = 0
        var i = 0

        val total = host.host.partitions[server.flavor.meta["cluster"]]!!
        val used = host.host.partitionsUsed[server.flavor.meta["cluster"]]!!

        while (i<total.size){
            val free = total.get(i) - used.get(i)
            if (free > maxFree){
                maxFree = free
            }
            i++
        }

        return maxFree.toDouble()
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
