package org.opendc.experiments.overprovision.k8s

import org.opendc.compute.api.Server
import org.opendc.compute.service.ComputeService
import org.opendc.compute.service.internal.HostView
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.compute.service.scheduler.FilterScheduler
import org.opendc.compute.service.scheduler.filters.ComputeFilter
import org.opendc.compute.service.scheduler.filters.RamFilter
import org.opendc.compute.service.scheduler.filters.VCpuFilter
import org.opendc.compute.service.scheduler.weights.HostWeigher
import java.util.Random

public fun createK8sScheduler(name: String, seeder: Random, service: ComputeService, cpuAllocationRatio: Double = 16.0, ramAllocationRatio: Double = 1.5): ComputeScheduler {
    return when (name) {
        "random" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = emptyList(),
            subsetSize = Int.MAX_VALUE,
            random = Random(seeder.nextLong())
        )
        "oversubscription" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio), RamFilter(ramAllocationRatio)),
            weighers = listOf(OverprovisionWeigher(service, 1.0)),
            subsetSize = Int.MAX_VALUE,
            random = Random(seeder.nextLong())
        )
        else -> {throw IllegalArgumentException("Unknown policy $name")}
    }
}

public class OverprovisionWeigher(val service: ComputeService, override val multiplier: Double) : HostWeigher {
    override fun getWeight(host: HostView, server: Server): Double {
        val host = host.host
        if (host is K8sNode && host.server != null){
            val hv = service.views[host.server!!.uid]
            if (hv != null){
                val provisionedCores = hv.provisionedCores
                val cpuCount = hv.host.model.cpuCount
                return (cpuCount - provisionedCores) * multiplier
            }
        }
        return 0.0
    }

    override fun toString(): String = "OverprovisionWeigher"
}
