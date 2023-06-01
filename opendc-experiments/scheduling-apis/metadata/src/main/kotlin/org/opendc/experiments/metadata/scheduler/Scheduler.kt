package org.opendc.experiments.metadata.scheduler

import org.opendc.compute.api.Server
import org.opendc.compute.service.internal.HostView
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.compute.service.scheduler.FilterScheduler
import org.opendc.compute.service.scheduler.ReplayScheduler
import org.opendc.compute.service.scheduler.filters.*
import org.opendc.compute.service.scheduler.weights.*
import java.util.Random

public fun createScheduler(name: String, seeder: Random, placements: Map<String, String> = emptyMap(), cpuAllocationRatio: Double = 16.0, ramAllocationRatio: Double = 1.5): ComputeScheduler {
    return when (name) {
        "mem" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio)),
            weighers = listOf(RamWeigher(multiplier = 1.0))
        )
        "mem-inv" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio)),
            weighers = listOf(RamWeigher(multiplier = -1.0))
        )
        "core-mem" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio)),
            weighers = listOf(CoreRamWeigher(multiplier = 1.0))
        )
        "core-mem-inv" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio)),
            weighers = listOf(CoreRamWeigher(multiplier = -1.0))
        )
        "active-servers" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio)),
            weighers = listOf(InstanceCountWeigher(multiplier = -1.0))
        )
        "active-servers-inv" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio)),
            weighers = listOf(InstanceCountWeigher(multiplier = 1.0))
        )
        "provisioned-cores" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio)),
            weighers = listOf(VCpuWeigher(cpuAllocationRatio, multiplier = 1.0))
        )
        "provisioned-cores-inv" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio)),
            weighers = listOf(VCpuWeigher(cpuAllocationRatio, multiplier = -1.0))
        )
        "random" -> FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(cpuAllocationRatio)),
            weighers = emptyList(),
            subsetSize = Int.MAX_VALUE,
            random = Random(seeder.nextLong())
        )
        "replay" -> ReplayScheduler(placements)
        else -> throw IllegalArgumentException("Unknown policy $name")
    }
}

