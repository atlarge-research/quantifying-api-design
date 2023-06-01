package org.opendc.experiments.oversubscription.trace

import mu.KotlinLogging
import org.opendc.compute.workload.ComputeWorkload
import org.opendc.compute.workload.VirtualMachine
import org.opendc.compute.workload.sampleByLoad
import java.util.*

public fun trace(name: String, format: String = "opendc-vm", isNanoseconds: Boolean = false, fraction : Double = 1.0) = TraceComputeWorkload(name, format, isNanoseconds, fraction)

/**
 * A [ComputeWorkload] from a trace.
 */
class TraceComputeWorkload(val name: String, val format: String, val isNanoseconds: Boolean, val fraction: Double = 1.0) {
    fun resolve(loader: ComputeWorkloadLoader, random: Random): List<VirtualMachine> {
        val vms = loader.get(name, format, isNanoseconds)
        return vms.shuffled(random)
    }
}


public fun TraceComputeWorkload.sampleByLoad(fraction: Double): LoadSampledComputeWorkload {
    return LoadSampledComputeWorkload(this, fraction)
}

/**
 * A [ComputeWorkload] that is sampled based on total load.
 */
class LoadSampledComputeWorkload(val source: TraceComputeWorkload, val fraction: Double, val name: String = source.name) {
    /**
     * The logging instance of this class.
     */
    private val logger = KotlinLogging.logger {}

    fun resolve(loader: ComputeWorkloadLoader, random: Random): List<VirtualMachine> {
        val vms = source.resolve(loader, random)
        val amount = (vms.size * fraction).toInt()
        return vms.slice(0..amount)
    }

}
