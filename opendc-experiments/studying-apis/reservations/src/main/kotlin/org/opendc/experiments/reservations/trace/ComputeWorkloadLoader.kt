package org.opendc.experiments.reservations.trace


import mu.KotlinLogging
import org.opendc.compute.workload.VirtualMachine
import org.opendc.simulator.compute.workload.SimTrace
import org.opendc.trace.*
import java.io.File
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.max
import kotlin.math.roundToLong

/**
 * A helper class for loading compute workload traces into memory.
 *
 * @param baseDir The directory containing the traces.
 */
public class ComputeWorkloadLoader(private val baseDir: File, timeUnits :Int = 1 ) {
    /**
     * The logger for this instance.
     */
    private val logger = KotlinLogging.logger {}

    /**
     * The cache of workloads.
     */
    private val cache = ConcurrentHashMap<String, List<VirtualMachine>>()

    /**
     * Read the metadata into a workload.
     */
    private fun parseMeta(trace: Trace): List<VirtualMachine> {
        val reader = checkNotNull(trace.getTable(TABLE_RESOURCES)).newReader()

        val idCol = reader.resolve(RESOURCE_ID)
        val startTimeCol = reader.resolve(RESOURCE_START_TIME)
        val stopTimeCol = reader.resolve(RESOURCE_STOP_TIME)
        val cpuCountCol = reader.resolve(RESOURCE_CPU_COUNT)
        val cpuCapacityCol = reader.resolve(RESOURCE_CPU_CAPACITY)
        val cpuUtilizationCol = reader.resolve(RESOURCE_CPU_UTILIZATION)
        val memCol = reader.resolve(RESOURCE_MEM_CAPACITY)

        var counter = 0
        val entries = mutableListOf<VirtualMachine>()

        return try {
            while (reader.nextRow()) {

                val id = reader.get(idCol) as String

                val submissionTime = reader.get(startTimeCol) as Instant
                val endTime = reader.get(stopTimeCol) as Instant
                val cpuCount = reader.getInt(cpuCountCol)
                val cpuCapacity = reader.getDouble(cpuCapacityCol)
                val cpuUtilization = reader.getDouble(cpuUtilizationCol)
                val memCapacity = reader.getDouble(memCol) / 1000.0 // Convert from KB to MB
                val uid = UUID.nameUUIDFromBytes("$id-${counter++}".toByteArray())


                entries.add(
                    VirtualMachine(
                        uid,
                        id,
                        cpuCount.toInt(),
                        cpuCapacity,
                        memCapacity.roundToLong(),
                        0.0,
                        submissionTime,
                        endTime,
                        SimTrace(DoubleArray(0), LongArray(0),LongArray(0), IntArray(0), 0 ),
                        cpuUtilization
                    )
                )
            }

            // Make sure the virtual machines are ordered by start time
            entries.sortBy { it.startTime }

            entries
        } catch (e: Exception) {
            e.printStackTrace()
            throw e
        } finally {
            reader.close()
        }
    }

    /**
     * Load the trace with the specified [name] and [format].
     */
    public fun get(name: String, format: String): List<VirtualMachine> {
        return cache.computeIfAbsent(name) {
            val path = baseDir.resolve(it)

            logger.info { "Loading trace $it at $path" }

            val trace = Trace.open(path, format)
            parseMeta(trace)
        }
    }

    /**
     * Clear the workload cache.
     */
    public fun reset() {
        cache.clear()
    }

    /**
     * A builder for a VM trace.
     */
    private class Builder {
        /**
         * The total load of the trace.
         */
        @JvmField var totalLoad: Double = 0.0

        /**
         * The internal builder for the trace.
         */
        private val builder = SimTrace.builder()

        /**
         * Add a fragment to the trace.
         *
         * @param timestamp Timestamp at which the fragment starts (in epoch millis).
         * @param deadline Timestamp at which the fragment ends (in epoch millis).
         * @param usage CPU usage of this fragment.
         * @param cores Number of cores used.
         */
        fun add(timestamp: Long, deadline: Long, usage: Double, cores: Int) {
            val duration = max(0, deadline - timestamp)
            totalLoad += (usage * duration) / 1000.0 // avg MHz * duration = MFLOPs
            builder.add(timestamp, deadline, usage, cores)
        }

        /**
         * Build the trace.
         */
        fun build(): SimTrace = builder.build()
    }
}
