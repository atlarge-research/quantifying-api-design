package org.opendc.experiments.metadata.trace.loader


import mu.KotlinLogging
import org.opendc.compute.workload.VirtualMachine
import org.opendc.experiments.metadata.trace.format.*
import org.opendc.simulator.compute.workload.SimTrace
import org.opendc.trace.*
import org.opendc.trace.spi.TraceFormat
import java.io.File
import java.nio.file.Path
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
    private val cache = ConcurrentHashMap<String, List<Workflow>>()

    /**
     * Read the metadata into a workload.
     */
    private fun parseMeta(trace: Trace, isNanoseconds: Boolean): List<Workflow> {
        val reader = checkNotNull(trace.getTable(TABLE_RESOURCES)).newReader()


        var counter = 0
        val entries = mutableMapOf<String, Workflow>()

        return try {
            while (reader.nextRow()) {

                val taskId = reader.get(COL_ID) as String

                var submissionTime = reader.get(COL_START_TIME) as Instant
                if (isNanoseconds) {
                    submissionTime = Instant.ofEpochMilli(submissionTime.toEpochMilli() / 1_000)
                }

                val cpuCount = reader.getLong(COL_CPU_COUNT)

                val workflowId = reader.get(COL_WORKFLOW_ID) as String
                val runtime = reader.get(COL_RUNTIME) as Long
                val objecId = reader.get(COL_OBJECT_ID) as String
                val objectSize = reader.get(COL_OBJECT_SIZE) as Long


                val task = Task(id = taskId, runtime = runtime, objectID = objecId, objectSize = objectSize)
                if (!entries.contains(workflowId)) {
                    entries[workflowId] = Workflow(
                        id = workflowId,
                        startTime = submissionTime,
                        cpuCount = cpuCount.toInt(),
                        tasks = mutableListOf()
                    )
                }

                entries[workflowId]!!.tasks.add(task)
            }

            entries.values.toList()
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
    public fun get(name: String, isNanoseconds: Boolean): List<Workflow> {
        return cache.computeIfAbsent(name) {
            val path = baseDir.resolve(it)

            logger.info { "Loading trace $it at $path" }

            val trace = TraceImpl(OdcVmTraceFormat(), path.toPath())
            parseMeta(trace, isNanoseconds)
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

class TraceImpl(val format: TraceFormat, val path: Path) : Trace {
    /**
     * A map containing the [TableImpl] instances associated with the trace.
     */
    private val tableMap = ConcurrentHashMap<String, TableImpl>()

    override val tables: List<String> = format.getTables(path)

    init {
        for (table in tables) {
            tableMap.computeIfAbsent(table) { TableImpl(this, it) }
        }
    }

    override fun containsTable(name: String): Boolean = tableMap.containsKey(name)

    override fun getTable(name: String): Table? = tableMap[name]

    override fun hashCode(): Int = Objects.hash(format, path)

    override fun equals(other: Any?): Boolean = other is TraceImpl && format == other.format && path == other.path
}

class TableImpl(val trace: TraceImpl, override val name: String) : Table {
    /**
     * The details of this table.
     */
    private val details = trace.format.getDetails(trace.path, name)

    override val columns: List<TableColumn<*>>
        get() = details.columns

    override val partitionKeys: List<TableColumn<*>>
        get() = details.partitionKeys

    override fun newReader(): TableReader = trace.format.newReader(trace.path, name)

    override fun newWriter(): TableWriter = trace.format.newWriter(trace.path, name)

    override fun toString(): String = "Table[name=$name]"

    override fun hashCode(): Int = Objects.hash(trace, name)

    override fun equals(other: Any?): Boolean = other is TableImpl && trace == other.trace && name == other.name
}
