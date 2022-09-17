package org.opendc.experiments.reservation

import mu.KotlinLogging
import org.opendc.compute.service.scheduler.FilterScheduler
import org.opendc.compute.service.scheduler.filters.ComputeFilter
import org.opendc.compute.service.scheduler.filters.RamFilter
import org.opendc.compute.service.scheduler.filters.VCpuFilter
import org.opendc.compute.workload.*
import org.opendc.compute.workload.export.parquet.ParquetComputeMetricExporter
import org.opendc.compute.workload.telemetry.SdkTelemetryManager
import org.opendc.harness.dsl.Experiment
import org.opendc.harness.dsl.anyOf
import org.opendc.simulator.core.runBlockingSimulation
import org.opendc.telemetry.compute.collectServiceMetrics
import org.opendc.telemetry.sdk.metrics.export.CoroutineMetricReader
import java.io.File
import java.util.*
import org.opendc.experiments.reservation.topology.clusterTopology
import org.opendc.experiments.reservation.trace.ComputeWorkloadLoader
import org.opendc.experiments.reservation.trace.sampleByLoad
import org.opendc.experiments.reservation.trace.trace

public class ReservationExperiment : Experiment(name = "reservation") {
    private val logger = KotlinLogging.logger {}
    val workloadTrace by anyOf(
        trace("bitbrains"),
        //trace("materna").sampleByLoad(1.0),
        //trace("solvinity").sampleByLoad(1.0),
    )

    // 1.0F, 0.5F, 0.25F, 0.0F
    private val reservationRatio: Float by anyOf(0.0F)
    // 0.7F, 0.75F,0.8F
    private val utilization: Float by anyOf( 0.85F)

    private val workloadLoader = ComputeWorkloadLoader(File("src/main/resources/trace"))

    override fun doRun(repeat: Int) : Unit = runBlockingSimulation {
        val seeder = Random(repeat.toLong())
        val workload = workloadTrace.resolve(workloadLoader, seeder)
        val exporter = ParquetComputeMetricExporter(
            File("output/${workloadTrace.name}"),
            "utilization=$utilization-reservation_ratio=$reservationRatio",
            4096
        )
        val topology = clusterTopology(File("src/main/resources/topology", "${workloadTrace.name}-$utilization.txt"))

        val telemetry = SdkTelemetryManager(clock)

        val computeScheduler = FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.0)),
            weighers = listOf(VCpuFitWeigher())
        )

        telemetry.registerMetricReader(CoroutineMetricReader(this, exporter))

        val runner = ReservationComputeService(
            coroutineContext,
            clock,
            telemetry,
            computeScheduler,
        )

        try{
            runner.apply(topology)
            runner.run(workload, seeder.nextLong(), reservationRatio)

            val serviceMetrics = collectServiceMetrics(telemetry.metricProducer)
            logger.debug {
                "Scheduler " +
                    "Success=${serviceMetrics.attemptsSuccess} " +
                    "Failure=${serviceMetrics.attemptsFailure} " +
                    "Error=${serviceMetrics.attemptsError} " +
                    "Pending=${serviceMetrics.serversPending} " +
                    "Active=${serviceMetrics.serversActive}"
            }
        }finally {
            runner.close()
            telemetry.close()
        }
    }

    private fun createTopology(name: String = "topology"): org.opendc.compute.workload.topology.Topology {
        val stream = checkNotNull(object {}.javaClass.getResourceAsStream("/env/$name.txt"))
        return stream.use { clusterTopology(stream) }
    }
}

public fun ComputeServiceHelper.apply(topology: org.opendc.compute.workload.topology.Topology, optimize: Boolean = false) {
    val hosts = topology.resolve()
    for (spec in hosts) {
        registerHost(spec, optimize)
    }
}
