package org.opendc.experiments.overprovision

import mu.KotlinLogging
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
import org.opendc.experiments.capelin.model.Topology
import org.opendc.experiments.capelin.topology.clusterTopology
import org.opendc.experiments.overprovision.k8s.MultitenantComputeService


public class OversubscriptionExperiment : Experiment(name = "oversubscription") {
    private val logger = KotlinLogging.logger {}
    val workloadTrace: ComputeWorkload by anyOf(
        trace("bitbrains").sampleByLoad(1.0),
    )
    val tenantAmount: Int by anyOf(
        2
    )

    val oversubscriptionRatio : Double by anyOf(2.0)

    private val vmPlacements by anyOf(emptyMap<String, String>())
    private val allocationPolicy: String by anyOf(
        "random",
    )
    private val k8sAllocationPolicy: String by anyOf(
        "oversubscription",
    )
    private val workloadLoader = ComputeWorkloadLoader(File("src/main/resources/trace"))

    override fun doRun(repeat: Int) : Unit = runBlockingSimulation {
        val seeder = Random(repeat.toLong())
        val workload = workloadTrace.resolve(workloadLoader, seeder)
        val exporter = ParquetComputeMetricExporter(
            File("output/${workloadTrace.name}"),
            "tenant=$tenantAmount-ratio=$oversubscriptionRatio",
            4096
        )
        val topology = clusterTopology(File("src/main/resources/topology", "${workloadTrace.name}-$tenantAmount-base.txt"))

        val telemetry = SdkTelemetryManager(clock)

        val computeScheduler = createComputeScheduler(allocationPolicy, seeder, vmPlacements, cpuAllocationRatio = oversubscriptionRatio, ramAllocationRatio = oversubscriptionRatio)

        //telemetry.registerMetricReader(CoroutineMetricReader(this, exporter))

        val runner = MultitenantComputeService(
            this,
            coroutineContext,
            clock,
            telemetry,
            computeScheduler,
            allocationPolicy= k8sAllocationPolicy,
            trace = workloadTrace.name,
            tenantAmount = tenantAmount,
        )

        try{
            runner.apply(topology)
            runner.run(workload, seeder.nextLong())

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
}
