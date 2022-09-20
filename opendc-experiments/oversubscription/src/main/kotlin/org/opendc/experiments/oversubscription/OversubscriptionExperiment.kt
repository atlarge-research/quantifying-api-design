package org.opendc.experiments.oversubscription

import mu.KotlinLogging
import org.opendc.compute.workload.export.parquet.ParquetComputeMetricExporter
import org.opendc.compute.workload.telemetry.SdkTelemetryManager
import org.opendc.harness.dsl.Experiment
import org.opendc.harness.dsl.anyOf
import org.opendc.simulator.core.runBlockingSimulation
import org.opendc.telemetry.compute.collectServiceMetrics
import java.io.File
import java.util.*
import org.opendc.experiments.capelin.topology.clusterTopology
import org.opendc.experiments.oversubscription.k8s.K8sComputeServiceHelper
import org.opendc.experiments.oversubscription.k8s.createK8sNodeScheduler
import org.opendc.experiments.oversubscription.k8s.createK8sPodScheduler
import org.opendc.experiments.oversubscription.trace.ComputeWorkloadLoader
import org.opendc.experiments.oversubscription.trace.TraceComputeWorkload
import org.opendc.experiments.oversubscription.trace.trace
import org.opendc.telemetry.sdk.metrics.export.CoroutineMetricReader

public class OversubscriptionExperiment : Experiment(name = "oversubscription") {
    private val logger = KotlinLogging.logger {}
    val workloadTrace: TraceComputeWorkload by anyOf(
        trace("bitbrains")
    )

    val oversubscriptionRatio : Float by anyOf(1.5F)

    private val vmPlacements by anyOf(emptyMap<String, String>())
    private val migration: Boolean by anyOf(
        false,
    )
    private val oversubscriptionApi: Boolean by anyOf(
        false,
    )

    private val nodeAllocationPolicy: String by anyOf(
        "provisioned-cores",
    )

    private val podAllocationPolicy: String by anyOf(
        "regular",
    )

    private val topologySample : Float = 1.0F

    private val workloadLoader = ComputeWorkloadLoader(File("src/main/resources/trace"))

    override fun doRun(repeat: Int) : Unit = runBlockingSimulation {
        val seeder = Random(repeat.toLong())
        val workload = workloadTrace.resolve(workloadLoader, seeder)
        val exporter = ParquetComputeMetricExporter(
            File("output/${workloadTrace.name}"),
            "policy=$nodeAllocationPolicy-ratio=$oversubscriptionRatio",
            4096
        )
        val topology = clusterTopology(File("src/main/resources/topology", "${workloadTrace.name}-base.txt"), sample = topologySample)
        val k8sTopology = clusterTopology(File("src/main/resources/topology", "${workloadTrace.name}-k8s.txt"), sample = topologySample)

        val telemetry = SdkTelemetryManager(clock)

        val k8sNodeScheduler = createK8sNodeScheduler(nodeAllocationPolicy, seeder, vmPlacements, cpuAllocationRatio = oversubscriptionRatio.toDouble(), ramAllocationRatio = oversubscriptionRatio.toDouble())
        val k8sPodScheduler = createK8sPodScheduler(podAllocationPolicy, seeder, vmPlacements, cpuAllocationRatio = oversubscriptionRatio.toDouble(), ramAllocationRatio = oversubscriptionRatio.toDouble())

        telemetry.registerMetricReader(CoroutineMetricReader(this, exporter))

        val runner = K8sComputeServiceHelper(
            coroutineContext,
            clock,
            telemetry,
            nodeScheduler = k8sNodeScheduler,
            podScheduler = k8sPodScheduler,
            k8sTopology = k8sTopology,
            oversubscription = oversubscriptionRatio,
            oversubscriptionApi = oversubscriptionApi,
            migration= migration,
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
