package org.opendc.experiments.oversubscription

import mu.KotlinLogging
import org.opendc.compute.workload.export.parquet.ParquetComputeMetricExporter
import org.opendc.compute.workload.telemetry.SdkTelemetryManager
import org.opendc.compute.workload.topology.Topology
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
import org.opendc.experiments.oversubscription.trace.sampleByLoad
import org.opendc.experiments.oversubscription.trace.trace
import org.opendc.telemetry.sdk.metrics.export.CoroutineMetricReader

public class OversubscriptionExperiment : Experiment(name = "oversubscription") {
    private val logger = KotlinLogging.logger {}
    private val utilization by anyOf(
        0.85F
    )
    val workloadTrace: TraceComputeWorkload by anyOf(
        trace("azure", isNanoseconds = true)
    )

    val oversubscriptionRatio : Float by anyOf(
        //1.3F,
        //1.5F,
        //1.7F
        3F,
        //4F,
        //5F,
    )

    private val vmPlacements by anyOf(emptyMap<String, String>())

    val ideal : Boolean by anyOf(
        false,
    )
    private val migration: Boolean by anyOf(
        true,
    )
    private val oversubscriptionApi: Boolean by anyOf(
        //true,
        false,
    )
    val clusterType : String by anyOf(
        "equal",
        //"diff",
    )
    val cpuAllocationRatio : Float by anyOf(
        //1.3F,
        //1.5F,
        16F
    )
    private val nodeAllocationPolicy: String by anyOf(
        "provisioned-cores-inv",
    )
    private val podAllocationPolicy: String by anyOf(
        "provisioned-cores-inv",
    )

    private val workloadLoader = ComputeWorkloadLoader(File("src/main/resources/trace"))

    override fun doRun(repeat: Int) : Unit = runBlockingSimulation {
        val seeder = Random(repeat.toLong())
        val workload = workloadTrace.resolve(workloadLoader, seeder)

        val exporter : ParquetComputeMetricExporter
        val topology: Topology
        val k8sTopology : Topology

        val sample = 1 + (1-utilization)
        if (!ideal){
            exporter = ParquetComputeMetricExporter(
                File("output/${workloadTrace.name}/utilization=$utilization/"),
                "api=$oversubscriptionApi-ratio=$oversubscriptionRatio-cluster=$clusterType",
                4096
            )
            topology = clusterTopology(File("src/main/resources/topology", "${workloadTrace.name}.txt"), sample = sample)
            k8sTopology = clusterTopology(File("src/main/resources/topology", "${workloadTrace.name}-${clusterType}.txt"), sample = sample*oversubscriptionRatio)
        } else {
            exporter = ParquetComputeMetricExporter(
                File("output/${workloadTrace.name}"),
                "ideal",
                4096
            )
            topology = clusterTopology(File("src/main/resources/topology", "${workloadTrace.name}-ideal.txt"))
            k8sTopology = clusterTopology(File("src/main/resources/topology", "${workloadTrace.name}-ideal-k8s.txt"))
        }


        val telemetry = SdkTelemetryManager(clock)

        val k8sNodeScheduler = createK8sNodeScheduler(nodeAllocationPolicy, seeder, vmPlacements, cpuAllocationRatio = (oversubscriptionRatio*cpuAllocationRatio).toDouble(), ramAllocationRatio = oversubscriptionRatio.toDouble())
        val k8sPodScheduler = createK8sPodScheduler(podAllocationPolicy, seeder, vmPlacements, cpuAllocationRatio = (oversubscriptionRatio*cpuAllocationRatio).toDouble(), ramAllocationRatio = oversubscriptionRatio.toDouble())

        telemetry.registerMetricReader(CoroutineMetricReader(this, exporter))

        val runner = K8sComputeServiceHelper(
            coroutineContext,
            clock,
            telemetry,
            nodeScheduler = k8sNodeScheduler,
            podScheduler = k8sPodScheduler,
            k8sTopology = k8sTopology,
            oversubscription = oversubscriptionRatio*cpuAllocationRatio,
            oversubscriptionApi = oversubscriptionApi,
            migration = migration,
        )

        try{
            runner.apply(topology)
            runner.run(workload, seeder.nextLong())

            // collect all remaining metrics that are in the buffer
            val metrics = telemetry.metricProducer.collectAllMetrics()
            exporter.export(metrics)

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
