package org.opendc.experiments.metadata

import mu.KotlinLogging
import org.opendc.compute.workload.export.parquet.ParquetComputeMetricExporter
import org.opendc.compute.workload.telemetry.SdkTelemetryManager
import org.opendc.experiments.capelin.topology.clusterTopology
import org.opendc.harness.dsl.Experiment
import org.opendc.harness.dsl.anyOf
import org.opendc.simulator.core.runBlockingSimulation
import org.opendc.telemetry.compute.collectServiceMetrics
import java.io.File
import java.util.*
import org.opendc.experiments.metadata.compute.ComputeServiceHelper
import org.opendc.experiments.metadata.scheduler.createScheduler
import org.opendc.experiments.metadata.storage.StorageService
import org.opendc.experiments.metadata.trace.loader.ComputeWorkloadLoader
import org.opendc.experiments.metadata.trace.loader.TraceComputeWorkload
import org.opendc.experiments.metadata.trace.loader.trace
import org.opendc.simulator.core.SimulationCoroutineScope
import org.opendc.telemetry.compute.table.StorageInfo
import org.opendc.telemetry.sdk.metrics.export.CoroutineMetricReader
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.random.asKotlinRandom

class MetadataExperiment : Experiment(name = "metadata") {
    private val logger = KotlinLogging.logger {}

    private val storageServers = 10L
    private val storageServerCores = 32L
    private val storageServerCoreSpeed : Double = 2.5

    val workloadTrace: TraceComputeWorkload by anyOf(
        trace("ibm", isNanoseconds = true)
    )

    private val vmPlacements by anyOf(emptyMap<String, String>())

    private val metadataApi: Boolean by anyOf(
        true,
        false,
    )

    private val allocationPolicy: String = "provisioned-cores-inv"
    private val workloadLoader = ComputeWorkloadLoader(File("src/main/resources/trace"))
    private var stop : Boolean = false

    override fun doRun(repeat: Int) : Unit = runBlockingSimulation(stop, EmptyCoroutineContext, ::experiment)

    suspend fun experiment(scope : SimulationCoroutineScope){
        val seeder = Random(36542)
        val workload = workloadTrace.resolve(workloadLoader, seeder)

        val telemetry = SdkTelemetryManager(scope.clock)
        val exporter = ParquetComputeMetricExporter(
            File("output/${workloadTrace.name}"),
            "api=${metadataApi}-servers=$storageServers-cores=$storageServerCores",
            4096
        )
        telemetry.registerMetricReader(CoroutineMetricReader(scope, exporter))

        val topology = clusterTopology(File("src/main/resources/topology", "${workloadTrace.name}.txt"))

        val scheduler = createScheduler(allocationPolicy, seeder, vmPlacements)

        val meterProvider = telemetry.createMeterProvider(StorageInfo(serversNum = storageServers, serversCpuCount = storageServerCores, serversCpuSpeed = storageServerCoreSpeed))
        val storage = StorageService(
            scope = scope,
            numServers = storageServers,
            numCores = storageServerCores,
            speed = storageServerCoreSpeed,
            meterProvider = meterProvider,
            randomSource = seeder.asKotlinRandom()
        )
        val runner = ComputeServiceHelper(
            scope,
            telemetry,
            scheduler = scheduler,
            topology = topology,
            storage = storage,
            metadataApi = metadataApi,
        )

        try {
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
            stop = true
        } finally {
            println("EXPERIMENT STOP")
            runner.close()
            telemetry.close()
        }
    }
}
