/*
 * Copyright (c) 2021 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.opendc.experiments.serverless

import com.typesafe.config.ConfigFactory
import io.opentelemetry.api.metrics.MeterProvider
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.opendc.experiments.serverless.trace.FunctionTraceWorkload
import org.opendc.experiments.serverless.trace.ServerlessTraceReader
import org.opendc.faas.service.FaaSService
import org.opendc.faas.service.autoscaler.FunctionTerminationPolicyFixed
import org.opendc.faas.service.router.RandomRoutingPolicy
import org.opendc.faas.simulator.SimFunctionDeployer
import org.opendc.faas.simulator.delay.ColdStartModel
import org.opendc.faas.simulator.delay.StochasticDelayInjector
import org.opendc.harness.dsl.Experiment
import org.opendc.harness.dsl.anyOf
import org.opendc.simulator.compute.model.MachineModel
import org.opendc.simulator.compute.model.MemoryUnit
import org.opendc.simulator.compute.model.ProcessingNode
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.simulator.core.runBlockingSimulation
import org.opendc.telemetry.sdk.toOtelClock
import java.io.File
import java.time.Duration
import java.util.*
import kotlin.math.max

/**
 * A reproduction of the experiments of Soufiane Jounaid's BSc Computer Science thesis:
 * OpenDC Serverless: Design, Implementation and Evaluation of a FaaS Platform Simulator.
 */
public class ServerlessExperiment : Experiment("Serverless") {
    /**
     * The logger for this portfolio instance.
     */
    private val logger = KotlinLogging.logger {}

    /**
     * The configuration to use.
     */
    private val config = ConfigFactory.load().getConfig("opendc.experiments.serverless20")

    /**
     * The routing policy to test.
     */
    private val routingPolicy by anyOf(RandomRoutingPolicy())

    /**
     * The cold start models to test.
     */
    private val coldStartModel by anyOf(ColdStartModel.LAMBDA, ColdStartModel.AZURE, ColdStartModel.GOOGLE)

    override fun doRun(repeat: Int): Unit = runBlockingSimulation {
        val meterProvider: MeterProvider = SdkMeterProvider
            .builder()
            .setClock(clock.toOtelClock())
            .build()

        val trace = ServerlessTraceReader().parse(File(config.getString("trace-path")))
        val traceById = trace.associateBy { it.id }
        val delayInjector = StochasticDelayInjector(coldStartModel, Random())
        val deployer = SimFunctionDeployer(clock, this, createMachineModel(), delayInjector) { FunctionTraceWorkload(traceById.getValue(it.name)) }
        val service =
            FaaSService(coroutineContext, clock, meterProvider, deployer, routingPolicy, FunctionTerminationPolicyFixed(coroutineContext, clock, timeout = Duration.ofMinutes(10)))
        val client = service.newClient()

        coroutineScope {
            for (entry in trace) {
                launch {
                    val function = client.newFunction(entry.id, entry.maxMemory.toLong())
                    var offset = Long.MIN_VALUE

                    for (sample in entry.samples) {
                        if (sample.invocations == 0) {
                            continue
                        }

                        if (offset < 0) {
                            offset = sample.timestamp - clock.millis()
                        }

                        delay(max(0, (sample.timestamp - offset) - clock.millis()))

                        logger.info { "Invoking function ${entry.id} ${sample.invocations} times [${sample.timestamp}]" }

                        repeat(sample.invocations) {
                            function.invoke()
                        }
                    }
                }
            }
        }

        client.close()
        service.close()
    }

    /**
     * Construct the machine model to test with.
     */
    private fun createMachineModel(): MachineModel {
        val cpuNode = ProcessingNode("Intel", "Xeon", "amd64", 2)

        return MachineModel(
            cpus = List(cpuNode.coreCount) { ProcessingUnit(cpuNode, it, 1000.0) },
            memory = List(4) { MemoryUnit("Crucial", "MTA18ASF4G72AZ-3G2B1", 3200.0, 32_000) }
        )
    }
}
