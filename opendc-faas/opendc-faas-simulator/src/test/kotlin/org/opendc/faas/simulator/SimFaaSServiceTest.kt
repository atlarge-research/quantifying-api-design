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

package org.opendc.faas.simulator

import io.mockk.coVerify
import io.mockk.spyk
import io.opentelemetry.api.metrics.MeterProvider
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.yield
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.opendc.faas.service.FaaSService
import org.opendc.faas.service.autoscaler.FunctionTerminationPolicyFixed
import org.opendc.faas.service.router.RandomRoutingPolicy
import org.opendc.faas.simulator.delay.ZeroDelayInjector
import org.opendc.faas.simulator.workload.SimFaaSWorkload
import org.opendc.simulator.compute.model.MachineModel
import org.opendc.simulator.compute.model.MemoryUnit
import org.opendc.simulator.compute.model.ProcessingNode
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.simulator.compute.workload.SimFlopsWorkload
import org.opendc.simulator.compute.workload.SimWorkload
import org.opendc.simulator.core.runBlockingSimulation
import java.time.Duration

/**
 * A test suite for the [FaaSService] implementation under simulated conditions.
 */
@OptIn(ExperimentalCoroutinesApi::class)
internal class SimFaaSServiceTest {

    private lateinit var machineModel: MachineModel

    @BeforeEach
    fun setUp() {
        val cpuNode = ProcessingNode("Intel", "Xeon", "amd64", 2)

        machineModel = MachineModel(
            cpus = List(cpuNode.coreCount) { ProcessingUnit(cpuNode, it, 1000.0) },
            memory = List(4) { MemoryUnit("Crucial", "MTA18ASF4G72AZ-3G2B1", 3200.0, 32_000) }
        )
    }

    @Test
    fun testSmoke() = runBlockingSimulation {
        val workload = spyk(object : SimFaaSWorkload, SimWorkload by SimFlopsWorkload(1000) {
            override suspend fun invoke() {}
        })
        val deployer = SimFunctionDeployer(clock, this, machineModel, ZeroDelayInjector) { workload }
        val service = FaaSService(
            coroutineContext, clock, MeterProvider.noop(), deployer, RandomRoutingPolicy(),
            FunctionTerminationPolicyFixed(coroutineContext, clock, timeout = Duration.ofMillis(10000))
        )

        val client = service.newClient()

        val function = client.newFunction("test", 128)
        function.invoke()
        delay(2000)

        service.close()

        yield()

        assertAll(
            { coVerify { workload.invoke() } },
        )
    }
}
