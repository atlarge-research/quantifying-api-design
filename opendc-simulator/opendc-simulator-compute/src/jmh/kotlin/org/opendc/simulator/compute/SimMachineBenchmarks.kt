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

package org.opendc.simulator.compute

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.opendc.simulator.compute.kernel.SimFairShareHypervisor
import org.opendc.simulator.compute.kernel.SimSpaceSharedHypervisor
import org.opendc.simulator.compute.model.MachineModel
import org.opendc.simulator.compute.model.MemoryUnit
import org.opendc.simulator.compute.model.ProcessingNode
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.simulator.compute.power.ConstantPowerModel
import org.opendc.simulator.compute.power.SimplePowerDriver
import org.opendc.simulator.compute.workload.SimTrace
import org.opendc.simulator.compute.workload.SimTraceWorkload
import org.opendc.simulator.core.runBlockingSimulation
import org.opendc.simulator.flow.FlowEngine
import org.openjdk.jmh.annotations.*
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@OptIn(ExperimentalCoroutinesApi::class)
class SimMachineBenchmarks {
    private lateinit var machineModel: MachineModel
    private lateinit var trace: SimTrace

    @Setup
    fun setUp() {
        val cpuNode = ProcessingNode("Intel", "Xeon", "amd64", 2)

        machineModel = MachineModel(
            cpus = List(cpuNode.coreCount) { ProcessingUnit(cpuNode, it, 1000.0) },
            memory = List(4) { MemoryUnit("Crucial", "MTA18ASF4G72AZ-3G2B1", 3200.0, 32_000) }
        )

        val random = ThreadLocalRandom.current()
        val builder = SimTrace.builder()
        repeat(10000) {
            val timestamp = it.toLong()
            val deadline = timestamp + 1000
            builder.add(timestamp, deadline, random.nextDouble(0.0, 4500.0), 1)
        }
        trace = builder.build()
    }

    @Benchmark
    fun benchmarkBareMetal() {
        return runBlockingSimulation {
            val engine = FlowEngine(coroutineContext, clock)
            val machine = SimBareMetalMachine(
                engine, machineModel, SimplePowerDriver(ConstantPowerModel(0.0))
            )
            return@runBlockingSimulation machine.runWorkload(SimTraceWorkload(trace))
        }
    }

    @Benchmark
    fun benchmarkSpaceSharedHypervisor() {
        return runBlockingSimulation {
            val engine = FlowEngine(coroutineContext, clock)
            val machine = SimBareMetalMachine(
                engine, machineModel, SimplePowerDriver(ConstantPowerModel(0.0))
            )
            val hypervisor = SimSpaceSharedHypervisor(engine, null, null)

            launch { machine.runWorkload(hypervisor) }

            val vm = hypervisor.newMachine(machineModel)

            try {
                return@runBlockingSimulation vm.runWorkload(SimTraceWorkload(trace))
            } finally {
                vm.cancel()
                machine.cancel()
            }
        }
    }

    @Benchmark
    fun benchmarkFairShareHypervisorSingle() {
        return runBlockingSimulation {
            val engine = FlowEngine(coroutineContext, clock)
            val machine = SimBareMetalMachine(
                engine, machineModel, SimplePowerDriver(ConstantPowerModel(0.0))
            )
            val hypervisor = SimFairShareHypervisor(engine, null, null, null)

            launch { machine.runWorkload(hypervisor) }

            val vm = hypervisor.newMachine(machineModel)

            try {
                return@runBlockingSimulation vm.runWorkload(SimTraceWorkload(trace))
            } finally {
                vm.cancel()
                machine.cancel()
            }
        }
    }

    @Benchmark
    fun benchmarkFairShareHypervisorDouble() {
        return runBlockingSimulation {
            val engine = FlowEngine(coroutineContext, clock)
            val machine = SimBareMetalMachine(
                engine, machineModel, SimplePowerDriver(ConstantPowerModel(0.0))
            )
            val hypervisor = SimFairShareHypervisor(engine, null, null, null)

            launch { machine.runWorkload(hypervisor) }

            coroutineScope {
                repeat(2) {
                    val vm = hypervisor.newMachine(machineModel)

                    launch {
                        try {
                            vm.runWorkload(SimTraceWorkload(trace))
                        } finally {
                            machine.cancel()
                        }
                    }
                }
            }
            machine.cancel()
        }
    }
}
