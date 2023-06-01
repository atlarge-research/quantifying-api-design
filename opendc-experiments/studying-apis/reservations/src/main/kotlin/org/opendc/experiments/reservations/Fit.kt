package org.opendc.experiments.reservations

import org.opendc.compute.service.scheduler.weights.HostWeigher

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

import org.opendc.compute.api.Server
import org.opendc.compute.service.internal.HostView
import org.opendc.compute.service.scheduler.filters.HostFilter

/**
 * A [HostWeigher] that weighs the hosts based on the remaining number of vCPUs available.
 *
 * @param allocationRatio Virtual CPU to physical CPU allocation ratio.
 */
public class VCpuFitWeigher(override val multiplier: Double = 1.0) : HostWeigher {

    override fun getWeight(host: HostView, server: Server): Double {
        val weight = -1.0 * kotlin.math.abs(server.flavor.cpuCount - host.host.model.cpuCount)
        return  weight
    }

    override fun toString(): String = "VCpuWeigher"
}


public class VCpuFitFilter() : HostFilter {
    override fun test(host: HostView, server: Server): Boolean {
        return host.host.model.cpuCount == server.flavor.cpuCount
    }
}
