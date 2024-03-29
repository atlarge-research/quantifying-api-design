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

package org.opendc.simulator.flow.internal

import kotlinx.coroutines.Delay
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Runnable
import org.opendc.simulator.flow.*
import java.time.Clock
import java.util.*
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext

/**
 * Internal implementation of the [FlowEngine] interface.
 *
 * @param context The coroutine context to use.
 * @param clock The virtual simulation clock.
 */
internal class FlowEngineImpl(private val context: CoroutineContext, clock: Clock) : FlowEngine, Runnable {
    /**
     * The [Delay] instance that provides scheduled execution of [Runnable]s.
     */
    @OptIn(InternalCoroutinesApi::class)
    private val delay = requireNotNull(context[ContinuationInterceptor] as? Delay) { "Invalid CoroutineDispatcher: no delay implementation" }

    /**
     * The queue of connection updates that are scheduled for immediate execution.
     */
    private val queue = FlowDeque()

    /**
     * A priority queue containing the connection updates to be scheduled in the future.
     */
    private val futureQueue = FlowTimerQueue()

    /**
     * The stack of engine invocations to occur in the future.
     */
    private val futureInvocations = ArrayDeque<Invocation>()

    /**
     * The systems that have been visited during the engine cycle.
     */
    private val visited = FlowDeque()

    /**
     * The index in the batch stack.
     */
    private var batchIndex = 0

    /**
     * The virtual [Clock] of this engine.
     */
    override val clock: Clock
        get() = _clock
    private val _clock: Clock = clock

    /**
     * Update the specified [ctx] synchronously.
     */
    fun scheduleSync(now: Long, ctx: FlowConsumerContextImpl) {
        ctx.doUpdate(now, visited, futureQueue, isImmediate = true)

        // In-case the engine is already running in the call-stack, return immediately. The changes will be picked
        // up by the active engine.
        if (batchIndex > 0) {
            return
        }

        doRunEngine(now)
    }

    /**
     * Enqueue the specified [ctx] to be updated immediately during the active engine cycle.
     *
     * This method should be used when the state of a flow context is invalidated/interrupted and needs to be
     * re-computed. In case no engine is currently active, the engine will be started.
     */
    fun scheduleImmediate(now: Long, ctx: FlowConsumerContextImpl) {
        queue.add(ctx)

        // In-case the engine is already running in the call-stack, return immediately. The changes will be picked
        // up by the active engine.
        if (batchIndex > 0) {
            return
        }

        doRunEngine(now)
    }

    override fun newContext(consumer: FlowSource, provider: FlowConsumerLogic): FlowConsumerContext = FlowConsumerContextImpl(this, consumer, provider)

    override fun pushBatch() {
        batchIndex++
    }

    override fun popBatch() {
        try {
            // Flush the work if the engine is not already running
            if (batchIndex == 1 && queue.isNotEmpty()) {
                doRunEngine(_clock.millis())
            }
        } finally {
            batchIndex--
        }
    }

    /* Runnable */
    override fun run() {
        val invocation = futureInvocations.poll() // Clear invocation from future invocation queue
        doRunEngine(invocation.timestamp)
    }

    /**
     * Run all the enqueued actions for the specified [timestamp][now].
     */
    private fun doRunEngine(now: Long) {
        val queue = queue
        val futureQueue = futureQueue
        val futureInvocations = futureInvocations
        val visited = visited

        try {
            // Increment batch index so synchronous calls will not launch concurrent engine invocations
            batchIndex++

            // Execute all scheduled updates at current timestamp
            while (true) {
                val ctx = futureQueue.poll(now) ?: break
                ctx.doUpdate(now, visited, futureQueue, isImmediate = false)
            }

            // Repeat execution of all immediate updates until the system has converged to a steady-state
            // We have to take into account that the onConverge callback can also trigger new actions.
            do {
                // Execute all immediate updates
                while (true) {
                    val ctx = queue.poll() ?: break
                    ctx.doUpdate(now, visited, futureQueue, isImmediate = true)
                }

                while (true) {
                    val ctx = visited.poll() ?: break
                    ctx.onConverge(now)
                }
            } while (queue.isNotEmpty())
        } finally {
            // Decrement batch index to indicate no engine is active at the moment
            batchIndex--
        }

        // Schedule an engine invocation for the next update to occur.
        val headDeadline = futureQueue.peekDeadline()
        if (headDeadline != Long.MAX_VALUE) {
            trySchedule(now, futureInvocations, headDeadline)
        }
    }

    /**
     * Try to schedule an engine invocation at the specified [target].
     *
     * @param now The current virtual timestamp.
     * @param target The virtual timestamp at which the engine invocation should happen.
     * @param scheduled The queue of scheduled invocations.
     */
    private fun trySchedule(now: Long, scheduled: ArrayDeque<Invocation>, target: Long) {
        val head = scheduled.peek()

        // Only schedule a new scheduler invocation in case the target is earlier than all other pending
        // scheduler invocations
        if (head == null || target < head.timestamp) {
            @OptIn(InternalCoroutinesApi::class)
            val handle = delay.invokeOnTimeout(target - now, this, context)
            scheduled.addFirst(Invocation(target, handle))
        }
    }

    /**
     * A future engine invocation.
     *
     * This class is used to keep track of the future engine invocations created using the [Delay] instance. In case
     * the invocation is not needed anymore, it can be cancelled via [cancel].
     */
    private class Invocation(
        @JvmField val timestamp: Long,
        @JvmField val handle: DisposableHandle
    ) {
        /**
         * Cancel the engine invocation.
         */
        fun cancel() = handle.dispose()
    }
}
