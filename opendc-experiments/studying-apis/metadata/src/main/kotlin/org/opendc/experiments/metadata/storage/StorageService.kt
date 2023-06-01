package org.opendc.experiments.metadata.storage

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.MeterProvider
import io.opentelemetry.api.metrics.ObservableLongMeasurement
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.opendc.simulator.core.SimulationCoroutineScope
import java.util.Collections.max
import kotlin.math.max
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.DurationUnit


public class StorageService(private val scope :SimulationCoroutineScope, private val numServers : Long, private val numCores : Long, private val speed : Double, meterProvider: MeterProvider){
    private val servers = mutableListOf<Server>()

    val meter = meterProvider.get("org.opendc.experiments.metadata")

    init {
        for (i in 1..numServers){
            val server = Server(i.toInt(), scope, numCores, speed)
            servers.add(server)
        }

        meter.gaugeBuilder("storage.buffer_size")
            .ofLongs()
            .buildWithCallback (::collectBufferSizes)
        meter.gaugeBuilder("storage.idle_time")
            .ofLongs()
            .buildWithCallback (::collectIdleTimes)
    }

    private val objectToServer = mutableMapOf<String, Server>()


    public fun SetIfNew(objectId : String, size : Long){
        if (objectToServer.contains(objectId)){
            return
        }

        val randServer = servers.random()

        randServer.Set(objectId, size)
        objectToServer[objectId] = randServer
    }

    suspend fun RequestObject(objectId : String, requestor: ObjectRequester) {
        val server = objectToServer[objectId]!!
        server.RequestObject(objectId, requestor)
    }

    fun GetDownloadEstimate(objectId: String) : Long{
        return objectToServer[objectId]!!.GetQueueTime()
    }

    public suspend fun Run(){
        for (server in servers){
            server.Run()
        }
    }

    suspend fun Close(){
        for (server in servers){
            server.Close()
        }
    }

    private class Server(val idx: Int, val scope :SimulationCoroutineScope, val numCores : Long, val speed : Double){
        val idToObject = mutableMapOf<String, Object>()
        val buffer = Channel<GetRequest>()
        var bufferAggSize = 0L
        var idleTime = Duration.ZERO
        val jobs : MutableList<Job> = mutableListOf()
        val times : MutableList<Pair<Long, Long>> = MutableList(numCores.toInt()){ Pair(0,0) }
        var previousCollect : Long = 0L

        suspend fun Run(){
            for (i in 0 until numCores.toInt()) {
                val job = scope.launch { proccessRequests(i) }
                jobs.add(job)
            }
        }

        suspend fun proccessRequests(idx: Int){
            while (true){
                times[idx] = Pair(scope.clock.millis(), times[idx].second)
                val request = buffer.receive()
                val now = scope.clock.millis()
                val diff = now.minus(max(times[idx].first, previousCollect)).milliseconds

                idleTime += diff
                times[idx] = Pair(times[idx].first, scope.clock.millis())

                val obj = idToObject[request.objectId]!!
                delay(1_000L*(obj.size/4_000_000))  // TODO: 4Gb/s?
                request.requestor.Receive(obj)
                bufferAggSize -= obj.size
            }
        }

        fun GetQueueTime() : Long{
            return (bufferAggSize / speed).toLong()
        }

        fun Close(){
            for (job in jobs){
                job.cancel()
            }
        }

        fun Set(objectId: String, size : Long) {
            idToObject[objectId] = Object(id = objectId, size = size)
        }

        suspend fun RequestObject(objectId: String, requester: ObjectRequester){
            bufferAggSize += idToObject[objectId]!!.size
            buffer.send(GetRequest(objectId, requester))
        }

        data class GetRequest(val objectId: String, val requestor: ObjectRequester)
    }

    private fun collectBufferSizes(result: ObservableLongMeasurement) {
        for (s in servers){
            val attr = Attributes.builder()
                .put(AttributeKey.stringKey("storage.server.id"), s.idx.toString())
                .put(AttributeKey.longKey("storage.server.num"), numServers)
                .put(AttributeKey.longKey("storage.cpu.count"), numCores)
                .put(AttributeKey.doubleKey("storage.cpu.speed"), speed)
                .build()
            result.record(s.bufferAggSize, attr)
        }
    }


    private fun collectIdleTimes(result: ObservableLongMeasurement) {
        val now = scope.clock.millis()

        for (s in servers){
            val attr = Attributes.builder()
                .put(AttributeKey.stringKey("storage.server.id"), s.idx.toString())
                .put(AttributeKey.longKey("storage.server.num"), numServers)
                .put(AttributeKey.longKey("storage.cpu.count"), numCores)
                .put(AttributeKey.doubleKey("storage.cpu.speed"), speed)
                .build()

            var idleTime = s.idleTime

            for (i in 0 until s.numCores.toInt()){
                val times = s.times[i]
                if (times.first > times.second){
                    idleTime += now.minus(times.first).milliseconds
                    s.times[i] = Pair(now, times.second)
                }
                s.previousCollect = now
            }
            result.record(idleTime.toLong(DurationUnit.MILLISECONDS), attr)

            s.idleTime = Duration.ZERO
        }
    }
}


public interface ObjectRequester {
    public suspend fun Receive(obj: Object?)
}

public data class Object(val id: String, val size: Long)

