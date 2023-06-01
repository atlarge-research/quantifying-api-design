package org.opendc.compute.workload.export.parquet

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import org.opendc.telemetry.compute.StorageTableReader
import org.opendc.trace.util.parquet.TIMESTAMP_SCHEMA
import java.io.File

public class ParquetStorageDataWriter(path: File, bufferSize: Int) :
    ParquetDataWriter<StorageTableReader>(path, SCHEMA, bufferSize) {

    public override fun convert(builder: GenericRecordBuilder, data: StorageTableReader) {
        builder["timestamp"] = data.timestamp.toEpochMilli()
        builder["storage_cpu_count"] = data.storage.serversCpuCount
        builder["storage_cpu_speed"] = data.storage.serversCpuSpeed
        builder["storage_servers_num"] = data.storage.serversNum
        builder["storage_server_id"] = data.serverId
        builder["storage_server_buffer_size"] = data.bufferSize
        builder["storage_server_idle_time"] = data.idleTime
    }

    override fun toString(): String = "storage-writer"

    private companion object {
        private val SCHEMA: Schema = SchemaBuilder
            .record("storage")
            .namespace("org.opendc.telemetry.compute")
            .fields()
            .name("timestamp").type(TIMESTAMP_SCHEMA).noDefault()
            .requiredString("storage_server_id")
            .requiredLong("storage_cpu_count")
            .requiredLong("storage_servers_num")
            .requiredDouble("storage_cpu_speed")
            .requiredLong("storage_server_buffer_size")
            .requiredLong("storage_server_idle_time")
            .endRecord()
    }
}
