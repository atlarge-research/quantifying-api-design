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

package org.opendc.compute.workload.export.parquet

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import org.opendc.telemetry.compute.table.ServiceTableReader
import org.opendc.trace.util.parquet.TIMESTAMP_SCHEMA
import java.io.File

/**
 * A Parquet event writer for [ServiceTableReader]s.
 */
public class ParquetServiceDataWriter(path: File, bufferSize: Int) :
    ParquetDataWriter<ServiceTableReader>(path, SCHEMA, bufferSize) {

    override fun convert(builder: GenericRecordBuilder, data: ServiceTableReader) {
        builder["migrations"] = data.migrations
        builder["migrations_improvement"] = data.migrationsImprovement
        builder["migrations_penalty"] = data.migrationsPenalty
        builder["migrations_oversubscription"] = data.migrationsOversubscription
        builder["attempts_retry"] = data.attemptsRetry
        builder["migrations_success"] = data.migrationsSuccess
        builder["migrations_failure"] = data.migrationsFailure

        builder["timestamp"] = data.timestamp.toEpochMilli()
        builder["hosts_up"] = data.hostsUp
        builder["hosts_down"] = data.hostsDown
        builder["servers_pending"] = data.serversPending
        builder["servers_active"] = data.serversActive
        builder["attempts_success"] = data.attemptsSuccess
        builder["attempts_failure"] = data.attemptsFailure
        builder["attempts_error"] = data.attemptsError
    }

    override fun toString(): String = "service-writer"

    private companion object {
        private val SCHEMA: Schema = SchemaBuilder
            .record("service")
            .namespace("org.opendc.telemetry.compute")
            .fields()
            .name("timestamp").type(TIMESTAMP_SCHEMA).noDefault()
            .requiredInt("migrations")
            .requiredDouble("migrations_improvement")
            .requiredLong("migrations_penalty")
            .requiredDouble("migrations_oversubscription")
            .requiredInt("migrations_success")
            .requiredInt("migrations_failure")
            .requiredInt("hosts_up")
            .requiredInt("hosts_down")
            .requiredInt("servers_pending")
            .requiredInt("servers_active")
            .requiredInt("attempts_success")
            .requiredInt("attempts_retry")
            .requiredInt("attempts_failure")
            .requiredInt("attempts_error")
            .endRecord()
    }
}
