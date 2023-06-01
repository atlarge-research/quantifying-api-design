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

package org.opendc.experiments.metadata.trace.format

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.opendc.trace.*
import org.opendc.trace.util.parquet.LocalParquetReader
import java.time.Instant

/**
 * A [TableReader] implementation for the resources table in the OpenDC virtual machine trace format.
 */
internal class OdcVmResourceTableReader(private val reader: LocalParquetReader<GenericRecord>) : TableReader {
    /**
     * The current record.
     */
    private var record: GenericRecord? = null

    /**
     * A flag to indicate that the columns have been initialized.
     */
    private var hasInitializedColumns = false

    override fun nextRow(): Boolean {
        val record = reader.read()
        this.record = record

        if (!hasInitializedColumns && record != null) {
            initColumns(record.schema)
            hasInitializedColumns = true
        }

        return record != null
    }

    override fun resolve(column: TableColumn<*>): Int = columns[column] ?: -1

    override fun isNull(index: Int): Boolean {
        check(index in 0..columns.size) { "Invalid column index" }
        return get(index) == null
    }

    override fun get(index: Int): Any? {
        val record = checkNotNull(record) { "Reader in invalid state" }

        return when (index) {
            COL_ID -> record[AVRO_COL_ID].toString()
            COL_WORKFLOW_ID -> record[AVRO_COL_WORKFLOW_ID].toString()
            COL_START_TIME -> Instant.ofEpochMilli(record[AVRO_COL_START_TIME] as Long)
            COL_RUNTIME -> getLong(index)
            COL_CPU_COUNT -> getLong(index)
            COL_OBJECT_ID -> record[AVRO_COL_OBJECT_ID].toString()
            COL_OBJECT_SIZE -> getLong(index)
            else -> throw IllegalArgumentException("Invalid column")
        }
    }

    override fun getBoolean(index: Int): Boolean {
        throw IllegalArgumentException("Invalid column")
    }

    override fun getInt(index: Int): Int {
        val record = checkNotNull(record) { "Reader in invalid state" }

        return when (index) {
            COL_CPU_COUNT -> record[AVRO_COL_CPU_COUNT] as Int
            COL_RUNTIME -> record[AVRO_COL_RUNTIME] as Int
            else -> throw IllegalArgumentException("Invalid column")
        }
    }

    override fun getLong(index: Int): Long {
        val record = checkNotNull(record) { "Reader in invalid state" }

        return when (index) {
            COL_CPU_COUNT -> record[AVRO_COL_CPU_COUNT] as Long
            COL_RUNTIME -> record[AVRO_COL_RUNTIME] as Long
            COL_OBJECT_SIZE -> record[AVRO_COL_OBJECT_SIZE] as Long
            else -> throw IllegalArgumentException("Invalid column")
        }
    }

    override fun getDouble(index: Int): Double {
        val record = checkNotNull(record) { "Reader in invalid state" }
        throw IllegalArgumentException("Invalid column")
    }

    override fun close() {
        reader.close()
    }

    override fun toString(): String = "OdcVmResourceTableReader"

    /**
     * Initialize the columns for the reader based on [schema].
     */
    private fun initColumns(schema: Schema) {
        try {
            AVRO_COL_ID = schema.getField("id").pos()
            AVRO_COL_WORKFLOW_ID = schema.getField("workflow_id").pos()
            AVRO_COL_START_TIME = (schema.getField("start_time") ?: schema.getField("submissionTime")).pos()
            AVRO_COL_START_TIME = schema.getField("start_time").pos()
            AVRO_COL_RUNTIME = schema.getField("runtime").pos()
            AVRO_COL_CPU_COUNT = schema.getField("cpu_count").pos()
            AVRO_COL_OBJECT_ID = schema.getField("object_id").pos()
            AVRO_COL_OBJECT_SIZE = schema.getField("object_size").pos()
        } catch (e: NullPointerException) {
            // This happens when the field we are trying to access does not exist
            throw IllegalArgumentException("Invalid schema")
        }
    }

    private var AVRO_COL_ID = -1
    private var AVRO_COL_WORKFLOW_ID = -1
    private var AVRO_COL_START_TIME = -1
    private var AVRO_COL_RUNTIME = -1
    private var AVRO_COL_CPU_COUNT = -1
    private var AVRO_COL_OBJECT_ID = -1
    private var AVRO_COL_OBJECT_SIZE = -1

    private val columns = mapOf<TableColumn<*>, Int>()
        /*RESOURCE_ID to COL_ID,
        RESOURCE_START_TIME to COL_START_TIME,
        RESOURCE_STOP_TIME to COL_STOP_TIME,
        RESOURCE_CPU_COUNT to COL_CPU_COUNT,
        RESOURCE_CPU_CAPACITY to COL_CPU_CAPACITY,
        RESOURCE_MEM_CAPACITY to COL_MEM_CAPACITY,
        RESOURCE_CPU_UTILIZATION to COL_CPU_UTILIZATION,
    )*/
}

public val COL_ID = 0
public val COL_WORKFLOW_ID = 1
public val COL_START_TIME = 2
public val COL_RUNTIME = 3
public val COL_CPU_COUNT = 4
public val COL_OBJECT_ID = 5
public val COL_OBJECT_SIZE = 6
