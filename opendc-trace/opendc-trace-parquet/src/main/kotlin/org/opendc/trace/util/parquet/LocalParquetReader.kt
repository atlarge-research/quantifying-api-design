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

package org.opendc.trace.util.parquet

import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.io.InputFile
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.isDirectory

/**
 * A helper class to read Parquet files.
 *
 * @param path The path to the Parquet file or directory to read.
 */
public class LocalParquetReader<out T>(path: Path) : AutoCloseable {
    /**
     * The input files to process.
     */
    private val filesIterator = if (path.isDirectory())
        Files.list(path)
            .filter { !it.isDirectory() }
            .sorted()
            .map { LocalInputFile(it) }
            .iterator()
    else
        listOf(LocalInputFile(path)).iterator()

    /**
     * The Parquet reader to use.
     */
    private var reader: ParquetReader<T>? = null

    /**
     * Construct a [LocalParquetReader] for the specified [file].
     */
    public constructor(file: File) : this(file.toPath())

    /**
     * Read a single entry in the Parquet file.
     */
    public fun read(): T? {
        return try {
            val next = reader?.read()
            if (next != null) {
                next
            } else {
                initReader()

                if (reader == null)
                    null
                else
                    read()
            }
        } catch (e: InterruptedException) {
            throw IOException(e)
        }
    }

    /**
     * Close the Parquet reader.
     */
    override fun close() {
        reader?.close()
    }

    /**
     * Initialize the next reader.
     */
    private fun initReader() {
        reader?.close()

        this.reader = if (filesIterator.hasNext()) {
            createReader(filesIterator.next())
        } else {
            null
        }
    }

    /**
     * Create a Parquet reader for the specified file.
     */
    private fun createReader(input: InputFile): ParquetReader<T> {
        return AvroParquetReader
            .builder<T>(input)
            .disableCompatibility()
            .build()
    }
}
