package org.opendc.telemetry.compute

import org.opendc.telemetry.compute.table.ServerInfo
import org.opendc.telemetry.compute.table.StorageInfo
import java.time.Instant

public interface StorageTableReader {
        /**
         * The timestamp of the current entry of the reader.
         */
        public val timestamp: Instant

        /**
         * The [ServerInfo] of the server to which the row belongs to.
         */
        public val storage: StorageInfo

        public val serverId: String?

        public val bufferSize: Long

        public val idleTime: Long
}
