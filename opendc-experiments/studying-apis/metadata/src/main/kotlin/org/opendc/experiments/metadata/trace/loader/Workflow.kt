package org.opendc.experiments.metadata.trace.loader

import java.time.Instant

data class Workflow(val id: String, val startTime: Instant, val cpuCount: Int, val tasks: MutableList<Task>)

data class Task(val id: String, val runtime: Long, val objectID: String, val objectSize: Long)
