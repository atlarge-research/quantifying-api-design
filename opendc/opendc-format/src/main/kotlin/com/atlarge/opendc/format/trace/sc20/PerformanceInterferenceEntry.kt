package com.atlarge.opendc.format.trace.sc20

internal data class PerformanceInterferenceEntry(
    val vms: List<String>,
    val performanceScore: Double
)
