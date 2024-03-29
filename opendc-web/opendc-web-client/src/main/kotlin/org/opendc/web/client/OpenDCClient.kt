/*
 * Copyright (c) 2022 AtLarge Research
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

package org.opendc.web.client

import org.opendc.web.client.auth.AuthController
import org.opendc.web.client.transport.HttpTransportClient
import org.opendc.web.client.transport.TransportClient
import java.net.URI

/**
 * Client implementation for the user-facing OpenDC REST API (version 2).
 *
 * @param client Low-level client for managing the underlying transport.
 */
public class OpenDCClient(client: TransportClient) {
    /**
     * Construct a new [OpenDCClient].
     *
     * @param baseUrl The base url of the API.
     * @param auth Helper class for managing authentication.
     */
    public constructor(baseUrl: URI, auth: AuthController) : this(HttpTransportClient(baseUrl, auth))

    /**
     * A resource for the available projects.
     */
    public val projects: ProjectResource = ProjectResource(client)

    /**
     * A resource for the topologies available to the user.
     */
    public val topologies: TopologyResource = TopologyResource(client)

    /**
     * A resource for the portfolios available to the user.
     */
    public val portfolios: PortfolioResource = PortfolioResource(client)

    /**
     * A resource for the scenarios available to the user.
     */
    public val scenarios: ScenarioResource = ScenarioResource(client)

    /**
     * A resource for the available schedulers.
     */
    public val schedulers: SchedulerResource = SchedulerResource(client)

    /**
     * A resource for the available workload traces.
     */
    public val traces: TraceResource = TraceResource(client)
}
