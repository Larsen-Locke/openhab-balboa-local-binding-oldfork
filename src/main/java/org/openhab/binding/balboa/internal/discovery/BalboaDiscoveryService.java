/**
 * Copyright (c) 2010-2026 Contributors to the openHAB project
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.openhab.binding.balboa.internal.discovery;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.openhab.binding.balboa.internal.BalboaBindingConstants;
import org.openhab.core.config.discovery.AbstractDiscoveryService;
import org.openhab.core.config.discovery.DiscoveryResult;
import org.openhab.core.config.discovery.DiscoveryResultBuilder;
import org.openhab.core.config.discovery.DiscoveryService;
import org.openhab.core.thing.ThingUID;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BalboaDiscoveryService} discovers Balboa Wi-Fi modules on the local network.
 * <p>
 * The module implements a simple UDP broadcast discovery protocol: a client broadcasts the
 * text "Discovery" to port 30303, and every Balboa Wi-Fi module on the local network segment
 * answers with a short text response starting with "BWGS", followed by its hostname and MAC
 * address on the next two lines. This is the same protocol used by Balboa's own mobile apps.
 *
 * @author Carsten Mogge - Initial contribution
 */
@NonNullByDefault
@Component(service = DiscoveryService.class, configurationPid = "discovery.balboa")
public class BalboaDiscoveryService extends AbstractDiscoveryService {

    private static final int DISCOVERY_PORT = 30303;
    private static final byte[] DISCOVERY_MESSAGE = "Discovery".getBytes(StandardCharsets.US_ASCII);
    private static final int SCAN_TIMEOUT_SECONDS = 5;
    private static final int SOCKET_TIMEOUT_MILLIS = 500;
    private static final int RESPONSE_BUFFER_SIZE = 512;

    private final Logger logger = LoggerFactory.getLogger(BalboaDiscoveryService.class);

    /**
     * Creates the discovery service.
     *
     * @throws IllegalArgumentException if the scan timeout is not valid
     */
    public BalboaDiscoveryService() throws IllegalArgumentException {
        super(Set.of(BalboaBindingConstants.THING_TYPE_BALBOA_IP), SCAN_TIMEOUT_SECONDS);
    }

    @Override
    protected void startScan() {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setBroadcast(true);
            socket.setSoTimeout(SOCKET_TIMEOUT_MILLIS);

            DatagramPacket request = new DatagramPacket(DISCOVERY_MESSAGE, DISCOVERY_MESSAGE.length,
                    InetAddress.getByName("255.255.255.255"), DISCOVERY_PORT);

            long deadline = System.currentTimeMillis() + SCAN_TIMEOUT_SECONDS * 1000L;
            while (System.currentTimeMillis() < deadline) {
                socket.send(request);

                // Collect responses to this broadcast until the socket read times out.
                boolean listening = true;
                while (listening) {
                    byte[] buffer = new byte[RESPONSE_BUFFER_SIZE];
                    DatagramPacket response = new DatagramPacket(buffer, buffer.length);
                    try {
                        socket.receive(response);
                        handleResponse(response);
                    } catch (SocketTimeoutException e) {
                        listening = false;
                    }
                }
            }
        } catch (IOException e) {
            logger.debug("Balboa discovery scan failed: {}", e.getMessage());
        }
    }

    /**
     * Parses a single discovery response and reports a {@link DiscoveryResult} if it is a valid
     * Balboa Wi-Fi module answer.
     */
    private void handleResponse(DatagramPacket response) {
        String payload = new String(response.getData(), response.getOffset(), response.getLength(),
                StandardCharsets.US_ASCII);
        if (!payload.toUpperCase().contains("BWGS")) {
            // Not a Balboa Wi-Fi module response, ignore.
            return;
        }

        String[] lines = payload.split("\r?\n");
        String hostname = lines.length > 0 ? lines[0].trim() : "";
        String macAddress = lines.length > 1 ? lines[1].trim() : "";
        String host = response.getAddress().getHostAddress();

        if (macAddress.isBlank()) {
            logger.debug("Ignoring Balboa discovery response from {} without a MAC address", host);
            return;
        }

        logger.debug("Discovered Balboa unit '{}' ({}) at {}", hostname, macAddress, host);

        ThingUID thingUID = new ThingUID(BalboaBindingConstants.THING_TYPE_BALBOA_IP, macAddress.replace(":", ""));

        Map<String, Object> properties = new HashMap<>();
        properties.put("host", host);
        properties.put("port", BalboaBindingConstants.DEFAULT_PORT);
        properties.put("uniqueID", macAddress);

        String label = hostname.isBlank() ? ("Balboa Spa (" + host + ")") : ("Balboa Spa (" + hostname + ")");

        DiscoveryResult discoveryResult = DiscoveryResultBuilder.create(thingUID).withProperties(properties)
                .withRepresentationProperty("uniqueID").withLabel(label).build();

        thingDiscovered(discoveryResult);
    }
}
