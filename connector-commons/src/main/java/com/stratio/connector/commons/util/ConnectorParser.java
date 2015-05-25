/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.commons.util;

/**
 * This class is the responsible to parse the information. Created by jmgomez on 3/09/14.
 */
public final class ConnectorParser {

    /**
     * Private constructor.
     */
    private ConnectorParser() {

    }

    /**
     * This method parse the hosts string. Use propertyValueRecovered.
     *
     * @param hosts the hosts string.
     * @return the hosts in an Array.
     * @deprecated use PropertyValueRecovered.
     */
    @Deprecated
    public static String[] hosts(String hosts) {
        return hosts.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("]", "").split(",");

    }

    /**
     * This method parse the ips string. Use propertyValueRecovered.
     *
     * @param ips the ips string.
     * @return the ips in an Array.
     * @deprecated use PropertyValueRecovered.
     */
    @Deprecated
    public static String[] ports(String ips) {
        return ips.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("]", "").split(",");
    }

    /**
     * This method parse the ips string. Use propertyValueRecovered.
     *
     * @param hostPorts the host:ips string.
     * @return the host:ips in an Array.
     * @deprecated use PropertyValueRecovered.
     */
    @Deprecated
    public static String[] hostPorts(String hostPorts) {
        return hostPorts.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("]", "").split(",");
    }

}
