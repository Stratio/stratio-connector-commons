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

package com.stratio.connector.commons;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * This class represent a logical connection to a database.
 */
public abstract class CommonsConnector implements IConnector {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The connectionHandler.
     */
    protected ConnectionHandler connectionHandler = null;

    /**
     * Create a logical connection.
     *
     * @param credentials the credentials.
     * @param config      the connection configuration.
     * @throws ConnectionException if the connection fail.
     */
    @Override
    public final void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {
        try {
            connectionHandler.createConnection(credentials, config);
        } catch (HandlerConnectionException e) {
            String msg = "fail creating the Connection. " + e.getMessage();
            logger.error(msg);
            throw new ConnectionException(msg, e);
        }
    }

    /**
     * It close the logical connection.
     *
     * @param clusterName the connection identifier.
     */
    @Override
    public final void close(ClusterName clusterName) {
        connectionHandler.closeConnection(clusterName.getName());

    }

    /**
     * This method shutdown the connector.
     * @throws ExecutionException if an error happens.
     */
    @Override
    public final void shutdown() throws ExecutionException {

        Map<String, Connection> connections = connectionHandler.getConnections();
        for (String connectionName : connections.keySet()) {
            connectionHandler.endWork(connectionName);
            connectionHandler.closeConnection(connectionName);
        }
    }

    /**
     * The connection status.
     *
     * @param name the cluster Name.
     * @return true if connection is open. False in other case.
     */
    @Override
    public final boolean isConnected(ClusterName name) {

        return connectionHandler.isConnected(name.getName());

    }

}
