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
package com.stratio.connector.commons.connection;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * This class is the responsible to handle the connections. Created by jmgomez on 28/08/14.
 */
public abstract class ConnectionHandler {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * The general settings.
     */
    protected IConfiguration configuration;

    /**
     * The connections.
     */
    private Map<String, Connection> connections = new HashMap<>();

    /**
     * Constructor.
     *
     * @param configuration
     *            the general settings.
     */
    public ConnectionHandler(IConfiguration configuration) {
        this.configuration = configuration;

    }

    /**
     * This method create a connection.
     *
     * @param credentials
     *            the cluster configuration.
     * @param config
     *            the connection options.
     * @throws HandlerConnectionException
     *             if the connection already exists.
     */
    public void createConnection(ICredentials credentials, ConnectorClusterConfig config)
                    throws HandlerConnectionException {
        Connection connection = createNativeConnection(credentials, config);

        String connectionName = config.getName().getName();
        if (!connections.containsKey(connectionName)) {
            connections.put(connectionName, connection);
            logger.info("Connected to [" + connectionName + "]");

        } else {
            throw new HandlerConnectionException("The connection [" + connectionName + "] already exists");
        }

    }

    /**
     * Close the connection.
     *
     * @param clusterName
     *            the connection name to be closed.
     */
    public void closeConnection(String clusterName) {
        if (connections.containsKey(clusterName)) {
            connections.get(clusterName).close();
            connections.remove(clusterName);
            logger.info("Disconnected from [" + clusterName + "]");
        }
    }

    /**
     * Return if a connection is connected.
     *
     * @param clusterName
     *            the connection name.
     * @return true if the connection is connected. False in other case.
     */
    public boolean isConnected(String clusterName) {
        boolean isConnected = false;
        if (connections.containsKey(clusterName)) {
            isConnected = connections.get(clusterName).isConnect();
        }
        return isConnected;
    }

    /**
     * Create a connection for the concrete database.
     *
     * @param credentials
     *            the credentials.
     * @param config
     *            the config.
     * @return a connection.
     */
    protected abstract Connection createNativeConnection(ICredentials credentials, ConnectorClusterConfig config)
                    throws CreateNativeConnectionException;

    /**
     * This method return a connection.
     *
     * @param name
     *            the connection name.
     * @return the connection.
     * @throws HandlerConnectionException
     *             if the connection does not exist.
     */
    public Connection getConnection(String name) throws HandlerConnectionException {
        Connection connection = null;
        if (connections.containsKey(name)) {
            connection = connections.get(name);
        } else {
            throw new HandlerConnectionException("The connection [" + name + "] does not exist");
        }
        return connection;
    }

    /**
     * This method start a work for a connections.
     *
     * @param targetCluster
     *            the connections cluster name.
     */
    public void startWork(String targetCluster) {
        Connection conn = null;
        try {
            conn = getConnection(targetCluster);
            conn.setWorkInProgress(true);

        } catch (HandlerConnectionException e) {
            String msg = "Error getting the Connection. " + e.getMessage();
            logger.error(msg);

        }
    }

    /**
     * This method finalize a work for a connections.
     *
     * @param targetCluster
     *            the connections cluster name.
     */
    public void endWork(String targetCluster) {
        Connection conn = null;
        try {
            conn = getConnection(targetCluster);
            conn.setWorkInProgress(false);
        } catch (HandlerConnectionException e) {
            String msg = "Error getting the Connection. " + e.getMessage();
            logger.error(msg);
        }
    }

    /**
     * This method return the connections.
     *
     * @return the connection.
     */
    public Map<String, Connection> getConnections() {
        return connections;
    }

}
