/*
 * Stratio Deep
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */
package com.stratio.connector.commons.connection;


import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.security.ICredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is the responsible to handle the connections.
 * Created by jmgomez on 28/08/14.
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
     * @param configuration the general settings.
     */
    public ConnectionHandler(IConfiguration configuration) {
        this.configuration = configuration;

    }

    /**
     * This method create a connection.
     *
     * @param credentials the cluster configuration.
     * @param config      the connection options.
     * @throws HandlerConnectionException if the connection already exists.
     */
    public void createConnection(ICredentials credentials, ConnectorClusterConfig config) throws HandlerConnectionException {
        Connection connection = createNativeConnection(credentials, config);

        String connectionName = config.getName().getName();
        if (!connections.containsKey(connectionName)) {
            connections.put(connectionName, connection);
            logger.info("Create a connection [" + connectionName + "]");

        } else {
            throw new HandlerConnectionException("The connection [" + connectionName + "] already exists");
        }


    }


    /**
     * Close the connection.
     *
     * @param clusterName the connection name to be closed.
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
     * @param clusterName the connection name.
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
     * @param credentials the credentials.
     * @param config      the config.
     * @return a connection.
     */
    protected abstract Connection createNativeConnection(ICredentials credentials, ConnectorClusterConfig config) throws CreateNativeConnectionException;

    /**
     * This method return a connection.
     *
     * @param name the connection name.
     * @return the connection.
     * @throws HandlerConnectionException if the connection does not exist.
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

    public Map<String, Connection> getConnections() {
        return connections;
    }


}
