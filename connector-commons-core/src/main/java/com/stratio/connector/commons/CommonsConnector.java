/*
 * Stratio Meta
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

package com.stratio.connector.commons;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;

/**
 * This class implements the connector for Elasticsearch.
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
     * Create a connection with ElasticSearch.
     *
     * @param credentials
     *            the credentials.
     * @param config
     *            the connection configuration.
     * @throws ConnectionException
     *             if the connection fail.
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
     * It close the connection to ElasticSearch.
     *
     * @param name
     *            the connection identifier.
     */
    @Override
    public final void close(ClusterName clusterName) {
        connectionHandler.closeConnection(clusterName.getName());

    }

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
     * @param name
     *            the cluster Name.
     * @return true if the driver's client is not null.
     */
    @Override
    public boolean isConnected(ClusterName name) {

        return connectionHandler.isConnected(name.getName());

    }

}
