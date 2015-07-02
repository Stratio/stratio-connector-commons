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

import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.security.ICredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represent a logical connection to a database.
 */
public abstract class CommonsConnector implements IConnector {

    /**
     * The Log.
     */
    private final transient Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The connectionHandler.
     */
    protected ConnectionHandler connectionHandler = null;

    /**
     * String  that contains the path to connector manifest.
     */
    private String connectorManifestPath;

    /**
     * String that contains the path to datastore manifest.
     */
    private String[] datastoreManifestPath=new String[1];

    /**
     * Buils a instance setting the  connector Manifest and the datastore Manifest.
     *
     *
     * @param connectorManifestFileName the filename of the connector manifest, Must be inside the the classpath.
     * @param datastoreManifestFileName the filename of the datastore manifest, Must be inside the the classpath.
     */
    public CommonsConnector(String connectorManifestFileName, String datastoreManifestFileName){
        connectorManifestPath=getClass().getResource(connectorManifestFileName).getPath();
        datastoreManifestPath[0]=getClass().getResource(datastoreManifestFileName).getPath();
    }
    /**
     * Create a logical connection.
     *
     * @param credentials the credentials.
     * @param config      the connection configuration.
     * @throws ConnectionException if the connection fail.
     */
    @Override
    public void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {
        logger.info("Conecting connector [" + this.getClass().getSimpleName() + "]");
        connectionHandler.createConnection(credentials, config);

    }

    /**
     * It close the logical connection.
     *
     * @param clusterName the connection identifier.
     */
    @Override
    public void close(ClusterName clusterName) {
        logger.info("Close connection to cluster [" + clusterName + "] from connector [" + this.getClass().getSimpleName()  + "]");
        connectionHandler.closeConnection(clusterName.getName());

    }

    /**
     * This method closeAllConnections the connector.
     *
     * @throws ExecutionException if an error happens.
     */
    @Override
    public void shutdown() throws ExecutionException {
        logger.info("shutting down connector [" + this.getClass().getSimpleName()  + "]");
        connectionHandler.closeAllConnections();

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

    /**
     * Return the connector manifestPath.
     * @return the connector manifestPath.
     */
    @Override
    public String getConnectorManifestPath() {
        return connectorManifestPath;

    }

    /**
     * Return the Datastore manifestPath.
     * @return the Datastore manifestPath.
     */
    @Override
    public String[] getDatastoreManifestPath() {
        return datastoreManifestPath.clone();
    }
}
