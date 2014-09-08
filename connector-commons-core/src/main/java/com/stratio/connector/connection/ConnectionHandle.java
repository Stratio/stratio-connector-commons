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
package com.stratio.connector.connection;


import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jmgomez on 28/08/14.
 */
public abstract class ConnectionHandle {

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
    private Map<String,Connection> connections = new HashMap<>();

    /**
     * Constructor.
     * @param configuration the general settings.
     */
    public ConnectionHandle(IConfiguration configuration) {
        this.configuration = configuration;

    }

    /**
     * This method create a connection.
     * @param credentials the cluster configuration.
     * @param config the connection options.
     */
    public void createConnection(ICredentials credentials, ConnectorClusterConfig config) {
        Connection connection = createConcreteConnection(credentials, config);

        connections.put(config.getName().getName(), connection);

        logger.info("Create a ElasticSearch connection [clusterName]");
        //TODO que hacer si ya existe la conexion.

    }



    public void closeConnection(String clusterName) {
        if (connections.containsKey(clusterName)){
            connections.get(clusterName).close();
            connections.remove(clusterName);
            logger.info("Disconnected from Elasticsearch ["+clusterName+"]");
        }
    }

    public boolean isConnected(String clusterName) {
        boolean isConnected = false;
        if (connections.containsKey(clusterName)){
            isConnected =  connections.get(clusterName).isConnect();
        }
        return  isConnected;
    }


    protected abstract Connection createConcreteConnection(ICredentials credentials, ConnectorClusterConfig config);

    public Connection getConnection(String name) {
        Connection connection = null;
        if (connections.containsKey(name)){
            connection = connections.get(name);
        }else{
            //REVIEW lanzar excepcion
        }
        return connection;
    }



}
