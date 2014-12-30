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

package com.stratio.connector.commons.engine;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;

/**
 * This abstract class is a Template for MetadataEngines.
 * <p/>
 * Created by dgomez on 22/09/14.
 *
 * @param <T>
 *            the native client
 */
public abstract class CommonsMetadataEngine<T> implements IMetadataEngine {

    private static final String GENERIC_CONNECTOR_UNAVAILABLE = "Error retrieving the connection for %s in %s.%s";
    /**
     * The Log.
     */
    private final transient Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * The connection handler.
     */
    private transient ConnectionHandler connectionHandler;

    /**
     * Constructor.
     *
     * @param connectionHandler
     *            the connector handler.
     */
    protected CommonsMetadataEngine(ConnectionHandler connectionHandler) {

        this.connectionHandler = connectionHandler;
    }

    /**
     * This method creates a catalog.
     *
     * @param targetCluster
     *            the target cluster where the catalog will be created.
     * @param catalogMetadata
     *            the catalog metadata info.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if an error happens.
     */
    @Override
    public final void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata)
                    throws ExecutionException, UnsupportedException {

        connectionHandler.startJob(targetCluster.getName());

        try {
            createCatalog(catalogMetadata, connectionHandler.getConnection(targetCluster.getName()));
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }

    }

    /**
     * This method creates a table.
     *
     * @param targetCluster
     *            the target cluster where the table will be created.
     * @param tableMetadata
     *            the table metadata.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if an error happens.
     */
    @Override
    public final void createTable(ClusterName targetCluster, TableMetadata tableMetadata) throws UnsupportedException,

    ExecutionException {

        connectionHandler.startJob(targetCluster.getName());

        try {
            createTable(tableMetadata, connectionHandler.getConnection(targetCluster.getName()));
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    /**
     * This method drop a catalog.
     *
     * @param targetCluster
     *            the target cluster where the catalog will be dropped.
     * @param name
     *            the catalog name.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if an error happens.
     */
    @Override
    public final void dropCatalog(ClusterName targetCluster, CatalogName name) throws UnsupportedException,

    ExecutionException {
        connectionHandler.startJob(targetCluster.getName());

        try {
            dropCatalog(name, connectionHandler.getConnection(targetCluster.getName()));
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }

    }

    /**
     * This method drop a table.
     *
     * @param targetCluster
     *            the target cluster where the table will be dropped.
     * @param name
     *            the table name.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if an error happens.
     */
    @Override
    public final void dropTable(ClusterName targetCluster, TableName name) throws UnsupportedException,

    ExecutionException {

        connectionHandler.startJob(targetCluster.getName());

        try {
            dropTable(name, connectionHandler.getConnection(targetCluster.getName()));
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    /**
     * This method creates an index.
     *
     * @param targetCluster
     *            the target cluster where the index will be created.
     * @param indexMetadata
     *            the index metainformation.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if an error happens.
     */
    @Override
    public final void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata) throws UnsupportedException,

    ExecutionException {

        connectionHandler.startJob(targetCluster.getName());

        try {

            createIndex(indexMetadata, connectionHandler.getConnection(targetCluster.getName()));

        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    /**
     * This method drop an index.
     *
     * @param targetCluster
     *            the target cluster where the index will be dropped.
     * @param indexMetadata
     *            the index metainformation.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if an error happens.
     */
    @Override
    public final void dropIndex(ClusterName targetCluster, IndexMetadata indexMetadata) throws UnsupportedException,
                    ExecutionException {
        try {
            connectionHandler.startJob(targetCluster.getName());
            dropIndex(indexMetadata, connectionHandler.getConnection(targetCluster.getName()));
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    /**
     * This method add, delete, or modify columns in an existing table.
     *
     * @param targetCluster
     *            the target cluster where the table will be altered.
     * @param name
     *            the table name.
     * @param alterOptions
     *            the alter options.
     * @throws UnsupportedException
     *             the unsupported exception
     * @throws ExecutionException
     *             the execution exception
     */
    @Override
    public void alterTable(ClusterName targetCluster, TableName name, AlterOptions alterOptions)
                    throws UnsupportedException, ExecutionException {
        try {
            connectionHandler.startJob(targetCluster.getName());
            alterTable(name, alterOptions, connectionHandler.getConnection(targetCluster.getName()));
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    /**
     * Alter options in an existing table.
     *
     * @param targetCluster
     *            the target cluster where the catalog will be altered.
     * @param catalogName
     *            the catalog name
     * @param options
     *            the options
     * @throws UnsupportedException
     *             if the operation is not supported
     * @throws ExecutionException
     *             if any error happen during the execution
     */
    @Override
    public void alterCatalog(ClusterName targetCluster, CatalogName catalogName, Map<Selector, Selector> options)
                    throws UnsupportedException, ExecutionException {
        try {
            connectionHandler.startJob(targetCluster.getName());
            alterCatalog(catalogName, options, connectionHandler.getConnection(targetCluster.getName()));
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    @Override
    public List<CatalogMetadata> provideMetadata(ClusterName targetCluster) throws ConnectorException {
        try {
            connectionHandler.startJob(targetCluster.getName());
            return provideMetadata(targetCluster, connectionHandler.getConnection(targetCluster.getName()));
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    @Override
    public CatalogMetadata provideCatalogMetadata(ClusterName targetCluster, CatalogName catalogName)
                    throws ConnectorException {
        try {
            connectionHandler.startJob(targetCluster.getName());
            return provideCatalogMetadata(catalogName, targetCluster,
                            connectionHandler.getConnection(targetCluster.getName()));
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    @Override
    public TableMetadata provideTableMetadata(ClusterName targetCluster, TableName tableName) throws ConnectorException {
        try {
            connectionHandler.startJob(targetCluster.getName());
            return provideTableMetadata(tableName, targetCluster,
                            connectionHandler.getConnection(targetCluster.getName()));
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    protected abstract List<CatalogMetadata> provideMetadata(ClusterName targetCluster, Connection<T> connection)
                    throws ConnectorException;

    protected abstract CatalogMetadata provideCatalogMetadata(CatalogName catalogName, ClusterName targetCluster,
                    Connection<T> connection) throws ConnectorException;

    protected abstract TableMetadata provideTableMetadata(TableName tableName, ClusterName targetCluster,
                    Connection<T> connection) throws ConnectorException;

    /**
     * Alter options in an existing table.
     *
     * @param catalogName
     *            the catalog name.
     * @param options
     *            the options.
     * @param connection
     *            the connection.
     * @throws UnsupportedException
     *             if the operation is not supported.
     * @throws ExecutionException
     *             if any error happen during the execution.
     */
    protected abstract void alterCatalog(CatalogName catalogName, Map<Selector, Selector> options,
                    Connection<T> connection) throws UnsupportedException, ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to add, delete, or modify
     * columns in an existing table.
     *
     * @param name
     *            the table name.
     * @param alterOptions
     *            the alter options.
     * @param connection
     *            the connection.
     */
    protected abstract void alterTable(TableName name, AlterOptions alterOptions, Connection<T> connection)
                    throws UnsupportedException, ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to create a catalog.
     *
     * @param catalogMetadata
     *            the catalog metadata.
     * @param connection
     *            the connection.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if an error happens.
     */
    protected abstract void createCatalog(CatalogMetadata catalogMetadata, Connection<T> connection)
                    throws UnsupportedException, ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to create a table.
     *
     * @param tableMetadata
     *            the table metadata.
     * @param connection
     *            the connection.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if an error happens.
     */
    protected abstract void createTable(TableMetadata tableMetadata, Connection<T> connection)
                    throws UnsupportedException, ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to drop a catalog.
     *
     * @param name
     *            the catalog name.
     * @param connection
     *            the connection.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if an error happens.
     */
    protected abstract void dropCatalog(CatalogName name, Connection<T> connection) throws UnsupportedException,
                    ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to drop a table.
     *
     * @param name
     *            the catalog name.
     * @param connection
     *            the connection.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if an error happens.
     */
    protected abstract void dropTable(TableName name, Connection<T> connection) throws UnsupportedException,
                    ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to create an index.
     *
     * @param indexMetadata
     *            the index metadata.
     * @param connection
     *            the connection.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if an error happens.
     */
    protected abstract void createIndex(IndexMetadata indexMetadata, Connection<T> connection)
                    throws UnsupportedException, ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to drop an index.
     *
     * @param indexMetadata
     *            the index metadata.
     * @param connection
     *            the connection.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if an error happens.
     */
    protected abstract void dropIndex(IndexMetadata indexMetadata, Connection<T> connection)
                    throws UnsupportedException, ExecutionException;

}
