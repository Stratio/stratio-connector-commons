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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;

/**
 * This abstract class is a Template for MetadataEngines.
 * <p/>
 * Created by dgomez on 22/09/14.
 *
 * @param <T>
 *            the native client
 */
public abstract class CommonsMetadataEngine<T> implements IMetadataEngine {

    /**
     * The Log.
     */
    final transient Logger logger = LoggerFactory.getLogger(this.getClass());

    final static String GENERIC_CONNECTOR_UNAVAILABLE = "Error retrieving the connection in %s.%s";

    /**
     * The connection handler.
     */
    transient ConnectionHandler connectionHandler;

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
     *            the target cluster witch the catalog will be created.
     * @param catalogMetadata
     *            the catalog metadata info.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if a error happens.
     */
    @Override
    public final void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata)
                    throws UnsupportedException, ExecutionException {
        try {

            connectionHandler.startWork(targetCluster.getName());
            createCatalog(catalogMetadata, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = String.format(GENERIC_CONNECTOR_UNAVAILABLE, targetCluster.getName(), e.getMessage());
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster.getName());
        }
    }

    /**
     * This method creates a table.
     *
     * @param targetCluster
     *            the target cluster witch the table will be created.
     * @param tableMetadata
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if a error happens.
     */
    @Override
    public final void createTable(ClusterName targetCluster, TableMetadata tableMetadata) throws UnsupportedException,
                    ExecutionException {
        try {
            connectionHandler.startWork(targetCluster.getName());
            createTable(tableMetadata, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = String.format(GENERIC_CONNECTOR_UNAVAILABLE, targetCluster.getName(), e.getMessage());
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster.getName());
        }
    }

    /**
     * This method drop a catalog.
     *
     * @param targetCluster
     *            the target cluster witch the catalog will be dropped.
     * @param name
     *            the catalog name.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if a error happens.
     */
    @Override
    public final void dropCatalog(ClusterName targetCluster, CatalogName name) throws UnsupportedException,
                    ExecutionException {
        try {
            connectionHandler.startWork(targetCluster.getName());
            dropCatalog(name, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = String.format(GENERIC_CONNECTOR_UNAVAILABLE, targetCluster.getName(), e.getMessage());
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster.getName());
        }

    }

    /**
     * This method drop a table.
     *
     * @param targetCluster
     *            the target cluster witch the table will be dropped.
     * @param name
     *            the table name.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if a error happens.
     */
    @Override
    public final void dropTable(ClusterName targetCluster, TableName name) throws UnsupportedException,
                    ExecutionException {
        try {
            connectionHandler.startWork(targetCluster.getName());
            dropTable(name, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = String.format(GENERIC_CONNECTOR_UNAVAILABLE, targetCluster.getName(), e.getMessage());
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster.getName());
        }
    }

    /**
     * This method creates an index.
     *
     * @param targetCluster
     *            the target cluster witch the index will be created.
     * @param indexMetadata
     *            the index metainformation.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if a error happens.
     */
    @Override
    public final void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata) throws UnsupportedException,
                    ExecutionException {
        try {
            connectionHandler.startWork(targetCluster.getName());
            createIndex(indexMetadata, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = String.format(GENERIC_CONNECTOR_UNAVAILABLE, targetCluster.getName(), e.getMessage());
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster.getName());
        }
    }

    /**
     * This method drop an index.
     *
     * @param targetCluster
     *            the target cluster witch the index will be dropped.
     * @param indexMetadata
     *            the index metainformation.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if a error happens.
     */
    @Override
    public final void dropIndex(ClusterName targetCluster, IndexMetadata indexMetadata) throws UnsupportedException,
                    ExecutionException {
        try {
            connectionHandler.startWork(targetCluster.getName());
            dropIndex(indexMetadata, connectionHandler.getConnection(targetCluster.getName()));
        } catch (HandlerConnectionException e) {
            String msg = String.format(GENERIC_CONNECTOR_UNAVAILABLE, targetCluster.getName(), e.getMessage());
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster.getName());
        }
    }

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
     *             if a error happens.
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
     *             if a error happens.
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
     *             if a error happens.
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
     *             if a error happens.
     */
    protected abstract void dropTable(TableName name, Connection<T> connection) throws UnsupportedException,
                    ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to create a index.
     *
     * @param indexMetadata
     *            the index metadata.
     * @param connection
     *            the connection.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if a error happens.
     */
    protected abstract void createIndex(IndexMetadata indexMetadata, Connection<T> connection)
                    throws UnsupportedException, ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to drop a index.
     *
     * @param indexMetadata
     *            the index metadata.
     * @param connection
     *            the connection.
     * @throws UnsupportedException
     *             if an operation is not supported.
     * @throws ExecutionException
     *             if a error happens.
     */
    protected abstract void dropIndex(IndexMetadata indexMetadata, Connection<T> connection)
                    throws UnsupportedException, ExecutionException;

}
