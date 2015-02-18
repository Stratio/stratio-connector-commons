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

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Relation;

/**
 * This abstract class is a Template for CommonsStorageEngine. Created by dgomez on 22/09/14.
 *
 * @param <T> the native client
 */
public abstract class CommonsStorageEngine<T> implements IStorageEngine {

    /**
     * The Log.
     */
    private transient final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The connection handler.
     */
    protected transient ConnectionHandler connectionHandler;

    /**
     * Constructor.
     *
     * @param connectionHandler the connector handler.
     */
    public CommonsStorageEngine(ConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }

    /**
     * Insert a single row in a table.
     *
     * @param targetCluster Target cluster.
     * @param targetTable   Target table metadata including fully qualified including catalog.
     * @param row           The row to be inserted.
     * @param isNotExists   Insert only if primary key doesn't exist yet.
     * @throws UnsupportedException If the required set of operations are not supported by the connector.
     * @throws ExecutionException   if the execution fails.
     */
    @Override
    public final void insert(ClusterName targetCluster, TableMetadata targetTable, Row row, boolean isNotExists)
            throws UnsupportedException, ExecutionException {

        try {
            connectionHandler.startJob(targetCluster.getName());
            if (logger.isDebugEnabled()) {
                logger.debug("Inserting one row in table [" + targetTable.getName().getName() + "] in cluster ["
                        + targetCluster.getName() + "]");
            }
            insert(targetTable, row, isNotExists, connectionHandler.getConnection(targetCluster.getName()));

            if (logger.isDebugEnabled()) {
                logger.debug("One row has been inserted successfully in table [" + targetTable.getName().getName()
                        + "]  in cluster [" + targetCluster.getName() + "]");
            }
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    /**
     * Insert a collection of rows in a table.
     *
     * @param targetCluster Target cluster.
     * @param targetTable   Target table metadata including fully qualified including catalog.
     * @param rows          Collection of rows to be inserted.
     * @param isNotExists   Insert only if primary key doesn't exist yet.
     * @throws UnsupportedException If the required set of operations are not supported by the connector.
     * @throws ExecutionException   if the execution fails.
     */
    @Override
    public final void insert(ClusterName targetCluster, TableMetadata targetTable, Collection<Row> rows,
            boolean isNotExists) throws UnsupportedException, ExecutionException {

        connectionHandler.startJob(targetCluster.getName());

        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Inserting several rows in table [" + targetTable.getName().getName() + "] in cluster ["
                        + targetCluster.getName() + "]");
            }
            insert(targetTable, rows, isNotExists, connectionHandler.getConnection(targetCluster.getName()));

            if (logger.isDebugEnabled()) {
                logger.debug("The rows has been inserted successfully in table [" + targetTable.getName().getName()
                        + "]  in cluster [" + targetCluster.getName() + "]");
            }
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    /**
     * This method updates data of a table according to some conditions.
     *
     * @param targetCluster the target cluster to insert.
     * @param tableName     Target table name including fully qualified including catalog.
     * @param assignments   Operations to be executed for every row.
     * @param whereClauses  Where clauses.
     * @throws ExecutionException   if an error occurs during the connection.
     * @throws UnsupportedException
     */
    @Override
    public void update(ClusterName targetCluster, TableName tableName, Collection<Relation> assignments,

            Collection<Filter> whereClauses) throws UnsupportedException, ExecutionException {

        try {
            connectionHandler.startJob(targetCluster.getName());
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Updating  table [" + tableName.getName() + "] in cluster [" + targetCluster.getName() + "]");
            }
            update(tableName, assignments, whereClauses, connectionHandler.getConnection(targetCluster.getName()));
            if (logger.isDebugEnabled()) {
                logger.debug("The  table [" + tableName.getName() + "] has been updated successfully in cluster ["
                        + targetCluster.getName() + "]");
            }
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    /**
     * This method deletes rows, on the indicated cluster, that meet the conditions of the where clauses.
     *
     * @param targetCluster the target cluster to insert.
     * @param tableName     Target table name including fully qualified including catalog.
     * @param whereClauses  Where clauses.
     * @throws ExecutionException   if an error occurs during the connection.
     * @throws UnsupportedException
     */
    @Override
    public void delete(ClusterName targetCluster, TableName tableName, Collection<Filter> whereClauses)
            throws ExecutionException, UnsupportedException {

        try {

            connectionHandler.startJob(targetCluster.getName());
            if (logger.isDebugEnabled()) {
                logger.debug("Deleting from  table [" + tableName.getName() + "] in cluster [" + targetCluster.getName()
                        + "]");
            }
            delete(tableName, whereClauses, connectionHandler.getConnection(targetCluster.getName()));
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "The rows has been successfully deleted in table [" + tableName.getName() + "] in cluster ["
                                + targetCluster.getName() + "]");
            }
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    /**
     * This method deletes all the rows of a table.
     *
     * @param targetCluster Target cluster.
     * @param tableName     Target table name including fully qualified including catalog.
     * @throws UnsupportedException
     * @throws ExecutionException
     */
    @Override
    public void truncate(ClusterName targetCluster, TableName tableName) throws UnsupportedException,
            ExecutionException {

        try {

            connectionHandler.startJob(targetCluster.getName());
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Tuncating table [" + tableName.getName() + "] in cluster [" + targetCluster.getName() + "]");
            }
            truncate(tableName, connectionHandler.getConnection(targetCluster.getName()));
            if (logger.isDebugEnabled()) {
                logger.debug("The table [" + tableName.getName() + "] has been successfully truncated in cluster ["
                        + targetCluster.getName() + "]");
            }
        } finally {
            connectionHandler.endJob(targetCluster.getName());
        }
    }

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to execute a truncate.
     *
     * @param tableName  Target table name including fully qualified including catalog.
     * @param connection the database connection.
     * @throws UnsupportedException
     * @throws ExecutionException
     */
    protected abstract void truncate(TableName tableName, Connection<T> connection) throws UnsupportedException,
            ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to execute a delete.
     *
     * @param tableName    Target table name including fully qualified including catalog.
     * @param whereClauses the filters to be applied.
     * @param connection   the database connection.
     * @throws UnsupportedException if an operation is not supported.
     * @throws ExecutionException   if a error happens.
     */
    protected abstract void delete(TableName tableName, Collection<Filter> whereClauses, Connection<T> connection)
            throws UnsupportedException, ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to execute an update.
     *
     * @param tableName    Target table name including fully qualified including catalog.
     * @param assignments  the assignments to update.
     * @param whereClauses the filters to be applied.
     * @param connection   the database connection.
     * @throws UnsupportedException if an operation is not supported.
     * @throws ExecutionException   if a error happens.
     */
    protected abstract void update(TableName tableName, Collection<Relation> assignments,
            Collection<Filter> whereClauses, Connection<T> connection) throws UnsupportedException,
            ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to execute a simple insert.
     *
     * @param targetTable Target table metadata including fully qualified including catalog.
     * @param row         The row to be inserted.
     * @param isNotExists Insert only if primary key doesn't exist yet.
     * @param connection  the database connection.
     * @throws UnsupportedException If the required set of operations are not supported by the connector.
     * @throws ExecutionException   if the execution fails.
     */
    protected abstract void insert(TableMetadata targetTable, Row row, boolean isNotExists, Connection<T> connection)
            throws UnsupportedException, ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to execute a bulk insert.
     *
     * @param targetTable Target table metadata including fully qualified including catalog.
     * @param rows        Collection of rows to be inserted.
     * @param isNotExists Insert only if primary key doesn't exist yet.
     * @param connection  the database connection.
     * @throws UnsupportedException If the required set of operations are not supported by the connector.
     * @throws ExecutionException   if the execution fails.
     */
    protected abstract void insert(TableMetadata targetTable, Collection<Row> rows, boolean isNotExists,
            Connection<T> connection) throws UnsupportedException, ExecutionException;
}
