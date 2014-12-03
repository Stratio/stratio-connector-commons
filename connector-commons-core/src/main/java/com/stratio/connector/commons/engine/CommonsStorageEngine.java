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
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
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
    final transient Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * The connection handler.
     */
    transient ConnectionHandler connectionHandler;

    /**
     * Constructor.
     *
     * @param connectionHandler the connector handler.
     */
    public CommonsStorageEngine(ConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }

    /**
     * This method inserts a single row in a table.
     *
     * @param targetCluster the target cluster to insert.
     * @param targetTable   the target table to insert.
     * @param row           the row to insert.
     * @throws UnsupportedException if an operation is not supported.
     * @throws ExecutionException   if a error happens.
     */
    @Override
    public final void insert(ClusterName targetCluster, TableMetadata targetTable, Row row)
            throws UnsupportedException, ExecutionException {
        try {
            connectionHandler.startWork(targetCluster.getName());
            insert(targetTable, row, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster.getName());
        }
    }

    /**
     * This method inserts a collection of rows in a table.
     *
     * @param targetCluster the target cluster to insert.
     * @param targetTable   the target table to insert.
     * @param rows          the rows to insert.
     * @throws UnsupportedException if an operation is not supported.
     * @throws ExecutionException   if an error happens.
     */
    @Override
    public final void insert(ClusterName targetCluster, TableMetadata targetTable, Collection<Row> rows)
            throws UnsupportedException, ExecutionException {
        try {
            connectionHandler.startWork(targetCluster.getName());
            insert(targetTable, rows, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster.getName());
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
            connectionHandler.startWork(targetCluster.getName());
            update(tableName, assignments, whereClauses, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster.getName());
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
            throws UnsupportedException, ExecutionException {
        try {
            connectionHandler.startWork(targetCluster.getName());
            delete(tableName, whereClauses, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster.getName());
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
    public void truncate(ClusterName targetCluster, TableName tableName)
            throws UnsupportedException, ExecutionException {
        try {
            connectionHandler.startWork(targetCluster.getName());
            truncate(tableName, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster.getName());
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
    protected abstract void truncate(TableName tableName, Connection<T> connection)
            throws UnsupportedException, ExecutionException;

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
            Collection<Filter> whereClauses, Connection<T> connection) throws UnsupportedException, ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to execute a simple insert.
     *
     * @param targetTable the target table to insert.
     * @param row         the row to insert.
     * @param connection  the database connection.
     * @throws UnsupportedException if an operation is not supported.
     * @throws ExecutionException   if a error happens.
     */
    protected abstract void insert(TableMetadata targetTable, Row row, Connection<T> connection)
            throws UnsupportedException, ExecutionException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to execute a bulk insert.
     *
     * @param targetTable the target table to insert.
     * @param rows        the row to insert.
     * @param connection  the database connection.
     * @throws UnsupportedException if an operation is not supported.
     * @throws ExecutionException   if a error happens.
     */
    protected abstract void insert(TableMetadata targetTable, Collection<Row> rows, Connection<T> connection)
            throws UnsupportedException, ExecutionException;
}
