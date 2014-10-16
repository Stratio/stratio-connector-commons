/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.connector.commons.engine;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * Created by dgomez on 22/09/14.
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

    @Override
    public final void insert(ClusterName targetCluster, TableMetadata targetTable, Row row) throws UnsupportedException,
            ExecutionException {
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

    protected abstract void insert(TableMetadata targetTable, Row row, Connection<T> connection)
            throws UnsupportedException,
            ExecutionException;

    protected abstract void insert(TableMetadata targetTable, Collection<Row> rows, Connection<T> connection)
            throws UnsupportedException, ExecutionException;
}
