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
public abstract class CommonsStorageEngine implements IStorageEngine {

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
    public void insert(ClusterName targetCluster, TableMetadata targetTable, Row row) throws UnsupportedException,
            ExecutionException {
        try {
            connectionHandler.startWork(targetCluster);
            insert(targetTable, row, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster);
        }
    }

    @Override
    public void insert(ClusterName targetCluster, TableMetadata targetTable, Collection<Row> rows)
            throws UnsupportedException, ExecutionException {
        try {
            connectionHandler.startWork(targetCluster);
            insert(targetTable, rows, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster);
        }
    }

    protected abstract void insert(TableMetadata targetTable, Row row, Connection connection)
            throws UnsupportedException,
            ExecutionException;

    protected abstract void insert(TableMetadata targetTable, Collection<Row> rows, Connection connection)
            throws UnsupportedException, ExecutionException;
}
