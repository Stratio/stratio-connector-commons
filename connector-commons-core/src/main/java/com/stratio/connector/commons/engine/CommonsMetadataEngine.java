package com.stratio.connector.commons.engine;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * Created by dgomez on 22/09/14.
 */
public abstract class CommonsMetadataEngine extends CommonsUtils implements IMetadataEngine {

    /**
     * The connection handler.
     */
    ConnectionHandler connectionHandler;

    /**
     * Constructor.
     *
     * @param connectionHandler the connector handler.
     */
    protected CommonsMetadataEngine(ConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }

    public abstract void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata,
            Connection connection) throws UnsupportedException, ExecutionException;

    public abstract void createTable(ClusterName targetCluster, TableMetadata tableMetadata, Connection connection)
            throws UnsupportedException, ExecutionException;

    public abstract void dropCatalog(ClusterName targetCluster, CatalogName name, Connection connection)
            throws UnsupportedException, ExecutionException;

    public abstract void dropTable(ClusterName targetCluster, TableName name, Connection connection)
            throws UnsupportedException, ExecutionException;

    public abstract void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata, Connection connection)
            throws UnsupportedException, ExecutionException;

    public abstract void dropIndex(ClusterName targetCluster, IndexMetadata indexMetadata, Connection connection)
            throws UnsupportedException, ExecutionException;

    public void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata)
            throws UnsupportedException, ExecutionException {
        try {

            startWork(targetCluster, connectionHandler);
            createCatalog(targetCluster, catalogMetadata, connectionHandler.getConnection(targetCluster.getName()));
            endWork(targetCluster, connectionHandler);

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }
    }

    public void createTable(ClusterName targetCluster, TableMetadata tableMetadata)
            throws UnsupportedException, ExecutionException {
        try {
            startWork(targetCluster, connectionHandler);
            createTable(targetCluster, tableMetadata, connectionHandler.getConnection(targetCluster.getName()));
            endWork(targetCluster, connectionHandler);
        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }
    }

    public void dropCatalog(ClusterName targetCluster, CatalogName name)
            throws UnsupportedException, ExecutionException {
        try {
            startWork(targetCluster, connectionHandler);
            dropCatalog(targetCluster, name, connectionHandler.getConnection(targetCluster.getName()));
            endWork(targetCluster, connectionHandler);
        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }

    }

    public void dropTable(ClusterName targetCluster, TableName name) throws UnsupportedException, ExecutionException {
        try {
            startWork(targetCluster, connectionHandler);
            dropTable(targetCluster, name, connectionHandler.getConnection(targetCluster.getName()));
            endWork(targetCluster, connectionHandler);
        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }
    }

    public void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata)
            throws UnsupportedException, ExecutionException {
        try {
            startWork(targetCluster, connectionHandler);
            createIndex(targetCluster, indexMetadata, connectionHandler.getConnection(targetCluster.getName()));
            endWork(targetCluster, connectionHandler);
        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }
    }

    public void dropIndex(ClusterName targetCluster, IndexMetadata indexMetadata)
            throws UnsupportedException, ExecutionException {
        try {
            startWork(targetCluster, connectionHandler);
            createIndex(targetCluster, indexMetadata, connectionHandler.getConnection(targetCluster.getName()));
            endWork(targetCluster, connectionHandler);
        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }
    }

}
