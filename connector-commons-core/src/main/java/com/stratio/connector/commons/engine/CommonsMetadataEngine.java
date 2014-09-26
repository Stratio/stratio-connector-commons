package com.stratio.connector.commons.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class CommonsMetadataEngine<T> implements IMetadataEngine {

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
    protected CommonsMetadataEngine(ConnectionHandler connectionHandler) {

        this.connectionHandler = connectionHandler;
    }
    @Override
    public final void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata) throws UnsupportedException,
            ExecutionException {
        try {

            connectionHandler.startWork(targetCluster);
            createCatalog(catalogMetadata, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster);
        }
    }
    @Override
    public final void createTable(ClusterName targetCluster, TableMetadata tableMetadata) throws UnsupportedException,
            ExecutionException {
        try {
            connectionHandler.startWork(targetCluster);
            createTable(tableMetadata, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster);
        }
    }
    @Override
    public final void dropCatalog(ClusterName targetCluster, CatalogName name) throws UnsupportedException,
            ExecutionException {
        try {
            connectionHandler.startWork(targetCluster);
            dropCatalog(name, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster);
        }

    }
    @Override
    public final void dropTable(ClusterName targetCluster, TableName name) throws UnsupportedException, ExecutionException {
        try {
            connectionHandler.startWork(targetCluster);
            dropTable(name, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster);
        }
    }
    @Override
    public final void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata) throws UnsupportedException,
            ExecutionException {
        try {
            connectionHandler.startWork(targetCluster);
            createIndex(indexMetadata, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster);
        }
    }
    @Override
    public final void dropIndex(ClusterName targetCluster, IndexMetadata indexMetadata) throws UnsupportedException,
            ExecutionException {
        try {
            connectionHandler.startWork(targetCluster);
            createIndex(indexMetadata, connectionHandler.getConnection(targetCluster.getName()));
        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            connectionHandler.endWork(targetCluster);
        }
    }

    protected abstract void createCatalog(CatalogMetadata catalogMetadata, Connection<T> connection)
            throws UnsupportedException, ExecutionException;

    protected abstract void createTable(TableMetadata tableMetadata, Connection<T> connection) throws
            UnsupportedException,
            ExecutionException;

    protected abstract void dropCatalog(CatalogName name, Connection<T> connection) throws UnsupportedException,
            ExecutionException;

    protected abstract void dropTable(TableName name, Connection<T> connection) throws UnsupportedException,
            ExecutionException;

    protected abstract void createIndex(IndexMetadata indexMetadata, Connection<T> connection) throws
            UnsupportedException,
            ExecutionException;

    protected abstract void dropIndex(IndexMetadata indexMetadata, Connection<T> connection) throws
            UnsupportedException,
            ExecutionException;

}
