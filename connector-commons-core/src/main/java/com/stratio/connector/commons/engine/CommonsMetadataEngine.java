package com.stratio.connector.commons.engine;

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




    public void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata, ConnectionHandler connection) throws UnsupportedException, ExecutionException {
        startWork(targetCluster , connection);
        createCatalog(targetCluster,catalogMetadata);
        endWork(targetCluster , connection);
    }


    public void createTable(ClusterName targetCluster, TableMetadata tableMetadata, ConnectionHandler connection) throws UnsupportedException, ExecutionException, HandlerConnectionException {
        startWork(targetCluster , connection);
        createTable(targetCluster,tableMetadata);
        endWork(targetCluster , connection);
    }


    public void dropCatalog(ClusterName targetCluster, CatalogName name, ConnectionHandler connection) throws UnsupportedException, ExecutionException {
        startWork(targetCluster , connection);
        dropCatalog(targetCluster,name);
        endWork(targetCluster , connection);
    }


    public void dropTable(ClusterName targetCluster, TableName name, ConnectionHandler connection) throws UnsupportedException, ExecutionException {
        startWork(targetCluster , connection);
        dropTable(targetCluster,name);
        endWork(targetCluster , connection);
    }


    public void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata, ConnectionHandler connection) throws UnsupportedException, ExecutionException {
        startWork(targetCluster , connection);
        createIndex(targetCluster,indexMetadata);
        endWork(targetCluster , connection);
    }


    public void dropIndex(ClusterName targetCluster, IndexMetadata indexMetadata, ConnectionHandler connection) throws UnsupportedException, ExecutionException {
        startWork(targetCluster , connection);
        dropIndex(targetCluster, indexMetadata);
        endWork(targetCluster , connection);
    }



    public abstract void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata) throws UnsupportedException, ExecutionException;


    public abstract void createTable(ClusterName targetCluster, TableMetadata tableMetadata) throws UnsupportedException, ExecutionException ;


    public abstract void dropCatalog(ClusterName targetCluster, CatalogName name) throws UnsupportedException, ExecutionException ;


    public abstract void dropTable(ClusterName targetCluster, TableName name) throws UnsupportedException, ExecutionException ;


    public abstract void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata) throws UnsupportedException, ExecutionException;


    public abstract void dropIndex(ClusterName targetCluster, IndexMetadata indexMetadata) throws UnsupportedException, ExecutionException ;



}
