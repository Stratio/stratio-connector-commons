package com.stratio.connector.commons.engine;

import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.TableMetadata;

import java.util.Collection;

/**
 * Created by dgomez on 22/09/14.
 */
public abstract class CommonsStorageEngine extends CommonsUtils implements IStorageEngine {


    public void insert(ClusterName targetCluster, TableMetadata targetTable, Row row, ConnectionHandler connectionHandler ) throws UnsupportedException, ExecutionException{

        startWork(targetCluster , connectionHandler);
        insert(targetCluster,targetTable,row);
        endWork(targetCluster, connectionHandler);

    }

    public void insert(ClusterName targetCluster, TableMetadata targetTable, Collection<Row> rows, ConnectionHandler connectionHandler ) throws UnsupportedException, ExecutionException{

        startWork(targetCluster , connectionHandler);
        insert(targetCluster,targetTable,rows);
        endWork(targetCluster , connectionHandler);

    }


    @Override
    public abstract void insert(ClusterName targetCluster, TableMetadata targetTable, Row row) throws UnsupportedException, ExecutionException;

    @Override
    public abstract void insert(ClusterName targetCluster, TableMetadata targetTable, Collection<Row> rows) throws UnsupportedException, ExecutionException;

}
