package com.stratio.connector.commons.engine;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;

/**
 * Created by dgomez on 22/09/14.
 */
public abstract class CommonsQueryEngine extends CommonsUtils implements IQueryEngine {

    /**
     * The connection handler.
     */
    ConnectionHandler connectionHandler;

    /**
     * Constructor.
     *
     * @param connectionHandler
     *            the connector handler.
     */
    public CommonsQueryEngine(ConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }

    public abstract QueryResult execute(LogicalWorkflow workflow, Connection connection) throws UnsupportedException,
                    ExecutionException;

    @Override
    public QueryResult execute(LogicalWorkflow workflow) throws UnsupportedException, ExecutionException {
        QueryResult result = null;
        try {
            /*
             * for (){ startWork(targetCluster, connectionHandler); }
             */
            result = execute(workflow, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            /*
             * for(){ endWork(targetCluster, connectionHandler); }
             */
        }
        return result;
    }
}
