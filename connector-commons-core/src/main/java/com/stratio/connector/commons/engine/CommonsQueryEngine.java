package com.stratio.connector.commons.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;

/**
 * Created by dgomez on 22/09/14.
 */
public abstract class CommonsQueryEngine implements IQueryEngine {

    /**
     * The Log.
     */
    final transient Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * The connection handler.
     */
    protected transient ConnectionHandler connectionHandler;

    /**
     * Constructor.
     *
     * @param connectionHandler the connector handler.
     */

    protected CommonsQueryEngine(ConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }

    public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow)
            throws UnsupportedException,
            ExecutionException{
        throw new UnsupportedException("This method is deprecated");
    }

    protected abstract QueryResult executeWorkFlow(LogicalWorkflow workflow) throws
            UnsupportedException,
            ExecutionException;

    @Override
    public final QueryResult execute(LogicalWorkflow workflow) throws UnsupportedException, ExecutionException {
        QueryResult result = null;
        ClusterName clusterName = null;
        try {
            for (LogicalStep project : workflow.getInitialSteps()) {
                clusterName = ((Project) project).getClusterName();
                connectionHandler.startWork(clusterName.getName());
            }
            result = executeWorkFlow(workflow);

        } finally {
            for (LogicalStep project : workflow.getInitialSteps()) {
                connectionHandler.endWork(((Project) project).getClusterName().getName());
            }
        }
        return result;
    }
}
