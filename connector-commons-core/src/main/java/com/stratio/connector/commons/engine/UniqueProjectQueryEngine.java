/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.connector.commons.engine;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;

/**
 * @author darroyo
 */
public abstract class UniqueProjectQueryEngine<T> extends CommonsQueryEngine implements IQueryEngine {

    /**
     * Constructor.
     *
     * @param connectionHandler the connector handler.
     */
    public UniqueProjectQueryEngine(ConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    protected QueryResult executeWorkFlow(LogicalWorkflow workflow) throws UnsupportedException, ExecutionException {
        chekSupport(workflow);
        ClusterName clusterName = null;
        QueryResult queryResult = null;
        try {
            clusterName = ((Project) workflow.getInitialSteps().get(0)).getClusterName();
            queryResult = execute(workflow, connectionHandler.getConnection(clusterName.getName()));
        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + clusterName.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }
        return queryResult;
    }

    protected abstract QueryResult execute(LogicalWorkflow workflow, Connection<T> connection)
            throws UnsupportedException, ExecutionException;

    private void chekSupport(LogicalWorkflow workflow) throws UnsupportedException {
        if (workflow.getInitialSteps().size() != 1) {
            throw new UnsupportedException("The connector can only execute queries with one Project");
        }
    }

}
