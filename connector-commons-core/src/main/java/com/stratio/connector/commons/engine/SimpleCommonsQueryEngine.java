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

import java.util.List;

import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;

/**
 * @author darroyo
 *
 */
public abstract class SimpleCommonsQueryEngine extends CommonsUtils implements IQueryEngine {

    /**
     * The connection handler.
     */
    ConnectionHandler connectionHandler;

    /**
     * @param connectionHandler
     */
    public SimpleCommonsQueryEngine(ConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }

    public QueryResult execute(LogicalWorkflow workflow) throws UnsupportedException, ExecutionException {

        ClusterName targetCluster;
        List<LogicalStep> initialSteps = workflow.getInitialSteps();
        if (initialSteps.size() != 1)
            throw new UnsupportedException("");
        targetCluster = initialSteps.get(0).getClusterName();

        QueryResult result = null;
        try {
            startWork(targetCluster, connectionHandler);

            result = execute(workflow, connectionHandler.getConnection(targetCluster.getName()));

        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + targetCluster.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        } finally {
            endWork(targetCluster, connectionHandler);
        }

        return result;

    }

}
