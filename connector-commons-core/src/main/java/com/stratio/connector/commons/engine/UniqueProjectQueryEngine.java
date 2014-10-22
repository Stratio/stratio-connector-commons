/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.commons.engine;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * @author darroyo
 */
public abstract class UniqueProjectQueryEngine<T> extends CommonsQueryEngine {

    /**
     * Constructor.
     *
     * @param connectionHandler the connector handler.
     */
    public UniqueProjectQueryEngine(ConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    @Override
    protected final QueryResult executeWorkFlow(LogicalWorkflow workflow) throws UnsupportedException,
            ExecutionException {
        checkIsSupported(workflow);
        ClusterName clusterName = null;
        QueryResult queryResult = null;
        try {
            clusterName = ((Project) workflow.getInitialSteps().get(0)).getClusterName();
            queryResult = execute((Project) workflow.getInitialSteps().get(0),
                    connectionHandler.getConnection(clusterName.getName()));
        } catch (HandlerConnectionException e) {
            String msg = "Error find Connection in " + clusterName.getName() + ". " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }
        return queryResult;
    }

    protected abstract QueryResult execute(Project workflow, Connection<T> connection) throws UnsupportedException,
            ExecutionException;

    private void checkIsSupported(LogicalWorkflow workflow) throws UnsupportedException {
        if (workflow.getInitialSteps().size() != 1) {
            throw new UnsupportedException("The connector can only execute queries with one Project");
        }
    }

}
