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

import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * This abstract class is a Template for CommonsQueryEngine.
 * Created by dgomez on 22/09/14.
 */
public abstract class CommonsQueryEngine implements IQueryEngine {


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

    /**
     * This method execute a query.
     *
     * @param workflow the workflow to be executed.
     * @return the query result.
     * @throws UnsupportedException if an operation is not supported.
     * @throws ExecutionException   if a error happens.
     */
    @Override
    public final QueryResult execute(LogicalWorkflow workflow) throws UnsupportedException, ExecutionException {
        QueryResult result = null;

        try {
            for (LogicalStep project : workflow.getInitialSteps()) {
                ClusterName clusterName = ((Project) project).getClusterName();
                connectionHandler.startJob(clusterName.getName());
            }
            result = executeWorkFlow(workflow);

        } finally {
            for (LogicalStep project : workflow.getInitialSteps()) {
                connectionHandler.endJob(((Project) project).getClusterName().getName());
            }
        }
        return result;
    }

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to execute a workflow.
     *
     * @param workflow the workflow.
     * @throws UnsupportedException if an operation is not supported.
     * @throws ExecutionException   if a error happens.
     */
    protected abstract QueryResult executeWorkFlow(LogicalWorkflow workflow) throws
            UnsupportedException,
            ExecutionException;
}
