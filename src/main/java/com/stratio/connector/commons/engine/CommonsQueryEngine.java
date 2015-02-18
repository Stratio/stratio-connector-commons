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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * This abstract class is a Template for CommonsQueryEngine. Created by dgomez on 22/09/14.
 */
public abstract class CommonsQueryEngine implements IQueryEngine {

    /**
     * The Log.
     */
    private transient final Logger logger = LoggerFactory.getLogger(this.getClass());

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
     * @throws ConnectorException if a error happens.
     */
    @Override
    public final QueryResult execute(LogicalWorkflow workflow) throws ConnectorException {
        QueryResult result = null;

        try {
            for (LogicalStep project : workflow.getInitialSteps()) {
                ClusterName clusterName = ((Project) project).getClusterName();
                connectionHandler.startJob(clusterName.getName());
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Executing [" + workflow.toString() + "]");
            }
            result = executeWorkFlow(workflow);

            if (logger.isDebugEnabled()) {
                logger.debug(
                        "The result form the query [" + workflow.toString() + "] has returned [" + result.getResultSet()
                                .size() + "] rows");
            }
        } finally {
            for (LogicalStep project : workflow.getInitialSteps()) {
                connectionHandler.endJob(((Project) project).getClusterName().getName());
            }
        }
        return result;
    }

    public final void pagedExecute(String var1, LogicalWorkflow var2, IResultHandler var3, int var4)
            throws ConnectorException {

    }

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to execute a workflow.
     *
     * @param workflow the workflow.
     * @throws ConnectorException if an error happens.
     */
    protected abstract QueryResult executeWorkFlow(LogicalWorkflow workflow) throws ConnectorException;
}
