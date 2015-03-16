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
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.connector.ISqlEngine;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.result.Result;

/**
 * This abstract class is a Template for CommonsSQKEngine.
 * Created by jmgomez on 23/01/15.
 */
public abstract class CommonsSqlEngine implements ISqlEngine {

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

    protected CommonsSqlEngine(ConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }



    /**
     * This method execute a aql query.
     *
     * @param sqlQuery the sql to be executed.
     * @return the query result.
     * @throws ConnectorException if a error happens.
     */
    @Override public Result execute(String sqlQuery) throws ConnectorException {
        QueryResult result = null;

            /* try {

            for (LogicalStep project : workflow.getInitialSteps()) {
                ClusterName clusterName = ((Project) project).getClusterName();
                connectionHandler.startJob(clusterName.getName());
            }
            */
            if (logger.isDebugEnabled()) {
                logger.debug("Executing SQL [" + sqlQuery + "]");
            }
            Long time = System.currentTimeMillis();
            result = executeSQL(sqlQuery);

            if (logger.isDebugEnabled()) {
                logger.debug(
                        "The result form the query [" + sqlQuery + "] has returned [" + result.getResultSet()
                                .size() + "] rows");

            }

            logger.info("TIME - The execute time for the query ["+sqlQuery+"] has been ["+(System.currentTimeMillis()-time)
                    +"]");

       /* } finally {
            for (LogicalStep project : workflow.getInitialSteps()) {
                connectionHandler.endJob(((Project) project).getClusterName().getName());
            }
        }*/
        return result;
    }




    /**
     * This method execute a async SQL query.
     *
     * @param queryId the queryId.
     * @param sqlQuery the sql.
     * @param resultHandler the result handler.
     *
     * @throws ConnectorException if a error happens.
     */
    @Override public void asyncExecute(String queryId, String sqlQuery, IResultHandler resultHandler)
            throws ConnectorException {
        QueryResult result = null;

        if (logger.isDebugEnabled()) {
            logger.debug("Executing SQL [" + sqlQuery + "] with id ["+queryId+"]");
        }
        Long time = System.currentTimeMillis();
        asyncExecuteSQL(queryId,sqlQuery,resultHandler);

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "The result form the query [" + sqlQuery + "with id ["+queryId+"] has returned [" + result.getResultSet()
                            .size() + "] rows");
        }


         logger.info("TIME - The async execute time for the query with id["+queryId+"] has been ["+(System.currentTimeMillis() -time)+"]");

    }

    /**
     * This method execute a async and paged SQL.
     *
     * @param queryId the queryId.
     * @param sqlQuery the sql.
     * @param resultHandler the result handler.
     *
     * @throws ConnectorException if a error happens.
     */
    @Override public void pagedExecute(String queryId, String sqlQuery, IResultHandler resultHandler, int pageSize)
            throws ConnectorException {
        QueryResult result = null;

        if (logger.isDebugEnabled()) {
            logger.debug("Executing paged SQL [" + sqlQuery + "] with id ["+queryId+"] and page size ["+pageSize+"]");
        }
        Long time = System.currentTimeMillis();
        pagedExecuteSQL(queryId, sqlQuery, resultHandler, pageSize);

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "The result form the query [" + sqlQuery + "with id ["+queryId+"] has returned [" + result.getResultSet()
                            .size() + "] rows");
        }
        logger.info("TIME - The paged execute time for the query with id["+queryId+"] has been ["+(System
                .currentTimeMillis() -time)+"]");

    }

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to execute a paged sql.
     *
     * @param queryId the QueryId.
     * @param sqlQuery the sql.
     * @param resultHandler the result handler.
     * @throws ConnectorException if an error happens.
     */
    protected abstract void pagedExecuteSQL(String queryId, String sqlQuery, IResultHandler resultHandler, int
            pageSize)  throws ConnectorException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to execute a async sql.
     *
     * @param queryId the QueryId.
     * @param sqlQuery the sql.
     * @param resultHandler the result handler.
     * @throws ConnectorException if an error happens.
     */
    protected abstract void asyncExecuteSQL(String queryId, String sqlQuery, IResultHandler resultHandler)  throws ConnectorException;

    /**
     * Abstract method which must be implemented by the concrete database metadataEngine to execute a sql.
     *
     * @param sqlQuery the sql.
     * @return  the query result.
     * @throws ConnectorException if an error happens.
     */
    protected abstract QueryResult executeSQL(String sqlQuery)  throws ConnectorException;
}
