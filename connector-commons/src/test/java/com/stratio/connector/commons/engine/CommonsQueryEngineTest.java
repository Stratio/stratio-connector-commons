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

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;

import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * CommonsQueryEngine Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>oct 24, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(QueryResult.class)
public class CommonsQueryEngineTest {

    private static final java.lang.Integer RESULT_SIZE = 10;
    LogicalWorkflow workflowSend = null;
    Boolean executeWorkFlow = false;
    String queryIdSend;
    CommonsQueryEngineStub commonsQueryEngineStub;
    @Mock ConnectionHandler connectionHandler;
    private IResultHandler resultHandlerSend;
    private boolean executeAsyncExecute = false;
    private boolean executeStop = false;

    private boolean pagedExecuteAsyncExecute = false;
    private int pageSizeExecute;

    @Before
    public void before() throws Exception {

        resultHandlerSend = null;
        pageSizeExecute = 0;
        workflowSend = null;
        executeWorkFlow = false;
        pagedExecuteAsyncExecute = false;
        String queryIdSend = "";
        executeAsyncExecute = false;
        executeStop = false;

        commonsQueryEngineStub = new CommonsQueryEngineStub(connectionHandler);
    }

    /**
     * Method: execute(LogicalWorkflow workflow)
     */
    @Test
    public void testasyncExecute() throws Exception {
        String queryID = "queryID";
        LogicalWorkflow logicalworkFlow = mock(LogicalWorkflow.class);
        IResultHandler resultHandler = mock(IResultHandler.class);

        commonsQueryEngineStub.asyncExecute(queryID, logicalworkFlow, resultHandler);

        assertTrue("executeWorkFlow is executed", executeAsyncExecute);
        assertEquals("The queryId is correct", queryIdSend, queryID);
        assertEquals("The workflow is correct", workflowSend, logicalworkFlow);
        assertEquals("The resultHandler is correct", resultHandler, resultHandlerSend);

    }


    @Test
    public void testasyncExecuteLog() throws Exception {



        Logger logger = mock(Logger.class);
        Whitebox.setInternalState(commonsQueryEngineStub,"logger",logger);


        when(logger.isDebugEnabled()).thenReturn(true);

        String queryID = "queryID";
        LogicalWorkflow logicalworkFlow = mock(LogicalWorkflow.class);
        IResultHandler resultHandler = mock(IResultHandler.class);
        commonsQueryEngineStub.asyncExecute(queryID, logicalworkFlow, resultHandler);


        verify(logger).debug("Async Executing [" + logicalworkFlow.toString() + "] : queryId ["+queryID+"]");
        verify(logger).debug("The async query [" + queryID + "] has ended");



    }

    /**
     * Method: executeWorkFlow(LogicalWorkflow workflow)
     */
    @Test
    public void testExecuteWorkFlow() throws Exception {
        LogicalWorkflow logicalworkFlow = mock(LogicalWorkflow.class);
        commonsQueryEngineStub.execute(logicalworkFlow);

        assertTrue("executeWorkFlow is executed", executeWorkFlow);
        assertEquals("The workflow is correct", workflowSend, logicalworkFlow);
    }





    @Test
    public void testExecuteWorkflowLog() throws ConnectorException {
        LogicalWorkflow logicalworkFlow = mock(LogicalWorkflow.class);


        Logger logger = mock(Logger.class);
        Whitebox.setInternalState(commonsQueryEngineStub,"logger",logger);


        when(logger.isDebugEnabled()).thenReturn(true);

       QueryResult result =  commonsQueryEngineStub.execute(logicalworkFlow);



        verify(logger).debug("Executing [" + logicalworkFlow.toString() + "]");
        verify(logger).debug("The query has finished. The result form the query [" + logicalworkFlow.toString() + "] has returned ["+ result.getResultSet().size() + "] rows");

    }



    /**
     * Method: executeWorkFlow(LogicalWorkflow workflow)
     */
    @Test
    public void testpagedExecuteWorkFlow() throws Exception {
        String queryID = "queryID";
        LogicalWorkflow logicalworkFlow = mock(LogicalWorkflow.class);
        IResultHandler resultHandler = mock(IResultHandler.class);

        commonsQueryEngineStub.pagedExecute(queryID, logicalworkFlow, resultHandler,10);

        assertTrue("executeWorkFlow must be executed", pagedExecuteAsyncExecute);
        assertEquals("The queryId must be correct", queryIdSend, queryID);
        assertEquals("The workflow must be correct", workflowSend, logicalworkFlow);
        assertEquals("The resultHandler must be correct", resultHandler, resultHandlerSend);
        assertEquals("The paged must be correct", pageSizeExecute, 10);
    }

    @Test
    public void testpagedExecuteWorkFlowLog() throws ConnectorException {



        Logger logger = mock(Logger.class);
        Whitebox.setInternalState(commonsQueryEngineStub,"logger",logger);


        when(logger.isDebugEnabled()).thenReturn(true);

        String queryID = "queryID";
        LogicalWorkflow logicalworkFlow = mock(LogicalWorkflow.class);
        IResultHandler resultHandler = mock(IResultHandler.class);

        commonsQueryEngineStub.pagedExecute(queryID, logicalworkFlow, resultHandler,10);

        verify(logger).debug("Async paged Executing [" + logicalworkFlow.toString() + "] : queryId ["+queryID+"]");
        verify(logger).debug("The async query [" + queryID + "] has ended" );

    }





    @Test
    public void testStop() throws ConnectorException {
        String queriID = "queryID";

        commonsQueryEngineStub.stop(queriID);

        assertTrue("executeWorkFlow is executed", executeStop);
        assertEquals("The queryId is correct", queryIdSend, queriID);
    }



    class CommonsQueryEngineStub extends CommonsQueryEngine {

        /**
         * Constructor.
         *
         * @param connectionHandler the connector handler.
         */
        protected CommonsQueryEngineStub(ConnectionHandler connectionHandler) {
            super(connectionHandler);
        }

        @Override protected QueryResult executeWorkFlow(LogicalWorkflow workflow)
                throws UnsupportedException, ExecutionException {
            workflowSend = workflow;
            executeWorkFlow = true;
            ResultSet resultSet = mock(ResultSet.class);
            when(resultSet.size()).thenReturn(RESULT_SIZE);
            QueryResult queryResult = mock(QueryResult.class);

            when(queryResult.getResultSet()).thenReturn(resultSet);
            return queryResult;
        }

        @Override protected void asyncExecuteWorkFlow(String queryId, LogicalWorkflow workflow,
                IResultHandler resultHandler) {
            queryIdSend = queryId;
            workflowSend = workflow;
            resultHandlerSend = resultHandler;
            executeAsyncExecute = true;
        }

        @Override protected void pagedExecuteWorkFlow(String queryId, LogicalWorkflow workflow,
                IResultHandler resultHandler, int pageSize) {
            queryIdSend = queryId;
            workflowSend = workflow;
            resultHandlerSend = resultHandler;
            pagedExecuteAsyncExecute = true;
            pageSizeExecute = pageSize;
        }

        @Override public void stop(String queryId) throws ConnectorException {
            queryIdSend = queryId;
            executeStop = true;
        }
    }

} 
