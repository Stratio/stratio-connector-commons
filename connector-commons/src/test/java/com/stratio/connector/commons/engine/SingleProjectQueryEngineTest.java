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
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * UniqueProjectQueryEngine Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>
 * oct 24, 2014
 * </pre>
 */
@RunWith(PowerMockRunner.class)
public class SingleProjectQueryEngineTest {

    UniqueProjectQueryEngineStub uniqueProjectQueryEngineStub;
    private String queryIdSend;
    private Project workflowSend;
    private IResultHandler resultHandlerSend;
    private boolean executeAsyncExecute;
    private boolean executeStop;
    private boolean executeExecute;
    private Project projectSend;
    private Connection connectionSend = null;
    @Mock
    private ConnectionHandler connectionHandler;

    @Before
    public void before() throws Exception {

        resultHandlerSend = null;
        workflowSend = null;
        projectSend = null;
        connectionSend = null;

        executeAsyncExecute = false;
        executeStop = false;
        executeExecute = false;

        uniqueProjectQueryEngineStub = new UniqueProjectQueryEngineStub(connectionHandler);
    }

    /**
     * Method: executeWorkFlow(LogicalWorkflow workflow)
     */
    @Test
    public void testasyncExecute() throws Exception {

        String queryID = "queryID";
        Project workflow = mock(Project.class);
        IResultHandler resultHandler = mock(IResultHandler.class);
        Connection connection = mock(Connection.class);
        uniqueProjectQueryEngineStub.asyncExecute(queryID, workflow, connection,resultHandler);

        assertTrue("executeWorkFlow is executed", executeAsyncExecute);
        assertEquals("The queryId is correct", queryIdSend, queryID);
        assertEquals("The workflow is correct", workflowSend, workflow);
        assertEquals("The resultHandler is correct", resultHandler, resultHandlerSend);
    }

    @Test
    public void testStop() throws Exception {

        String queryID = "queryID";

        uniqueProjectQueryEngineStub.stop(queryID);

        assertTrue("executeWorkFlow is executed", executeStop);
        assertEquals("The queryId is correct", queryIdSend, queryID);

    }

    /**
     * Method: execute(Project workflow, Connection<T> connection)
     */
    @Test
    public void testExecute() throws Exception {

        Project project = mock(Project.class);
        Connection connection = mock(Connection.class);
        uniqueProjectQueryEngineStub.execute(project, connection);

        assertTrue("executeWorkFlow is executed", executeExecute);
        assertEquals("The workflow is correct", project, projectSend);
        assertEquals("The connection is correct", connection, connectionSend);

    }

    class UniqueProjectQueryEngineStub extends SingleProjectQueryEngine {

        /**
         * Constructor.
         *
         * @param connectionHandler the connector handler.
         */
        public UniqueProjectQueryEngineStub(ConnectionHandler connectionHandler) {
            super(connectionHandler);
        }

        @Override
        public void asyncExecute(String queryId, Project project, Connection connection, IResultHandler resultHandler)
                throws ConnectorException {
            queryIdSend = queryId;
            workflowSend = project;
            resultHandlerSend = resultHandler;
            executeAsyncExecute = true;

        }




        @Override protected void pagedExecute(String queryId,Project project, Connection connection, IResultHandler resultHandler) {


        }

        @Override
        public void stop(String queryId) throws ConnectorException {
            queryIdSend = queryId;
            executeStop = true;
        }

        @Override
        protected QueryResult execute(Project project, Connection connection) throws UnsupportedException,
                ExecutionException {

            projectSend = project;
            connectionSend = connection;
            executeExecute = true;

            return null;
        }


    }

}
