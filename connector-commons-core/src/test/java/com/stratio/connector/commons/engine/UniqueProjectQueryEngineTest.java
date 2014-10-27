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
 * @since <pre>oct 24, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
public class UniqueProjectQueryEngineTest {

    UniqueProjectQueryEngineStub uniqueProjectQueryEngineStub;
    private String queryIdSend;
    private LogicalWorkflow workflowSend;
    private IResultHandler resultHandlerSend;
    private boolean executeAsyncExecute;
    private boolean executeStop;
    private boolean executeExecute;
    private Project projectSend;
    private Connection connectionSend = null;
    @Mock private ConnectionHandler connectionHandler;

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
        LogicalWorkflow workflow = mock(LogicalWorkflow.class);
        IResultHandler resultHandler = mock(IResultHandler.class);

        uniqueProjectQueryEngineStub.asyncExecute(queryID, workflow, resultHandler);

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

    class UniqueProjectQueryEngineStub extends UniqueProjectQueryEngine {

        /**
         * Constructor.
         *
         * @param connectionHandler the connector handler.
         */
        public UniqueProjectQueryEngineStub(ConnectionHandler connectionHandler) {
            super(connectionHandler);
        }

        @Override public void asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
                throws ConnectorException {
            queryIdSend = queryId;
            workflowSend = workflow;
            resultHandlerSend = resultHandler;
            executeAsyncExecute = true;

        }

        @Override public void stop(String queryId) throws ConnectorException {
            queryIdSend = queryId;
            executeStop = true;
        }

        @Override protected QueryResult execute(Project workflow, Connection connection)
                throws UnsupportedException, ExecutionException {

            projectSend = workflow;
            connectionSend = connection;
            executeExecute = true;

            return null;
        }
    }

} 
