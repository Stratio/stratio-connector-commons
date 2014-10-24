package com.stratio.connector.commons.engine;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.result.QueryResult;

/** 
* CommonsQueryEngine Tester. 
* 
* @author <Authors name> 
* @since <pre>oct 24, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)
public class CommonsQueryEngineTest {

    private IResultHandler resultHandlerSend;
    LogicalWorkflow workflowSend=null;
    Boolean executeWorkFlow = false;
    String queryIdSend;
    private boolean executeAsyncExecute = false;
    private boolean executeStop = false;
    CommonsQueryEngineStub commonsQueryEngineStub;
    @Mock ConnectionHandler connectionHandler;
    @Before
public void before() throws Exception {


        resultHandlerSend = null;
        workflowSend=null;
        executeWorkFlow = false;
        String queryIdSend ="";
        executeAsyncExecute = false;
        executeStop = false;

        commonsQueryEngineStub = new CommonsQueryEngineStub(connectionHandler);
} 



/** 
* 
* Method: execute(LogicalWorkflow workflow) 
* 
*/ 
@Test
public void testasyncExecute() throws Exception {
    String queriID = "queryID";
    LogicalWorkflow logicalworkFlow = mock(LogicalWorkflow.class);
    IResultHandler resultHandler = mock (IResultHandler.class);

    commonsQueryEngineStub.asyncExecute(queriID, logicalworkFlow, resultHandler);

    assertTrue("executeWorkFlow is executed",executeAsyncExecute);
    assertEquals("The queryId is correct",queryIdSend,queriID);
    assertEquals("The workflow is correct",workflowSend,logicalworkFlow);
    assertEquals("The resultHandler is correct",resultHandler,resultHandlerSend);

}



    /**
* 
* Method: executeWorkFlow(LogicalWorkflow workflow) 
* 
*/ 
@Test
public void testExecuteWorkFlow() throws Exception {
    LogicalWorkflow logicalworkFlow = mock(LogicalWorkflow.class);
    commonsQueryEngineStub.executeWorkFlow(logicalworkFlow);

    assertTrue("executeWorkFlow is executed",executeWorkFlow);
    assertEquals("The workflow is correct",workflowSend,logicalworkFlow);
}

    @Test
    public void testStop() throws ConnectorException {
        String queriID = "queryID";

        commonsQueryEngineStub.stop(queriID);

        assertTrue("executeWorkFlow is executed",executeStop);
        assertEquals("The queryId is correct",queryIdSend,queriID);
    }

    class CommonsQueryEngineStub extends CommonsQueryEngine{



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
            executeWorkFlow  =true;
            return null;
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
    }

} 
