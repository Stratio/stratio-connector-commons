package com.stratio.connector.commons.engine;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import org.junit.After;
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
import com.stratio.crossdata.common.result.QueryResult;

/** 
* CommonsSqlEngine Tester. 
* 
* @author <Authors name> 
* @since <pre>feb 23, 2015</pre> 
* @version 1.0 
*/

@RunWith(PowerMockRunner.class)
@PrepareForTest(QueryResult.class)
public class CommonsSqlEngineTest {

    private static final java.lang.Integer RESULT_SIZE = 10;
    private String queryIdSend;
    private String sqlQuerySend;
    CommonsSqlEngineStub commonsSqlEngineStub;
    private IResultHandler resultHandlerSend;
    private boolean pagedExecuteAsyncExecute;
    private int pageSizeExecute;

    @Mock ConnectionHandler connectionHandler;
    private boolean executeWorkFlow;
    private boolean executeAsyncExecute;

    @Before
    public void before() throws Exception {


        resultHandlerSend = null;
        pageSizeExecute = 0;
        sqlQuerySend = null;
        executeWorkFlow = false;
        pagedExecuteAsyncExecute = false;
        queryIdSend = "";
        executeAsyncExecute = false;


        commonsSqlEngineStub = new CommonsSqlEngineStub(connectionHandler);
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: execute(String sqlQuery) 
* 
*/ 
@Test
public void testExecute() throws Exception {
    String sql = "SQL";
    commonsSqlEngineStub.execute(sql);

    assertTrue("executeWorkFlow is executed", executeWorkFlow);
    assertEquals("The sql must be correct", sqlQuerySend, sql);
}

    @Test
    public void testExecuteLog() throws ConnectorException {
        String sql = "SQL";


        Logger logger = mock(Logger.class);
        Whitebox.setInternalState(commonsSqlEngineStub, "logger", logger);


        when(logger.isDebugEnabled()).thenReturn(true);

        QueryResult result =  (QueryResult)commonsSqlEngineStub.execute(sql);



        verify(logger).debug("Executing SQL [" + sql + "]");
        verify(logger).debug( "The result form the query [" + sql + "] has returned [" + result.getResultSet().size() + "] "
                + "rows");

    }

/** 
* 
* Method: asyncExecute(String queryId, String sqlQuery, IResultHandler resultHandler) 
* 
*/ 
@Test
public void testAsyncExecute() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: pagedExecute(String queryId, String sqlQuery, IResultHandler resultHandler, int pageSize) 
* 
*/ 
@Test
public void testPagedExecute() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: pagedExecuteSQL(String queryId, String sqlQuery, IResultHandler resultHandler, int
            pageSize) 
* 
*/ 
@Test
public void testPagedExecuteSQL() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: asyncExecuteSQL(String queryId, String sqlQuery, IResultHandler resultHandler) 
* 
*/ 
@Test
public void testAsyncExecuteSQL() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: executeSQL(String sqlQuery) 
* 
*/ 
@Test
public void testExecuteSQL() throws Exception { 
//TODO: Test goes here... 
}


    class CommonsSqlEngineStub extends CommonsSqlEngine {


        /**
         * Constructor.
         *
         * @param connectionHandler the connector handler.
         */
        protected CommonsSqlEngineStub(ConnectionHandler connectionHandler) {
            super(connectionHandler);
        }


        @Override protected void pagedExecuteSQL(String queryId, String sqlQuery, IResultHandler resultHandler,
                int pageSize) throws ConnectorException {
            queryIdSend = queryId;
            sqlQuerySend = sqlQuery;
            resultHandlerSend = resultHandler;
            pagedExecuteAsyncExecute = true;
            pageSizeExecute = pageSize;


        }

        @Override protected void asyncExecuteSQL(String queryId, String sqlQuery, IResultHandler resultHandler)
                throws ConnectorException {
            queryIdSend = queryId;
            sqlQuerySend = sqlQuery;
            resultHandlerSend = resultHandler;
            executeAsyncExecute = true;
        }

        @Override protected QueryResult executeSQL(String sqlQuery) throws ConnectorException {
            sqlQuerySend = sqlQuery;
            executeWorkFlow = true;
            ResultSet resultSet = mock(ResultSet.class);
            when(resultSet.size()).thenReturn(RESULT_SIZE);
            QueryResult queryResult = mock(QueryResult.class);

            when(queryResult.getResultSet()).thenReturn(resultSet);
            return queryResult;
        }
    }

} 
