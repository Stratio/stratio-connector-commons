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
    private IResultHandler resultHandlerSend;
    private boolean pagedExecuteAsyncExecute;
    private int pageSizeExecute;

    @Mock
    ConnectionHandler connectionHandler;
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

    }

    @After
    public void after() throws Exception {
    }


    /**
     * Method: asyncExecute(String queryId, String sqlQuery, IResultHandler resultHandler)
     */
    @Test
    public void testAsyncExecute() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: pagedExecute(String queryId, String sqlQuery, IResultHandler resultHandler, int pageSize)
     */
    @Test
    public void testPagedExecute() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: pagedExecuteSQL(String queryId, String sqlQuery, IResultHandler resultHandler, int
     * pageSize)
     */
    @Test
    public void testPagedExecuteSQL() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: asyncExecuteSQL(String queryId, String sqlQuery, IResultHandler resultHandler)
     */
    @Test
    public void testAsyncExecuteSQL() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: executeSQL(String sqlQuery)
     */
    @Test
    public void testExecuteSQL() throws Exception {
//TODO: Test goes here... 
    }


}