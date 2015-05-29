package com.stratio.connector.commons.ptest.storage.insert;

import static junit.framework.TestCase.assertEquals;

import java.io.FileNotFoundException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ptest.thread.InsertThread;
import com.stratio.connector.commons.ptest.util.EficiencyBean;
import com.stratio.connector.commons.ptest.util.TextFileParser;
import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.crossdata.common.exceptions.ConnectorException;

public abstract class InsertOneGenericPT extends GenericConnectorTest {

    public static final int EXPECTED_ROWS = 4000004;

    static final transient Logger LOGGER = LoggerFactory.getLogger(SelectorHelper.class);

    @Before
    public void setUp() throws ConnectorException {
        super.setUp();
        if (!TextFileParser.existTestFiles()) {
            try {
                TextFileParser.generateFiles();
            } catch (FileNotFoundException e) {
                throw new ConnectorException("Exception generated textFiles.", e);
            }
        }
    }

    @Test
    public void insetInSameTableWithNotExistsEqualsFalse() throws ConnectorException, InterruptedException {

        InsertThread insertThread0 = insertThreadBuilder(0);
        InsertThread insertThread1 = insertThreadBuilder(1);
        InsertThread insertThread2 = insertThreadBuilder(2);
        InsertThread insertThread3 = insertThreadBuilder(3);

        insertThread0.start();
        insertThread1.start();
        insertThread2.start();
        insertThread3.start();

        insertThread0.join();
        insertThread1.join();
        insertThread2.join();
        insertThread3.join();

        getConnectorHelper().refresh(CATALOG);

        LOGGER.info("The insert has been finished. The verification starts.");
        verifyAllRowWasInserted();
    }

    protected void verifyAllRowWasInserted() throws ConnectorException {
        int rowsReturned = getConnector().getQueryEngine().execute("", EficiencyBean.getLogicalWorkFlowCreatorSelectAll
                (CATALOG, getClusterName())).getResultSet().size();
        assertEquals("There must be recovered " + EXPECTED_ROWS + " rows", EXPECTED_ROWS, rowsReturned);
    }

    private InsertThread insertThreadBuilder(int id) {
        return new InsertThread(id, getClusterName(), getConnector(), CATALOG, TABLE, false);
    }

}
