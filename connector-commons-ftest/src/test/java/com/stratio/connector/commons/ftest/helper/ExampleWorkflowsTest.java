/*
 * Stratio Deep
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */

package com.stratio.connector.commons.ftest.helper;


import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.workFlow.ExampleWorkflows;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;

import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.junit.*;


import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Created by jjlopez on 16/09/14.
 */
public abstract class ExampleWorkflowsTest extends GenericConnectorTest {

    private ExampleWorkflows exampleWorkflows;



    private static boolean insertData = false;

    @Before
    public void setUp() throws InitializationException, ConnectionException, UnsupportedException, ExecutionException {
            setDeleteBeteweenTest(false);
            super.setUp();
        exampleWorkflows = new ExampleWorkflows(CATALOG,TABLE);
        if (!insertData){
            deleteCatalog(CATALOG);
            TableMetadata targetTable = new TableMetadata(new TableName(CATALOG, TABLE), null, null, null, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
            for (int i=0;i<1;i++) {
                connector.getStorageEngine().insert(getClusterName(), targetTable, exampleWorkflows.getRows(i));
            }
            System.out.println("Insert Finish");
            insertData=true;
        }
    }



        @Test
    public void basicSelect() throws UnsupportedException, ExecutionException {

        System.out.println("*********************************** INIT FUNCTIONAL TEST basicSelect ***********************************");

        LogicalWorkflow logicalWorkflow=exampleWorkflows.getBasicSelect();
        QueryResult qr =  connector.getQueryEngine().execute(getClusterName(), logicalWorkflow);
        assertEquals(qr.getResultSet().size(), 4);
    }

    @Test
    public void basicSelectAsterisk() throws UnsupportedException, ExecutionException {
        System.out.println("*********************************** INIT FUNCTIONAL TEST basicSelectAsterisk ***********************************");

        LogicalWorkflow logicalWorkflow=exampleWorkflows.getBasicSelectAsterisk();
        QueryResult qr =  connector.getQueryEngine().execute(getClusterName(), logicalWorkflow);
        assertEquals(qr.getResultSet().size(), 4);
    }

    @Test
    public void selectIndexedField() throws UnsupportedException, ExecutionException {

        System.out.println("*********************************** INIT FUNCTIONAL TEST basicSelectAsterisk ***********************************");

        LogicalWorkflow logicalWorkflow=exampleWorkflows.getSelectIndexedField();


        QueryResult   qr =     connector.getQueryEngine().execute(getClusterName(), logicalWorkflow);
        assertEquals(qr.getResultSet().size(), 1);
    }

    @Test
    public void selectNonIndexedField() throws UnsupportedException, ExecutionException {
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNonIndexedField ***********************************");

        LogicalWorkflow logicalWorkflow=exampleWorkflows.getSelectNonIndexedField();

        QueryResult   qr =   connector.getQueryEngine().execute(getClusterName(), logicalWorkflow);


    }

    @Test
    public void selectMixedWhere() throws UnsupportedException, ExecutionException {
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectMixedWhere ***********************************");

        LogicalWorkflow logicalWorkflow=exampleWorkflows.getSelectMixedWhere();

        QueryResult qr =  connector.getQueryEngine().execute(getClusterName(), logicalWorkflow);
        assertEquals(qr.getResultSet().size(), 4);


    }


}
