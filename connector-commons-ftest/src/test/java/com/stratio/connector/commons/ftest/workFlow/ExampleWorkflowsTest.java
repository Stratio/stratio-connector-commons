/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.connector.commons.ftest.workFlow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * Created by jjlopez on 16/09/14.
 */
public abstract class ExampleWorkflowsTest extends GenericConnectorTest {

    private static boolean insertData = false;
    private ExampleWorkflows exampleWorkflows;

    @Before
    public void setUp() throws InitializationException, ConnectionException, UnsupportedException, ExecutionException {
        setDeleteBeteweenTest(false);
        super.setUp();

        exampleWorkflows = new ExampleWorkflows(CATALOG, TABLE, getClusterName());
        if (!insertData) {
            setDeleteBeteweenTest(true);
            deleteCatalog(CATALOG);
            setDeleteBeteweenTest(false);

            TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
            tableMetadataBuilder.addColumn(ExampleWorkflows.COLUMN_ID, ColumnType.INT)
                    .addColumn(ExampleWorkflows.COLUMN_NAME, ColumnType.VARCHAR)
                    .addColumn(ExampleWorkflows.COLUMN_AGE, ColumnType.INT)
                    .addColumn(ExampleWorkflows.COLUMN_BOOL, ColumnType.BOOLEAN);

            TableMetadata targetTable = tableMetadataBuilder.build();

            for (int i = 0; i < 100; i++) {
                connector.getStorageEngine().insert(getClusterName(), targetTable, exampleWorkflows.getRows(i));
                refresh(CATALOG);
            }
            System.out.println("Insert Finish");
            insertData = true;

        }
    }

    @Test
    public void basicSelect() throws UnsupportedException, ExecutionException {

        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST basicSelect ***********************************");

        LogicalWorkflow logicalWorkflow = exampleWorkflows.getBasicSelect();
        QueryResult qr = connector.getQueryEngine().execute(logicalWorkflow);
        assertEquals("The result number is correct", 1000000, qr.getResultSet().size());

        Row oneRow = qr.getResultSet().iterator().next();
        assertTrue("The alias age is correct", oneRow.getCells().containsKey("users.name"));
        assertTrue("The alias name is correct", oneRow.getCells().containsKey("users.age"));
    }

    @Test
    public void basicSelectAsterisk() throws UnsupportedException, ExecutionException {
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST basicSelectAsterisk ***********************************");

        LogicalWorkflow logicalWorkflow = exampleWorkflows.getBasicSelectAsterisk();
        QueryResult qr = connector.getQueryEngine().execute(logicalWorkflow);
        assertEquals("All record are recovered", 1000000, qr.getResultSet().size());
    }

    @Test
    public void selectIndexedField() throws UnsupportedException, ExecutionException {

        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST basicSelectAsterisk ***********************************");

        LogicalWorkflow logicalWorkflow = exampleWorkflows.getSelectIndexedField();

        QueryResult qr = connector.getQueryEngine().execute(logicalWorkflow);
        assertEquals("The items number in the resultset is correct", 1623, qr.getResultSet().size());

        Row oneRow = qr.getResultSet().iterator().next();
        assertEquals("The cells number is correct", 2, oneRow.getCells().size());
        assertTrue("The alias age is correct", oneRow.getCells().containsKey("users.name"));
        assertTrue("The alias name is correct", oneRow.getCells().containsKey("users.age"));
    }

    @Test
    public void selectNonIndexedField() throws UnsupportedException, ExecutionException {
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNonIndexedField ***********************************");

        LogicalWorkflow logicalWorkflow = exampleWorkflows.getSelectNonIndexedField();

        QueryResult qr = connector.getQueryEngine().execute(logicalWorkflow);

        assertEquals("The items number in the resultset is correct", 9009, qr.getResultSet().size());

        Row oneRow = qr.getResultSet().iterator().next();

        assertEquals("The cells number is correct", 2, oneRow.getCells().size());
        assertTrue("The alias age is correct", oneRow.getCells().containsKey("users.name"));
        assertTrue("The alias name is correct", oneRow.getCells().containsKey("users.age"));

    }

    @Test
    public void selectMixedWhere() throws UnsupportedException, ExecutionException {
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectMixedWhere ***********************************");

        LogicalWorkflow logicalWorkflow = exampleWorkflows.getSelectMixedWhere();

        QueryResult qr = connector.getQueryEngine().execute(logicalWorkflow);
        assertEquals("The result is correct", 7, qr.getResultSet().size());

    }

}
