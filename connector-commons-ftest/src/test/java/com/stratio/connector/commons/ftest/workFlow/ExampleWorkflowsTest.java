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

        System.out.println("*********************************** INIT FUNCTIONAL TEST basicSelect ***********************************");

        LogicalWorkflow logicalWorkflow = exampleWorkflows.getBasicSelect();
        QueryResult qr = connector.getQueryEngine().execute(logicalWorkflow);
        assertEquals("The result number is correct", 1000000, qr.getResultSet().size());

        Row oneRow = qr.getResultSet().iterator().next();
        assertTrue("The alias age is correct", oneRow.getCells().containsKey("users.name"));
        assertTrue("The alias name is correct", oneRow.getCells().containsKey("users.age"));
    }

    @Test
    public void basicSelectAsterisk() throws UnsupportedException, ExecutionException {
        System.out.println("*********************************** INIT FUNCTIONAL TEST basicSelectAsterisk ***********************************");

        LogicalWorkflow logicalWorkflow = exampleWorkflows.getBasicSelectAsterisk();
        QueryResult qr = connector.getQueryEngine().execute(logicalWorkflow);
        assertEquals("All record are recovered", 1000000, qr.getResultSet().size());
    }

    @Test
    public void selectIndexedField() throws UnsupportedException, ExecutionException {

        System.out.println("*********************************** INIT FUNCTIONAL TEST basicSelectAsterisk ***********************************");

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
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNonIndexedField ***********************************");

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
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectMixedWhere ***********************************");

        LogicalWorkflow logicalWorkflow = exampleWorkflows.getSelectMixedWhere();

        QueryResult qr = connector.getQueryEngine().execute(logicalWorkflow);
        assertEquals("The result is correct", 7, qr.getResultSet().size());

    }

}
