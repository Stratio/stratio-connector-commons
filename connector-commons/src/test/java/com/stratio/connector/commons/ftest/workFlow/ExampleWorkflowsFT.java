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

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.*;
import com.stratio.crossdata.common.result.QueryResult;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by jjlopez on 16/09/14.
 */
public abstract class ExampleWorkflowsFT extends GenericConnectorTest {

    private static boolean insertData = false;
    private ExampleWorkflows exampleWorkflows;

    @Before
    public void setUp() throws ConnectorException {
        setDeleteBeteweenTest(false);
        super.setUp();

        exampleWorkflows = new ExampleWorkflows(CATALOG, TABLE, getClusterName());
        if (!insertData) {
            setDeleteBeteweenTest(true);
            deleteCatalog(CATALOG);
            setDeleteBeteweenTest(false);

            TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE,
                    getClusterName().getName());
            tableMetadataBuilder.addColumn(ExampleWorkflows.COLUMN_ID, new ColumnType(DataType.INT))
                    .addColumn(ExampleWorkflows.COLUMN_NAME, new ColumnType(DataType.VARCHAR))
                    .addColumn(ExampleWorkflows.COLUMN_AGE, new ColumnType(DataType.INT))
                    .addColumn(ExampleWorkflows.COLUMN_BOOL, new ColumnType(DataType.BOOLEAN))
                    .withPartitionKey(ExampleWorkflows.COLUMN_ID);

            TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());

            for (int i = 0; i < 100; i++) {
                connector.getStorageEngine().insert(getClusterName(), targetTable, exampleWorkflows.getRows(i), false);
                refresh(CATALOG);
            }

            insertData = true;
            if (getConnectorHelper().isIndexMandatory()) {
                Map<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
                ColumnName columnName = new ColumnName(CATALOG, TABLE, ExampleWorkflows.COLUMN_NAME);
                columns.put(columnName, new ColumnMetadata(columnName, null, new ColumnType(DataType.VARCHAR)));
                IndexMetadata indexMetadata = new IndexMetadata(new IndexName(CATALOG, TABLE, "IndexTest"
                        + this.getClass().getName() + "SelectIndexedField"), columns, IndexType.DEFAULT, null);

                connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
            }

        }
    }

    @Test
    public void basicSelect() throws ConnectorException {

        LogicalWorkflow logicalWorkflow = exampleWorkflows.getBasicSelect();
        QueryResult qr = connector.getQueryEngine().execute("", logicalWorkflow);
        assertEquals("The result number is correct", 1000000, qr.getResultSet().size());

        Row oneRow = qr.getResultSet().iterator().next();
        assertTrue("The alias age is correct", oneRow.getCells().containsKey("users.name"));
        assertTrue("The alias name is correct", oneRow.getCells().containsKey("users.age"));
    }

    @Test
    public void basicSelectAsterisk() throws ConnectorException {

        LogicalWorkflow logicalWorkflow = exampleWorkflows.getBasicSelectAsterisk();
        QueryResult qr = connector.getQueryEngine().execute("", logicalWorkflow);
        assertEquals("All record are recovered", 1000000, qr.getResultSet().size());
    }

    @Test
    public void selectIndexedField() throws ConnectorException {

        LogicalWorkflow logicalWorkflow = exampleWorkflows.getSelectIndexedField();

        QueryResult qr = connector.getQueryEngine().execute("", logicalWorkflow);
        assertEquals("The items number in the resultset is correct", 1623, qr.getResultSet().size());

        Row oneRow = qr.getResultSet().iterator().next();
        assertEquals("The cells number is correct", 2, oneRow.getCells().size());
        assertTrue("The alias age is correct", oneRow.getCells().containsKey("users.name"));
        assertTrue("The alias name is correct", oneRow.getCells().containsKey("users.age"));
    }

    @Test
    public void selectNonIndexedField() throws ConnectorException {

        LogicalWorkflow logicalWorkflow = exampleWorkflows.getSelectNonIndexedField();

        QueryResult qr = connector.getQueryEngine().execute("", logicalWorkflow);

        assertEquals("The items number in the resultset is correct", 9009, qr.getResultSet().size());

        Row oneRow = qr.getResultSet().iterator().next();

        assertEquals("The cells number is correct", 2, oneRow.getCells().size());
        assertTrue("The alias age is correct", oneRow.getCells().containsKey("users.name"));
        assertTrue("The alias name is correct", oneRow.getCells().containsKey("users.age"));

    }

    @Test
    public void selectMixedWhere() throws ConnectorException {

        LogicalWorkflow logicalWorkflow = exampleWorkflows.getSelectMixedWhere();

        QueryResult qr = connector.getQueryEngine().execute("", logicalWorkflow);
        assertEquals("The result is correct", 7, qr.getResultSet().size());

    }

}
