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
package com.stratio.connector.commons.ftest.functionalInsert;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator.ConnectorField;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 */
public abstract class GenericSimpleInsertTest extends GenericConnectorTest {

    public static final String COLUMN_4 = "COLUMN_4".toLowerCase();
    public static final String VALUE_1 = "value1";
    protected static final String COLUMN_1 = "COLUMN_1".toLowerCase();
    protected static final String COLUMN_2 = "COLUMN_2".toLowerCase();
    protected static final String COLUMN_3 = "COLUMN_3".toLowerCase();
    protected static final String OTHER_VALUE_1 = "OTHER VALUE";
    protected static final String VALUE_4 = "value4";
    protected static final String OTHER_VALUE_4 = "other value 4";
    protected static final int VALUE_2 = 2;
    protected static final boolean VALUE_3 = true;

    @Test
    public void testSimpleInsertWithPK() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST testSimpleInsert  ***********************************");

        insertRow(clusterName, VALUE_4, ColumnType.VARCHAR, VALUE_1, true);

        verifyInsert(clusterName, VALUE_4);
    }

    @Test
    public void testSimpleInsertWithoutPK() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST testSimpleInsert  ***********************************");

        insertRow(clusterName, VALUE_4, ColumnType.VARCHAR, VALUE_1, false);

        verifyInsert(clusterName, VALUE_4);
    }

    @Test
    public void testInsertSamePK() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertSamePK "
                + clusterName.getName() + " ***********************************");

        insertRow(clusterName, VALUE_4, ColumnType.VARCHAR, VALUE_1, true);
        insertRow(clusterName, OTHER_VALUE_4, ColumnType.VARCHAR, VALUE_1, true);

        verifyInsert(clusterName, OTHER_VALUE_4);

    }

    @Test
    public void testInsertDuplicateCompositePK() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testDuplicateCompositePK "
                + clusterName.getName() + " ***********************************");

        insertRowCompositePK(clusterName, COLUMN_1, COLUMN_2);
        try {
            insertRowCompositePK(clusterName, COLUMN_1, COLUMN_2);
        } catch (Exception e) {
        }

        ResultSet resultIterator = createResultSet(clusterName);

        assertEquals("The records number is correct " + clusterName.getName(), 1, resultIterator.size());

    }

    @Test
    public void testInsertCompositePK() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testCompositePK "
                + clusterName.getName() + " ***********************************");

        insertRowCompositePK(clusterName, COLUMN_1, COLUMN_2);
        insertRowCompositePK(clusterName, COLUMN_1, COLUMN_3);

        ResultSet resultIterator = createResultSet(clusterName);

        assertEquals("The records number is correct " + clusterName.getName(), 2, resultIterator.size());

    }

    @Test
    public void testInsertString() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertSamePK "
                + clusterName.getName() + " ***********************************");

        Object value4 = "String";
        insertRow(clusterName, value4, ColumnType.VARCHAR, VALUE_1, true);

        ResultSet resultIterator = createResultSet(clusterName);
        assertEquals("It has only one result", 1, resultIterator.size());
        for (Row recoveredRow : resultIterator) {
            assertEquals("The type is correct ", String.class.getCanonicalName(), recoveredRow.getCell(COLUMN_4)
                    .getValue().getClass().getCanonicalName());
            assertEquals("The value is correct ", value4, recoveredRow.getCell(COLUMN_4).getValue());
        }

    }

    @Test
    public void testInsertInteger() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST testInsertInteger ***********************************");
        Object value4 = new Integer(1);
        insertRow(clusterName, value4, ColumnType.INT, VALUE_1, true);

        ResultSet resultIterator = createResultSet(clusterName);
        assertEquals("It has only one result", 1, resultIterator.size());
        for (Row recoveredRow : resultIterator) {
            assertEquals("The type is correct ", Integer.class.getCanonicalName(), recoveredRow.getCell(COLUMN_4)
                    .getValue().getClass().getCanonicalName());
            assertEquals("The value is correct ", value4, recoveredRow.getCell(COLUMN_4).getValue());
        }

    }

    @Test
    public void testInsertLong() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertSamePK "
                + clusterName.getName() + " ***********************************");
        Object value4 = 1L;
        insertRow(clusterName, value4, ColumnType.BIGINT, VALUE_1, true);

        ResultSet resultIterator = createResultSet(clusterName);
        assertEquals("It has only one result", 1, resultIterator.size());
        for (Row recoveredRow : resultIterator) {
            assertEquals("The type is correct ", Long.class.getCanonicalName(), recoveredRow.getCell(COLUMN_4)
                    .getValue().getClass().getCanonicalName());
            assertEquals("The value is correct ", value4, recoveredRow.getCell(COLUMN_4).getValue());
        }

    }

    @Test
    public void testInsertBoolean() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertSamePK "
                + clusterName.getName() + " ***********************************");
        Object value4 = new Boolean(true);
        insertRow(clusterName, value4, ColumnType.BOOLEAN, VALUE_1, true);

        ResultSet resultIterator = createResultSet(clusterName);
        assertEquals("It has only one result", 1, resultIterator.size());
        for (Row recoveredRow : resultIterator) {
            assertEquals("The type is correct ", Boolean.class.getCanonicalName(), recoveredRow.getCell(COLUMN_4)
                    .getValue().getClass().getCanonicalName());
            assertEquals("The value is correct ", value4, recoveredRow.getCell(COLUMN_4).getValue());
        }

    }

    @Test
    public void testInsertDate() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertSamePK "
                + clusterName.getName() + " ***********************************");
        Object value4 = new Date();
        insertRow(clusterName, value4, ColumnType.NATIVE, VALUE_1, true);

        ResultSet resultIterator = createResultSet(clusterName);
        assertEquals("It has only one result", 1, resultIterator.size());
        for (Row recoveredRow : resultIterator) {
            assertEquals("The type is correct ", Date.class.getCanonicalName(), recoveredRow.getCell(COLUMN_4)
                    .getValue().getClass().getCanonicalName());
            assertEquals("The value is correct ", value4, recoveredRow.getCell(COLUMN_4).getValue());
        }

    }

    @Test
    public void testInsertDouble() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertSamePK "
                + clusterName.getName() + " ***********************************");
        Object value4 = new Double(25.32);
        insertRow(clusterName, value4, ColumnType.DOUBLE, VALUE_1, true);

        ResultSet resultIterator = createResultSet(clusterName);
        assertEquals("It has only one result", 1, resultIterator.size());
        for (Row recoveredRow : resultIterator) {
            assertEquals("The type is correct ", Double.class.getCanonicalName(), recoveredRow.getCell(COLUMN_4)
                    .getValue().getClass().getCanonicalName());
            assertEquals("The value is correct ", value4, recoveredRow.getCell(COLUMN_4).getValue());
        }

    }

    private void verifyInsert(ClusterName clusterName, String test_value_4) throws UnsupportedException,
            ExecutionException {
        ResultSet resultIterator = createResultSet(clusterName);

        for (Row recoveredRow : resultIterator) {
            assertEquals("The value is correct ", VALUE_1, recoveredRow.getCell(COLUMN_1).getValue());
            assertEquals("The value is correct ", VALUE_2, recoveredRow.getCell(COLUMN_2).getValue());
            assertEquals("The value is correct ", VALUE_3, recoveredRow.getCell(COLUMN_3).getValue());
            assertEquals("The value is correct ", test_value_4, recoveredRow.getCell(COLUMN_4).getValue());
        }

        assertEquals("The records number is correct " + clusterName.getName(), 1, resultIterator.size());
    }

    protected ResultSet createResultSet(ClusterName clusterName) throws UnsupportedException, ExecutionException {
        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow());
        return queryResult.getResultSet();
    }

    protected ResultSet createResultSetTyped(ClusterName clusterName, ColumnType colType) throws UnsupportedException,
            ExecutionException {
        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlowValue4Type(colType));
        return queryResult.getResultSet();
    }

    protected void insertRow(ClusterName cluesterName, Object value_4, ColumnType colType_4, String PK_VALUE,
            boolean withPK) throws UnsupportedException, ExecutionException {
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();

        cells.put(COLUMN_1, new Cell(PK_VALUE));
        cells.put(COLUMN_2, new Cell(VALUE_2));
        cells.put(COLUMN_3, new Cell(VALUE_3));
        cells.put(COLUMN_4, new Cell(value_4));

        row.setCells(cells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.VARCHAR)
                .addColumn(COLUMN_3, ColumnType.VARCHAR).addColumn(COLUMN_4, colType_4);
        if (withPK) {
            tableMetadataBuilder.withPartitionKey(COLUMN_1);
        }
        TableMetadata targetTable = tableMetadataBuilder.build();

        if (getConnectorHelper().isTableMandatory()) {
            connector.getMetadataEngine().createTable(getClusterName(), targetTable);
        }
        connector.getStorageEngine().insert(cluesterName, targetTable, row);
        refresh(CATALOG);
    }

    private void insertRowCompositePK(ClusterName cluesterName, String partitionKey, String clusterKey)
            throws UnsupportedException, ExecutionException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();

        cells.put(partitionKey, new Cell(VALUE_1));
        cells.put(clusterKey, new Cell(VALUE_2));
        cells.put(COLUMN_3, new Cell(VALUE_3));
        cells.put(COLUMN_4, new Cell(VALUE_4));

        row.setCells(cells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.VARCHAR)
                .addColumn(COLUMN_3, ColumnType.VARCHAR).addColumn(COLUMN_4, ColumnType.VARCHAR);

        tableMetadataBuilder.withPartitionKey(COLUMN_1);
        tableMetadataBuilder.withClusterKey(clusterKey);

        TableMetadata targetTable = tableMetadataBuilder.build();
        if (getConnectorHelper().isCatalogMandatory()) {
            connector.getMetadataEngine()
                    .createCatalog(getClusterName(),
                            new CatalogMetadata(new CatalogName(CATALOG), Collections.EMPTY_MAP,
                                    Collections.EMPTY_MAP));
        }
        connector.getStorageEngine().insert(cluesterName, targetTable, row);
        refresh(CATALOG);
    }

    private LogicalWorkflow createLogicalWorkFlow() {

        return new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName()).addColumnName(COLUMN_1, COLUMN_2, COLUMN_3,
                COLUMN_4).getLogicalWorkflow();

    }

    private LogicalWorkflow createLogicalWorkFlowValue4Type(ColumnType colType) {
        LinkedList<ConnectorField> linkList = new LinkedList<ConnectorField>();
        LogicalWorkFlowCreator lwfC = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
        linkList.add(lwfC.createConnectorField(COLUMN_1, COLUMN_1, ColumnType.VARCHAR));
        linkList.add(lwfC.createConnectorField(COLUMN_2, COLUMN_2, ColumnType.VARCHAR));
        linkList.add(lwfC.createConnectorField(COLUMN_3, COLUMN_3, ColumnType.VARCHAR));
        linkList.add(lwfC.createConnectorField(COLUMN_4, COLUMN_4, colType));
        return new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName())
                .addColumnName(COLUMN_1, COLUMN_2, COLUMN_3, COLUMN_4).addSelect(linkList).getLogicalWorkflow();

    }

}
