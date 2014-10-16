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
package com.stratio.connector.commons.ftest.functionalTestQuery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.QualifiedNames;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * Created by jmgomez on 17/07/14.
 */
public abstract class GenericQueryTest extends GenericConnectorTest {

    public static final String COLUMN_1 = "bin1";
    public static final String COLUMN_2 = "bin2";
    public static final String COLUMN_3 = "bin3";

    private Integer DEFAULT_GET_TO_SEARCH = 100;

    protected Integer getRowsToSearch() {
        return DEFAULT_GET_TO_SEARCH;
    }

    @Test
    public void selectAllFromTable() throws UnsupportedException, ExecutionException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectAllFromTable ***********************************");

        for (int i = 0; i < getRowsToSearch(); i++) {
            insertRow(i, clusterNodeName);
        }

        insertRecordNotReturnedInSearch(clusterNodeName);

        refresh(CATALOG);

        LinkedList<LogicalWorkFlowCreator.ConnectorField> fields = new LinkedList<>();
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
        fields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_1, COLUMN_1, ColumnType.VARCHAR));
        fields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_2, COLUMN_2, ColumnType.VARCHAR));
        fields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_3, COLUMN_3, ColumnType.VARCHAR));

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_1, COLUMN_2, COLUMN_3)
                        .addSelect(fields).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);
        Set<Object> proveSet = new HashSet<>();
        ResultSet resultSet = queryResult.getResultSet();

        Iterator<Row> rowIterator = resultSet.iterator();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();

            Map<String, Cell> cells = row.getCells();
            String[] columnName = cells.keySet().toArray(new String[0]);

            validateFieldOrder(columnName);

            for (String cell : row.getCells().keySet()) {
                proveSet.add(cell + row.getCell(cell).getValue());
            }
        }

        validateResult(proveSet, resultSet);

    }

    @Test
    public void validateMetadataTest() throws UnsupportedException, ExecutionException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectAllFromTable ***********************************");
        insertTypedRow();

        refresh(CATALOG);

        LinkedList<LogicalWorkFlowCreator.ConnectorField> fields = new LinkedList<>();
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
        fields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_1, "alias" + COLUMN_1, ColumnType.TEXT));
        fields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_2, "alias" + COLUMN_2, ColumnType.INT));
        fields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_3, "alias" + COLUMN_3, ColumnType.BOOLEAN));

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_1, COLUMN_2, COLUMN_3)
                        .addSelect(fields).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        ResultSet resultSet = queryResult.getResultSet();
        List<ColumnMetadata> columnMetadata = resultSet.getColumnMetadata();
        validateMetadata(columnMetadata);

    }

    protected void insertTypedRow() throws UnsupportedException, ExecutionException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("String"));
        cells.put(COLUMN_2, new Cell(new Integer(10)));
        cells.put(COLUMN_3, new Cell(true));

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.INT)
                        .addColumn(COLUMN_3, ColumnType.BOOLEAN);

        TableMetadata targetTable = tableMetadataBuilder.build();

        row.setCells(cells);
        connector.getStorageEngine().insert(getClusterName(), targetTable, row);
    }

    protected void validateMetadata(List<ColumnMetadata> columnMetadata) {
        assertNotNull("The metadate is not null", columnMetadata);
        ColumnMetadata[] metadata = columnMetadata.toArray(new ColumnMetadata[0]);

        assertEquals("the table name is correct", QualifiedNames.getTableQualifiedName(CATALOG, TABLE),
                        metadata[0].getTableName());
        assertEquals("the table name is correct", QualifiedNames.getTableQualifiedName(CATALOG, TABLE),
                        metadata[1].getTableName());
        assertEquals("the table name is correct", QualifiedNames.getTableQualifiedName(CATALOG, TABLE),
                        metadata[2].getTableName());

        assertEquals("The first column type is correct", ColumnType.TEXT, metadata[0].getType());
        assertEquals("The second column type is correct", ColumnType.INT, metadata[1].getType());
        assertEquals("The third column type is correct", ColumnType.BOOLEAN, metadata[2].getType());

        assertEquals("The first column name is correct",
                        QualifiedNames.getColumnQualifiedName(CATALOG, TABLE, COLUMN_1), metadata[0].getColumnName());
        assertEquals("The first column name is correct",
                        QualifiedNames.getColumnQualifiedName(CATALOG, TABLE, COLUMN_2), metadata[1].getColumnName());
        assertEquals("The first column name is correct",
                        QualifiedNames.getColumnQualifiedName(CATALOG, TABLE, COLUMN_3), metadata[2].getColumnName());

        assertEquals("The first column alias is correct", "alias" + COLUMN_1, metadata[0].getColumnAlias());
        assertEquals("The first column alias is correct", "alias" + COLUMN_2, metadata[1].getColumnAlias());
        assertEquals("The first column alias is correct", "alias" + COLUMN_3, metadata[2].getColumnAlias());

    }

    private void validateResult(Set<Object> proveSet, ResultSet resultSet) {
        assertEquals("The record number is correct", getRowsToSearch(), (Integer) resultSet.size());
        for (int i = 0; i < getRowsToSearch(); i++) {
            assertTrue("Return correct record", proveSet.contains("bin1ValueBin1_r" + i));
            assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r" + i));
            assertTrue("Return correct record", proveSet.contains("bin3ValueBin3_r" + i));
        }
    }

    private void validateFieldOrder(String[] columnName) {
        assertEquals("The first column is sorted", COLUMN_1, columnName[0]);
        assertEquals("The second column is sorted", COLUMN_2, columnName[1]);
        assertEquals("The third column is sorted", COLUMN_3, columnName[2]);
    }

    private void insertRecordNotReturnedInSearch(ClusterName clusterNodeName) throws ExecutionException,
                    UnsupportedException {
        insertRow(1, "type2", clusterNodeName);
        insertRow(2, "type2", clusterNodeName);
        insertRow(1, "otherTable", clusterNodeName);
        insertRow(2, "otherTable", clusterNodeName);
    }

    private void insertRow(int ikey, ClusterName clusterNodeName) throws UnsupportedOperationException,
                    ExecutionException, UnsupportedException {
        insertRow(ikey, TABLE, clusterNodeName);
    }

    private void insertRow(int ikey, String Table, ClusterName clusterNodeName) throws UnsupportedOperationException,
                    ExecutionException, UnsupportedException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("ValueBin1_r" + ikey));
        cells.put(COLUMN_2, new Cell("ValueBin2_r" + ikey));
        cells.put(COLUMN_3, new Cell("ValueBin3_r" + ikey));
        row.setCells(cells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, Table);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.VARCHAR)
                        .addColumn(COLUMN_3, ColumnType.VARCHAR);

        TableMetadata targetTable = tableMetadataBuilder.build();

        connector.getStorageEngine().insert(clusterNodeName, targetTable, row);

    }

}
