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
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.QualifiedNames;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * Created by jmgomez on 17/07/14.
 */
public abstract class GenericQueryFT extends GenericConnectorTest {

    public static final String COLUMN_1 = "bin1";
    public static final String ALIAS_COLUMN_1 = "alias_" + COLUMN_1;
    public static final String COLUMN_2 = "bin2";
    public static final String ALIAS_COLUMN_2 = "alias_" + COLUMN_2;
    public static final String COLUMN_3 = "bin3";
    public static final String ALIAS_COLUMN_3 = "alias_" + COLUMN_3;

    private Integer DEFAULT_GET_TO_SEARCH = 100;

    protected Integer getRowsToSearch() {
        return DEFAULT_GET_TO_SEARCH;
    }

    @Test
    public void selectAllFromTable() throws ConnectorException {

        ClusterName clusterNodeName = getClusterName();

        for (int i = 0; i < getRowsToSearch(); i++) {
            insertRow(i, clusterNodeName);
        }

        insertRecordNotReturnedInSearch(clusterNodeName);

        refresh(CATALOG);

        LinkedList<LogicalWorkFlowCreator.ConnectorField> fields = new LinkedList<>();
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
        fields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_1, ALIAS_COLUMN_1, new ColumnType(DataType
                .VARCHAR)));
        fields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_2, ALIAS_COLUMN_2, new ColumnType(DataType
                .VARCHAR)));
        fields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_3, ALIAS_COLUMN_3, new ColumnType(DataType
                .VARCHAR)));

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_1, COLUMN_2, COLUMN_3)
                        .addSelect(fields).build();
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
    public void validateMetadataTest() throws ConnectorException {


        insertTypedRow();

        refresh(CATALOG);

        LinkedList<LogicalWorkFlowCreator.ConnectorField> fields = new LinkedList<>();
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
        fields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_1, ALIAS_COLUMN_1, new ColumnType(DataType
                .TEXT)));
        fields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_2, ALIAS_COLUMN_2, new ColumnType(DataType.INT)));
        fields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_3, ALIAS_COLUMN_3, new ColumnType(DataType
                .BOOLEAN))  );

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_1, COLUMN_2, COLUMN_3)
                        .addSelect(fields).build();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        ResultSet resultSet = queryResult.getResultSet();
        List<ColumnMetadata> columnMetadata = resultSet.getColumnMetadata();
        validateMetadata(columnMetadata);

    }

    protected void insertTypedRow() throws ConnectorException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("String"));
        cells.put(COLUMN_2, new Cell(new Integer(10)));
        cells.put(COLUMN_3, new Cell(true));

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE,getClusterName().getName());
        tableMetadataBuilder.addColumn(COLUMN_1, new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_2, new ColumnType
                (DataType.INT)).addColumn(COLUMN_3, new ColumnType(DataType.BOOLEAN));

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());

        row.setCells(cells);
        connector.getStorageEngine().insert(getClusterName(), targetTable, row, false);
    }

    protected void validateMetadata(List<ColumnMetadata> columnMetadata) {
        assertNotNull("The metadata is not null", columnMetadata);
        ColumnMetadata[] metadata = columnMetadata.toArray(new ColumnMetadata[0]);


        assertEquals("the table name is correct", QualifiedNames.getTableQualifiedName(CATALOG, TABLE),

        metadata[0].getName().getTableName().toString());
        assertEquals("the table name is correct", QualifiedNames.getTableQualifiedName(CATALOG, TABLE), metadata[1]
                        .getName().getTableName().toString());
        assertEquals("the table name is correct", QualifiedNames.getTableQualifiedName(CATALOG, TABLE), metadata[2]
                        .getName().getTableName().toString());

        assertEquals("The first column type is correct", new ColumnType(DataType.TEXT), metadata[0].getColumnType());
        assertEquals("The second column type is correct", new ColumnType(DataType.INT), metadata[1].getColumnType());
        assertEquals("The third column type is correct", new ColumnType(DataType.BOOLEAN), metadata[2].getColumnType());

        assertEquals("The first column name is correct",
                        QualifiedNames.getColumnQualifiedName(CATALOG, TABLE, COLUMN_1), metadata[0].getName()
                                        .getQualifiedName());
        assertEquals("The first column name is correct",
                        QualifiedNames.getColumnQualifiedName(CATALOG, TABLE, COLUMN_2), metadata[1].getName()
                                        .getQualifiedName());
        assertEquals("The first column name is correct",
                        QualifiedNames.getColumnQualifiedName(CATALOG, TABLE, COLUMN_3), metadata[2].getName()
                                        .getQualifiedName());

        assertEquals("The first column alias is correct", ALIAS_COLUMN_1, metadata[0].getName().getAlias());
        assertEquals("The first column alias is correct", ALIAS_COLUMN_2, metadata[1].getName().getAlias());
        assertEquals("The first column alias is correct", ALIAS_COLUMN_3, metadata[2].getName().getAlias());

    }

    private void validateResult(Set<Object> proveSet, ResultSet resultSet) {
        assertEquals("The record number is correct", getRowsToSearch(), (Integer) resultSet.size());
        for (int i = 0; i < getRowsToSearch(); i++) {
            assertTrue("Return correct record", proveSet.contains(ALIAS_COLUMN_1 + "ValueBin1_r" + i));
            assertTrue("Return correct record", proveSet.contains(ALIAS_COLUMN_2 + "ValueBin2_r" + i));
            assertTrue("Return correct record", proveSet.contains(ALIAS_COLUMN_3 + "ValueBin3_r" + i));
        }
    }

    private void validateFieldOrder(String[] columnName) {
        assertEquals("The first column is sorted", ALIAS_COLUMN_1, columnName[0]);
        assertEquals("The second column is sorted", ALIAS_COLUMN_2, columnName[1]);
        assertEquals("The third column is sorted", ALIAS_COLUMN_3, columnName[2]);
    }

    private void insertRecordNotReturnedInSearch(ClusterName clusterNodeName) throws ConnectorException {
        insertRow(1, "type2", clusterNodeName);
        insertRow(2, "type2", clusterNodeName);
        insertRow(1, "otherTable", clusterNodeName);
        insertRow(2, "otherTable", clusterNodeName);
    }

    private void insertRow(int ikey, ClusterName clusterNodeName) throws UnsupportedOperationException,
                    ConnectorException {
        insertRow(ikey, TABLE, clusterNodeName);
    }

    private void insertRow(int ikey, String Table, ClusterName clusterNodeName) throws UnsupportedOperationException,
                    ConnectorException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("ValueBin1_r" + ikey));
        cells.put(COLUMN_2, new Cell("ValueBin2_r" + ikey));
        cells.put(COLUMN_3, new Cell("ValueBin3_r" + ikey));
        row.setCells(cells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, Table,getClusterName().getName());
        tableMetadataBuilder.addColumn(COLUMN_1,
                new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_2, new ColumnType(DataType.VARCHAR))
                .addColumn(COLUMN_3, new ColumnType(DataType.VARCHAR));

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());

        connector.getStorageEngine().insert(clusterNodeName, targetTable, row, false);

    }

}
