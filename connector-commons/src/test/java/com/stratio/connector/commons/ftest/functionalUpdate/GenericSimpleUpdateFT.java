/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.commons.ftest.functionalUpdate;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.data.*;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.*;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Checks either an update in a single row or an update in several rows, both without filters.
 */
public abstract class GenericSimpleUpdateFT extends GenericConnectorTest<IConnector> {

    public static final String VALUE_1 = "value1";
    public static final String VALUE_1_UPDATED = "otherValue";
    protected static final String COLUMN_1 = "COLUMN_1".toLowerCase();
    protected static final String COLUMN_2 = "COLUMN_2".toLowerCase();
    protected static final String COLUMN_3 = "COLUMN_3".toLowerCase();
    protected static final long VALUE_2 = 2;
    protected static final boolean VALUE_3 = true;
    protected static final long VALUE_2_UPDATED = 1;
    protected static final boolean VALUE_3_UPDATED = false;

    public static Collection<Filter> filterCollection = null;

    public void setFilterCollection(Collection<Filter> filters) {
        filterCollection = filters;
    }

    @Test
    public void updateGenericsFieldsFT() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        insertRow(clusterName);
        verifyInsert(clusterName, 1);

        updateRow(clusterName);
        verifyUpdate(clusterName, 1);

    }

    @Test
    public void multiUpdateGenericsFieldsFT() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        insertRow(clusterName);
        insertRow(clusterName);
        verifyInsert(clusterName, 2);

        updateRow(clusterName);
        verifyUpdate(clusterName, 2);

    }

    protected void verifyUpdate(ClusterName clusterName, int insertedRows) throws ConnectorException {
        ResultSet resultIterator = createResultSet(clusterName);

        for (Row recoveredRow : resultIterator) {
            assertEquals("Checking the updated value", VALUE_1_UPDATED, recoveredRow.getCell(COLUMN_1).getValue());
            assertEquals("Checking the updated value", VALUE_2_UPDATED, recoveredRow.getCell(COLUMN_2).getValue());
            assertEquals("Checking the updated value", VALUE_3_UPDATED, recoveredRow.getCell(COLUMN_3).getValue());
        }

        assertEquals("The records number is correct " + clusterName.getName(), insertedRows, resultIterator.size());
    }

    private void verifyUpdateRowsWithDifferentFields(ClusterName clusterName) throws ConnectorException {
        ResultSet resultIterator = createResultSet(clusterName);

        for (Row recoveredRow : resultIterator) {
            assertEquals("Checking the updated value", VALUE_1_UPDATED, recoveredRow.getCell(COLUMN_1).getValue());
            if (recoveredRow.getCell(COLUMN_2).getValue() != null) {
                assertEquals("The value is correct ", VALUE_2_UPDATED, recoveredRow.getCell(COLUMN_2).getValue());
                assertEquals("The value is correct ", null, recoveredRow.getCell(COLUMN_3).getValue());
            } else {
                assertEquals("The value is correct ", VALUE_3_UPDATED, recoveredRow.getCell(COLUMN_3).getValue());
                assertEquals("The value is correct ", null, recoveredRow.getCell(COLUMN_2).getValue());
            }
        }

        assertEquals("The records number is correct " + clusterName.getName(), 2, resultIterator.size());

    }

    private void updateRow(ClusterName clusterName) throws UnsupportedException, ConnectorException {

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE,
                getClusterName().getName());
        tableMetadataBuilder.addColumn(COLUMN_1, new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_2,
                new ColumnType(DataType
                        .BIGINT))
                .addColumn(COLUMN_3, new ColumnType(DataType.BOOLEAN));

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());

        if (getConnectorHelper().isTableMandatory()) {
            connector.getMetadataEngine().createTable(clusterName, targetTable);
        }

        Relation rel1 = getBasicRelation(COLUMN_1, Operator.ASSIGN, VALUE_1_UPDATED);
        Relation rel2 = getBasicRelation(COLUMN_2, Operator.ASSIGN, VALUE_2_UPDATED);
        Relation rel3 = getBasicRelation(COLUMN_3, Operator.ASSIGN, VALUE_3_UPDATED);

        List<Relation> assignments = Arrays.asList(rel1, rel2, rel3);

        connector.getStorageEngine().update(clusterName, new TableName(CATALOG, TABLE), assignments, filterCollection);
        refresh(CATALOG);

    }

    private Relation getBasicRelation(String column1, Operator assign, Object valueUpdated) {
        Selector leftSelector = new ColumnSelector(new ColumnName(CATALOG, TABLE, column1));
        Selector rightSelector = null;
        if (valueUpdated instanceof Long) {
            rightSelector = new IntegerSelector((int) (long) valueUpdated);
        } else if (valueUpdated instanceof String) {
            rightSelector = new StringSelector((String) valueUpdated);
        } else if (valueUpdated instanceof Boolean) {
            rightSelector = new BooleanSelector((Boolean) valueUpdated);
        }
        return new Relation(leftSelector, assign, rightSelector);
    }

    private void verifyInsert(ClusterName clusterName, int rowsInserted) throws ConnectorException {
        ResultSet resultIterator = createResultSet(clusterName);

        for (Row recoveredRow : resultIterator) {
            assertEquals("The value is correct ", VALUE_1, recoveredRow.getCell(COLUMN_1).getValue());
            assertEquals("The value is correct ", VALUE_2, recoveredRow.getCell(COLUMN_2).getValue());
            assertEquals("The value is correct ", VALUE_3, recoveredRow.getCell(COLUMN_3).getValue());
        }

        assertEquals("The records number is correct " + clusterName.getName(), rowsInserted, resultIterator.size());
    }

    private void verifyInsertRowsWithDifferentFields(ClusterName clusterName) throws ConnectorException {
        ResultSet resultIterator = createResultSet(clusterName);

        for (Row recoveredRow : resultIterator) {
            assertEquals("The value is correct ", VALUE_1, recoveredRow.getCell(COLUMN_1).getValue());
            if (recoveredRow.getCell(COLUMN_2).getValue() != null) {
                assertEquals("The value is correct ", VALUE_2, recoveredRow.getCell(COLUMN_2).getValue());
            } else {
                assertEquals("The value is correct ", VALUE_3, recoveredRow.getCell(COLUMN_3).getValue());
            }

        }

        assertEquals("The records number is correct " + clusterName.getName(), 2, resultIterator.size());
    }

    protected ResultSet createResultSet(ClusterName clusterName) throws ConnectorException {
        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow());
        return queryResult.getResultSet();
    }

    protected void insertRow(ClusterName cluesterName) throws ConnectorException {
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();

        cells.put(COLUMN_1, new Cell(VALUE_1));
        cells.put(COLUMN_2, new Cell(VALUE_2));
        cells.put(COLUMN_3, new Cell(VALUE_3));

        row.setCells(cells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE,
                getClusterName().getName());
        tableMetadataBuilder.addColumn(COLUMN_1, new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_2,
                new ColumnType(DataType
                        .BIGINT))
                .addColumn(COLUMN_3, new ColumnType(DataType.BOOLEAN));

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());

        if (getConnectorHelper().isTableMandatory()) {
            connector.getMetadataEngine().createTable(getClusterName(), targetTable);
        }
        connector.getStorageEngine().insert(cluesterName, targetTable, row, false);
        refresh(CATALOG);
    }

    private void insertRowsWithDifferentFields(ClusterName clusterName)
            throws UnsupportedException, ConnectorException {
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell(VALUE_1));
        cells.put(COLUMN_2, new Cell(VALUE_2));
        row.setCells(cells);

        Row otherRow = new Row();
        Map<String, Cell> otherCells = new HashMap<>();
        otherCells.put(COLUMN_1, new Cell(VALUE_1));
        otherCells.put(COLUMN_3, new Cell(VALUE_3));
        otherRow.setCells(otherCells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE,
                getClusterName().getName());
        tableMetadataBuilder.addColumn(COLUMN_1, new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_2,
                new ColumnType(DataType
                        .BIGINT))
                .addColumn(COLUMN_3, new ColumnType(DataType.BOOLEAN));

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());

        if (getConnectorHelper().isTableMandatory()) {
            connector.getMetadataEngine().createTable(getClusterName(), targetTable);
        }
        connector.getStorageEngine().insert(clusterName, targetTable, row, false);
        connector.getStorageEngine().insert(clusterName, targetTable, otherRow, false);
        refresh(CATALOG);

    }

    private LogicalWorkflow createLogicalWorkFlow() {
        return new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName()).addColumnName(COLUMN_1, COLUMN_2, COLUMN_3)
                .build();

    }

}
