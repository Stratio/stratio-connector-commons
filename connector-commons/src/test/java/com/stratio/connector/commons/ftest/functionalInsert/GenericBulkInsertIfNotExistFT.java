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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ValidationException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;

/**
 *
 */
public abstract class GenericBulkInsertIfNotExistFT extends GenericConnectorTest {

    public static final String COLUMN_1 = "name1";
    public static final String COLUMN_2 = "name2";
    public static final String COLUMN_3 = "name3";
    public static final String VALUE_1 = "value1_R";
    public static final String VALUE_2 = "value2_R";
    public static final String VALUE_3 = "value3_R";
    public static final int DEFAULT_ROWS_TO_INSERT = 100;
    public static final String COLUMN_KEY = "key";

    protected int getRowToInsert() {
        return DEFAULT_ROWS_TO_INSERT;
    }

    @Test
    public void testBulkInsert() throws ConnectorException, ValidationException, UnsupportedOperationException {

        ClusterName clusterName = getClusterName();
        insertBulk(clusterName);
        verifyInsert(clusterName);

    }

    @Test
    public void testBulkInsertWithDuplicates() throws ConnectorException, ValidationException,
            UnsupportedOperationException {

        ClusterName clusterName = getClusterName();
        insertDuplicatesBulk(clusterName);
        verifyInsert(clusterName);

    }

    private void insertDuplicatesBulk(ClusterName cluesterName) throws ConnectorException {

        // Insert 50 rows
        Set<Row> rows = new HashSet<Row>();

        for (int i = 0; i < 50; i++) {

            Row row = new Row();
            Map<String, Cell> cells = new HashMap();
            cells.put(COLUMN_KEY, new Cell(i));
            cells.put(COLUMN_1, new Cell(VALUE_1 + i));
            cells.put(COLUMN_2, new Cell(VALUE_2 + i));
            cells.put(COLUMN_3, new Cell(VALUE_3 + i));
            row.setCells(cells);
            rows.add(row);
        }

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE,
                getClusterName().getName());
        tableMetadataBuilder.addColumn(COLUMN_KEY, new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_1, new ColumnType
                (DataType.VARCHAR))
                .addColumn(COLUMN_2, new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_3, new ColumnType
                (DataType.VARCHAR));

        tableMetadataBuilder.withPartitionKey(COLUMN_1);

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());

        connector.getStorageEngine().insert(cluesterName, targetTable, rows, true);

        refresh(CATALOG);

        // Insert getRowToInsert() rows. First 50 with different value
        rows = new HashSet<Row>();

        for (int i = 0; i < 10; i++) {

            Row row = new Row();
            Map<String, Cell> cells = new HashMap();
            cells.put(COLUMN_KEY, new Cell("wrong"));
            cells.put(COLUMN_1, new Cell(VALUE_1 + i));
            cells.put(COLUMN_2, new Cell("wrong"));
            cells.put(COLUMN_3, new Cell("wrong"));
            row.setCells(cells);
            rows.add(row);
        }

        for (int i = 10; i < getRowToInsert(); i++) {
            Row row = new Row();
            Map<String, Cell> cells = new HashMap();
            cells.put(COLUMN_KEY, new Cell(i));
            cells.put(COLUMN_1, new Cell(VALUE_1 + i));
            cells.put(COLUMN_2, new Cell(VALUE_2 + i));
            cells.put(COLUMN_3, new Cell(VALUE_3 + i));
            row.setCells(cells);
            rows.add(row);
        }

        connector.getStorageEngine().insert(cluesterName, targetTable, rows, true);

        refresh(CATALOG);
    }

    private void insertBulk(ClusterName cluesterName) throws ConnectorException {
        Set<Row> rows = new HashSet<Row>();

        for (int i = 0; i < getRowToInsert(); i++) {

            Row row = new Row();
            Map<String, Cell> cells = new HashMap();
            cells.put(COLUMN_KEY, new Cell(i));
            cells.put(COLUMN_1, new Cell(VALUE_1 + i));
            cells.put(COLUMN_2, new Cell(VALUE_2 + i));
            cells.put(COLUMN_3, new Cell(VALUE_3 + i));
            row.setCells(cells);
            rows.add(row);
        }

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE,
                getClusterName().getName());
        tableMetadataBuilder.addColumn(COLUMN_KEY, new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_1, new ColumnType
                (DataType.VARCHAR))
                .addColumn(COLUMN_2, new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_3,
                new ColumnType(DataType.VARCHAR));

        tableMetadataBuilder.withPartitionKey(COLUMN_1);

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());

        connector.getStorageEngine().insert(cluesterName, targetTable, rows, true);

        refresh(CATALOG);
    }

    private void verifyInsert(ClusterName cluesterName) throws ConnectorException {
        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow());
        ResultSet resultIterator = queryResult.getResultSet();

        assertEquals("The records number is correct " + cluesterName.getName(), getRowToInsert(),
                resultIterator.size());

        int rowRecovered = 0;
        for (Row recoveredRow : resultIterator) {

            Object key = recoveredRow.getCell(COLUMN_KEY).getValue();

            assertEquals("The value_1 is wrong  ", VALUE_1 + key, recoveredRow.getCell(COLUMN_1).getValue());
            assertEquals("The value_2 is wrong ", VALUE_2 + key, recoveredRow.getCell(COLUMN_2).getValue());
            assertEquals("The value_3 is wrong ", VALUE_3 + key, recoveredRow.getCell(COLUMN_3).getValue());
        }

    }

    private LogicalWorkflow createLogicalWorkFlow() {

        return new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName()).addColumnName(COLUMN_KEY, COLUMN_1,
                COLUMN_2, COLUMN_3).build();

    }

}
