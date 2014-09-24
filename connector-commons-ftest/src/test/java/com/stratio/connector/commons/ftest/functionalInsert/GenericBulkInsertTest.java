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

package com.stratio.connector.commons.ftest.functionalInsert;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
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
import com.stratio.meta.common.exceptions.ValidationException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 *
 */
public abstract class GenericBulkInsertTest extends GenericConnectorTest {

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
    public void testBulkInsertWithPK() throws ExecutionException, ValidationException, UnsupportedOperationException,
                    UnsupportedException {

        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testBulkInsertWithPK ***********************************");
        insertBulk(clusterName, true);
        verifyInsert(clusterName);

    }

    @Test
    public void testBulkInsertWithoutPK() throws ExecutionException, ValidationException,
                    UnsupportedOperationException, UnsupportedException {

        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testBulkInsertWithoutPK ***********************************");
        insertBulk(clusterName, false);
        verifyInsert(clusterName);

    }

    private void insertBulk(ClusterName cluesterName, boolean withPK) throws UnsupportedException, ExecutionException {
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

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_KEY, ColumnType.VARCHAR).addColumn(COLUMN_1, ColumnType.VARCHAR)
                        .addColumn(COLUMN_2, ColumnType.VARCHAR).addColumn(COLUMN_3, ColumnType.VARCHAR);
        if (withPK) {
            tableMetadataBuilder.withPartitionKey(COLUMN_1);
        }
        TableMetadata targetTable = tableMetadataBuilder.build();

        connector.getStorageEngine().insert(cluesterName, targetTable, rows);

        refresh(CATALOG);
    }

    private void verifyInsert(ClusterName cluesterName) throws UnsupportedException, ExecutionException {
        QueryResult queryResult = connector.getQueryEngine().execute(cluesterName, createLogicalWorkFlow());
        ResultSet resultIterator = queryResult.getResultSet();

        assertEquals("The records number is correct " + cluesterName.getName(), getRowToInsert(), resultIterator.size());

        int rowRecovered = 0;
        for (Row recoveredRow : resultIterator) {
            System.out.println("Row number: [" + ++rowRecovered + "]");
            Object key = recoveredRow.getCell(COLUMN_KEY).getValue();

            assertEquals("The value_1 is wrong  ", VALUE_1 + key, recoveredRow.getCell(COLUMN_1).getValue());
            assertEquals("The value_2 is wrong ", VALUE_2 + key, recoveredRow.getCell(COLUMN_2).getValue());
            assertEquals("The value_3 is wrong ", VALUE_3 + key, recoveredRow.getCell(COLUMN_3).getValue());
        }

    }

    private LogicalWorkflow createLogicalWorkFlow() {

        return new LogicalWorkFlowCreator(CATALOG, TABLE).addColumnName(COLUMN_KEY, COLUMN_1, COLUMN_2, COLUMN_3)
                        .getLogicalWorkflow();

    }

}