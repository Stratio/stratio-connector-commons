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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 */
public abstract class GenericSimpleInsertTest extends GenericConnectorTest {

    private static final String COLUMN_1 = "COLUMN 1";
    private static final String COLUMN_2 = "COLUMN 2";
    private static final String COLUMN_3 = "COLUMN 3";
    private static final String COLUMN_4 = "COLUMN 4";

    private static final String VALUE_1 = "value1";
    private static final String OTHER_VALUE_1 = "OTHER VALUE";
    private static final String VALUE_4 = "value4";
    private static final String OTHER_VALUE_4 = "other value 4";
    private static final int VALUE_2 = 2;
    private static final boolean VALUE_3 = true;

    @Test
    public void testSimpleInsertWithPK() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST testSimpleInsert  ***********************************");

        insertRow(clusterName, VALUE_4, VALUE_1, true);

        verifyInsert(clusterName, VALUE_4);
    }

    @Test
    public void testSimpleInsertWithOutPK() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST testSimpleInsert  ***********************************");

        insertRow(clusterName, VALUE_4, VALUE_1, false);

        verifyInsert(clusterName, VALUE_4);
    }

    @Test
    public void testInsertSamePK() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST testInsertSamePK " + clusterName.getName()
                        + " ***********************************");

        insertRow(clusterName, VALUE_4, VALUE_1, true);
        insertRow(clusterName, OTHER_VALUE_4, VALUE_1, true);

        verifyInsert(clusterName, OTHER_VALUE_4);

    }

    @Test
    public void testInsertString() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST testInsertSamePK " + clusterName.getName()
                        + " ***********************************");

        Object value4 = "String";
        insertRow(clusterName, value4, VALUE_1, true);

        ResultSet resultIterator = createResultSet(clusterName);
        assertEquals("It have only one result", 1, resultIterator.size());
        for (Row recoveredRow : resultIterator) {
            assertEquals("The type is correct ", String.class.getCanonicalName(),
                    recoveredRow.getCell(COLUMN_4).getValue().getClass().getCanonicalName());
            assertEquals("The value is correct ", value4, recoveredRow.getCell(COLUMN_4).getValue());
        }

    }

    @Test
    public void testInsertInteger() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST testInsertInteger ***********************************");
        Object value4 = new Integer(1);
        insertRow(clusterName, value4, VALUE_1, true);

        ResultSet resultIterator = createResultSet(clusterName);
        assertEquals("It have only one result", 1, resultIterator.size());
        for (Row recoveredRow : resultIterator) {
            assertEquals("The type is correct ", Integer.class.getCanonicalName(),
                    recoveredRow.getCell(COLUMN_4).getValue().getClass().getCanonicalName());
            assertEquals("The value is correct ", value4, recoveredRow.getCell(COLUMN_4).getValue());
        }

    }

    @Test
    public void testInsertLong() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST testInsertSamePK " + clusterName.getName()
                        + " ***********************************");
        Object value4 = 1L;
        insertRow(clusterName, value4, VALUE_1, true);

        ResultSet resultIterator = createResultSet(clusterName);
        assertEquals("It have only one result", 1, resultIterator.size());
        for (Row recoveredRow : resultIterator) {
            assertEquals("The type is correct ", Long.class.getCanonicalName(),
                    recoveredRow.getCell(COLUMN_4).getValue().getClass().getCanonicalName());
            assertEquals("The value is correct ", value4, recoveredRow.getCell(COLUMN_4).getValue());
        }

    }

    @Test
    public void testInsertBoolean() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST testInsertSamePK " + clusterName.getName()
                        + " ***********************************");
        Object value4 = new Boolean(true);
        insertRow(clusterName, value4, VALUE_1, true);

        ResultSet resultIterator = createResultSet(clusterName);
        assertEquals("It have only one result", 1, resultIterator.size());
        for (Row recoveredRow : resultIterator) {
            assertEquals("The type is correct ", Boolean.class.getCanonicalName(),
                    recoveredRow.getCell(COLUMN_4).getValue().getClass().getCanonicalName());
            assertEquals("The value is correct ", value4, recoveredRow.getCell(COLUMN_4).getValue());
        }

    }

    @Test
    public void testInsertDate() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST testInsertSamePK " + clusterName.getName()
                        + " ***********************************");
        Object value4 = new Date();
        insertRow(clusterName, value4, VALUE_1, true);

        ResultSet resultIterator = createResultSet(clusterName);
        assertEquals("It have only one result", 1, resultIterator.size());
        for (Row recoveredRow : resultIterator) {
            assertEquals("The type is correct ", Date.class.getCanonicalName(),
                    recoveredRow.getCell(COLUMN_4).getValue().getClass().getCanonicalName());
            assertEquals("The value is correct ", value4, recoveredRow.getCell(COLUMN_4).getValue());
        }

    }

    private void verifyInsert(ClusterName clusterName, String test_value_4)
            throws UnsupportedException, ExecutionException {
        ResultSet resultIterator = createResultSet(clusterName);

        for (Row recoveredRow : resultIterator) {
            assertEquals("The value is correct ", VALUE_1, recoveredRow.getCell(COLUMN_1).getValue());
            assertEquals("The value is correct ", VALUE_2, recoveredRow.getCell(COLUMN_2).getValue());
            assertEquals("The value is correct ", VALUE_3, recoveredRow.getCell(COLUMN_3).getValue());
            assertEquals("The value is correct ", test_value_4, recoveredRow.getCell(COLUMN_4).getValue());
        }

        assertEquals("The records number is correct " + clusterName.getName(), 1, resultIterator.size());
    }

    private ResultSet createResultSet(ClusterName clusterName) throws UnsupportedException, ExecutionException {
        QueryResult queryResult = connector.getQueryEngine().execute(clusterName, createLogicalWorkFlow());
        return queryResult.getResultSet();
    }

    private void insertRow(ClusterName cluesterName, Object value_4, String PK_VALUE, boolean withPK)
            throws UnsupportedException, ExecutionException {
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();

        cells.put(COLUMN_1, new Cell(PK_VALUE));
        cells.put(COLUMN_2, new Cell(VALUE_2));
        cells.put(COLUMN_3, new Cell(VALUE_3));
        cells.put(COLUMN_4, new Cell(value_4));

        row.setCells(cells);

        List<ColumnName> pk = new ArrayList<>();
        if (withPK) {
            ColumnName columnPK = new ColumnName(CATALOG, TABLE, COLUMN_1);
            pk.add(columnPK);
        }

        connector.getStorageEngine().insert(cluesterName,
                new TableMetadata(new TableName(CATALOG, TABLE), null, null, null, null, pk, Collections.EMPTY_LIST),
                row);
        refresh(CATALOG);
    }

    private LogicalWorkflow createLogicalWorkFlow() {

        return new LogicalWorkFlowCreator(CATALOG,TABLE).addColumnName(COLUMN_1,COLUMN_2,COLUMN_3,
                COLUMN_4).getLogicalWorkflow();

    }

}
