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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;

public abstract class GenericOrderByTest extends GenericConnectorTest {

    public static final String COLUMN_TEXT = "text";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";

    public static final int SORT_AGE = 1;

    @Test
    public void sortDescTest() throws ConnectorException {

        ClusterName clusterName = getClusterName();


        insertRow(1, "text", 10, 20, clusterName);// row,text,money,age
        insertRow(2, "text", 9, 17, clusterName);
        insertRow(3, "text", 11, 26, clusterName);
        insertRow(4, "text", 10, 30, clusterName);
        insertRow(5, "text", 20, 42, clusterName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlanLimit(SORT_AGE);

        // return COLUMN_TEXT order by age DESC

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        assertEquals(5, queryResult.getResultSet().size());

        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();

        assertEquals("text5", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text4", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text3", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text1", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text2", rowIterator.next().getCell(COLUMN_TEXT).getValue());

    }

    @Test
    public void sortTestMultifield() throws ConnectorException {

        ClusterName clusterName = getClusterName();


        insertRow(1, "text", 10, 20, clusterName);// row,text,money,age
        insertRow(2, "text", 9, 17, clusterName);
        insertRow(3, "text", 11, 26, clusterName);
        insertRow(4, "text", 10, 30, clusterName);
        insertRow(5, "text", 20, 42, clusterName);
        insertRow(6, "text", 10, 10, clusterName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlanMultifield();

        // return COLUMN_TEXT order by money asc, age asc

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        assertEquals(6, queryResult.getResultSet().size());

        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();

        assertEquals("text2", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text6", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text1", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text4", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text3", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text5", rowIterator.next().getCell(COLUMN_TEXT).getValue());

    }

    private void fail() {
        assertTrue(false);

    }

    private LogicalWorkflow createLogicalPlanLimit(int sortAge) {

        List<LogicalStep> stepList = new ArrayList<>();

        List<ColumnName> columns = new ArrayList<>();

        throw new RuntimeException("Not yet generic supported.");
    }

    private LogicalWorkflow createLogicalPlan(int sortAge) {

        List<LogicalStep> stepList = new ArrayList<>();

        List<ColumnName> columns = new ArrayList<>();

        columns.add(new ColumnName(CATALOG, TABLE, COLUMN_TEXT));
        columns.add(new ColumnName(CATALOG, TABLE, COLUMN_AGE));
        TableName tableName = new TableName(CATALOG, TABLE);
        Project project = new Project(null, tableName, getClusterName(), columns);
        stepList.add(project);

        switch (sortAge) {
        case SORT_AGE:

            throw new RuntimeException("Not yet generic supported.\n");

        }
        return new LogicalWorkflow(stepList);

    }

    private LogicalWorkflow createLogicalPlanMultifield() {

        List<LogicalStep> stepList = new ArrayList<>();

        List<ColumnName> columns = new ArrayList<>();

        columns.add(new ColumnName(CATALOG, TABLE, COLUMN_TEXT));
        columns.add(new ColumnName(CATALOG, TABLE, COLUMN_AGE));

        TableName tableName = new TableName(CATALOG, TABLE);
        Project project = new Project(null, tableName, getClusterName(), columns);

        stepList.add(project);
        LogicalStep gropuBy;

        throw new RuntimeException("Not yet generic supported.");

    }

    private void insertRow(int ikey, String texto, int money, int age, ClusterName clusterName)
            throws UnsupportedOperationException, ConnectorException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_TEXT, new Cell(texto + ikey));
        cells.put(COLUMN_AGE, new Cell(age));
        cells.put(COLUMN_MONEY, new Cell(money));
        row.setCells(cells);
        connector.getStorageEngine().insert(
                clusterName,
                new TableMetadata(new TableName(CATALOG, TABLE), null, null, null, null,
                        Collections.EMPTY_LIST, Collections.EMPTY_LIST), row);

    }

}