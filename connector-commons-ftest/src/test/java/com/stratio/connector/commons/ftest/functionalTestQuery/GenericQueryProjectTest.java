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
package com.stratio.connector.commons.ftest.functionalTestQuery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.meta.common.data.Cell;
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
 *
 */
public abstract class GenericQueryProjectTest extends GenericConnectorTest {

    public static final String COLUMN_1 = "bin1";
    public static final String COLUMN_2 = "bin2";
    public static final String COLUMN_3 = "bin3";
    private static final Integer DEFAULT_GET_TO_SEARCH = 1000;

    protected Integer getRowsToSearch() {
        return DEFAULT_GET_TO_SEARCH;
    }

    @Test
    public void selectFilterProject() throws UnsupportedException, ExecutionException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectFilterProject ***********************************");

        for (int i = 0; i < getRowsToSearch(); i++) {
            insertRow(i, clusterNodeName);

        }

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlan();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> probeSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            for (String cell : row.getCells().keySet()) {
                probeSet.add(cell + row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct", getRowsToSearch(), (Integer) queryResult.getResultSet().size());
        for (int i = 0; i < getRowsToSearch(); i++) {

            assertTrue("Return correct record", probeSet.contains("bin1ValueBin1_r" + i));
            assertTrue("Return correct record", probeSet.contains("bin2ValueBin2_r" + i));
            assertFalse("This record should not be returned", probeSet.contains("bin3ValueBin3_r+i"));
        }

    }

    private LogicalWorkflow createLogicalPlan() {
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnName> columns = new ArrayList<>();
        columns.add(new ColumnName(CATALOG, TABLE, COLUMN_1));
        columns.add(new ColumnName(CATALOG, TABLE, COLUMN_2));
        TableName tableName = new TableName(CATALOG, TABLE);
        Project project = new Project(null, tableName, columns);
        stepList.add(project);
        return new LogicalWorkflow(stepList);
    }

    private void insertRow(int ikey, ClusterName clusterNodeName)
            throws UnsupportedOperationException, ExecutionException, UnsupportedException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("ValueBin1_r" + ikey));
        cells.put(COLUMN_2, new Cell("ValueBin2_r" + ikey));
        cells.put(COLUMN_3, new Cell("ValueBin3_r" + ikey));
        row.setCells(cells);
        connector.getStorageEngine().insert(clusterNodeName,
                new TableMetadata(new TableName(CATALOG, TABLE), null, null, null, null, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST), row);

    }

}
