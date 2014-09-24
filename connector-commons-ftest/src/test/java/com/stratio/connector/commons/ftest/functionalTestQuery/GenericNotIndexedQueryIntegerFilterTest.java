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

import static com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator.COLUMN_1;
import static com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator.COLUMN_2;
import static com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator.COLUMN_3;
import static com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator.COLUMN_AGE;
import static com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator.COLUMN_MONEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * Created by jmgomez on 17/07/14.
 */
public abstract class GenericNotIndexedQueryIntegerFilterTest extends GenericConnectorTest {

    LogicalWorkFlowCreator logicalWorkFlowCreator;

    @Before
    public void setUp() throws ConnectionException, ExecutionException, InitializationException, UnsupportedException {
        super.setUp();
        logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE);
    }

    @Test
    public void selectNotIndexedFilterEqual() throws ExecutionException, UnsupportedException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterEqual ***********************************");

        insertRow(1, 10, 5, clusterNodeName);
        insertRow(2, 9, 1, clusterNodeName);
        insertRow(3, 11, 1, clusterNodeName);
        insertRow(4, 10, 1, clusterNodeName);
        insertRow(5, 20, 1, clusterNodeName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addEqualFilter(COLUMN_AGE, new Integer(10), false).getLogicalWorkflow();

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 2, queryResult.getResultSet().size());

        //First row
        assertTrue("Return correct record", proveSet.contains("column1={Row=1;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=1;Column2}"));
        assertTrue("Return correct record", proveSet.contains("money={5}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));

        //Fourth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=4;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=4;Column2}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));

    }

    @Test
    public void selectNotIndexedFilterBetween() throws ExecutionException, UnsupportedException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterBetween ***********************************");

        insertRow(1, 1, 10, clusterNodeName);
        insertRow(2, 1, 9, clusterNodeName);
        insertRow(3, 1, 11, clusterNodeName);
        insertRow(4, 1, 10, clusterNodeName);
        insertRow(5, 1, 20, clusterNodeName);
        insertRow(6, 1, 11, clusterNodeName);
        insertRow(7, 1, 8, clusterNodeName);
        insertRow(8, 1, 12, clusterNodeName);

        refresh(CATALOG);

        //  LogicalWorkflow logicalPlan = createLogicalWorkFlow(BETWEEN_FILTER);
        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addBetweenFilter(COLUMN_AGE, null, null).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 14, proveSet.size());
        assertTrue("Return correct record", proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record", proveSet.contains("bin1ValueBin1_r2"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r2"));
        assertTrue("Return correct record", proveSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record", proveSet.contains("money10"));
        assertTrue("Return correct record", proveSet.contains("money9"));
        assertTrue("Return correct record", proveSet.contains("money11"));
        assertTrue("Return correct record", proveSet.contains("age1"));

    }

    @Test
    public void selectNoNotIndexedFilterGreaterEqual() throws ExecutionException, UnsupportedException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNoNotIndexedFilterGreaterEqual ***********************************");

        insertRow(1, 10, 15, clusterNodeName);
        insertRow(2, 9, 10, clusterNodeName);
        insertRow(3, 11, 9, clusterNodeName);
        insertRow(4, 10, 7, clusterNodeName);
        insertRow(5, 7, 9, clusterNodeName);
        insertRow(6, 11, 100, clusterNodeName);
        insertRow(7, 8, 1, clusterNodeName);
        insertRow(8, 12, 10, clusterNodeName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addGreaterEqualFilter(COLUMN_AGE, new Integer("10"), false).getLogicalWorkflow();

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 5, queryResult.getResultSet().size());

        //First row
        assertTrue("Return correct record", proveSet.contains("column1={Row=1;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=1;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));
        assertTrue("Return correct record", proveSet.contains("money={15}"));
        //Third row
        assertTrue("Return correct record", proveSet.contains("column1={Row=3;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=3;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={11}"));
        assertTrue("Return correct record", proveSet.contains("money={9}"));
        //fourth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=4;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=4;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));
        assertTrue("Return correct record", proveSet.contains("money={7}"));

        //sixth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=6;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=6;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={11}"));
        assertTrue("Return correct record", proveSet.contains("money={100}"));

        //eigth row
        assertTrue("Return correct record", proveSet.contains("column2={Row=8;Column2}"));
        assertTrue("Return correct record", proveSet.contains("column1={Row=8;Column1}"));
        assertTrue("Return correct record", proveSet.contains("age={12}"));
        assertTrue("Return correct record", proveSet.contains("money={10}"));

    }

    @Test
    public void selectNotIndexedFilterGreater() throws UnsupportedException, ExecutionException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterGreater ***********************************");

        insertRow(1, 10, 1, clusterNodeName);
        insertRow(2, 9, 1, clusterNodeName);
        insertRow(3, 11, 1, clusterNodeName);
        insertRow(4, 10, 1, clusterNodeName);
        insertRow(5, 20, 1, clusterNodeName);
        insertRow(6, 7, 1, clusterNodeName);
        insertRow(7, 8, 1, clusterNodeName);
        insertRow(8, 12, 1, clusterNodeName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addGreaterFilter(COLUMN_AGE, new Integer("10"), false).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 3, queryResult.getResultSet().size());
        //First row
        assertTrue("Return correct record", proveSet.contains("column1={Row=3;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=3;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={11}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        //Fifth row
        assertTrue("Return correct record", proveSet.contains("column2={Row=5;Column2}"));
        assertTrue("Return correct record", proveSet.contains("column1={Row=5;Column1}"));
        assertTrue("Return correct record", proveSet.contains("age={20}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        //Eigth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=8;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=8;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={12}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

    }

    @Test
    public void selectNotIndexedFilterLower() throws UnsupportedException, ExecutionException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterLower ***********************************");

        insertRow(1, 10, 1, clusterNodeName);
        insertRow(2, 9, 1, clusterNodeName);
        insertRow(3, 11, 1, clusterNodeName);
        insertRow(4, 10, 1, clusterNodeName);
        insertRow(5, 20, 1, clusterNodeName);
        insertRow(6, 7, 1, clusterNodeName);
        insertRow(7, 8, 1, clusterNodeName);
        insertRow(8, 12, 1, clusterNodeName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addNLowerFilter(COLUMN_AGE, new Integer("10"), false).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 3, queryResult.getResultSet().size());

        //Second row
        assertTrue("Return correct record", proveSet.contains("column1={Row=2;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=2;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={9}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        //Sixth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=6;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=6;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={7}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        //Seventh row
        assertTrue("Return correct record", proveSet.contains("column1={Row=7;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=7;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={8}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

    }

    @Test
    public void selectNotIndexedFilterLowerEqual() throws UnsupportedException, ExecutionException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterLowerEqual ***********************************");

        insertRow(1, 10, 1, clusterNodeName);
        insertRow(2, 9, 1, clusterNodeName);
        insertRow(3, 11, 1, clusterNodeName);
        insertRow(4, 10, 1, clusterNodeName);
        insertRow(5, 20, 1, clusterNodeName);
        insertRow(6, 7, 1, clusterNodeName);
        insertRow(7, 8, 1, clusterNodeName);
        insertRow(8, 12, 1, clusterNodeName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addLowerEqualFilter(COLUMN_AGE, new Integer("10"), false).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 5, queryResult.getResultSet().size());

        //First row
        assertTrue("Return correct record", proveSet.contains("column1={Row=1;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=1;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        //Second row
        assertTrue("Return correct record", proveSet.contains("column1={Row=2;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=2;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={9}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));
        //fourth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=4;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=4;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));
        //Sixth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=6;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=6;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={7}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        //Seventh row
        assertTrue("Return correct record", proveSet.contains("column1={Row=7;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=7;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={8}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

    }

    @Test
    public void selectNotIndexedFilterDistinct() throws UnsupportedException, ExecutionException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNoPKFilterDistinct ***********************************");

        insertRow(1, 10, 1, clusterNodeName);
        insertRow(2, 9, 1, clusterNodeName);
        insertRow(3, 11, 5, clusterNodeName);
        insertRow(4, 10, 1, clusterNodeName);
        insertRow(5, 10, 7, clusterNodeName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addDistinctFilter(COLUMN_AGE, new Integer("10"), false).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 2, queryResult.getResultSet().size());

        //Second row
        assertTrue("Return correct record", proveSet.contains("column1={Row=2;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=2;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={9}"));
        assertTrue("Return correct record", proveSet.contains("money={5}"));

        //Third row
        assertTrue("Return correct record", proveSet.contains("column1={Row=3;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=3;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={11}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

    }

    private Set<Object> createCellsResult(QueryResult queryResult) {
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            for (String cell : row.getCells().keySet()) {
                proveSet.add(cell + "={" + row.getCell(cell).getValue() + "}");

            }
        }
        return proveSet;
    }

    private void insertRow(int ikey, int age, int money, ClusterName clusterNodeName)
            throws UnsupportedOperationException, ExecutionException, UnsupportedException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("Row=" + ikey + ";Column1"));
        cells.put(COLUMN_2, new Cell("Row=" + ikey + ";Column2"));
        cells.put(COLUMN_3, new Cell("Row=" + ikey + ";Column3"));
        cells.put(COLUMN_AGE, new Cell(age));
        cells.put(COLUMN_MONEY, new Cell(money));

        row.setCells(cells);
        connector.getStorageEngine().insert(clusterNodeName,
                new TableMetadata(new TableName(CATALOG, TABLE), null, null, null, null, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST), row);

    }

}
