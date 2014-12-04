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

import static com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator.COLUMN_1;
import static com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator.COLUMN_2;
import static com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator.COLUMN_3;
import static com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator.COLUMN_AGE;
import static com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator.COLUMN_KEY;
import static com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator.COLUMN_MONEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * Created by jmgomez on 17/07/14.
 */
public abstract class GenericNotIndexedQueryIntegerFilterTest extends GenericConnectorTest {

    LogicalWorkFlowCreator logicalWorkFlowCreator;

    @Before
    public void setUp() throws ConnectorException {
        super.setUp();
        logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
    }

    @Test
    public void selectNotIndexedFilterEqual() throws ConnectorException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterEqual ***********************************");

        insertRow(1, 10, 5, clusterNodeName, false);
        insertRow(2, 9, 1, clusterNodeName, false);
        insertRow(3, 11, 1, clusterNodeName, false);
        insertRow(4, 10, 1, clusterNodeName, false);
        insertRow(5, 20, 1, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addEqualFilter(COLUMN_AGE, new Integer(10), false, false).getLogicalWorkflow();

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 2, queryResult.getResultSet().size());

        // First row
        assertTrue("Return correct record", proveSet.contains("column1={Row=1;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=1;Column2}"));
        assertTrue("Return correct record", proveSet.contains("money={5}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));

        // Fourth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=4;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=4;Column2}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));

    }

    @Test
    public void selectPKEqualsFilter() throws ConnectorException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectPKEqualsFilter ***********************************");

        insertRow(1, 1, 10, clusterNodeName, true);
        insertRow(2, 1, 9, clusterNodeName, true);
        insertRow(3, 6, 11, clusterNodeName, true);
        insertRow(4, 7, 11, clusterNodeName, true);

        refresh(CATALOG);

        LinkedList<LogicalWorkFlowCreator.ConnectorField> conectorList = new LinkedList();
        conectorList.add(logicalWorkFlowCreator.createConnectorField(COLUMN_1, COLUMN_1, ColumnType.TEXT));
        conectorList.add(logicalWorkFlowCreator.createConnectorField(COLUMN_2, COLUMN_2, ColumnType.TEXT));
        conectorList.add(logicalWorkFlowCreator.createConnectorField(COLUMN_3, COLUMN_3, ColumnType.TEXT));
        conectorList.add(logicalWorkFlowCreator.createConnectorField(COLUMN_AGE, COLUMN_AGE, ColumnType.INT));
        conectorList.add(logicalWorkFlowCreator.createConnectorField(COLUMN_MONEY, COLUMN_MONEY, ColumnType.INT));

        logicalWorkFlowCreator.addSelect(conectorList);
        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addEqualFilter(COLUMN_KEY, 1, false, true).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number must be only one", 1, queryResult.getResultSet().size());

        assertTrue("The result must contains money={10}", proveSet.contains("money={10}"));
        assertTrue("The result must contains age={1}", proveSet.contains("age={1}"));
        assertTrue("The result must contains column3={Row=1;Column3}", proveSet.contains("column3={Row=1;Column3}"));
        assertTrue("The result must contains column1={Row=1;Column1}", proveSet.contains("column1={Row=1;Column1}"));
        assertTrue("The result must contains column2={Row=2;Column2}", proveSet.contains("column2={Row=1;Column2}"));

    }

    @Test @Ignore
    public void selectNotIndexedFilterBetween() throws ConnectorException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterBetween ***********************************");

        insertRow(1, 1, 10, clusterNodeName, false);
        insertRow(2, 1, 9, clusterNodeName, false);
        insertRow(3, 1, 11, clusterNodeName, false);
        insertRow(4, 1, 10, clusterNodeName, false);
        insertRow(5, 1, 20, clusterNodeName, false);
        insertRow(6, 1, 11, clusterNodeName, false);
        insertRow(7, 1, 8, clusterNodeName, false);
        insertRow(8, 1, 12, clusterNodeName, false);

        refresh(CATALOG);

        // LogicalWorkflow logicalPlan = createLogicalWorkFlow(BETWEEN_FILTER);
        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addBetweenFilter(COLUMN_AGE, null, null).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

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
    public void selectNoNotIndexedFilterGreaterEqual() throws ConnectorException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNoNotIndexedFilterGreaterEqual ***********************************");

        insertRow(1, 10, 15, clusterNodeName, false);
        insertRow(2, 9, 10, clusterNodeName, false);
        insertRow(3, 11, 9, clusterNodeName, false);
        insertRow(4, 10, 7, clusterNodeName, false);
        insertRow(5, 7, 9, clusterNodeName, false);
        insertRow(6, 11, 100, clusterNodeName, false);
        insertRow(7, 8, 1, clusterNodeName, false);
        insertRow(8, 12, 10, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addGreaterEqualFilter(COLUMN_AGE, new Integer("10"), false, false).getLogicalWorkflow();

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);
        ResultSet aux = queryResult.getResultSet();

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 5, queryResult.getResultSet().size());

        // First row
        assertTrue("Return correct record", proveSet.contains("column1={Row=1;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=1;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));
        assertTrue("Return correct record", proveSet.contains("money={15}"));
        // Third row
        assertTrue("Return correct record", proveSet.contains("column1={Row=3;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=3;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={11}"));
        assertTrue("Return correct record", proveSet.contains("money={9}"));
        // fourth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=4;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=4;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));
        assertTrue("Return correct record", proveSet.contains("money={7}"));

        // sixth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=6;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=6;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={11}"));
        assertTrue("Return correct record", proveSet.contains("money={100}"));

        // eigth row
        assertTrue("Return correct record", proveSet.contains("column2={Row=8;Column2}"));
        assertTrue("Return correct record", proveSet.contains("column1={Row=8;Column1}"));
        assertTrue("Return correct record", proveSet.contains("age={12}"));
        assertTrue("Return correct record", proveSet.contains("money={10}"));

    }

    @Test
    public void selectNotIndexedFilterGreater() throws ConnectorException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterGreater ***********************************");

        insertRow(1, 10, 1, clusterNodeName, false);
        insertRow(2, 9, 1, clusterNodeName, false);
        insertRow(3, 11, 1, clusterNodeName, false);
        insertRow(4, 10, 1, clusterNodeName, false);
        insertRow(5, 20, 1, clusterNodeName, false);
        insertRow(6, 7, 1, clusterNodeName, false);
        insertRow(7, 8, 1, clusterNodeName, false);
        insertRow(8, 12, 1, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addGreaterFilter(COLUMN_AGE, new Integer("10"), false).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 3, queryResult.getResultSet().size());
        // First row
        assertTrue("Return correct record", proveSet.contains("column1={Row=3;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=3;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={11}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        // Fifth row
        assertTrue("Return correct record", proveSet.contains("column2={Row=5;Column2}"));
        assertTrue("Return correct record", proveSet.contains("column1={Row=5;Column1}"));
        assertTrue("Return correct record", proveSet.contains("age={20}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        // Eigth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=8;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=8;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={12}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

    }

    @Test
    public void selectNotIndexedFilterLower() throws ConnectorException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterLower ***********************************");

        insertRow(1, 10, 1, clusterNodeName, false);
        insertRow(2, 9, 1, clusterNodeName, false);
        insertRow(3, 11, 1, clusterNodeName, false);
        insertRow(4, 10, 1, clusterNodeName, false);
        insertRow(5, 20, 1, clusterNodeName, false);
        insertRow(6, 7, 1, clusterNodeName, false);
        insertRow(7, 8, 1, clusterNodeName, false);
        insertRow(8, 12, 1, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addNLowerFilter(COLUMN_AGE, new Integer("10"), false).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 3, queryResult.getResultSet().size());

        // Second row
        assertTrue("Return correct record", proveSet.contains("column1={Row=2;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=2;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={9}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        // Sixth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=6;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=6;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={7}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        // Seventh row
        assertTrue("Return correct record", proveSet.contains("column1={Row=7;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=7;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={8}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

    }

    @Test
    public void selectNotIndexedFilterLowerEqual() throws ConnectorException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterLowerEqual ***********************************");

        insertRow(1, 10, 1, clusterNodeName, false);
        insertRow(2, 9, 1, clusterNodeName, false);
        insertRow(3, 11, 1, clusterNodeName, false);
        insertRow(4, 10, 1, clusterNodeName, false);
        insertRow(5, 20, 1, clusterNodeName, false);
        insertRow(6, 7, 1, clusterNodeName, false);
        insertRow(7, 8, 1, clusterNodeName, false);
        insertRow(8, 12, 1, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addLowerEqualFilter(COLUMN_AGE, new Integer("10"), false).getLogicalWorkflow();
        QueryResult queryResult = connector.getQueryEngine().execute(logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 5, queryResult.getResultSet().size());

        // First row
        assertTrue("Return correct record", proveSet.contains("column1={Row=1;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=1;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        // Second row
        assertTrue("Return correct record", proveSet.contains("column1={Row=2;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=2;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={9}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));
        // fourth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=4;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=4;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));
        // Sixth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=6;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=6;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={7}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        // Seventh row
        assertTrue("Return correct record", proveSet.contains("column1={Row=7;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=7;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={8}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

    }

    @Test
    public void selectNotIndexedFilterDistinct() throws ConnectorException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNoPKFilterDistinct ***********************************");

        insertRow(1, 10, 1, clusterNodeName, false);
        insertRow(2, 9, 1, clusterNodeName, false);
        insertRow(3, 11, 5, clusterNodeName, false);
        insertRow(4, 10, 1, clusterNodeName, false);
        insertRow(5, 10, 7, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns()
                .addDistinctFilter(COLUMN_AGE, new Integer("10"), false, false).getLogicalWorkflow();
        QueryResult queryResult = connector.getQueryEngine().execute(logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 2, queryResult.getResultSet().size());

        // Second row
        assertTrue("Return correct record", proveSet.contains("column1={Row=2;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=2;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={9}"));
        assertTrue("Return correct record", proveSet.contains("money={5}"));

        // Third row
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

    private void insertRow(int ikey, int age, int money, ClusterName clusterNodeName, boolean withPk)
            throws UnsupportedOperationException, ConnectorException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        if (withPk) {
            cells.put(COLUMN_KEY, new Cell(ikey));
        }
        cells.put(COLUMN_1, new Cell("Row=" + ikey + ";Column1"));
        cells.put(COLUMN_2, new Cell("Row=" + ikey + ";Column2"));
        cells.put(COLUMN_3, new Cell("Row=" + ikey + ";Column3"));
        cells.put(COLUMN_AGE, new Cell(age));
        cells.put(COLUMN_MONEY, new Cell(money));

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.VARCHAR)
                .addColumn(COLUMN_3, ColumnType.VARCHAR).addColumn(COLUMN_AGE, ColumnType.INT)
                .addColumn(COLUMN_MONEY, ColumnType.INT);
        if (withPk) {
            tableMetadataBuilder.addColumn(COLUMN_KEY, ColumnType.INT).withPartitionKey(COLUMN_KEY);
        }

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper());

        row.setCells(cells);
        connector.getStorageEngine().insert(clusterNodeName, targetTable, row);

    }

}
