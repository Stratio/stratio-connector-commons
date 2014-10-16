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
import static com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator.COLUMN_MONEY;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
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
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * Created by jmgomez on 17/07/14.
 */
public abstract class GenericPKQueryIntegerFilterTest extends GenericConnectorTest {

    private static final String COLUMN_PK = "COLUMN_PK";
    LogicalWorkFlowCreator logicalWorkFlowCreator;

    @Before
    public void setUp() throws ConnectionException, ExecutionException, InitializationException, UnsupportedException {
        super.setUp();
        logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
    }

    @Test
    public void selectPKFilterEqual() throws ExecutionException, UnsupportedException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterEqual ***********************************");

        insertRow(1, 10, 5, clusterNodeName, false);
        insertRow(2, 9, 1, clusterNodeName, false);
        insertRow(3, 11, 1, clusterNodeName, false);
        insertRow(4, 10, 1, clusterNodeName, false);
        insertRow(5, 20, 1, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addColumnName(COLUMN_PK)
                .addEqualFilter(COLUMN_PK, new Integer(2), false, true).getLogicalWorkflow();

        QueryResult queryResult = connector.getQueryEngine().execute(logicalPlan);

        assertEquals("The record number is correct", 1, queryResult.getResultSet().size());
        assertEquals("The value is correct", new Integer(2), queryResult.getResultSet().getRows().get(0).getCell
                (COLUMN_PK)
                .getValue());

    }

    @Test
    public void selectPKDoubleFilterEqual() throws ExecutionException, UnsupportedException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterEqual ***********************************");

        insertRow(1, 10, 5, clusterNodeName, false);
        insertRow(2, 9, 1, clusterNodeName, false);
        insertRow(3, 11, 1, clusterNodeName, false);
        insertRow(4, 10, 1, clusterNodeName, false);
        insertRow(5, 20, 1, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addColumnName(COLUMN_PK)
                .addEqualFilter(COLUMN_PK, new Integer(2), false, true).addEqualFilter(COLUMN_PK, new Integer(3),
                        false, true).getLogicalWorkflow();

        QueryResult queryResult = connector.getQueryEngine().execute(logicalPlan);

        assertEquals("The record number is correct", 0, queryResult.getResultSet().size());

    }

    @Test
    public void selectPKGeatEqualFilterEqual() throws ExecutionException, UnsupportedException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterEqual ***********************************");

        insertRow(1, 10, 5, clusterNodeName, false);
        insertRow(2, 9, 1, clusterNodeName, false);
        insertRow(3, 11, 1, clusterNodeName, false);
        insertRow(4, 10, 1, clusterNodeName, false);
        insertRow(5, 20, 1, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addColumnName(COLUMN_PK)
                .addGreaterEqualFilter(COLUMN_PK, new Integer(2), false, true).getLogicalWorkflow();

        QueryResult queryResult = connector.getQueryEngine().execute(logicalPlan);

        assertEquals("The record number is correct", 4, queryResult.getResultSet().size());

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
            throws UnsupportedOperationException, ExecutionException, UnsupportedException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("Row=" + ikey + ";Column1"));
        cells.put(COLUMN_2, new Cell("Row=" + ikey + ";Column2"));
        cells.put(COLUMN_3, new Cell("Row=" + ikey + ";Column3"));
        cells.put(COLUMN_AGE, new Cell(age));
        cells.put(COLUMN_MONEY, new Cell(money));
        cells.put(COLUMN_PK, new Cell(ikey));

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.VARCHAR)
                .addColumn(COLUMN_3, ColumnType.VARCHAR).addColumn(COLUMN_AGE, ColumnType.INT)
                .addColumn(COLUMN_MONEY, ColumnType.INT);
        tableMetadataBuilder.withPartitionKey(COLUMN_PK);

        TableMetadata targetTable = tableMetadataBuilder.build();

        row.setCells(cells);
        connector.getStorageEngine().insert(clusterNodeName, targetTable, row);

    }

}
