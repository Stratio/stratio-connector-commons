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

import static com.stratio.connector.commons.test.util.LogicalWorkFlowCreator.COLUMN_1;
import static com.stratio.connector.commons.test.util.LogicalWorkFlowCreator.COLUMN_2;
import static com.stratio.connector.commons.test.util.LogicalWorkFlowCreator.COLUMN_3;
import static com.stratio.connector.commons.test.util.LogicalWorkFlowCreator.COLUMN_AGE;
import static com.stratio.connector.commons.test.util.LogicalWorkFlowCreator.COLUMN_MONEY;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * Created by jmgomez on 17/07/14.
 */
public abstract class GenericPKQueryIntegerFilterFT extends GenericConnectorTest {

    public static final String ALIAS_COLUMN_1 = "alias_" + COLUMN_1;
    private static final String COLUMN_PK = "column_pk";
    private static final String ALIAS_COLUMN_AGE = "alias_" + COLUMN_AGE;
    LogicalWorkFlowCreator logicalWorkFlowCreator;

    @Before
    public void setUp() throws ConnectorException {
        super.setUp();
        logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
    }

    @Test
    public void selectPKFilterEqual() throws ConnectorException {

        ClusterName clusterNodeName = getClusterName();

        insertRow(1, 10, 5, clusterNodeName, false);
        insertRow(2, 9, 1, clusterNodeName, false);
        insertRow(3, 11, 1, clusterNodeName, false);
        insertRow(4, 10, 1, clusterNodeName, false);
        insertRow(5, 20, 1, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addColumnName(COLUMN_PK)
                .addEqualFilter(COLUMN_PK, new Integer(2), false, true).build();

        QueryResult queryResult = connector.getQueryEngine().execute("", logicalPlan);

        assertEquals("The record number is correct", 1, queryResult.getResultSet().size());
        assertEquals("The value is correct", new Integer(2),
                queryResult.getResultSet().getRows().get(0).getCell(COLUMN_PK).getValue());

    }

    @Test
    public void selectPKDoubleFilterEqual() throws ConnectorException {

        ClusterName clusterNodeName = getClusterName();

        insertRow(1, 10, 5, clusterNodeName, false);
        insertRow(2, 9, 1, clusterNodeName, false);
        insertRow(3, 11, 1, clusterNodeName, false);
        insertRow(4, 10, 1, clusterNodeName, false);
        insertRow(5, 20, 1, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addColumnName(COLUMN_PK)
                .addEqualFilter(COLUMN_PK, new Integer(2), false, true)
                .addEqualFilter(COLUMN_PK, new Integer(3), false, true).build();

        QueryResult queryResult = connector.getQueryEngine().execute("", logicalPlan);

        assertEquals("The record number is correct", 0, queryResult.getResultSet().size());

    }

    @Test
    public void selectPKDoubleFilterDistinct() throws ConnectorException {

        ClusterName clusterNodeName = getClusterName();

        insertRow(1, 10, 5, clusterNodeName, true);
        insertRow(2, 9, 1, clusterNodeName, true);
        insertRow(3, 11, 1, clusterNodeName, true);
        insertRow(4, 10, 1, clusterNodeName, true);
        insertRow(5, 20, 1, clusterNodeName, true);

        refresh(CATALOG);

        LinkedList<LogicalWorkFlowCreator.ConnectorField> connectorFields = new LinkedList<>();
        connectorFields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_1, ALIAS_COLUMN_1, new ColumnType(
                DataType.VARCHAR)));
        connectorFields.add(logicalWorkFlowCreator.createConnectorField(COLUMN_AGE, ALIAS_COLUMN_AGE, new ColumnType
                (DataType.INT)));
        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addColumnName(COLUMN_PK)
                .addSelect(connectorFields).addDistinctFilter(COLUMN_PK, new Integer(2), false, true)
                .addDistinctFilter(COLUMN_PK, new Integer(3), false, true).build();

        QueryResult queryResult = connector.getQueryEngine().execute("", logicalPlan);

        assertEquals("The record number is correct", 3, queryResult.getResultSet().size());

    }

    @Test
    public void selectPKGreatEqualFilterEqual() throws ConnectorException {

        ClusterName clusterNodeName = getClusterName();

        insertRow(1, 10, 5, clusterNodeName, false);
        insertRow(2, 9, 1, clusterNodeName, false);
        insertRow(3, 11, 1, clusterNodeName, false);
        insertRow(4, 10, 1, clusterNodeName, false);
        insertRow(5, 20, 1, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addColumnName(COLUMN_PK)
                .addGreaterEqualFilter(COLUMN_PK, new Integer(2), false, false).build();

        QueryResult queryResult = connector.getQueryEngine().execute("", logicalPlan);

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
            throws UnsupportedOperationException, ConnectorException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("Row=" + ikey + ";Column1"));
        cells.put(COLUMN_2, new Cell("Row=" + ikey + ";Column2"));
        cells.put(COLUMN_3, new Cell("Row=" + ikey + ";Column3"));
        cells.put(COLUMN_AGE, new Cell(age));
        cells.put(COLUMN_MONEY, new Cell(money));
        cells.put(COLUMN_PK, new Cell(ikey));

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE,
                getClusterName().getName());
        tableMetadataBuilder.addColumn(COLUMN_1,
                new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_2, new ColumnType(DataType
                .VARCHAR))
                .addColumn(COLUMN_3,
                        new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_AGE, new ColumnType(DataType.INT))
                .addColumn(COLUMN_MONEY,
                        new ColumnType(DataType.INT))
                .addColumn(COLUMN_PK, new ColumnType(DataType.INT));
        tableMetadataBuilder.withPartitionKey(COLUMN_PK);

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());

        row.setCells(cells);
        connector.getStorageEngine().insert(clusterNodeName, targetTable, row, false);

    }

}
