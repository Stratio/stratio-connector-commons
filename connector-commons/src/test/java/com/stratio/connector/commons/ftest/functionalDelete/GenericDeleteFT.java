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

package com.stratio.connector.commons.ftest.functionalDelete;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.data.*;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.*;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class GenericDeleteFT extends GenericConnectorTest {

    private static String COLUMN_PK = "idpk";
    private static String COLUMN_1 = "name";

    @Test
    public void deleteByPKEQStringTest() throws ConnectorException {

        ClusterName clusterName = getClusterName();

        insertTestData(clusterName);

        connector.getStorageEngine().delete(clusterName, new TableName(CATALOG, TABLE),
                createDeleteFilter(Operations.DELETE_PK_EQ, Operator.EQ));

        QueryResult queryResult = connector.getQueryEngine().execute("", createLogicalWorkFlow(CATALOG, TABLE));
        assertEquals("One record has been deleted", 3, queryResult.getResultSet().size());
        Set<String> sRows = recoveredIds(queryResult);
        assertTrue("The row is correct", sRows.contains("0"));
        assertTrue("The row is correct", sRows.contains("2"));
        assertTrue("The row is correct", sRows.contains("3"));

    }

    @Test
    public void deleteByPKLTStringTest() throws ConnectorException {

        ClusterName clusterName = getClusterName();
        insertTestData(clusterName);

        connector.getStorageEngine().delete(clusterName, new TableName(CATALOG, TABLE),
                createDeleteFilter(Operations.DELETE_PK_LT, Operator.LT));

        QueryResult queryResult = connector.getQueryEngine().execute("", createLogicalWorkFlow(CATALOG, TABLE));
        assertEquals("One record has been deleted", 3, queryResult.getResultSet().size());
        Set<String> sRows = recoveredIds(queryResult);
        assertTrue("The row is correct", sRows.contains("1"));
        assertTrue("The row is correct", sRows.contains("2"));
        assertTrue("The row is correct", sRows.contains("3"));

    }

    @Test
    public void deleteByPKLETStringTest() throws ConnectorException {

        ClusterName clusterName = getClusterName();
        insertTestData(clusterName);

        connector.getStorageEngine().delete(clusterName, new TableName(CATALOG, TABLE),
                createDeleteFilter(Operations.DELETE_PK_LET, Operator.LET));

        QueryResult queryResult = connector.getQueryEngine().execute("", createLogicalWorkFlow(CATALOG, TABLE));
        assertEquals("One record has been deleted", 2, queryResult.getResultSet().size());
        Set<String> sRows = recoveredIds(queryResult);

        assertTrue("The row is correct", sRows.contains("2"));
        assertTrue("The row is correct", sRows.contains("3"));

    }

    @Test
    public void deleteByPKGTStringTest() throws ConnectorException {

        ClusterName clusterName = getClusterName();
        insertTestData(clusterName);

        connector.getStorageEngine().delete(clusterName, new TableName(CATALOG, TABLE),
                createDeleteFilter(Operations.DELETE_PK_GT, Operator.GT));

        QueryResult queryResult = connector.getQueryEngine().execute("", createLogicalWorkFlow(CATALOG, TABLE));
        assertEquals("One record has been deleted", 2, queryResult.getResultSet().size());
        Set<String> sRows = recoveredIds(queryResult);

        assertTrue("The row is correct", sRows.contains("0"));
        assertTrue("The row is correct", sRows.contains("1"));

    }

    @Test
    public void deleteByPKGETStringTest() throws ConnectorException {

        ClusterName clusterName = getClusterName();
        insertTestData(clusterName);

        connector.getStorageEngine().delete(clusterName, new TableName(CATALOG, TABLE),
                createDeleteFilter(Operations.DELETE_PK_GET, Operator.GET));

        QueryResult queryResult = connector.getQueryEngine().execute("", createLogicalWorkFlow(CATALOG, TABLE));
        assertEquals("One record has been deleted", 1, queryResult.getResultSet().size());
        Set<String> sRows = recoveredIds(queryResult);

        assertTrue("The row is correct", sRows.contains("0"));

    }

    private void insertTestData(ClusterName clusterName) throws ConnectorException {
        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE,
                getClusterName().getName());

        tableMetadataBuilder.addColumn(COLUMN_PK,
                new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_1, new ColumnType(DataType.VARCHAR))
                .withPartitionKey(COLUMN_PK);

        connector.getStorageEngine().insert(clusterName,
                tableMetadataBuilder.build(getConnectorHelper().isPKMandatory()), createRow("0", "value1"),
                false);
        connector.getStorageEngine().insert(clusterName,
                tableMetadataBuilder.build(getConnectorHelper().isPKMandatory()), createRow("1", "value2"),
                false);
        connector.getStorageEngine().insert(clusterName,
                tableMetadataBuilder.build(getConnectorHelper().isPKMandatory()), createRow("2", "value3"),
                false);

        connector.getStorageEngine().insert(clusterName,
                tableMetadataBuilder.build(getConnectorHelper().isPKMandatory()), createRow("3", "value3"),
                false);

        getConnectorHelper().refresh(CATALOG);
        QueryResult queryResult = connector.getQueryEngine().execute("", createLogicalWorkFlow(CATALOG, TABLE));

        assertEquals("There are three records in table", 4, queryResult.getResultSet().size());

    }

    private Set<String> recoveredIds(QueryResult queryResult) {
        Set<String> sRows = new HashSet<>();
        for (Row row : queryResult.getResultSet().getRows()) {
            sRows.add(row.getCell(COLUMN_PK).getValue().toString());
        }

        return sRows;

    }

    private Collection<Filter> createDeleteFilter(Operations operations, Operator operator) {

        Collection<Filter> filters = new LinkedList<>();
        ColumnName name = new ColumnName(CATALOG, TABLE, COLUMN_PK);
        ColumnSelector primaryKey = new ColumnSelector(name);

        Selector rightTerm = new StringSelector("1");

        Relation relation = new Relation(primaryKey, operator, rightTerm);
        Set<Operations> operation = new HashSet<>();
        operation.add(operations);
        filters.add(new Filter(operation, relation));
        return filters;
    }

    private Row createRow(String valuePK, String valueColumn1) {
        Map<String, Cell> cells = new HashMap<>();
        Row row = new Row();
        cells.put(COLUMN_PK, new Cell(valuePK));
        cells.put(COLUMN_1, new Cell(valueColumn1));
        row.setCells(cells);
        return row;
    }

    private LogicalWorkflow createLogicalWorkFlow(String catalog, String table) {
        return new LogicalWorkFlowCreator(catalog, table, getClusterName()).addColumnName(COLUMN_PK, COLUMN_1)
                .build();
    }

}