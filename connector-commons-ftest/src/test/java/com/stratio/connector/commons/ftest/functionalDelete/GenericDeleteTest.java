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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;

public abstract class GenericDeleteTest extends GenericConnectorTest {


    private static String COLUMN_PK = "idpk";
    private static String COLUMN_1 = "name";

    @Test
    public void deleteByPKEQStringTest() throws ConnectorException {

        ClusterName clusterName = getClusterName();
         insertTestData(clusterName);


        connector.getStorageEngine().delete(clusterName, new TableName(CATALOG, TABLE), createDeleteFilter(
                Operations.DELETE_PK_EQ, Operator.EQ));


        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(CATALOG, TABLE));
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


        connector.getStorageEngine().delete(clusterName, new TableName(CATALOG, TABLE), createDeleteFilter(
                Operations.DELETE_PK_LT, Operator.LT));


        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(CATALOG, TABLE));
        assertEquals("One record has been deleted", 3, queryResult.getResultSet().size());
        Set<String> sRows = recoveredIds(queryResult);
        assertTrue("The row is correct", sRows.contains("1"));
        assertTrue("The row is correct", sRows.contains("2"));
        assertTrue("The row is correct", sRows.contains("3"));

    }


    private void insertTestData(ClusterName clusterName) throws ConnectorException {
        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_PK, ColumnType.VARCHAR).addColumn(COLUMN_1, ColumnType.VARCHAR).withPartitionKey(
                COLUMN_PK);



        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()),
                createRow("0", "value1"));
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()),
                createRow("1", "value2"));
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()),
                createRow("2", "value3"));
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()),
                createRow("3", "value3"));


        getConnectorHelper().refresh(CATALOG);
        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(CATALOG, TABLE));

        assertEquals("There are three records in table", 4, queryResult.getResultSet().size());


    }

    private Set<String> recoveredIds(QueryResult queryResult) {
        Set<String> sRows = new HashSet<>();
        for  (Row row :queryResult.getResultSet().getRows()){
            sRows.add(row.getCell(COLUMN_PK).getValue().toString());

        }
        return sRows;
    }

    private Collection<Filter> createDeleteFilter(Operations operations, Operator operator) {
        Collection<Filter> filters = new LinkedList<>();
        ColumnName name = new ColumnName(CATALOG, TABLE, COLUMN_PK);
        ColumnSelector primaryKey =  new ColumnSelector(name);


        Selector rightTerm = new IntegerSelector(1);

        Relation relation = new Relation(primaryKey, operator, rightTerm);
        filters.add(new Filter(operations, relation));
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

    //mirar que hace esto bien
    private LogicalWorkflow createLogicalWorkFlow(String catalog, String table) {
        return new LogicalWorkFlowCreator(catalog, table, getClusterName()).addColumnName(COLUMN_PK, COLUMN_1)
                .getLogicalWorkflow();
    }

//buscar insert rows, EL MÉTODO PARA REFACTORIZAR
}