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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator.ConnectorField;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.OrderDirection;

public abstract class GenericOrderByFT extends GenericConnectorTest {

    public static final String COLUMN_TEXT = "text";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";

    public static final String TEXT_VALUE = "text";

    @Test
    public void sortAscTest() throws ConnectorException {

        prepareDataForTest();
        // insertRow(1, TEXT_VALUE, 10, 20, tableMetadata, getClusterName());// row,text,money,age
        // insertRow(2, TEXT_VALUE, 9, 17, tableMetadata, getClusterName());// ej => text:text2, money:9, age:17
        // insertRow(3, TEXT_VALUE, 11, 26, tableMetadata, getClusterName());
        // insertRow(4, TEXT_VALUE, 10, 30, tableMetadata, getClusterName());
        // insertRow(5, TEXT_VALUE, 20, 42, tableMetadata, getClusterName());
        // insertRow(6, TEXT_VALUE, 10, 10, tableMetadata, getClusterName());

        // Select COLUMN_TEXT

        LogicalWorkFlowCreator logWFCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
        logWFCreator.addColumnName(COLUMN_TEXT).addColumnName(COLUMN_AGE).addSelect(getSelectClause(logWFCreator));
        logWFCreator.addOrderByClause(COLUMN_AGE, OrderDirection.ASC);

        // return COLUMN_TEXT order by age ASC
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logWFCreator.build());

        assertEquals("The result should have 6 rows", 6, queryResult.getResultSet().size());

        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();

        assertEquals("Columns order is not the expected", "text6", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text2", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text1", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text3", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text4", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text5", rowIterator.next().getCell(COLUMN_TEXT).getValue());
    }

    @Test
    public void sortDescTest() throws ConnectorException {

        prepareDataForTest();
        // insertRow(1, TEXT_VALUE, 10, 20, tableMetadata, getClusterName());// row,text,money,age
        // insertRow(2, TEXT_VALUE, 9, 17, tableMetadata, getClusterName());// ej => text:text2, money:9, age:17
        // insertRow(3, TEXT_VALUE, 11, 26, tableMetadata, getClusterName());
        // insertRow(4, TEXT_VALUE, 10, 30, tableMetadata, getClusterName());
        // insertRow(5, TEXT_VALUE, 20, 42, tableMetadata, getClusterName());
        // insertRow(6, TEXT_VALUE, 10, 10, tableMetadata, getClusterName());

        // Select COLUMN_TEXT

        LogicalWorkFlowCreator logWFCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
        logWFCreator.addColumnName(COLUMN_TEXT).addColumnName(COLUMN_AGE).addSelect(getSelectClause(logWFCreator));
        logWFCreator.addOrderByClause(COLUMN_AGE, OrderDirection.DESC);

        // return COLUMN_TEXT order by age DESC
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logWFCreator.build());

        assertEquals("The result should have 6 rows", 6, queryResult.getResultSet().size());

        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();

        assertEquals("Columns order is not the expected", "text5", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text4", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text3", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text1", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text2", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text6", rowIterator.next().getCell(COLUMN_TEXT).getValue());

    }

    @Test
    public void sortTestMultifield() throws ConnectorException {

        prepareDataForTest();
        // insertRow(1, TEXT_VALUE, 10, 20, tableMetadata, getClusterName());// row,text,money,age
        // insertRow(2, TEXT_VALUE, 9, 17, tableMetadata, getClusterName());// ej => text:text2, money:9, age:17
        // insertRow(3, TEXT_VALUE, 11, 26, tableMetadata, getClusterName());
        // insertRow(4, TEXT_VALUE, 10, 30, tableMetadata, getClusterName());
        // insertRow(5, TEXT_VALUE, 20, 42, tableMetadata, getClusterName());
        // insertRow(6, TEXT_VALUE, 10, 10, tableMetadata, getClusterName());

        // Select COLUMN_TEXT
        LogicalWorkFlowCreator logWFCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
        logWFCreator.addColumnName(COLUMN_TEXT).addColumnName(COLUMN_AGE).addSelect(getSelectClause(logWFCreator));
        logWFCreator.addOrderByClause(COLUMN_MONEY, OrderDirection.ASC).addOrderByClause(COLUMN_AGE,
                        OrderDirection.DESC);

        // return COLUMN_TEXT order by money asc, age asc
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logWFCreator.build());

        assertEquals("The result should have 6 rows", 6, queryResult.getResultSet().size());

        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();

        assertEquals("Columns order is not the expected", "text2", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text4", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text1", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text6", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text3", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("Columns order is not the expected", "text5", rowIterator.next().getCell(COLUMN_TEXT).getValue());

    }

    private LinkedList<ConnectorField> getSelectClause(LogicalWorkFlowCreator logWFCreator) {
        ConnectorField field = logWFCreator.createConnectorField(COLUMN_TEXT, COLUMN_TEXT, new ColumnType(DataType
                .VARCHAR));
        LinkedList<ConnectorField> selectClause = new LinkedList<ConnectorField>();
        selectClause.add(field);
        return selectClause;
    }

    protected void prepareDataForTest() throws ConnectorException {

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE,getClusterName().getName());
        tableMetadataBuilder.addColumn(COLUMN_TEXT,
                new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_AGE, new ColumnType(DataType.INT))
                .addColumn(COLUMN_MONEY, new ColumnType(DataType.INT)).withPartitionKey(COLUMN_TEXT);
        TableMetadata tableMetadata = tableMetadataBuilder.build();

        if (this.getConnectorHelper().isCatalogMandatory()) {
            // TODO createCatalog
        }
        if (this.getConnectorHelper().isTableMandatory()) {
            getConnectorHelper().getConnector().getMetadataEngine()
                            .createTable(getClusterName(), tableMetadataBuilder.build());
        }
        if (this.getConnectorHelper().isIndexMandatory()) {
            // TODO createIndexes
        }
        insertRow(1, TEXT_VALUE, 10, 20, tableMetadata, getClusterName());// row,text,money,age
        insertRow(2, TEXT_VALUE, 9, 17, tableMetadata, getClusterName());// ej => text:text2, money:9, age:17
        insertRow(3, TEXT_VALUE, 11, 26, tableMetadata, getClusterName());
        insertRow(4, TEXT_VALUE, 10, 30, tableMetadata, getClusterName());
        insertRow(5, TEXT_VALUE, 20, 42, tableMetadata, getClusterName());
        insertRow(6, TEXT_VALUE, 10, 10, tableMetadata, getClusterName());

        refresh(CATALOG);

    }

    private void insertRow(int ikey, String texto, int money, int age, TableMetadata tableMetadata,
                    ClusterName clusterName) throws ConnectorException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_TEXT, new Cell(texto + ikey));
        cells.put(COLUMN_AGE, new Cell(age));
        cells.put(COLUMN_MONEY, new Cell(money));
        row.setCells(cells);
        connector.getStorageEngine().insert(clusterName, tableMetadata, row, false);

    }

}