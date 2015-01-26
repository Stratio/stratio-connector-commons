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
import java.util.LinkedList;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator.ConnectorField;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;

public abstract class GenericGroupByFT extends GenericConnectorTest {

    public static final String COLUMN_ID = "id";
    public static final String COLUMN_TEXT = "text";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";

    @Test
    public void basicGroupByFT() throws Exception {

        insertRow(1, "text", 10, 20);// row,text,money,age
        insertRow(2, "text", 9, 17);
        insertRow(3, "text", 11, 26);
        insertRow(4, "text", 10, 30);
        insertRow(5, "text", 20, 42);
        insertRow(6, "text", 20, 48);

        LogicalWorkFlowCreator logicalWorkflowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName())
                        .addColumnName(COLUMN_ID).addColumnName(COLUMN_TEXT).addColumnName(COLUMN_AGE)
                        .addColumnName(COLUMN_MONEY).addGroupBy(COLUMN_AGE);
        ConnectorField cField = logicalWorkflowCreator.createConnectorField(COLUMN_TEXT, COLUMN_TEXT, ColumnType.TEXT);
        LinkedList<ConnectorField> selectFields = new LinkedList<>();
        selectFields.add(cField);
        logicalWorkflowCreator.addSelect(selectFields);
        LogicalWorkflow logicalWorkflow = logicalWorkflowCreator.getLogicalWorkflow();

        QueryResult queryResult = connector.getQueryEngine().execute(logicalWorkflow);

        assertEquals("There should be only 4 different ages", 4, queryResult.getResultSet().size());

    }

    protected void insertRow(int pk, String text, int age, int money) throws ConnectorException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();

        cells.put(COLUMN_ID, new Cell(pk));
        cells.put(COLUMN_TEXT, new Cell(text));
        cells.put(COLUMN_AGE, new Cell(age));
        cells.put(COLUMN_MONEY, new Cell(money));

        row.setCells(cells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_ID, ColumnType.INT).addColumn(COLUMN_TEXT, ColumnType.VARCHAR)
                        .addColumn(COLUMN_AGE, ColumnType.INT).addColumn(COLUMN_MONEY, ColumnType.INT);

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());

        if (getConnectorHelper().isTableMandatory()) {
            connector.getMetadataEngine().createTable(getClusterName(), targetTable);
        }
        connector.getStorageEngine().insert(getClusterName(), targetTable, row, false);

        refresh(CATALOG);
    }

}