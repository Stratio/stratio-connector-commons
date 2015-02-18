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
import java.util.Map;

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

public abstract class GenericLimitFT extends GenericConnectorTest {

    public static final String COLUMN_TEXT = "text";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";
    private static final int RESULT_NUMBER = 15456;

    @Test
    public void limitTest() throws Exception {

        ClusterName clusterName = getClusterName();

        for (int i = 0; i < 25463; i++) {
            insertRow(i, "text", clusterName);
        }

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlan(RESULT_NUMBER);

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        assertEquals("The limited result is correct", RESULT_NUMBER, queryResult.getResultSet().size());

    }

    private LogicalWorkflow createLogicalPlan(int limit) {

        return new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName())
                        .addColumnName(COLUMN_TEXT, COLUMN_AGE, COLUMN_MONEY).addLimit(limit).build();

    }

    private void insertRow(int ikey, String texto, ClusterName cLusterName) throws UnsupportedOperationException,
                    ConnectorException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_TEXT, new Cell(texto + ikey));
        cells.put(COLUMN_AGE, new Cell(10));
        cells.put(COLUMN_MONEY, new Cell(20));
        row.setCells(cells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_TEXT,
                new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_AGE, new ColumnType(DataType.INT))
                .addColumn(COLUMN_MONEY, new ColumnType(DataType.INT));

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());

        connector.getStorageEngine().insert(cLusterName, targetTable, row, false);

    }

}