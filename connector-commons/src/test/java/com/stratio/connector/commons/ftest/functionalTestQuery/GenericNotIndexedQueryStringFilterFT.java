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

import static com.stratio.connector.commons.util.TextConstant.danteParadise;
import static com.stratio.connector.commons.util.TextConstant.names;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * Created by jmgomez on 17/07/14.
 */
public abstract class GenericNotIndexedQueryStringFilterFT extends GenericConnectorTest {

    private static final String COLUMN_TEXT = "column_text";

    LogicalWorkFlowCreator logicalWorkFlowCreator;

    @Before
    public void setUp() throws ConnectorException {
        super.setUp();
        logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
    }

    @Test
    public void selectNotIndexedFilterUpperCaseEqual() throws ConnectorException {

        ClusterName clusterNodeName = getClusterName();

        insertRow(names, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_TEXT)
                .addEqualFilter(COLUMN_TEXT, names[10], false, false).build();

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        assertEquals("The record number is correct", 1, queryResult.getResultSet().size());

    }

    @Test
    public void selectNotIndexedFilterUpperCaseDistinct() throws ConnectorException {
        ClusterName clusterNodeName = getClusterName();

        insertRow(names, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_TEXT)
                .addDistinctFilter(COLUMN_TEXT, names[5], false, false).build();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        assertEquals("The record number is correct", names.length - 1, queryResult.getResultSet().size());

    }

    @Test
    public void selectNotIndexedFilterLowerCaseEqual() throws ConnectorException {

        ClusterName clusterNodeName = getClusterName();

        insertRow(names, clusterNodeName, true);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_TEXT)
                .addEqualFilter(COLUMN_TEXT, names[10].toLowerCase(), false, false).build();

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        assertEquals("The record number is correct", 1, queryResult.getResultSet().size());

    }

    @Test
    public void selectNotIndexedFilterLowerCaseCaseDistinct() throws ConnectorException {
        ClusterName clusterNodeName = getClusterName();

        insertRow(names, clusterNodeName, true);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_TEXT)
                .addDistinctFilter(COLUMN_TEXT, names[5].toLowerCase(), false, false).build();
        QueryResult queryResult = connector.getQueryEngine().execute(logicalPlan);

        assertEquals("The record number is correct", names.length - 1, queryResult.getResultSet().size());

    }

    @Test
    public void selectNotIndexedFilterMatch() throws ConnectorException {

        insertRow(danteParadise, getClusterName(), false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_TEXT)
                .addMatchFilter(COLUMN_TEXT, "matter").build();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        ResultSet resultSet = queryResult.getResultSet();
        assertEquals("The record number is correct", 2, resultSet.size());

        for (Row row : resultSet) {
            assertTrue("the return text contains matter",
                    ((String) row.getCell(COLUMN_TEXT).getValue()).contains("matter"));
        }

    }

    private void insertRow(String[] text, ClusterName clusterNodeName, boolean toLowerCase)
            throws UnsupportedOperationException, ConnectorException {

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE,
                getClusterName().getName());
        tableMetadataBuilder.addColumn("id", new ColumnType(DataType.INT)).addColumn(COLUMN_TEXT, new ColumnType
                (DataType.VARCHAR));
        tableMetadataBuilder.addIndex(IndexType.FULL_TEXT, "indexText", COLUMN_TEXT);
        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());
        TableName tableName = new TableName(CATALOG, TABLE);

        // Creating indexMetadata

        if (iConnectorHelper.isIndexMandatory()) {
            Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
            Object[] parameters = null;
            columns.put(new ColumnName(tableName, COLUMN_TEXT), new ColumnMetadata(new ColumnName(tableName,
                    COLUMN_TEXT), parameters, new ColumnType(DataType.TEXT)));
            IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, "indexText"), columns,
                    IndexType.FULL_TEXT, Collections.EMPTY_MAP);
            connector.getMetadataEngine().createIndex(clusterNodeName, indexMetadata);
        }
        Collection<Row> rows = new ArrayList();

        for (int i = 0; i < text.length; i++) {
            Map<String, Cell> cells = new HashMap<>();
            String field = text[i];
            if (toLowerCase) {
                field = field.toLowerCase();
            }

            cells.put(COLUMN_TEXT, new Cell(field));
            cells.put("id", new Cell(i));
            Row row = new Row();
            row.setCells(cells);
            rows.add(row);

        }

        connector.getStorageEngine().insert(clusterNodeName, targetTable, rows, false);

    }

}
