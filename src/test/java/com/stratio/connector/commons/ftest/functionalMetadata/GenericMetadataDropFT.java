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
package com.stratio.connector.commons.ftest.functionalMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.connector.commons.test.util.TableMetadataBuilder;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.Selector;

public abstract class GenericMetadataDropFT extends GenericConnectorTest {

    private static String COLUMN_1 = "name1";
    private static String COLUMN_2 = "name2";
    public final String OTHER_TABLE = "other" + TABLE;
    private String OTHER_CATALOG = "other_" + CATALOG;

    @Test
    public void dropTableTest() throws ConnectorException {
        ClusterName clusterName = getClusterName();
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("value1"));
        cells.put(COLUMN_2, new Cell(2));
        row.setCells(cells);
        if (iConnectorHelper.isTableMandatory()) {
            createTable();
        }

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()), row, false);

        tableMetadataBuilder = new TableMetadataBuilder(OTHER_CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()), row, false);

        tableMetadataBuilder = new TableMetadataBuilder(CATALOG, OTHER_TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()), row, false);

        refresh(CATALOG);
        refresh(OTHER_CATALOG);

        connector.getMetadataEngine().dropTable(clusterName, (new TableName(CATALOG, TABLE)));

        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(CATALOG, TABLE));
        assertEquals("Table [" + CATALOG + "." + TABLE + "] must be delete", 0, queryResult.getResultSet().size());

        queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(OTHER_CATALOG, TABLE));
        assertNotEquals("Table [" + OTHER_CATALOG + "." + TABLE + "] must exist", 0, queryResult.getResultSet().size());

        queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(CATALOG, OTHER_TABLE));
        assertNotEquals("Table [" + CATALOG + "." + OTHER_TABLE + " must  exist", 0, queryResult.getResultSet().size());
        if (iConnectorHelper.isTableMandatory()) {
            connector.getMetadataEngine().dropTable(getClusterName(), new TableName(CATALOG, TABLE));
        }
    }

    private void createTable() throws ConnectorException {
        TableName tableName = new TableName(CATALOG, TABLE);
        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        LinkedHashMap<ColumnName, ColumnMetadata> columnsMap = new LinkedHashMap<>();

        Collection<ColumnType> allSupportedColumnType = getConnectorHelper().getAllSupportedColumnType();

        ColumnName columnName = new ColumnName(CATALOG, TABLE, COLUMN_1);
        columnsMap.put(columnName, new ColumnMetadata(columnName, null, ColumnType.TEXT));
        ColumnName otherColumnName = new ColumnName(CATALOG, TABLE, COLUMN_2);
        columnsMap.put(otherColumnName, new ColumnMetadata(otherColumnName, null, ColumnType.INT));
        // ColumnMetadata (list of columns to create a single index)
        List<ColumnMetadata> columns = new ArrayList<>();
        Object[] parameters = null;
        columns.add(new ColumnMetadata(new ColumnName(tableName, "columnName_1"), parameters, ColumnType.TEXT));

        TableMetadata tableMetadata = new TableMetadata(tableName, options, columnsMap, Collections.EMPTY_MAP,
                        getClusterName(), new LinkedList(), new LinkedList());
        connector.getMetadataEngine().createTable(getClusterName(), tableMetadata);
    }

    @Test
    public void dropCatalogTest() throws ConnectorException {

        ClusterName clusterName = getClusterName();
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("value1"));
        cells.put(COLUMN_2, new Cell(2));
        row.setCells(cells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()), row, false);

        tableMetadataBuilder = new TableMetadataBuilder(OTHER_CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()), row, false);

        tableMetadataBuilder = new TableMetadataBuilder(CATALOG, OTHER_TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()), row, false);

        refresh(CATALOG);
        refresh(OTHER_CATALOG);

        connector.getMetadataEngine().dropCatalog(clusterName, new CatalogName(CATALOG));

        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(CATALOG, TABLE));
        assertEquals("Table [" + CATALOG + "." + TABLE + "] deleted", 0, queryResult.getResultSet().size());

        queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(OTHER_CATALOG, TABLE));
        assertNotEquals("Table [" + OTHER_CATALOG + "." + TABLE + "] exist", 0, queryResult.getResultSet().size());

        queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(CATALOG, OTHER_TABLE));
        assertEquals("Table [" + CATALOG + "." + OTHER_TABLE + " deleted", 0, queryResult.getResultSet().size());

        deleteCatalog(OTHER_CATALOG);

    }

    private LogicalWorkflow createLogicalWorkFlow(String catalog, String table) {

        return new LogicalWorkFlowCreator(catalog, table, getClusterName()).addColumnName(COLUMN_1, COLUMN_2)
                        .getLogicalWorkflow();
    }

    @After
    public void tearDown() throws ConnectionException, UnsupportedException, ExecutionException {
        super.tearDown();
        deleteCatalog(OTHER_CATALOG);

        if (logger.isDebugEnabled()) {
            logger.debug("Delete Catalog: " + OTHER_CATALOG);

        }
    }

}