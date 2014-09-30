/*
 * Stratio Deep
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */
package com.stratio.connector.commons.ftest.functionalMetadata;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.statements.structures.selectors.Selector;

public abstract class GenericMetadataDropTest extends GenericConnectorTest {

    private static String COLUMN_1 = "name1";
    private static String COLUMN_2 = "name2";
    public final String OTHER_TABLE = "other" + TABLE;
    private String OTHER_CATALOG = "other_" + CATALOG;

    @Test
    public void dropTableTest() throws UnsupportedException, ExecutionException {
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
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(), row);

        tableMetadataBuilder = new TableMetadataBuilder(OTHER_CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(), row);

        tableMetadataBuilder = new TableMetadataBuilder(CATALOG, OTHER_TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(), row);

        refresh(CATALOG);
        refresh(OTHER_CATALOG);

        connector.getMetadataEngine().dropTable(clusterName, (new TableName(CATALOG, TABLE)));

        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(CATALOG, TABLE));
        assertEquals("Table [" + CATALOG + "." + TABLE + "] deleted", 0, queryResult.getResultSet().size());

        queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(OTHER_CATALOG, TABLE));
        assertNotEquals("Table [" + OTHER_CATALOG + "." + TABLE + "] exist", 0, queryResult.getResultSet().size());

        queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(CATALOG, OTHER_TABLE));
        assertNotEquals("Table [" + CATALOG + "." + OTHER_TABLE + " exist", 0, queryResult.getResultSet().size());
        if (iConnectorHelper.isTableMandatory()) {
            connector.getMetadataEngine().dropTable(getClusterName(), new TableName(CATALOG, TABLE));
        }
    }

    private void createTable() throws UnsupportedException, ExecutionException {
        TableName tableName = new TableName(CATALOG, TABLE);
        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        Map<ColumnName, ColumnMetadata> columnsMap = new HashMap<>();

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
                        getClusterName(), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        connector.getMetadataEngine().createTable(getClusterName(), tableMetadata);
    }

    @Test
    public void dropCatalogTest() throws UnsupportedException, ExecutionException {

        ClusterName clusterName = getClusterName();
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("value1"));
        cells.put(COLUMN_2, new Cell(2));
        row.setCells(cells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(), row);

        tableMetadataBuilder = new TableMetadataBuilder(OTHER_CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(), row);

        tableMetadataBuilder = new TableMetadataBuilder(CATALOG, OTHER_TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(), row);

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