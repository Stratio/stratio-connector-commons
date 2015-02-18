/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.commons.ftest.functionalMetadata;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.CatalogMetadataBuilder;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.data.AlterOperation;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.result.QueryResult;

public abstract class GenericMetadataAlterTableFT extends GenericConnectorTest {

    protected static final String COLUMN_1 = "name1";
    protected static final String COLUMN_2 = "name2";

    @Test
    public void addColumnFT() throws ConnectorException {

        int insertElement = 0;
        ClusterName clusterName = getClusterName();

        // Create the catalog and the table with COLUMN_1
        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, new ColumnType(DataType.VARCHAR));

        CatalogMetadataBuilder catalogMetadataBuilder = new CatalogMetadataBuilder(CATALOG);
        catalogMetadataBuilder.withTables(tableMetadataBuilder.build(getConnectorHelper().isPKMandatory()));
        if (iConnectorHelper.isCatalogMandatory()) {
            connector.getMetadataEngine().createCatalog(clusterName, catalogMetadataBuilder.build());
        }
        if (iConnectorHelper.isTableMandatory()) {
            connector.getMetadataEngine().createTable(clusterName,
                            tableMetadataBuilder.build(getConnectorHelper().isPKMandatory()));
        } else {

            Row row = new Row();
            Map<String, Cell> cells = new HashMap<>();
            cells.put(COLUMN_1, new Cell("value1"));
            row.setCells(cells);
            connector.getStorageEngine().insert(clusterName,
                            tableMetadataBuilder.build(getConnectorHelper().isPKMandatory()), row, false);
            insertElement++;
            refresh(CATALOG);
        }

        // ADD the column: COLUMN_2 with alterTable
        ColumnMetadata columnMetadata = new ColumnMetadata(new ColumnName(CATALOG, TABLE, COLUMN_2), new Object[0],
                       new ColumnType(DataType.INT));
        AlterOptions alterOptions = new AlterOptions(AlterOperation.ADD_COLUMN, null, columnMetadata);

        connector.getMetadataEngine().alterTable(clusterName, new TableName(CATALOG, TABLE), alterOptions);

        refresh(CATALOG);

        // INSERT a row with column2 != null
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("value1"));
        cells.put(COLUMN_2, new Cell(25));
        row.setCells(cells);

        tableMetadataBuilder.addColumn(COLUMN_2, new ColumnType(DataType.INT));
        connector.getStorageEngine().insert(clusterName,
                        tableMetadataBuilder.build(getConnectorHelper().isPKMandatory()), row, false);
        insertElement++;

        refresh(CATALOG);

        // Verify the proper column field is returned
        QueryResult queryResult = connector.getQueryEngine().execute(
                        new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName()).addColumnName(COLUMN_1)
                                        .addColumnName(COLUMN_2).build());
        ResultSet resultSet = queryResult.getResultSet();

        assertEquals("The number of element recovered must be the same of the number of element inserted",
                        insertElement, resultSet.size());
        assertEquals("The number or column must be two", 2, resultSet.getColumnMetadata().size());

    }

    @Test
    public void dropColumnFT() throws ConnectorException {

        ClusterName clusterName = getClusterName();

        // Create the catalog and the table with COLUMN_1 and COLUMN_2
        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, new ColumnType(DataType.VARCHAR)).addColumn(COLUMN_2, new ColumnType
                (DataType.INT));

        CatalogMetadataBuilder catalogMetadataBuilder = new CatalogMetadataBuilder(CATALOG);
        catalogMetadataBuilder.withTables(tableMetadataBuilder.build(getConnectorHelper().isPKMandatory()));
        if (iConnectorHelper.isCatalogMandatory()) {
            connector.getMetadataEngine().createCatalog(clusterName, catalogMetadataBuilder.build());
        }
        if (iConnectorHelper.isTableMandatory()) {
            connector.getMetadataEngine().createTable(clusterName,
                            tableMetadataBuilder.build(getConnectorHelper().isPKMandatory()));
        }

        // Insert a row
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("value1"));
        cells.put(COLUMN_2, new Cell(25));
        row.setCells(cells);

        connector.getStorageEngine().insert(clusterName,
                        tableMetadataBuilder.build(getConnectorHelper().isPKMandatory()), row, false);

        // Verify both fields are returned
        QueryResult queryResult = connector.getQueryEngine().execute(
                        new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName()).addColumnName(COLUMN_1)
                                        .addColumnName(COLUMN_2).build());
        assertEquals("Table [" + CATALOG + "." + TABLE + "] ", 1, queryResult.getResultSet().size());

        Row receivedRow = queryResult.getResultSet().getRows().get(0);
        assertEquals("Error recovering the inserted data", "value1", receivedRow.getCell(COLUMN_1).getValue());
        assertEquals("Error recovering the inserted data", 25, receivedRow.getCell(COLUMN_2).getValue());

        // DROP the column: COLUMN_2 with alterTable
        ColumnMetadata columnMetadata = new ColumnMetadata(new ColumnName(CATALOG, TABLE, COLUMN_2), new Object[0],
                       new ColumnType(DataType.INT));
        AlterOptions alterOptions = new AlterOptions(AlterOperation.DROP_COLUMN, null, columnMetadata);

        refresh(CATALOG);

        connector.getMetadataEngine().alterTable(clusterName, new TableName(CATALOG, TABLE), alterOptions);

        refresh(CATALOG);

        // Verify if the column has been dropped
        queryResult = connector.getQueryEngine().execute(
                        new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName()).addColumnName(COLUMN_1)
                                        .addColumnName(COLUMN_2).build());
        assertEquals("Table [" + CATALOG + "." + TABLE + "] ", 1, queryResult.getResultSet().size());

        receivedRow = queryResult.getResultSet().getRows().get(0);
        assertEquals("Error recovering the column which has not been modified", "value1", receivedRow.getCell(COLUMN_1)
                        .getValue());
        assertEquals("Founded not null value in the dropped column", null, receivedRow.getCell(COLUMN_2).getValue());

    }

}
