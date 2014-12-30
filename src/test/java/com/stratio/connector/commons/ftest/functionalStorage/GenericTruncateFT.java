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
package com.stratio.connector.commons.ftest.functionalStorage;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * @author lfernandez
 */
public abstract class GenericTruncateFT extends GenericConnectorTest<IConnector> {

    private static String COLUMN_1 = "name1";
    private static String COLUMN_2 = "name2";

    @Test
    public void truncateTest() throws ConnectorException {

        // TODO verify if an existing index is not dropped when truncating
        ClusterName clusterName = getClusterName();

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("value1"));
        cells.put(COLUMN_2, new Cell(2));
        row.setCells(cells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);

        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName,
                        tableMetadataBuilder.build(getConnectorHelper().isPKMandatory()), row, false);

        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(CATALOG, TABLE));
        Assert.assertEquals(1, queryResult.getResultSet().size());
        Assert.assertEquals(2, queryResult.getResultSet().getRows().get(0).size());

        connector.getStorageEngine().truncate(clusterName, (new TableName(CATALOG, TABLE)));

        queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(CATALOG, TABLE));

        Assert.assertEquals("Table [" + CATALOG + "." + TABLE + "] deleted", 0, queryResult.getResultSet().size());

    }

    private LogicalWorkflow createLogicalWorkFlow(String catalog, String table) {

        return new LogicalWorkFlowCreator(catalog, table, getClusterName()).addColumnName(COLUMN_1, COLUMN_2)
                        .getLogicalWorkflow();
    }
}
