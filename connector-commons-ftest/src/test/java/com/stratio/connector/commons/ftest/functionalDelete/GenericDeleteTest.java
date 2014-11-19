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
import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.data.*;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class GenericDeleteTest extends GenericConnectorTest {


    private static String COLUMN_PK = "idPK";
    private static String COLUMN_1 = "name";

    @Test
    public void deleteByPKStringTest() throws ConnectorException {

        ClusterName clusterName = getClusterName();
        Collection<Filter> filters = new HashSet<>(1);
        Operations operations = Operations.DELETE_PK_EQ;
        ColumnName name = new ColumnName(CATALOG, TABLE, COLUMN_PK);
        ColumnSelector primaryKey =  new ColumnSelector(name);
        Operator operator = Operator.EQ;
        StringSelector rightTerm = new StringSelector("id1");

        Relation relation = new Relation(primaryKey, operator, rightTerm);
        Filter filter = new Filter(operations, relation);


        Row row = new Row();
        Row row1 = new Row();
        Row row2 = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_PK, new Cell("id1"));
        cells.put(COLUMN_1, new Cell("value1"));
        row.setCells(cells);
        cells.put(COLUMN_PK, new Cell("id2"));
        cells.put(COLUMN_1, new Cell("value2"));
        row1.setCells(cells);
        cells.put(COLUMN_PK, new Cell("id3"));
        cells.put(COLUMN_1, new Cell("value3"));
        row2.setCells(cells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_PK, ColumnType.VARCHAR).addColumn(COLUMN_1, ColumnType.VARCHAR);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()), row);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()), row1);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()), row2);

        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(CATALOG, TABLE));
        Assert.assertEquals(3, queryResult.getResultSet().size());
        Assert.assertEquals(2, queryResult.getResultSet().getRows().get(0).size());

        connector.getStorageEngine().delete(clusterName, new TableName(CATALOG, TABLE), filters);

        Assert.assertEquals(2, queryResult.getResultSet().size());
        Assert.assertEquals(2, queryResult.getResultSet().getRows().get(0).size());

        queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow(CATALOG, TABLE));
        assertEquals("Table [" + CATALOG + "." + TABLE + "] deleted", 0, queryResult.getResultSet().size());
    }
//mirar que hace esto bien
    private LogicalWorkflow createLogicalWorkFlow(String catalog, String table) {
        return new LogicalWorkFlowCreator(catalog, table, getClusterName()).addColumnName(COLUMN_PK, COLUMN_1)
                .getLogicalWorkflow();
    }

//buscar insert rows, EL MÉTODO PARA REFACTORIZAR
}