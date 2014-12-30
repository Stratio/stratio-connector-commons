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
package com.stratio.connector.commons.ftest.functionalUpdate;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

public abstract class GenericSimpleUpdateWithFiltersFT extends GenericConnectorTest<IConnector> {

    protected static final String COLUMN_1 = "COLUMN_1".toLowerCase();
    protected static final String COLUMN_2 = "COLUMN_2".toLowerCase();

    @Test
    public void multiUpdateGenericsFieldsWithFilterIndexGetTest() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        insertRow(clusterName, "row1", 20);
        insertRow(clusterName, "row2", 21);
        insertRow(clusterName, "row3", 19);
        insertRow(clusterName, "row4", 20);

        verifyInsert(clusterName, 4);

        Collection<Filter> filterCollection = new ArrayList<Filter>();
        filterCollection.add(new Filter(Operations.UPDATE_NON_INDEXED_GET,
                        getBasicRelation(COLUMN_2, Operator.GET, 20l)));

        updateRow(clusterName, filterCollection);
        verifyUpdate(clusterName, 3);

    }

    @Test
    public void multiUpdateGenericsFieldsWithFilterEqualPkTest() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        insertRow(clusterName, "row1", 20);
        insertRow(clusterName, "row2", 21);
        insertRow(clusterName, "row3", 19);
        insertRow(clusterName, "row4", 20);

        verifyInsert(clusterName, 4);

        Collection<Filter> filterCollection = new ArrayList<Filter>();
        filterCollection.add(new Filter(Operations.UPDATE_PK_EQ, getBasicRelation(COLUMN_1, Operator.EQ, "row1")));

        updateRow(clusterName, filterCollection);
        verifyUpdate(clusterName, 1);

    }

    protected void verifyUpdate(ClusterName clusterName, int expectedMatchedRows) throws ConnectorException {
        ResultSet resultIterator = createResultSet(clusterName);

        int matchedRows = 0;
        for (Row recoveredRow : resultIterator) {
            if (recoveredRow.getCell(COLUMN_1).getValue().equals("matched")) {
                matchedRows++;
            }
        }

        assertEquals("The records number is correct " + clusterName.getName(), expectedMatchedRows, matchedRows);
    }

    private void updateRow(ClusterName clusterName, Collection<Filter> filterCollection) throws UnsupportedException,
                    ConnectorException {

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.BIGINT);

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());

        if (getConnectorHelper().isTableMandatory()) {
            connector.getMetadataEngine().createTable(clusterName, targetTable);
        }

        Relation rel1 = getBasicRelation(COLUMN_1, Operator.ASSIGN, "matched");

        List<Relation> assignments = Arrays.asList(rel1);

        connector.getStorageEngine().update(clusterName, new TableName(CATALOG, TABLE), assignments, filterCollection);
        refresh(CATALOG);

    }

    private Relation getBasicRelation(String column1, Operator assign, Object valueUpdated) {
        Selector leftSelector = new ColumnSelector(new ColumnName(CATALOG, TABLE, column1));
        Selector rightSelector = null;
        if (valueUpdated instanceof Long || valueUpdated instanceof Integer) {
            rightSelector = new IntegerSelector((int) (long) valueUpdated);
        } else if (valueUpdated instanceof String) {
            rightSelector = new StringSelector((String) valueUpdated);
        } else if (valueUpdated instanceof Boolean) {
            rightSelector = new BooleanSelector((Boolean) valueUpdated);
        }
        return new Relation(leftSelector, assign, rightSelector);
    }

    private void verifyInsert(ClusterName clusterName, int rowsInserted) throws ConnectorException {
        ResultSet resultIterator = createResultSet(clusterName);
        assertEquals("The records number is correct " + clusterName.getName(), rowsInserted, resultIterator.size());
    }

    protected ResultSet createResultSet(ClusterName clusterName) throws ConnectorException {
        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow());
        return queryResult.getResultSet();
    }

    protected void insertRow(ClusterName cluesterName, Object value1, Object value2) throws ConnectorException {
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();

        cells.put(COLUMN_1, new Cell(value1));
        cells.put(COLUMN_2, new Cell(value2));

        row.setCells(cells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.BIGINT)
                        .withPartitionKey(COLUMN_1);

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper().isPKMandatory());

        if (getConnectorHelper().isTableMandatory()) {
            connector.getMetadataEngine().createTable(getClusterName(), targetTable);
        }
        connector.getStorageEngine().insert(cluesterName, targetTable, row, false);
        refresh(CATALOG);
    }

    private LogicalWorkflow createLogicalWorkFlow() {
        return new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName()).addColumnName(COLUMN_1, COLUMN_2)
                        .getLogicalWorkflow();

    }
}
