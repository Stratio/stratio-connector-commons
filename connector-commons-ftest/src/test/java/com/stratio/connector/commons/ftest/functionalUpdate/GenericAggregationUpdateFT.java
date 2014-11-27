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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.RelationSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

public abstract class GenericAggregationUpdateFT extends GenericConnectorTest<IConnector> {

    protected static final String COLUMN_1 = "COLUMN_1".toLowerCase();
    protected static final String COLUMN_2 = "COLUMN_2".toLowerCase();

    public static final String VALUE_1 = "value1";
    protected static final long VALUE_2 = 2;

    public static final String VALUE_1_UPDATED = "othervalue";
    protected static final long TWO = 2;
    protected static final long FIVE = 5;

    /**
     * Test a basic increase field as "FIELD = FIELD + 2"
     *
     * @throws ConnectorException
     *             the connector exception
     */
    @Test
    public void updateAddFunctionFT() throws ConnectorException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST updateAddFields "
                        + clusterName.getName() + " ***********************************");
        insertRow(clusterName);
        verifyInsert(clusterName, 1);

        updateRow(clusterName, Operator.ADD, TWO);
        verifyUpdate(clusterName, 1, VALUE_2 + TWO);

    }

    /**
     * Test a basic decrement field as "FIELD = FIELD - 5"
     *
     * @throws ConnectorException
     *             the connector exception
     */
    @Test
    public void updateSubstractFunctionFT() throws ConnectorException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST updateAddFields "
                        + clusterName.getName() + " ***********************************");
        insertRow(clusterName);
        verifyInsert(clusterName, 1);

        updateRow(clusterName, Operator.SUBTRACT, FIVE);
        verifyUpdate(clusterName, 1, VALUE_2 - FIVE);

    }

    /**
     * Test a basic decrement field as "FIELD = FIELD * 5"
     *
     * @throws ConnectorException
     *             the connector exception
     */
    @Test
    public void updateMultiplicationFunctionFT() throws ConnectorException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST updateAddFields "
                        + clusterName.getName() + " ***********************************");
        insertRow(clusterName);
        verifyInsert(clusterName, 1);

        updateRow(clusterName, Operator.MULTIPLICATION, FIVE);
        verifyUpdate(clusterName, 1, VALUE_2 * FIVE);

    }

    protected void verifyUpdate(ClusterName clusterName, int insertedRows, Long valueExpected)
                    throws ConnectorException {
        ResultSet resultIterator = createResultSet(clusterName);

        for (Row recoveredRow : resultIterator) {
            assertEquals("Checking the updated value", VALUE_1_UPDATED, recoveredRow.getCell(COLUMN_1).getValue());
            assertEquals("Checking the updated value", valueExpected, recoveredRow.getCell(COLUMN_2).getValue());
        }

        assertEquals("The records number is correct " + clusterName.getName(), insertedRows, resultIterator.size());
    }

    private void updateRow(ClusterName clusterName, Operator operator, long value) throws UnsupportedException,
                    ConnectorException {

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.BIGINT);

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper());

        if (getConnectorHelper().isTableMandatory()) {
            connector.getMetadataEngine().createTable(clusterName, targetTable);
        }

        Relation rel1 = getBasicRelation(COLUMN_1, Operator.ASSIGN, VALUE_1_UPDATED);
        Relation innerRelation = getBasicRelation(COLUMN_2, operator, value);
        Relation rel2 = getBasicRelation(COLUMN_2, Operator.ASSIGN, innerRelation);

        List<Relation> assignments = Arrays.asList(rel1, rel2);

        connector.getStorageEngine().update(clusterName, new TableName(CATALOG, TABLE), assignments, null);
        refresh(CATALOG);

    }

    private Relation getBasicRelation(String column1, Operator assign, Object valueUpdated) {
        Selector leftSelector = new ColumnSelector(new ColumnName(CATALOG, TABLE, column1));
        Selector rightSelector = null;
        if (valueUpdated instanceof Long) {
            rightSelector = new IntegerSelector((int) (long) valueUpdated);
        } else if (valueUpdated instanceof String) {
            rightSelector = new StringSelector((String) valueUpdated);
        } else if (valueUpdated instanceof Boolean) {
            rightSelector = new BooleanSelector((Boolean) valueUpdated);
        } else if (valueUpdated instanceof Relation) {
            rightSelector = new RelationSelector((Relation) valueUpdated);
        }
        return new Relation(leftSelector, assign, rightSelector);
    }

    private void verifyInsert(ClusterName clusterName, int rowsInserted) throws ConnectorException {
        ResultSet resultIterator = createResultSet(clusterName);

        for (Row recoveredRow : resultIterator) {
            assertEquals("The value is correct ", VALUE_1, recoveredRow.getCell(COLUMN_1).getValue());
            assertEquals("The value is correct ", VALUE_2, recoveredRow.getCell(COLUMN_2).getValue());
        }

        assertEquals("The records number is correct " + clusterName.getName(), rowsInserted, resultIterator.size());
    }

    protected ResultSet createResultSet(ClusterName clusterName) throws ConnectorException {
        QueryResult queryResult = connector.getQueryEngine().execute(createLogicalWorkFlow());
        return queryResult.getResultSet();
    }

    protected void insertRow(ClusterName cluesterName) throws ConnectorException {
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();

        cells.put(COLUMN_1, new Cell(VALUE_1));
        cells.put(COLUMN_2, new Cell(VALUE_2));

        row.setCells(cells);

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR).addColumn(COLUMN_2, ColumnType.BIGINT);

        TableMetadata targetTable = tableMetadataBuilder.build(getConnectorHelper());

        if (getConnectorHelper().isTableMandatory()) {
            connector.getMetadataEngine().createTable(getClusterName(), targetTable);
        }
        connector.getStorageEngine().insert(cluesterName, targetTable, row);
        refresh(CATALOG);
    }

    private LogicalWorkflow createLogicalWorkFlow() {
        return new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName()).addColumnName(COLUMN_1, COLUMN_2)
                        .getLogicalWorkflow();

    }

}
