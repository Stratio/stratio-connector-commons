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

package com.stratio.connector.commons.test.util;

import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.*;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.*;
import com.stratio.crossdata.common.statements.structures.window.TimeUnit;
import com.stratio.crossdata.common.statements.structures.window.WindowType;

import java.util.*;

/**
 * Created by jmgomez on 16/09/14.
 */
public class LogicalWorkFlowCreator {

    public static final String COLUMN_KEY = "id";
    public static final String COLUMN_1 = "column1";
    public static final String COLUMN_2 = "column2";
    public static final String COLUMN_3 = "column3";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";
    private final ClusterName clusterName;
    public String table = this.getClass().getSimpleName();
    public String catalog = "catalog_functional_test";
    Select select;
    List<ColumnName> columns = new ArrayList<>();
    List<Filter> filters = new ArrayList<>();
    private Limit limit;
    private Window window;
    private GroupBy groupBy;
    private OrderBy orderBy;

    public LogicalWorkFlowCreator(String catalog, String table, ClusterName clusterName) {
        this.catalog = catalog;
        this.table = table;
        this.clusterName = clusterName;
    }

    public LogicalWorkflow build() {

        List<LogicalStep> logiclaSteps = new ArrayList<>();

        Set<Operations> projectOperation = new HashSet<>();
        projectOperation.add(Operations.PROJECT);
        Project project = new Project(projectOperation, new TableName(catalog, table), clusterName, columns);
        LogicalStep lastStep = project;
        for (Filter filter : filters) {
            lastStep.setNextStep(filter);
            lastStep = filter;
        }

        if (window != null) {
            lastStep.setNextStep(window);
            lastStep = window;
        }

        if (groupBy != null) {
            lastStep.setNextStep(groupBy);
            lastStep = groupBy;
        }

        if (orderBy != null) {
            lastStep.setNextStep(orderBy);
            lastStep = orderBy;
        }

        if (limit != null) {

            lastStep.setNextStep(limit);
            lastStep = limit;

        }

        if (select == null) {
            Map<Selector, String> selectColumn = new LinkedHashMap<>();
            Map<String, ColumnType> typeMap = new LinkedHashMap();
            Map<Selector, ColumnType> typeMapColumnName = new LinkedHashMap<>();
            for (ColumnName columnName : project.getColumnList()) {
                ColumnSelector columnSelector = new ColumnSelector(
                        new ColumnName(catalog, table, columnName.getName()));
                selectColumn.put(columnSelector, columnName.getName());
                typeMap.put(columnName.getAlias(), new ColumnType(DataType.VARCHAR));
                typeMapColumnName.put(columnSelector, new ColumnType(DataType.VARCHAR));
            }
            Set<Operations> selectOperator = new HashSet<>();
            selectOperator.add(Operations.SELECT_OPERATOR);

            select = new Select(selectOperator, selectColumn, typeMap, typeMapColumnName); // The select is

            // mandatory
            // . If it
            // doesn't
            // exist we
            // create with all project's columns with varchar type.

        }
        lastStep.setNextStep(select);
        select.setPrevious(lastStep);

        logiclaSteps.add(project);

        LogicalWorkflow logWorkflow = new LogicalWorkflow(logiclaSteps);
        logWorkflow.setLastStep(select);
        return logWorkflow;

    }

    public LogicalWorkFlowCreator addDefaultColumns() {

        addColumnName(COLUMN_1);
        addColumnName(COLUMN_2);
        addColumnName(COLUMN_AGE);
        addColumnName(COLUMN_MONEY);

        return this;
    }

    public LogicalWorkFlowCreator addBetweenFilter(String columnName, Object leftTerm, Object rightTerm) {

        throw new RuntimeException("Not yet implemented");
        // return this;
    }

    public LogicalWorkFlowCreator addColumnName(String... columnName) {
        for (int i = 0; i < columnName.length; i++) {
            columns.add(new ColumnName(catalog, table, columnName[i]));
        }

        return this;
    }

    public LogicalWorkFlowCreator addEqualFilter(String columnName, Object value, Boolean indexed, boolean pk) {
        Selector columnSelector = new ColumnSelector(new ColumnName(catalog, table, columnName));

        Operations operation = Operations.FILTER_INDEXED_EQ;
        if (pk) {
            operation = Operations.FILTER_PK_EQ;
        } else if (indexed) {
            operation = Operations.FILTER_INDEXED_EQ;
        } else {
            operation = Operations.FILTER_NON_INDEXED_EQ;
        }
        Set<Operations> filterperator = new HashSet<>();
        filterperator.add(operation);
        filters.add(new Filter(filterperator, new Relation(columnSelector, Operator.EQ, returnSelector(value))));
        return this;

    }

    private void createFilterEQ(String columnName, Object value, Operations operations) {
        Selector columnSelector = new ColumnSelector(new ColumnName(catalog, table, columnName));
        Set<Operations> op = new HashSet<>();
        op.add(operations);
        filters.add(new Filter(op, new Relation(columnSelector, Operator.ASSIGN, returnSelector(value))));
    }

    private Selector returnSelector(Object value) {
        Selector valueSelector = null;

        if (value instanceof String) {
            valueSelector = new StringSelector((String) value);
        } else if (value instanceof Integer) {
            valueSelector = new IntegerSelector((Integer) value);
        }
        if (value instanceof Boolean) {
            valueSelector = new BooleanSelector((Boolean) value);
        }
        return valueSelector;
    }

    public LogicalWorkFlowCreator addGreaterEqualFilter(String columnName, Object term, Boolean indexed, boolean pk) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.GET,
                returnSelector(term));

        Set<Operations> operation = new HashSet<>();

        if (pk) {
            operation.add(Operations.FILTER_PK_GET);
        } else if (indexed) {
            operation.add(Operations.FILTER_INDEXED_GET);
        } else {
            operation.add(Operations.FILTER_NON_INDEXED_GET);
        }
        filters.add(new Filter(operation, relation));

        return this;

    }

    public LogicalWorkFlowCreator addGreaterFilter(String columnName, Object term, Boolean indexed) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.GT,
                returnSelector(term));
        Set<Operations> operation = new HashSet<>();
        if (indexed) {
            operation.add(Operations.FILTER_INDEXED_GT);
        } else {
            operation.add(Operations.FILTER_NON_INDEXED_GT);
        }
        filters.add(new Filter(operation, relation));
        return this;

    }

    public LogicalWorkFlowCreator addLowerEqualFilter(String columnName, Object term, Boolean indexed) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.LET,
                returnSelector(term));
        Set<Operations> operation = new HashSet<>();

        if (indexed) {
            operation.add(Operations.FILTER_INDEXED_LET);

        } else {
            operation.add(Operations.FILTER_NON_INDEXED_LET);

        }

        filters.add(new Filter(operation, relation));
        return this;

    }

    public LogicalWorkFlowCreator addNLowerFilter(String columnName, Object term, Boolean indexed) {
        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.LT,
                returnSelector(term));
        Set<Operations> operation = new HashSet<>();

        if (indexed) {
            operation.add(Operations.FILTER_INDEXED_LT);

        } else {
            operation.add(Operations.FILTER_NON_INDEXED_LT);

        }
        filters.add(new Filter(operation, relation));
        return this;
    }

    public LogicalWorkFlowCreator addDistinctFilter(String columnName, Object term, Boolean indexed, Boolean PK) {
        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)),
                Operator.DISTINCT, returnSelector(term));
        Set<Operations> operation = new HashSet<>();

        if (PK) {
            operation.add(Operations.FILTER_PK_DISTINCT);

        } else if (indexed) {
            operation.add(Operations.FILTER_INDEXED_DISTINCT);

        } else {
            operation.add(Operations.FILTER_NON_INDEXED_DISTINCT);

        }
        filters.add(new Filter(operation, relation));
        return this;
    }

    public LogicalWorkFlowCreator addMatchFilter(String columnName, String textToFind) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)),

                Operator.MATCH, returnSelector(textToFind));
        Set<Operations> operation = new HashSet<>();
        operation.add(Operations.FILTER_INDEXED_MATCH);
        filters.add(new Filter(operation, relation));

        return this;
    }

    public LogicalWorkFlowCreator addLikeFilter(String columnName, String textToFind) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.LIKE,
                returnSelector(textToFind));
        Set<Operations> operation = new HashSet<>();
        operation.add(Operations.FILTER_INDEXED_MATCH);
        filters.add(new Filter(operation, relation));

        return this;
    }

    public LogicalWorkFlowCreator addSelect(LinkedList<ConnectorField> fields) {
        Map<Selector, String> mapping = new LinkedHashMap<>();
        Map<String, ColumnType> types = new LinkedHashMap<>();
        Map<Selector, ColumnType> typeMapFormColumnName = new LinkedHashMap<>();
        for (ConnectorField connectorField : fields) {
            ColumnSelector columnSelector = new ColumnSelector(new ColumnName(catalog, table, connectorField.name));
            mapping.put(columnSelector, connectorField.alias);
            types.put(connectorField.alias, connectorField.columnType);
            typeMapFormColumnName.put(columnSelector, connectorField.columnType);
        }
        Set<Operations> operation = new HashSet<>();
        operation.add(Operations.PROJECT);
        select = new Select(operation, mapping, types, typeMapFormColumnName);

        return this;

    }

    public LogicalWorkFlowCreator addWindow(WindowType type, int limit) throws UnsupportedException {
        Set<Operations> operation = new HashSet<>();
        operation.add(Operations.FILTER_FUNCTION_EQ);
        window = new Window(operation, type);
        switch (type) {
            case NUM_ROWS:
                window.setNumRows(limit);
                break;
            case TEMPORAL:
                window.setTimeWindow(limit, TimeUnit.SECONDS);
                break;
            default:
                throw new UnsupportedException("Window " + type + " not supported");

        }
        return this;
    }

    public ConnectorField createConnectorField(String name, String alias, ColumnType columnType) {
        return new ConnectorField(name, alias, columnType);

    }

    public LogicalWorkFlowCreator addLimit(int limit) {
        Set<Operations> operation = new HashSet<>();
        operation.add(Operations.SELECT_LIMIT);
        this.limit = new Limit(operation, limit);
        return this;
    }

    public LogicalWorkFlowCreator addGroupBy(String... fields) {
        List<Selector> ids = new ArrayList<Selector>();
        for (String field : fields) {
            ids.add(new ColumnSelector(new ColumnName(catalog, table, field)));
        }
        Set<Operations> operation = new HashSet<>();
        operation.add(Operations.SELECT_GROUP_BY);
        this.groupBy = new GroupBy(operation, ids);
        return this;
    }

    public LogicalWorkFlowCreator addOrderByClause(String field, OrderDirection direction) {
        if (orderBy == null) {
            Set<Operations> operation = new HashSet<>();
            operation.add(Operations.SELECT_ORDER_BY);
            orderBy = new OrderBy(operation, new LinkedList<OrderByClause>());
        }

        List<OrderByClause> previousList = orderBy.getIds();
        previousList.add(new OrderByClause(direction, new ColumnSelector(new ColumnName(catalog, table, field))));

        // clone??
        orderBy.setIds(previousList);
        return this;
    }

    public class ConnectorField {
        public String name;
        public String alias;
        public ColumnType columnType;

        public ConnectorField(String name, String alias, ColumnType columnType) {
            this.name = name;
            this.alias = alias;
            this.columnType = columnType;
        }

    }

}
