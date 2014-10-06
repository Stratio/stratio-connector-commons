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

package com.stratio.connector.commons.ftest.workFlow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Limit;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.statements.structures.selectors.BooleanSelector;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/**
 * Created by jmgomez on 16/09/14.
 */
public class LogicalWorkFlowCreator {

    private final ClusterName clusterName;
    Select select;
    public static final String COLUMN_1 = "column1";
    public static final String COLUMN_2 = "column2";
    public static final String COLUMN_3 = "column3";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";
    public String table = this.getClass().getSimpleName();
    public String catalog = "catalog_functional_test";
    List<ColumnName> columns = new ArrayList<>();
    List<Filter> filters = new ArrayList<>();
    private Limit limit;

    public LogicalWorkFlowCreator(String catalog, String table, ClusterName clusterName) {
        this.catalog = catalog;
        this.table = table;
        this.clusterName = clusterName;
    }

    public LogicalWorkflow getLogicalWorkflow() {

        List<LogicalStep> logiclaSteps = new ArrayList<>();

        Project project = new Project(Operations.PROJECT, new TableName(catalog, table), clusterName, columns);
        LogicalStep lastStep = project;
        for (Filter filter : filters) {
            lastStep.setNextStep(filter);
            lastStep = filter;
        }
        if (limit != null) {

            lastStep.setNextStep(limit);
            lastStep = limit;

        }
        if (select == null) {
            Map<ColumnName, String> selectColumn = new LinkedHashMap<>();
            Map<String, ColumnType> typeMap = new LinkedHashMap();
            for (ColumnName columnName : project.getColumnList()) {
                selectColumn.put(new ColumnName(catalog, table, columnName.getName()), columnName.getName());
                typeMap.put(columnName.getName(), ColumnType.VARCHAR);
            }

            select = new Select(Operations.PROJECT, selectColumn, typeMap); // The select is mandatory. If it doesn't
            // exist we
            // create with all project's columns with varchar type.

        }
        lastStep.setNextStep(select);

        logiclaSteps.add(project);

        return new LogicalWorkflow(logiclaSteps);

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
        filters.add(new Filter(operation, new Relation(columnSelector, Operator.EQ, returnSelector(value))));
        return this;

    }

    private void createFilterEQ(String columnName, Object value, Operations operations) {
        Selector columnSelector = new ColumnSelector(new ColumnName(catalog, table, columnName));

        filters.add(new Filter(operations, new Relation(columnSelector, Operator.ASSIGN, returnSelector(value))));
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

    public LogicalWorkFlowCreator addGreaterEqualFilter(String columnName, Object term, Boolean indexed) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.GET,
                        returnSelector(term));

        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_GET, relation));
        } else {
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_GET, relation));

        }

        return this;

    }

    public LogicalWorkFlowCreator addGreaterFilter(String columnName, Object term, Boolean indexed) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.GT,
                        returnSelector(term));
        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_GT, relation));
        } else {
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_GT, relation));
        }

        return this;

    }

    public LogicalWorkFlowCreator addLowerEqualFilter(String columnName, Object term, Boolean indexed) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.LET,
                        returnSelector(term));
        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_LET, relation));
        } else {
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_LET, relation));
        }

        return this;

    }

    public LogicalWorkFlowCreator addNLowerFilter(String columnName, Object term, Boolean indexed) {
        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.LT,
                        returnSelector(term));
        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_LT, relation));
        } else {
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_LT, relation));
        }

        return this;
    }

    public LogicalWorkFlowCreator addDistinctFilter(String columnName, Object term, Boolean indexed) {
        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)),
                        Operator.DISTINCT, returnSelector(term));
        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_DISTINCT, relation));
        } else {
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_DISTINCT, relation));
        }

        return this;
    }

    public LogicalWorkFlowCreator addMatchFilter(String columnName, String textToFind) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)),
                        Operator.MATCH, returnSelector(textToFind));

        filters.add(new Filter(Operations.FILTER_FULLTEXT, relation));

        return this;
    }

    public LogicalWorkFlowCreator addLikeFilter(String columnName, String textToFind) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.LIKE,
                        returnSelector(textToFind));

        filters.add(new Filter(Operations.FILTER_FULLTEXT, relation));

        return this;
    }

    public LogicalWorkFlowCreator addSelect(LinkedList<ConnectorField> fields) {
        Map<ColumnName, String> mapping = new LinkedHashMap<>();
        Map<String, ColumnType> types = new LinkedHashMap<>();

        for (ConnectorField connectorField : fields) {
            mapping.put(new ColumnName(catalog, table, connectorField.name), connectorField.alias);
            types.put(connectorField.name, connectorField.columnType);
        }

        select = new Select(Operations.PROJECT, mapping, types);

        return this;

    }

    public ConnectorField createConnectorField(String name, String alias, ColumnType columnType) {
        return new ConnectorField(name, alias, columnType);

    }

    public LogicalWorkFlowCreator addLimit(int limit) {
        this.limit = new Limit(Operations.SELECT_LIMIT, limit);
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
