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

package com.stratio.connector.commons.ftest.workFlow;

import static com.stratio.connector.commons.ftest.helper.TextConstant.names;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * Example workflows to test basic functionality of the different connectors. This class assumes the existence of a
 * catalog named {@code example} and with two tables named {@code users}, and {@code information}.
 * <p/>
 * Table {@code users} contains the following fields:
 * <li>
 * <ul>
 * id: integer, PK
 * </ul>
 * <ul>
 * name: text, PK, indexed
 * </ul>
 * <ul>
 * age: integer
 * </ul>
 * <ul>
 * bool: boolean
 * </ul>
 * </li>
 * <p/>
 * Table {@code information} contains the following fields:
 * <li>
 * <ul>
 * id: integer, PK
 * </ul>
 * <ul>
 * phrase: text, indexed
 * </ul>
 * <ul>
 * email: text
 * </ul>
 * <ul>
 * score: double
 * </ul>
 * </li>
 */
public class ExampleWorkflows {

    public static final String COLUMN_ID = "id";
    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_BOOL = "bool";
    public static final String ALIAS_ID = "users.id";
    public static final String ALIAS_NAME = "users.name";
    public static final String ALIAS_AGE = "users.age";
    public static Boolean[] booleans = { true, false };
    private final ClusterName clusterName;
    public String table;
    public String catalog;

    public ExampleWorkflows(String catalog, String table, ClusterName clusterName) {
        this.catalog = catalog;
        this.table = table;
        this.clusterName = clusterName;
    }

    public List<Row> getRows(int init) {

        List<Row> rows = new ArrayList<Row>();

        for (int i = init * 10000; i < (init + 1) * 10000; i++) {
            Row row = new Row();
            row.addCell(COLUMN_ID, new Cell(i));
            row.addCell(COLUMN_NAME, new Cell(names[i % names.length].toLowerCase()));
            row.addCell(COLUMN_AGE, new Cell(i % 111));
            row.addCell(COLUMN_BOOL, new Cell(booleans[i % booleans.length]));
            rows.add(row);
        }
        return rows;
    }


    public Project getProject(ColumnName... columnNames) {
        TableName table = new TableName(columnNames[0].getTableName().getCatalogName().getName(), columnNames[0]
                        .getTableName().getName());
        return new Project(Operations.PROJECT, table, clusterName, Arrays.asList(columnNames));
    }


    public Select getSelect(String[] alias, ColumnType[] types, ColumnName... columnNames) {
        Map<ColumnName, String> columnMap = new LinkedHashMap<>();
        Map<String, ColumnType> columntype = new LinkedHashMap<>();
        int aliasIndex = 0;
        Map<ColumnName, ColumnType> typeMapFromColumnName = new LinkedHashMap();
        for (ColumnName column : columnNames) {
            ColumnName columnName = new ColumnName(catalog, table, column.getName());
            columnMap.put(columnName, alias[aliasIndex]);
            columntype.put(column.getQualifiedName(), types[aliasIndex]);
            typeMapFromColumnName.put(columnName,types[aliasIndex]);
            aliasIndex++;
        }

        return new Select(Operations.SELECT_OPERATOR, columnMap, columntype, typeMapFromColumnName);
    }


    public Filter getFilter(Operations filterOp, ColumnName column, Operator op, Selector right) {
        Selector left = new ColumnSelector(column);
        Relation r = new Relation(left, op, right);
        return new Filter(filterOp, r);
    }


    public LogicalWorkflow getBasicSelect() {
        ColumnName name = new ColumnName(catalog, table, COLUMN_NAME);
        ColumnName age = new ColumnName(catalog, table, COLUMN_AGE);
        String[] outputNames = { ALIAS_NAME, ALIAS_AGE };
        ColumnType[] types = { ColumnType.VARCHAR, ColumnType.INT };
        LogicalStep project = getProject(name, age);
        LogicalStep select = getSelect(outputNames, types, name, age);
        project.setNextStep(select);
        LogicalWorkflow lw = new LogicalWorkflow(Arrays.asList(project));
        return lw;
    }

    /**
     * Get a basic select. SELECT * FROM example.users;
     *
     *
     */
    public LogicalWorkflow getBasicSelectAsterisk() {
        ColumnName id = new ColumnName(catalog, table, COLUMN_ID);
        ColumnName name = new ColumnName(catalog, table, COLUMN_NAME);
        ColumnName age = new ColumnName(catalog, table, COLUMN_AGE);
        ColumnName bool = new ColumnName(catalog, table, COLUMN_BOOL);
        String[] outputNames = { COLUMN_ID, COLUMN_NAME, COLUMN_AGE, COLUMN_BOOL };
        LogicalStep project = getProject(id, name, age, bool);
        LogicalStep select = getSelect(outputNames, new ColumnType[] { ColumnType.INT, ColumnType.TEXT, ColumnType.INT,
                        ColumnType.BOOLEAN }, id, name, age, bool);
        project.setNextStep(select);
        LogicalWorkflow lw = new LogicalWorkflow(Arrays.asList(project));
        return lw;
    }

    public LogicalWorkflow getSelectIndexedField() {
        ColumnName id = new ColumnName(catalog, table, COLUMN_ID);
        ColumnName name = new ColumnName(catalog, table, COLUMN_NAME);
        ColumnName age = new ColumnName(catalog, table, COLUMN_AGE);
        String[] outputNames = { ALIAS_NAME, ALIAS_AGE };
        ColumnType[] types = { ColumnType.VARCHAR, ColumnType.INT };
        LogicalStep project = getProject(id, name, age);
        LogicalStep filter = getFilter(Operations.FILTER_INDEXED_EQ, name, Operator.EQ,
                        new StringSelector(names[0].toLowerCase()));
        project.setNextStep(filter);
        LogicalStep select = getSelect(outputNames, types, name, age);
        filter.setNextStep(select);
        LogicalWorkflow lw = new LogicalWorkflow(Arrays.asList(project));
        return lw;
    }

    /*
     * /** Get a basic select with a single where clause on a non-indexed field. SELECT users.id, users.name, users.age
     * FROM example.users WHERE users.age=42;
     * 
     * @return A {@link com.stratio.meta.common.logicalplan.LogicalWorkflow}.
     */
    public LogicalWorkflow getSelectNonIndexedField() {
        ColumnName id = new ColumnName(catalog, table, COLUMN_ID);
        ColumnName name = new ColumnName(catalog, table, COLUMN_NAME);
        ColumnName age = new ColumnName(catalog, table, COLUMN_AGE);
        String[] outputNames = { ALIAS_NAME, ALIAS_AGE };
        ColumnType[] types = { ColumnType.VARCHAR, ColumnType.INT };
        LogicalStep project = getProject(id, name, age);
        LogicalStep filter = getFilter(Operations.FILTER_NON_INDEXED_EQ, age, Operator.EQ, new IntegerSelector(42));
        project.setNextStep(filter);
        LogicalStep select = getSelect(outputNames, types, name, age);
        filter.setNextStep(select);
        LogicalWorkflow lw = new LogicalWorkflow(Arrays.asList(project));
        return lw;
    }


    public LogicalWorkflow getSelectMixedWhere() {
        ColumnName id = new ColumnName(catalog, table, COLUMN_ID);
        ColumnName name = new ColumnName(catalog, table, COLUMN_NAME);
        ColumnName age = new ColumnName(catalog, table, COLUMN_AGE);
        String[] outputNames = { ALIAS_ID, ALIAS_NAME, ALIAS_AGE };
        ColumnType[] types = { ColumnType.INT, ColumnType.VARCHAR, ColumnType.INT };
        LogicalStep project = getProject(id, name, age);
        LogicalStep filterName = getFilter(Operations.FILTER_INDEXED_EQ, name, Operator.EQ,
                        new StringSelector(names[1].toLowerCase()));
        project.setNextStep(filterName);
        LogicalStep filterAge = getFilter(Operations.FILTER_NON_INDEXED_EQ, age, Operator.EQ, new IntegerSelector(40));
        filterName.setNextStep(filterAge);
        LogicalStep select = getSelect(outputNames, types, id, name, age);
        filterAge.setNextStep(select);
        LogicalWorkflow lw = new LogicalWorkflow(Arrays.asList(project));
        return lw;
    }

}
