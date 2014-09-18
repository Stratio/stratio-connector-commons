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

import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.statements.structures.selectors.*;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by jmgomez on 16/09/14.
 */
public class LogicalWorkFlowCreator {


    public String table = this.getClass().getSimpleName();
    public String catalog = "catalog_functional_test";

    public LogicalWorkFlowCreator(String catalog, String table) {
        this.catalog = catalog;
        this.table = table;
    }

    public static final String COLUMN_1 = "column1";
    public static final String COLUMN_2 = "column2";
    public static final String COLUMN_3 = "column3";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";


    List<ColumnName> columns = new ArrayList<>();
    List<Filter> filters = new ArrayList<>();

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

    public LogicalWorkflow getLogicalWorkflow() {
        List<LogicalStep> logiclaSteps = new ArrayList<>();
        if (!filters.isEmpty()) {
            logiclaSteps.addAll(filters);
        }
        if (!columns.isEmpty()) {
            logiclaSteps.add(new Project(Operations.PROJECT, new TableName(catalog, table), columns));
        }
        return new LogicalWorkflow(logiclaSteps);

    }

    public LogicalWorkFlowCreator addColumnName(String columnName) {
        columns.add(new ColumnName(catalog, table, columnName));

        return this;
    }


    public LogicalWorkFlowCreator addEqualFilter(String columnName, Object value, Boolean indexed) {
        Selector columnSelector = new ColumnSelector(new ColumnName(catalog, table, columnName));

        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_EQ, new Relation(columnSelector, Operator.ASSIGN, returnSelector(value))));
        }else{
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_EQ, new Relation(columnSelector, Operator.ASSIGN, returnSelector(value))));
        }

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


        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.GET, returnSelector(term));

        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_GET, relation));
        }else{
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_GET, relation));

        }

        return this;

    }

    public LogicalWorkFlowCreator addGreaterFilter(String columnName, Object term, Boolean indexed) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.GT, returnSelector(term));
        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_GT, relation));
        }else{
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_GT, relation));
        }

        return this;

    }

    public LogicalWorkFlowCreator addLowerEqualFilter(String columnName, Object term, Boolean indexed) {


        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.LET, returnSelector(term));
        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_LET, relation));
        }else {
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_LET, relation));
        }

        return this;

    }

    public LogicalWorkFlowCreator addNLowerFilter(String columnName, Object term, Boolean indexed) {
        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.LT, returnSelector(term));
        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_LT, relation));
        }else {
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_LT, relation));
        }

        return this;
    }


    public LogicalWorkFlowCreator addDistinctFilter(String columnName, Object term, Boolean indexed) {
        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.DISTINCT, returnSelector(term));
        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_DISTINCT, relation));
        }else {
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_DISTINCT, relation));
        }

        return this;
    }

    public LogicalWorkFlowCreator addMatchFilter(String columnName, String textToFind) {


        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.MATCH, returnSelector(textToFind));

       filters.add(new Filter(Operations.FILTER_FULLTEXT, relation));


        return this;
    }
}
