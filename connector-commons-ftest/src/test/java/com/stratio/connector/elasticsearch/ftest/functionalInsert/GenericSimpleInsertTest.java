/*
 * Stratio Meta
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
package com.stratio.connector.elasticsearch.ftest.functionalInsert;


import com.stratio.connector.elasticsearch.ftest.GenericConnectorTest;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;


/**
 */
public abstract class GenericSimpleInsertTest extends GenericConnectorTest {


    private static final String COLUMN_1 = "name1";
    private static final String COLUMN_2 = "name2";
    private static final String COLUMN_3 = "name3";

    private static final String VALUE_1 = "value1";
    private static final int VALUE_2 = 2;
    private static final boolean VALUE_3 = true;



    @Test
    public void testSimpleInsert() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testSimpleInsert "+ clusterName.getName()+" ***********************************");

        insertRow(clusterName);



        verifyInsert(clusterName);
    }

    private void verifyInsert(ClusterName cluesterName) throws UnsupportedException, ExecutionException {
        QueryResult queryResult = connector.getQueryEngine().execute(cluesterName,createLogicalPlan());
        ResultSet resultIterator = queryResult.getResultSet();


        for(Row recoveredRow: resultIterator){
        	 assertEquals("The value is correct " + cluesterName.getName(), VALUE_1, recoveredRow.getCell(COLUMN_1).getValue());
        	 assertEquals("The value is correct " + cluesterName.getName(), VALUE_2, recoveredRow.getCell(COLUMN_2).getValue());
        	 assertEquals("The value is correct "+cluesterName.getName(), VALUE_3, recoveredRow.getCell(COLUMN_3).getValue());
        }

        assertEquals("The records number is correct "+cluesterName.getName(), 1, resultIterator.size());
    }

    private void insertRow(ClusterName cluesterName) throws UnsupportedException, ExecutionException {
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell(VALUE_1));
        cells.put(COLUMN_2, new Cell(VALUE_2));
        cells.put(COLUMN_3, new Cell(VALUE_3));
        row.setCells(cells);

        connector.getStorageEngine().insert(cluesterName,  new TableMetadata(new TableName(SCHEMA, TABLE),null,null,null,null,null,null), row);
        refresh(SCHEMA);
    }


    private LogicalWorkflow createLogicalPlan() {
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnName> columns = new ArrayList<>();

        columns.add(new ColumnName(SCHEMA,TABLE,COLUMN_1));
        columns.add(new ColumnName(SCHEMA,TABLE,COLUMN_2));
        columns.add(new ColumnName(SCHEMA,TABLE,COLUMN_3));
        TableName tableName = new TableName(SCHEMA,TABLE);
        Project project = new Project(null,tableName,columns);
        stepList.add(project);
        return new LogicalWorkflow(stepList);
    }


}
