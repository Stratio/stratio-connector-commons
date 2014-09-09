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

import java.util.*;

import static org.junit.Assert.assertEquals;


/**
 */
public abstract class GenericSimpleInsertTest extends GenericConnectorTest {


    private static final String COLUMN_1 = "COLUMN 1";
    private static final String COLUMN_2 = "COLUMN 2";
    private static final String COLUMN_3 = "COLUMN 3";
    private static final String COLUMN_4 = "COLUMN 4";


    private static final String VALUE_1 = "value1";
    private static final String OTHER_VALUE_1 = "OTHER VALUE";
    private static final String VALUE_4 = "value4";
    private static final String OTHER_VALUE_4 = "other value 4";
    private static final int VALUE_2 = 2;
    private static final boolean VALUE_3 = true;




    @Test
    public void testSimpleInsertWithPK() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testSimpleInsert  ***********************************");


        insertRow(clusterName, VALUE_4, VALUE_1, true);



        verifyInsert(clusterName, VALUE_4);
    }


    @Test
    public void testSimpleInsertWithOutPK() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testSimpleInsert  ***********************************");

        insertRow(clusterName, VALUE_4, VALUE_1,false);



        verifyInsert(clusterName, VALUE_4);
    }

    @Test
    public void testInsertSamePK() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST testInsertSamePK "+ clusterName.getName()+" ***********************************");

        insertRow(clusterName, VALUE_4, VALUE_1,true);
        insertRow(clusterName, OTHER_VALUE_4, VALUE_1,true);

        verifyInsert(clusterName, OTHER_VALUE_4);

    }




    private void verifyInsert(ClusterName cluesterName, String test_value_4) throws UnsupportedException, ExecutionException {
        QueryResult queryResult = connector.getQueryEngine().execute(cluesterName, createLogicalWorkFlow());
        ResultSet resultIterator = queryResult.getResultSet();


        for(Row recoveredRow: resultIterator){
        	 assertEquals("The value is correct " , VALUE_1, recoveredRow.getCell(COLUMN_1).getValue());
        	 assertEquals("The value is correct " , VALUE_2, recoveredRow.getCell(COLUMN_2).getValue());
        	 assertEquals("The value is correct ", VALUE_3, recoveredRow.getCell(COLUMN_3).getValue());
            assertEquals("The value is correct ",  test_value_4, recoveredRow.getCell(COLUMN_4).getValue());
        }

        assertEquals("The records number is correct "+cluesterName.getName(), 1, resultIterator.size());
    }



    private void insertRow(ClusterName cluesterName, String value_4, String PK_VALUE, boolean withPK) throws UnsupportedException, ExecutionException {
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();

        cells.put(COLUMN_1, new Cell(PK_VALUE));
        cells.put(COLUMN_2, new Cell(VALUE_2));
        cells.put(COLUMN_3, new Cell(VALUE_3));
        cells.put(COLUMN_4, new Cell(value_4));
        row.setCells(cells);

        List<ColumnName> pk = new ArrayList<>();
        if (withPK) {
            ColumnName columnPK = new ColumnName(CATALOG, TABLE, COLUMN_1);
            pk.add(columnPK);
        }


        connector.getStorageEngine().insert(cluesterName,  new TableMetadata(new TableName(CATALOG, TABLE),null,null,null,null,pk, Collections.EMPTY_LIST), row);
        refresh(CATALOG);
    }


    private LogicalWorkflow createLogicalWorkFlow() {
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnName> columns = new ArrayList<>();

        columns.add(new ColumnName(CATALOG,TABLE,COLUMN_1));
        columns.add(new ColumnName(CATALOG,TABLE,COLUMN_2));
        columns.add(new ColumnName(CATALOG,TABLE,COLUMN_3));
        columns.add(new ColumnName(CATALOG,TABLE,COLUMN_4));
        TableName tableName = new TableName(CATALOG,TABLE);
        Project project = new Project(null,tableName,columns);
        stepList.add(project);
        return new LogicalWorkflow(stepList);
    }


}
