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

package com.stratio.connector.commons.ftest.functionalTestQuery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.meta.common.data.Cell;
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

public abstract class GenericOrderByTest extends GenericConnectorTest {

    public static final String COLUMN_TEXT = "text";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";

    public static final int SORT_AGE = 1;

    @Test
    public void sortDescTest() throws UnsupportedException, ExecutionException {

        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST sortDescTest " + clusterName.getName()
                        + " ***********************************");

        insertRow(1, "text", 10, 20, clusterName);//row,text,money,age
        insertRow(2, "text", 9, 17, clusterName);
        insertRow(3, "text", 11, 26, clusterName);
        insertRow(4, "text", 10, 30, clusterName);
        insertRow(5, "text", 20, 42, clusterName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlanLimit(SORT_AGE);

        //return COLUMN_TEXT order by age DESC

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterName, logicalPlan);

        assertEquals(5, queryResult.getResultSet().size());

        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();

        assertEquals("text5", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text4", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text3", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text1", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text2", rowIterator.next().getCell(COLUMN_TEXT).getValue());

    }

    @Test
    public void sortTestMultifield() throws ExecutionException, UnsupportedException {

        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST sortDescTest " + clusterName.getName()
                        + " ***********************************");

        insertRow(1, "text", 10, 20, clusterName);//row,text,money,age
        insertRow(2, "text", 9, 17, clusterName);
        insertRow(3, "text", 11, 26, clusterName);
        insertRow(4, "text", 10, 30, clusterName);
        insertRow(5, "text", 20, 42, clusterName);
        insertRow(6, "text", 10, 10, clusterName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlanMultifield();

        //return COLUMN_TEXT order by money asc, age asc

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterName, logicalPlan);

        assertEquals(6, queryResult.getResultSet().size());

        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();

        assertEquals("text2", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text6", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text1", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text4", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text3", rowIterator.next().getCell(COLUMN_TEXT).getValue());
        assertEquals("text5", rowIterator.next().getCell(COLUMN_TEXT).getValue());

    }

    private void fail() {
        assertTrue(false);

    }

    private LogicalWorkflow createLogicalPlanLimit(int sortAge) {

        List<LogicalStep> stepList = new ArrayList<>();

        List<ColumnName> columns = new ArrayList<>();

        //Limit limit = new Limit(10);
        //stepList.add(limit);

        //
        //
        //	     columns.add(new ColumnName(CATALOG,TABLE,COLUMN_TEXT)); //REVIEW cambiado para que compile
        //	     columns.add(new ColumnName(CATALOG,TABLE,COLUMN_AGE));
        //        TableName tableName = new TableName(CATALOG,TABLE);
        //	     Project project = new Project(null,tableName,columns);
        //	     stepList.add(project);
        //
        //
        //	        switch (sortAge){
        //	        	case SORT_AGE: //stepList.add(new Sort(COLUMN_AGE, Sort.DESC)); break; //REVIEW cuando haya SORT de meta
        //	        	            throw new RuntimeException("Esperando a meta");
        //	// 2 Sort? o uno con lista de parámetros y luego lista de tipo ASC o DESC
        ////    	        	case SORT_AGE_MONEY: stepList.add(createNotEqualsFilter(filterType, object)); stepList.add(createBetweenFilter(9,11)); break;
        ////    	        	case SORT_AGE_TEXT: stepList.add(createNotEqualsFilter(filterType, object)); stepList.add(createBetweenFilter(9,11)); break;
        //	        }
        //	        return new LogicalWorkflow(stepList);
        //REVIEW cuando haya LIMIT de meta
        throw new RuntimeException("Not yet generic supported.");
    }

    private LogicalWorkflow createLogicalPlan(int sortAge) {

        List<LogicalStep> stepList = new ArrayList<>();

        List<ColumnName> columns = new ArrayList<>();

        columns.add(new ColumnName(CATALOG, TABLE, COLUMN_TEXT)); //REVIEW cambiado para que compile
        columns.add(new ColumnName(CATALOG, TABLE, COLUMN_AGE));
        TableName tableName = new TableName(CATALOG, TABLE);
        Project project = new Project(null, tableName, columns);
        stepList.add(project);

        switch (sortAge) {
        case SORT_AGE: //stepList.add(new Sort(COLUMN_AGE, Sort.DESC)); break;
            //REVIEW cuando haya SORT de meta
            throw new RuntimeException("Not yet generic supported.\n");

            // 2 Sort? o uno con lista de parámetros y luego lista de tipo ASC o DESC
            //        	case SORT_AGE_MONEY: stepList.add(createNotEqualsFilter(filterType, object)); stepList.add(createBetweenFilter(9,11)); break;
            //        	case SORT_AGE_TEXT: stepList.add(createNotEqualsFilter(filterType, object)); stepList.add(createBetweenFilter(9,11)); break;
        }
        return new LogicalWorkflow(stepList);

    }

    private LogicalWorkflow createLogicalPlanMultifield() {

        List<LogicalStep> stepList = new ArrayList<>();

        List<ColumnName> columns = new ArrayList<>();

        columns.add(new ColumnName(CATALOG, TABLE, COLUMN_TEXT));
        columns.add(new ColumnName(CATALOG, TABLE, COLUMN_AGE));

        TableName tableName = new TableName(CATALOG, TABLE);
        Project project = new Project(null, tableName, columns); //REVIEW cambiado para que compile

        stepList.add(project);
        LogicalStep gropuBy;

        //stepList.add(new Sort(COLUMN_MONEY, Sort.ASC));
        //stepList.add(new Sort(COLUMN_AGE, Sort.ASC));

        //Limit limit = new Limit(10);
        //stepList.add(limit);
        //return new LogicalWorkflow(stepList);
        //REVIEW cuando haya SORT de meta
        throw new RuntimeException("Not yet generic supported.");

    }

    private void insertRow(int ikey, String texto, int money, int age, ClusterName clusterName)
            throws UnsupportedOperationException, ExecutionException, UnsupportedException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_TEXT, new Cell(texto + ikey));
        cells.put(COLUMN_AGE, new Cell(age));
        cells.put(COLUMN_MONEY, new Cell(money));
        row.setCells(cells);
        connector.getStorageEngine().insert(clusterName,
                new TableMetadata(new TableName(CATALOG, TABLE), null, null, null, null, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST), row);

    }

}