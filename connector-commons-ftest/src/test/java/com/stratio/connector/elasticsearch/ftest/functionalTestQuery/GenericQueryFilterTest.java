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

package com.stratio.connector.elasticsearch.ftest.functionalTestQuery;


import com.stratio.connector.elasticsearch.ftest.GenericConnectorTest;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;


/**
 * Created by jmgomez on 17/07/14.
 */
public abstract class GenericQueryFilterTest extends GenericConnectorTest {

    public static final String COLUMN_1 = "bin1";
    public static final String COLUMN_2 = "bin2";
    public static final String COLUMN_3 = "bin3";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";

    private static final int EQUAL_FILTER =1;
    private static final int BETWEEN_FILTER =2;
    private static final int HIGH_FILTER =3;
    private static final int LOW_FILTER =4;
    private static final int HIGH_BETWEEN_FILTER =5;



    @Test
    public void selectFilterEqual() throws ExecutionException, UnsupportedException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectFilterEqual ***********************************");

        insertRow(1,10,1, clusterNodeName);
        insertRow(2,9,1, clusterNodeName);
        insertRow(3,11,1, clusterNodeName);
        insertRow(4,10,1, clusterNodeName);
        insertRow(5,20,1, clusterNodeName);

        refresh(CATALOG);


        LogicalWorkflow logicalPlan = createLogicalPlan(EQUAL_FILTER);
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName,logicalPlan);
       
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            }
        }
        

        assertEquals("The record number is correct",6,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("age10"));
        assertTrue("Return correct record",proveSet.contains("money1"));



    }





    @Test
    public void selectFilterBetween() throws ExecutionException, UnsupportedException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectFilterBetween ***********************************");

    	insertRow(1,1,10, clusterNodeName);
    	insertRow(2,1,9, clusterNodeName);
    	insertRow(3,1,11, clusterNodeName);
    	insertRow(4,1,10, clusterNodeName);
    	insertRow(5,1,20, clusterNodeName);
    	insertRow(6,1,11, clusterNodeName);
    	insertRow(7,1,8, clusterNodeName);
    	insertRow(8,1,12, clusterNodeName);

        refresh(CATALOG);


        LogicalWorkflow logicalPlan = createLogicalPlan(BETWEEN_FILTER);
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);
        
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",14,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r2"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r2"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record",proveSet.contains("money10"));
        assertTrue("Return correct record",proveSet.contains("money9"));
        assertTrue("Return correct record",proveSet.contains("money11"));
        assertTrue("Return correct record",proveSet.contains("age1"));

    }


   

    @Test
    public void selectHighBetween() throws ExecutionException, UnsupportedException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectHighBetween ***********************************");


    	insertRow(1,10, 15, clusterNodeName);
    	insertRow(2,9,10, clusterNodeName);
    	insertRow(3,11,9, clusterNodeName);
    	insertRow(4,10,7, clusterNodeName);
    	insertRow(5,7,9, clusterNodeName);
    	insertRow(6,11,100, clusterNodeName);
    	insertRow(7,8,1, clusterNodeName);
    	insertRow(8,12,10, clusterNodeName);

        refresh(CATALOG);


        LogicalWorkflow logicalPlan = createLogicalPlan(HIGH_BETWEEN_FILTER);
        QueryResult queryResult = (QueryResult)  connector.getQueryEngine().execute(clusterNodeName, logicalPlan);
        
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",8,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r8"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r8"));
        assertTrue("Return correct record",proveSet.contains("age11"));
        assertTrue("Return correct record",proveSet.contains("money9"));
        assertTrue("Return correct record",proveSet.contains("money10"));
        assertTrue("Return correct record",proveSet.contains("age12"));

    }




    @Test
    public void selectFilterHigh() throws UnsupportedException, ExecutionException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectFilterHigh ***********************************");

    	insertRow(1,10,1, clusterNodeName);
    	insertRow(2,9,1, clusterNodeName);
    	insertRow(3,11,1, clusterNodeName);
    	insertRow(4,10,1, clusterNodeName);
    	insertRow(5,20,1, clusterNodeName);
    	insertRow(6,7,1, clusterNodeName);
    	insertRow(7,8,1, clusterNodeName);
    	insertRow(8,12,1, clusterNodeName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlan(HIGH_FILTER);
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);
        
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",15,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r5"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r5"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r8"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r8"));
        assertTrue("Return correct record",proveSet.contains("age10"));
        assertTrue("Return correct record",proveSet.contains("age11"));
        assertTrue("Return correct record",proveSet.contains("age12"));
        assertTrue("Return correct record",proveSet.contains("age20"));
        assertTrue("Return correct record",proveSet.contains("money1"));




    }



    @Test
    public void selectFilterLow() throws UnsupportedException, ExecutionException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectFilterHigh ***********************************");


    	insertRow(1,10,1, clusterNodeName);
    	insertRow(2,9,1, clusterNodeName);
    	insertRow(3,11,1, clusterNodeName);
    	insertRow(4,10,1, clusterNodeName);
    	insertRow(5,20,1, clusterNodeName);
    	insertRow(6,7,1, clusterNodeName);
    	insertRow(7,8,1, clusterNodeName);
    	insertRow(8,12,1, clusterNodeName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlan(LOW_FILTER);
        QueryResult queryResult = (QueryResult)  connector.getQueryEngine().execute(clusterNodeName,logicalPlan);
        
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct", 15, proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r2"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r2"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r7"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r7"));
        assertTrue("Return correct record",proveSet.contains("age10"));
        assertTrue("Return correct record",proveSet.contains("age9"));
        assertTrue("Return correct record",proveSet.contains("age7"));
        assertTrue("Return correct record",proveSet.contains("age8"));
        assertTrue("Return correct record",proveSet.contains("money1"));

    }


    private LogicalWorkflow createLogicalPlan(int filterType) {
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnName> columns = new ArrayList<>();

        columns.add(new ColumnName(CATALOG,TABLE,COLUMN_1));
        columns.add(new ColumnName(CATALOG,TABLE,COLUMN_2));
        columns.add(new ColumnName(CATALOG,TABLE,COLUMN_AGE));
        columns.add(new ColumnName(CATALOG,TABLE,COLUMN_MONEY));
        TableName tableName = new TableName(CATALOG,TABLE);
        Project project = new Project(null,tableName,columns);
        stepList.add(project);
        if (EQUAL_FILTER==filterType || HIGH_FILTER == filterType || LOW_FILTER == filterType || HIGH_BETWEEN_FILTER == filterType){
            stepList.add(createEqualsFilter(filterType));
        }
        if (BETWEEN_FILTER==filterType || HIGH_BETWEEN_FILTER==filterType){
            stepList.add(createBetweenFilter());
        }
        return new LogicalWorkflow(stepList);
    }

    private LogicalStep  createBetweenFilter() {
//        Relation relation =  relation = new Relation(new ColumnSelector(new ColumnName(CATALOG, TABLE, COLUMN_MONEY)),Operator.BETWEEN,
//        relation.setType(Relation.TYPE_BETWEEN);
//        List<Term<?>> terms = new ArrayList<>();
//        terms.add(new IntegerTerm("9"));
//        terms.add(new IntegerTerm("11"));
//        relation.setTerms(terms);
//        Filter f = new Filter(Operations.SELECT_WHERE_BETWEEN, RelationType.BETWEEN, relation);
//        return f;
        fail("Falta operacion Between");
        return null; //REVIEW para que compile por la nueva version de meta
    }

    private Filter createEqualsFilter(int filterType) {
        Relation relation = null;
        Operations operation = null;
        if (filterType==EQUAL_FILTER) {
            relation = new Relation(new ColumnSelector(new ColumnName(CATALOG, TABLE, COLUMN_AGE)), Operator.COMPARE, new IntegerSelector("10"));
            operation= Operations.FILTER_NON_INDEXED_EQ;
        }
        else if (filterType==HIGH_FILTER || filterType == HIGH_BETWEEN_FILTER) {
            relation = new Relation(new ColumnSelector(new ColumnName(CATALOG, TABLE, COLUMN_AGE)), Operator.GET, new IntegerSelector("10"));
            operation= Operations.FILTER_NON_INDEXED_GET;
        }else  if (filterType==LOW_FILTER) {
            relation = new Relation(new ColumnSelector(new ColumnName(CATALOG, TABLE, COLUMN_AGE)), Operator.LET, new IntegerSelector("10"));
            operation= Operations.FILTER_NON_INDEXED_LET;
        }

        return new Filter( operation, relation);

    }

    
        private void insertRow(int ikey, int age, int money, ClusterName clusterNodeName) throws UnsupportedOperationException, ExecutionException, UnsupportedException {
 	
        	Row row = new Row();
            Map<String, Cell> cells = new HashMap<>();
            cells.put(COLUMN_1, new Cell("ValueBin1_r"+ikey));
            cells.put(COLUMN_2, new Cell("ValueBin2_r"+ikey));
            cells.put(COLUMN_3, new Cell("ValueBin3_r"+ikey));
            cells.put(COLUMN_AGE, new Cell(age));
            cells.put(COLUMN_MONEY, new Cell(money));
            row.setCells(cells);        
            connector.getStorageEngine().insert(clusterNodeName,new TableMetadata(new TableName(CATALOG, TABLE),null,null,null,null,Collections.EMPTY_LIST,Collections.EMPTY_LIST), row);
            
        }


}
