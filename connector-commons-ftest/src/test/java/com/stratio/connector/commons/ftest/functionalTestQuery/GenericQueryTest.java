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

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;



/**
 * Created by jmgomez on 17/07/14.
 */
public abstract class GenericQueryTest extends GenericConnectorTest {

    public static final String COLUMN_1 = "bin1";
    public static final String COLUMN_2 = "bin2";
    public static final String COLUMN_3 = "bin3";

    private Integer DEFAULT_GET_TO_SEARCH = 100;


    protected Integer getRowsToSearch( ){
        return DEFAULT_GET_TO_SEARCH;
    }

   @Test
    public void selectAllFromTable() throws UnsupportedException, ExecutionException {

       ClusterName clusterNodeName = getClusterName();
       System.out.println("*********************************** INIT FUNCTIONAL TEST selectAllFromTable ***********************************");

        for (int i=0;i<getRowsToSearch();i++){
            insertRow(i, clusterNodeName);
        }

       insertRecordNotReturnedInSearch(clusterNodeName);


        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlan();
        QueryResult queryResult = (QueryResult)  connector.getQueryEngine().execute(clusterNodeName, logicalPlan);
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",3*getRowsToSearch(),proveSet.size());
       for (int i=0;i<getRowsToSearch();i++) {
           assertTrue("Return correct record", proveSet.contains("bin1ValueBin1_r"+i));
           assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r"+i));
           assertTrue("Return correct record", proveSet.contains("bin3ValueBin3_r"+i));
       }


    }

    private void insertRecordNotReturnedInSearch(ClusterName clusterNodeName) throws ExecutionException, UnsupportedException {
        insertRow(1,"type2", clusterNodeName);
        insertRow(2,"type2", clusterNodeName);
        insertRow(1,"otherTable", clusterNodeName);
        insertRow(2,"otherTable", clusterNodeName);
    }



    
    private LogicalWorkflow createLogicalPlan() {
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnName> columns = new ArrayList<>();

        columns.add(new ColumnName(CATALOG,TABLE,COLUMN_1));
        columns.add(new ColumnName(CATALOG,TABLE,COLUMN_2));
        columns.add(new ColumnName(CATALOG,TABLE,COLUMN_3));
        TableName tableName = new TableName(CATALOG,TABLE);
        Project project = new Project(null,tableName,columns);
        stepList.add(project);
        return new LogicalWorkflow(stepList);
    }

    private void insertRow(int ikey, ClusterName clusterNodeName) throws UnsupportedOperationException, ExecutionException, UnsupportedException {
     	insertRow(ikey, TABLE, clusterNodeName);
    }
    

private void insertRow(int ikey, String Table, ClusterName clusterNodeName) throws UnsupportedOperationException, ExecutionException, UnsupportedException {
     	
    	Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("ValueBin1_r"+ikey));
        cells.put(COLUMN_2, new Cell("ValueBin2_r"+ikey));
        cells.put(COLUMN_3, new Cell("ValueBin3_r"+ikey));
        row.setCells(cells);        
         connector.getStorageEngine().insert(clusterNodeName,new TableMetadata(new TableName(CATALOG, Table),null,null,null,null,Collections.EMPTY_LIST,Collections.EMPTY_LIST), row);
        
    }

}
