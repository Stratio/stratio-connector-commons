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

package com.stratio.connector.elasticsearch.ftest.functionalTestQuery;

import com.stratio.connector.elasticsearch.ftest.GenericConnectorTest;
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


public abstract class GenericLimitTest extends GenericConnectorTest {

	public static final String COLUMN_TEXT = "text";
	public static final String COLUMN_AGE = "age";
	public static final String COLUMN_MONEY = "money";

	@Test
	public void limitTest() throws Exception {

        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST limitTest "+ clusterName.getName()+" ***********************************");

		insertRow(1, "text", 10, 20,clusterName);// row,text,money,age
		insertRow(2, "text", 9, 17,clusterName);
		insertRow(3, "text", 11, 26,clusterName);
		insertRow(4, "text", 10, 30,clusterName);
		insertRow(5, "text", 20, 42,clusterName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlan(2);

		// limit 2
		QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterName,logicalPlan);

		assertEquals(2, queryResult.getResultSet().size());

	}
	
	private LogicalWorkflow createLogicalPlan(int limit) {

		List<LogicalStep> stepList = new ArrayList<>();

		List<ColumnName> columns = new ArrayList<>();

		columns.add(new ColumnName(CATALOG,TABLE, COLUMN_TEXT)); //REVIEW todo esto se ha cambiado para que compile
		columns.add(new ColumnName(CATALOG,TABLE, COLUMN_AGE));
        TableName tableName = new TableName(CATALOG,TABLE);
		Project project =  new Project(null,tableName, columns);
		stepList.add(project);

		//stepList.add(new Limit(limit));
        //return new LogicalWorkflow(stepList);

        //REVIEW cuando haya LIMIT de meta
        throw new RuntimeException("Esperando a meta");

	}

	private void insertRow(int ikey, String texto, int money, int age, ClusterName cLusterName) throws UnsupportedOperationException, ExecutionException, UnsupportedException {

		Row row = new Row();
	    Map<String, Cell> cells = new HashMap<>();
	    cells.put(COLUMN_TEXT, new Cell(texto+ikey));
	    cells.put(COLUMN_AGE, new Cell(age));
	    cells.put(COLUMN_MONEY, new Cell(money));
	    row.setCells(cells);        
	     connector.getStorageEngine().insert(cLusterName,new TableMetadata(new TableName(CATALOG,TABLE),null,null,null,null,Collections.EMPTY_LIST, Collections.EMPTY_LIST), row);
	        
	    
	}

}