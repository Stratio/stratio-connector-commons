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
package com.stratio.connector.elasticsearch.ftest.functionalMetadata;

import com.stratio.connector.elasticsearch.ftest.GenericConnectorTest;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.junit.After;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public abstract class GenericMetadataDropTest extends GenericConnectorTest {


    public  final String OTHER_TABLE = "other" + TABLE;
    private static String COLUMN_1 = "name1";
    private static String COLUMN_2 = "name2";
    private  String OTHER_CATALOG = "other_"+CATALOG ;




	@Test
	public void dropTableTest() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
		Row row = new Row();
		Map<String, Cell> cells = new HashMap<>();
		cells.put(COLUMN_1, new Cell("value1"));
		cells.put(COLUMN_2, new Cell(2));
		row.setCells(cells);


		 connector.getStorageEngine().insert(clusterName, new TableMetadata(new TableName(CATALOG, TABLE), null, null, null, null, Collections.EMPTY_LIST,Collections.EMPTY_LIST), row);
		 connector.getStorageEngine().insert(clusterName, new TableMetadata(new TableName(OTHER_CATALOG, TABLE), null, null, null, null, Collections.EMPTY_LIST,Collections.EMPTY_LIST), row);
		 connector.getStorageEngine().insert(clusterName, new TableMetadata(new TableName(CATALOG, OTHER_TABLE), null, null, null, null, Collections.EMPTY_LIST,Collections.EMPTY_LIST), row);


        refresh(CATALOG);
        refresh(OTHER_CATALOG);

        connector.getMetadataEngine().dropTable(clusterName, (new TableName(CATALOG, TABLE)));


        QueryResult queryResult = connector.getQueryEngine().execute(clusterName,createLogicalWorkFlow(CATALOG, TABLE));
		assertEquals("Table ["+CATALOG+"."+TABLE+"] deleted", 0, queryResult.getResultSet().size());

        queryResult = connector.getQueryEngine().execute(clusterName,createLogicalWorkFlow(OTHER_CATALOG, TABLE));
        assertNotEquals("Table ["+OTHER_CATALOG+"."+TABLE+"] exist", 0, queryResult.getResultSet().size());

        queryResult = connector.getQueryEngine().execute(clusterName,createLogicalWorkFlow(CATALOG, OTHER_TABLE));
        assertNotEquals("Table ["+CATALOG+"."+OTHER_TABLE+" exist", 0, queryResult.getResultSet().size());






	}
	
	@Test
	public void dropCatalogTest() throws UnsupportedException, ExecutionException {

        ClusterName clusterName = getClusterName();
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("value1"));
        cells.put(COLUMN_2, new Cell(2));
        row.setCells(cells);


        connector.getStorageEngine().insert(clusterName, new TableMetadata(new TableName(CATALOG, TABLE), null, null, null, null, Collections.EMPTY_LIST,Collections.EMPTY_LIST), row);
        connector.getStorageEngine().insert(clusterName, new TableMetadata(new TableName(OTHER_CATALOG, TABLE), null, null, null, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST), row);
        connector.getStorageEngine().insert(clusterName, new TableMetadata(new TableName(CATALOG, OTHER_TABLE), null, null, null, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST), row);


        refresh(CATALOG);
        refresh(OTHER_CATALOG);

        connector.getMetadataEngine().dropCatalog(clusterName, new CatalogName(CATALOG));


        QueryResult queryResult = connector.getQueryEngine().execute(clusterName,createLogicalWorkFlow(CATALOG, TABLE));
        assertEquals("Table ["+CATALOG+"."+TABLE+"] deleted", 0, queryResult.getResultSet().size());

        queryResult = connector.getQueryEngine().execute(clusterName,createLogicalWorkFlow(OTHER_CATALOG, TABLE));
        assertNotEquals("Table ["+OTHER_CATALOG+"."+TABLE+"] exist", 0, queryResult.getResultSet().size());

        queryResult = connector.getQueryEngine().execute(clusterName,createLogicalWorkFlow(CATALOG, OTHER_TABLE));
        assertEquals("Table [" + CATALOG + "." + OTHER_TABLE + " deleted", 0, queryResult.getResultSet().size());

        deleteCatalog(OTHER_CATALOG);

	}


    private LogicalWorkflow createLogicalWorkFlow(String catalog, String table) {
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnName> columns = new ArrayList<>();

        columns.add(new ColumnName(CATALOG, TABLE,COLUMN_1));
        columns.add(new ColumnName(CATALOG, TABLE,COLUMN_2));
        TableName tableName = new TableName(catalog, table);
        Project project = new Project(null,tableName,columns);
        stepList.add(project);
        return new LogicalWorkflow(stepList);
    }

    @After
    public void tearDown() throws ConnectionException{
        super.tearDown();
        deleteCatalog(OTHER_CATALOG);

        if (logger.isDebugEnabled()){
            logger.debug("Delete Catalog: "+OTHER_CATALOG);

        }
    }


	
}