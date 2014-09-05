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
package com.stratio.connector.elasticsearch.ftest.functionalMetadata;

import com.stratio.connector.elasticsearch.ftest.GenericConnectorTest;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public abstract class GenericDropTest extends GenericConnectorTest {

	@Test
	public void dropCollectionTest() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
		Row row = new Row();
		Map<String, Cell> cells = new HashMap<>();
		cells.put("name1", new Cell("value1"));
		cells.put("name2", new Cell(2));
		row.setCells(cells);

        TableMetadata targetTable = new TableMetadata(new TableName(SCHEMA,TABLE),null,null,null,null,null,null);
		 connector.getStorageEngine().insert(clusterName, new TableMetadata(new TableName(SCHEMA, TABLE), null, null, null, null, null,null), row);
		 connector.getStorageEngine().insert(clusterName, new TableMetadata(new TableName(SCHEMA + "b", TABLE), null, null, null, null, null,null), row);
		 connector.getStorageEngine().insert(clusterName, new TableMetadata(new TableName(SCHEMA, "Collect"), null, null, null, null, null,null), row);

//
//        refresh(SCHEMA);
//        refresh(SCHEMA+"b");
//
//        connector.getMedatadaProvider()).dropTable(SCHEMA, TABLE);
//
//
//		assertEquals("Collection deleted", false, nodeClient.admin().indices().typesExists(new TypesExistsRequest(new String[]{CATALOG}, COLLECTION)).actionGet().isExists() );
//
//		assertEquals( true, nodeClient.admin().indices().typesExists(new TypesExistsRequest(new String[]{CATALOG+"b"}, COLLECTION)).actionGet().isExists() );
//
//		nodeClient.admin().indices().delete(new DeleteIndexRequest(CATALOG+"b")).actionGet();

        fail("not yet implemented");
	}
	
	@Test
	public void dropCatalogTest() throws UnsupportedException, ExecutionException {

		Row row = new Row();
		Map<String, Cell> cells = new HashMap<>();
		cells.put("name1", new Cell("value1"));
		cells.put("name2", new Cell(2));
		row.setCells(cells);
        ClusterName clusterName = getClusterName();

		 connector.getStorageEngine().insert(clusterName, new TableMetadata(new TableName(SCHEMA, "Collect"), null, null, null, null, null,null), row);

        refresh(SCHEMA);



        fail("not yet implemented");
//        assertEquals("Catalog deleted", true, nodeClient.admin().indices().exists(new IndicesExistsRequest(CATALOG)).actionGet().isExists());
//		((IMetadataProvider) stratioElasticConnector.getMedatadaProvider()).dropCatalog(CATALOG);
//
//        refresh(CATALOG);
//
//        assertEquals("Catalog deleted", false, nodeClient.admin().indices().exists(new IndicesExistsRequest(CATALOG)).actionGet().isExists());

	}
	
	
}