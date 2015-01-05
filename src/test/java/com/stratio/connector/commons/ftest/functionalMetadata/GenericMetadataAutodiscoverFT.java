/*
 * 
 */

package com.stratio.connector.commons.ftest.functionalMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.awt.Window.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.commons.metadata.CatalogMetadataBuilder;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;

public abstract class GenericMetadataAutodiscoverFT extends GenericConnectorTest {


	protected abstract TableMetadata prepareEnvironment() throws ConnectorException;
	

	protected static final String COLUMN_1 = "id";
	protected static final String COLUMN_2 = "name";
	protected static final String INDEX_NAME = "index1";

	protected String sCatalogName = CATALOG;
	protected String sTableName = TABLE;
	protected String sClusterName = getClusterName().getName();
	
	private TableMetadata tableMetadata;

	/**
	 * The Log.
	 */
	final Logger logger = LoggerFactory.getLogger(this.getClass());

	
	@Before
	public void setUp() throws ConnectorException {
		
		super.setUp();
		tableMetadata = prepareEnvironment();
		
	}


	/**
	 * Tests: provideTableMetadata
	 * 
	 * @throws UnsupportedException
	 * @throws ConnectorException
	 */

	@Test
	public void provideTableMetadataFT() throws UnsupportedException, ConnectorException {

		//Test performance
		TableMetadata tableMetadataProvided = getConnector().getMetadataEngine().provideTableMetadata(getClusterName(), tableMetadata.getName());

		//Results verification
		assertTrue(tableMetadataProvided.getName().getName().equals(tableMetadata.getName().getName()));
		assertTrue(tableMetadataProvided.getClusterRef().getName().equals(sClusterName));
		assertTrue(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));
		Map<IndexName, IndexMetadata> indexes = tableMetadataProvided.getIndexes();
		
		IndexType typeProvided = resolveIndexType(indexes);
		IndexType typeExpected = resolveIndexType(tableMetadata.getIndexes());
		assertEquals(typeExpected, typeProvided);
		
		String columnNameProvided_1 = tableMetadataProvided.getColumns().get(new ColumnName(tableMetadata.getName(), COLUMN_1)).getName().getName();
		assertEquals(COLUMN_1, columnNameProvided_1);
		
		String columnNameProvided_2 = tableMetadataProvided.getColumns().get(new ColumnName(tableMetadata.getName(), COLUMN_2)).getName().getName();
		assertEquals(COLUMN_2, columnNameProvided_2);
	}


	private IndexType resolveIndexType(Map<IndexName, IndexMetadata> indexList) {
		IndexType typeProvided =null;
		for (Entry<IndexName, IndexMetadata> indexName :indexList.entrySet()){
			if (indexName.getKey().getName().equals(INDEX_NAME)){
				typeProvided = indexName.getValue().getType();
			}
		}
		return typeProvided;
	}
}
