/*
 * 
 */

package com.stratio.connector.commons.ftest.functionalMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.CatalogMetadataBuilder;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;

public abstract class GenericMetadataAutodiscoverFT extends GenericConnectorTest {
	private static final String COLUMN_1 = "id";
	private static final String COLUMN_2 = "name";
	static final String INDEX_NAME = "index1";


	/**
	 * The Log.
	 */
	final Logger logger = LoggerFactory.getLogger(this.getClass());


	@Test
	public void provideTableMetadataTest() throws UnsupportedException, ConnectorException {

		//Data preparation
		String catalogName = CATALOG;
		String tableName = TABLE;
		String clusterName = getClusterName().getName();


		TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(catalogName,
				tableName, clusterName);
		tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.INT).addColumn(COLUMN_2, ColumnType.TEXT);
		tableMetadataBuilder.withPartitionKey(COLUMN_1);
		TableMetadata tableMetadata = tableMetadataBuilder.build();	

		//Checking features for each database
		if(getConnectorHelper().isCatalogMandatory()){
			//TODO 
			//CatalogMetadataBuilder catalogMetadataBuilder = new CatalogMetadataBuilder(catalogName);
		}
		if (getConnectorHelper().isTableMandatory()){
			//TODO 
			getConnector().getMetadataEngine().createTable(getClusterName(), tableMetadata);

		} 
		if (getConnectorHelper().isInsertMandatory()){
			Row row1 = new Row();
			Row row2 = new Row();

			//CREAR DISTINTAS ROWS E INSERTARLAS
			boolean ifNotExists = true;
			getConnector().getStorageEngine().insert(getClusterName(), tableMetadata, row1, ifNotExists);
			getConnector().getStorageEngine().insert(getClusterName(), tableMetadata, row2, ifNotExists);
		}


		IndexName name = new IndexName(tableMetadata.getName(), INDEX_NAME);
		Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
		Object[] parameters2 = null;
		ColumnName columnName1 = new ColumnName(tableMetadata.getName(), COLUMN_1);
		columns.put(columnName1, new ColumnMetadata(columnName1, parameters2, ColumnType.VARCHAR));
		ColumnName columnName2 = new ColumnName(tableMetadata.getName(), COLUMN_2);
		columns.put(columnName2, new ColumnMetadata(columnName2, parameters2, ColumnType.TEXT));
		IndexMetadata indexMetadata = new IndexMetadata(name, columns,
				IndexType.DEFAULT, Collections.EMPTY_MAP);
		getConnector().getMetadataEngine().createIndex(getClusterName(), indexMetadata);

		//Test performance
		TableMetadata tableMetadataProvided = getConnector().getMetadataEngine().provideTableMetadata(getClusterName(), tableMetadata.getName());

		//Results verification
		assertTrue(tableMetadataProvided.getName().getName().equals(tableName));
		assertTrue(tableMetadataProvided.getClusterRef().getName().equals(clusterName));
        assertTrue(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));
		assertEquals(tableMetadataProvided.getIndexes().get(name).getType(), IndexType.DEFAULT);
		assertEquals(tableMetadataProvided.getColumns().get(columnName1).getName(), columnName1);
		assertEquals(tableMetadataProvided.getColumns().get(columnName2).getName(), columnName2);

	}
}
