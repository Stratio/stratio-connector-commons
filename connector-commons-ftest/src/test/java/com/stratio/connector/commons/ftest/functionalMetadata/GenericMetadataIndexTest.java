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
package com.stratio.connector.commons.ftest.functionalMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.IndexName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.IndexType;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

public abstract class GenericMetadataIndexTest extends GenericConnectorTest {

    static final String INDEX_NAME = "index1";
    static final String INDEX_NAME_2 = "index2";
    static final String COLUMN_INDEX_NAME = "columnName_2";
    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Create a default index key with a single column.
     *
     * @throws UnsupportedException
     * @throws ExecutionException
     */
    @Test
    public void createDefaultIndexTest() throws UnsupportedException, ExecutionException {

        // TODO create the catalog and the table if needed
        System.out.println("*********************************** INIT FUNCTIONAL TEST createDefaultIndexTest ***********************************");
        TableName tableName = new TableName(CATALOG, TABLE);

        // Creating the indexMetadata with the previous columns
        List<ColumnMetadata> columns = new ArrayList<>();
        Object[] parameters = null;
        columns.add(new ColumnMetadata(new ColumnName(tableName, "columnName_1"), parameters, ColumnType.TEXT));
        IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, INDEX_NAME), columns,
                        IndexType.DEFAULT, Collections.EMPTY_MAP);

        // Creating other indexMetadata with columnName insteadOf indexName
        List<ColumnMetadata> columns2 = new ArrayList<>();
        Object[] parameters2 = null;
        columns2.add(new ColumnMetadata(new ColumnName(tableName, COLUMN_INDEX_NAME), parameters2, ColumnType.TEXT));
        IndexMetadata indexMetadata2 = new IndexMetadata(new IndexName(tableName, COLUMN_INDEX_NAME), columns2,
                        IndexType.DEFAULT, Collections.EMPTY_MAP);

        // Creating index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata2);

        assertTrue(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));
        assertTrue(iConnectorHelper.containsIndex(CATALOG, TABLE, COLUMN_INDEX_NAME));

    }

    /**
     * Testing the fulltext index
     *
     * @throws UnsupportedException
     * @throws ExecutionException
     */
    @Test
    public void createTextIndexTest() throws UnsupportedException, ExecutionException {

        // TODO create the catalog and the table if needed
        // TODO the connectors must check the columnType (Varchar, fulltext?)

        System.out.println("*********************************** INIT FUNCTIONAL TEST createTextIndexTest ***********************************");
        TableName tableName = new TableName(CATALOG, TABLE);

        // Creating the indexMetadata with 1 column
        List<ColumnMetadata> columns = new ArrayList<>();
        Object[] parameters = null;
        columns.add(new ColumnMetadata(new ColumnName(tableName, "columnName_1"), parameters, ColumnType.TEXT));
        IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, INDEX_NAME), columns,
                        IndexType.FULL_TEXT, Collections.EMPTY_MAP);

        // Creating other indexMetadata with 2 columns
        List<ColumnMetadata> columns2 = new ArrayList<>();
        Object[] parameters2 = null;
        columns2.add(new ColumnMetadata(new ColumnName(tableName, "columnName_2"), parameters2, ColumnType.VARCHAR));
        columns2.add(new ColumnMetadata(new ColumnName(tableName, "columnName_3"), parameters2, ColumnType.TEXT));
        IndexMetadata indexMetadata2 = new IndexMetadata(new IndexName(tableName, INDEX_NAME_2), columns2,
                        IndexType.FULL_TEXT, Collections.EMPTY_MAP);

        // Creating index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        assertTrue(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));

        // The text index must be applied only over a single column.
        try {
            connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata2);
            // failDoubleTextIndex();
        } catch (Exception e) {
        }

        assertFalse("The index text must be applied only onver a single column",
                        iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME_2));

    }

    private void failDoubleTextIndex() {
        assertFalse("The index text must be applied only onver a single column", true);
    }

    /**
     * Create a default index with several columns.
     *
     * @throws UnsupportedException
     * @throws ExecutionException
     */
    @Test
    public void createMultiIndexTest() throws UnsupportedException, ExecutionException {

        // TODO create the catalog and the table if needed

        System.out.println("*********************************** INIT FUNCTIONAL TEST createCompoundIndexTest ***********************************");
        TableName tableName = new TableName(CATALOG, TABLE);

        // Creating other indexMetadata with 2 columns
        List<ColumnMetadata> columns2 = new ArrayList<>();
        Object[] parameters2 = null;
        columns2.add(new ColumnMetadata(new ColumnName(tableName, "columnName_2"), parameters2, ColumnType.VARCHAR));
        columns2.add(new ColumnMetadata(new ColumnName(tableName, "columnName_3"), parameters2, ColumnType.TEXT));
        IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, INDEX_NAME), columns2,
                        IndexType.DEFAULT, Collections.EMPTY_MAP);

        // Creating the index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        assertTrue(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));

    }

    /**
     * Create a default index with several columns.
     *
     * @throws UnsupportedException
     * @throws ExecutionException
     */
    @Test
    public void createCustomIndexTest() throws UnsupportedException, ExecutionException {

        // TODO create the catalog and the table if needed

        System.out.println("*********************************** INIT FUNCTIONAL TEST createCompoundIndexTest ***********************************");
        TableName tableName = new TableName(CATALOG, TABLE);

        // Creating a indexMetadata with 1 columns
        List<ColumnMetadata> columns2 = new ArrayList<>();
        Object[] parameters2 = null;
        columns2.add(new ColumnMetadata(new ColumnName(tableName, "columnName_2"), parameters2, ColumnType.VARCHAR));
        columns2.add(new ColumnMetadata(new ColumnName(tableName, "columnName_3"), parameters2, ColumnType.TEXT));

        // Options
        Map<Selector, Selector> options = new HashMap<Selector, Selector>();
        StringSelector optSelector = new StringSelector("index_type");
        optSelector.setAlias("index_type");
        StringSelector optValue = new StringSelector("compound");
        options.put(optSelector, optValue);

        StringSelector optSelector2 = new StringSelector("compound_fields");
        optSelector2.setAlias("compound_fields");
        StringSelector optValue2 = new StringSelector("field1:asc, field2:desc");
        options.put(optSelector2, optValue2);

        IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, INDEX_NAME), columns2,
                        IndexType.CUSTOM, options);

        // Creating the index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        assertTrue(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));

    }

    @Test
    public void createDuplicatedIndexTest() throws UnsupportedException, ExecutionException { // TODO create the catalog
                                                                                              // and the table if needed

        System.out.println("*********************************** INIT FUNCTIONAL TEST createDuplicatedIndexTest ***********************************");
        TableName tableName = new TableName(CATALOG, TABLE);

        // Creating other indexMetadata with 2 columns
        List<ColumnMetadata> columns2 = new ArrayList<>();
        Object[] parameters2 = null;
        columns2.add(new ColumnMetadata(new ColumnName(tableName, "columnName_2"), parameters2, ColumnType.VARCHAR));
        columns2.add(new ColumnMetadata(new ColumnName(tableName, "columnName_3"), parameters2, ColumnType.TEXT));
        IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, INDEX_NAME), columns2,
                        IndexType.DEFAULT, Collections.EMPTY_MAP);

        // Creating the index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        assertTrue(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));

        int previousIndexCount = iConnectorHelper.countIndexes(CATALOG, TABLE);

        // Creating the same index again
        try {
            connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        } catch (Exception e) {
        }

        assertTrue(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));

        int indexCount = iConnectorHelper.countIndexes(CATALOG, TABLE);
        assertEquals(previousIndexCount, indexCount);
    }

    @Test
    public void dropIndexTest() throws UnsupportedException, ExecutionException {
        System.out.println("*********************************** INIT FUNCTIONAL TEST dropIndexTest ***********************************");
        TableName tableName = new TableName(CATALOG, TABLE);

        // Creating the indexMetadata with 1 column
        List<ColumnMetadata> columns = new ArrayList<>();
        Object[] parameters = null;
        columns.add(new ColumnMetadata(new ColumnName(tableName, "columnName_1"), parameters, ColumnType.TEXT));
        IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, INDEX_NAME), columns,
                        IndexType.DEFAULT, Collections.EMPTY_MAP);

        // Creating the index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        assertTrue(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));

        // Creating other indexMetadata
        List<ColumnMetadata> columns2 = new ArrayList<>();
        Object[] parameters2 = null;
        columns2.add(new ColumnMetadata(new ColumnName(tableName, "columnName_2"), parameters2, ColumnType.VARCHAR));
        IndexMetadata indexMetadata2 = new IndexMetadata(new IndexName(tableName, INDEX_NAME_2), columns2,
                        IndexType.FULL_TEXT, Collections.EMPTY_MAP);

        // Creating the index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata2);
        assertTrue(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME_2));

        connector.getMetadataEngine().dropIndex(getClusterName(), indexMetadata);
        connector.getMetadataEngine().dropIndex(getClusterName(), indexMetadata2);

        assertFalse(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));
        assertFalse(iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME_2));

        // TODO An exception could to be thrown when dropping a index which does not exist
        try {
            connector.getMetadataEngine().dropIndex(getClusterName(), indexMetadata);
            logger.debug("Dropping a not existing index does not cause an exception");
        } catch (Exception e) {
        }
        ;

    }

}