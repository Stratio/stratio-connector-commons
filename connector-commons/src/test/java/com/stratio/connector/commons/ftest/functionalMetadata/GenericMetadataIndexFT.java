/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package com.stratio.connector.commons.ftest.functionalMetadata;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.*;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public abstract class GenericMetadataIndexFT extends GenericConnectorTest {

    static final String INDEX_NAME = "index1";
    static final String INDEX_NAME_2 = "index2";
    static final String COLUMN_INDEX_NAME = "columnname_2";
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
    public void createDefaultIndexTest() throws ConnectorException {

        // TODO create the catalog and the table if needed

        TableName tableName = new TableName(CATALOG, TABLE);

        // Creating the indexMetadata with the previous columns
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = null;
        ColumnName colName = new ColumnName(tableName, "columnName_1");
        columns.put(colName, new ColumnMetadata(colName, parameters, new ColumnType(DataType.TEXT)));
        IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, INDEX_NAME), columns,
                IndexType.DEFAULT, Collections.EMPTY_MAP);

        // Creating other indexMetadata with columnName insteadOf indexName
        Map<ColumnName, ColumnMetadata> columns2 = new HashMap<>();
        Object[] parameters2 = null;
        ColumnName colName2 = new ColumnName(tableName, COLUMN_INDEX_NAME);
        columns2.put(colName2, new ColumnMetadata(colName2, parameters2, new ColumnType(DataType.TEXT)));
        IndexMetadata indexMetadata2 = new IndexMetadata(new IndexName(tableName, COLUMN_INDEX_NAME), columns2,
                IndexType.DEFAULT, Collections.EMPTY_MAP);

        // Creating index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata2);

        assertTrue("The index " + INDEX_NAME + " has not been found",
                iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));
        assertTrue("The index " + COLUMN_INDEX_NAME + " has not been found",
                iConnectorHelper.containsIndex(CATALOG, TABLE, COLUMN_INDEX_NAME));

    }

    /**
     * Testing the fulltext index
     *
     * @throws UnsupportedException
     * @throws ExecutionException
     */
    @Test
    public void createTextIndexTest() throws ConnectorException {

        // TODO create the catalog and the table if needed
        // TODO the connectors must check the columnType (Varchar, fulltext?)

        TableName tableName = new TableName(CATALOG, TABLE);

        // Creating the indexMetadata with 1 column
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = null;
        ColumnName colName = new ColumnName(tableName, "columnName_1");
        columns.put(colName, new ColumnMetadata(colName, parameters, new ColumnType(DataType.TEXT)));
        IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, INDEX_NAME), columns,
                IndexType.FULL_TEXT, Collections.EMPTY_MAP);

        // Creating other indexMetadata with 2 columns
        Map<ColumnName, ColumnMetadata> columns2 = new HashMap<>();
        Object[] parameters2 = null;

        columns2.put(new ColumnName(tableName, "columnName_2"), new ColumnMetadata(new ColumnName(tableName,
                "columnName_2"), parameters2, new ColumnType(DataType.VARCHAR)));
        columns2.put(new ColumnName(tableName, "columnName_3"), new ColumnMetadata(new ColumnName(tableName,
                "columnName_3"), parameters2, new ColumnType(DataType.TEXT)));
        IndexMetadata indexMetadata2 = new IndexMetadata(new IndexName(tableName, INDEX_NAME_2), columns2,
                IndexType.FULL_TEXT, Collections.EMPTY_MAP);

        // Creating index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        assertTrue("The index " + INDEX_NAME + " has not been found",
                iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));

        // The text index must be applied only over a single column.
        try {
            connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata2);
            failDoubleTextIndex();
        } catch (Exception e) {
        }

        assertFalse("The index text must be applied only onver a single column",
                iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME_2));

    }

    private void failDoubleTextIndex() {
        Assert.fail("The index text must be applied only t a single column");
    }

    /**
     * Create a default index with several columns.
     *
     * @throws UnsupportedException
     * @throws ExecutionException
     */
    @Test
    public void createMultiIndexTest() throws ConnectorException {

        // TODO create the catalog and the table if needed

        TableName tableName = new TableName(CATALOG, TABLE);

        // Creating other indexMetadata with 2 columns
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters2 = null;
        columns.put(new ColumnName(tableName, "columnName_2"), new ColumnMetadata(new ColumnName(tableName,
                "columnName_2"), parameters2, new ColumnType(DataType.VARCHAR)));
        columns.put(new ColumnName(tableName, "columnName_2"), new ColumnMetadata(new ColumnName(tableName,
                "columnName_3"), parameters2, new ColumnType(DataType.TEXT)));
        IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, INDEX_NAME), columns,
                IndexType.DEFAULT, Collections.EMPTY_MAP);

        // Creating the index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        assertTrue("The index " + INDEX_NAME + " has not been found",
                iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));

    }

    /**
     * Create a default index with several columns.
     *
     * @throws UnsupportedException
     * @throws ExecutionException
     */
    @Test
    public void createCustomIndexTest() throws ConnectorException {

        // TODO create the catalog and the table if needed

        TableName tableName = new TableName(CATALOG, TABLE);

        // Creating a indexMetadata with 1 columns
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters2 = null;
        columns.put(new ColumnName(tableName, "columnName_2"), new ColumnMetadata(new ColumnName(tableName,
                "columnName_2"), parameters2, new ColumnType(DataType.VARCHAR)));
        columns.put(new ColumnName(tableName, "columnName_3"), new ColumnMetadata(new ColumnName(tableName,
                "columnName_3"), parameters2, new ColumnType(DataType.TEXT)));

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

        StringSelector optSelector3 = new StringSelector("unique");
        optSelector3.setAlias("unique");
        BooleanSelector optValue3 = new BooleanSelector(true);
        options.put(optSelector3, optValue3);

        IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, INDEX_NAME), columns,
                IndexType.CUSTOM, options);

        // Creating the index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        assertTrue("The index " + INDEX_NAME + " has not been found",
                iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));

    }

    @Test
    public void createDuplicatedIndexTest() throws ConnectorException { // TODO create the catalog
        // and the table if needed

        TableName tableName = new TableName(CATALOG, TABLE);

        // Creating other indexMetadata with 2 columns
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters2 = null;
        columns.put(new ColumnName(tableName, "columnName_2"), new ColumnMetadata(new ColumnName(tableName,
                "columnName_2"), parameters2, new ColumnType(DataType.VARCHAR)));
        columns.put(new ColumnName(tableName, "columnName_3"), new ColumnMetadata(new ColumnName(tableName,
                "columnName_3"), parameters2, new ColumnType(DataType.TEXT)));
        IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, INDEX_NAME), columns,
                IndexType.DEFAULT, Collections.EMPTY_MAP);

        // Creating the index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        assertTrue("The index " + INDEX_NAME + " has not been found",
                iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));

        int previousIndexCount = iConnectorHelper.countIndexes(CATALOG, TABLE);

        // Creating the same index again
        try {
            connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        } catch (Exception e) {
        }

        assertTrue("The index " + INDEX_NAME + " has not been found",
                iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));

        int indexCount = iConnectorHelper.countIndexes(CATALOG, TABLE);
        assertEquals("Duplicate indexes should not be inserted", previousIndexCount, indexCount);
    }

    @Test
    public void dropIndexTest() throws ConnectorException {

        TableName tableName = new TableName(CATALOG, TABLE);

        // Creating the indexMetadata with 1 column
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = null;
        columns.put(new ColumnName(tableName, "columnName_1"), new ColumnMetadata(new ColumnName(tableName,
                "columnName_1"), parameters, new ColumnType(DataType.TEXT)));
        IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, INDEX_NAME), columns,
                IndexType.DEFAULT, Collections.EMPTY_MAP);

        // Creating the index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        assertTrue("The index " + INDEX_NAME + " has not been found",
                iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));

        // Creating other indexMetadata
        Map<ColumnName, ColumnMetadata> columns2 = new HashMap<>();
        Object[] parameters2 = null;
        columns2.put(new ColumnName(tableName, "columnName_2"), new ColumnMetadata(new ColumnName(tableName,
                "columnName_2"), parameters2, new ColumnType(DataType.VARCHAR)));
        IndexMetadata indexMetadata2 = new IndexMetadata(new IndexName(tableName, INDEX_NAME_2), columns2,
                IndexType.FULL_TEXT, Collections.EMPTY_MAP);

        // Creating the index
        connector.getMetadataEngine().createIndex(getClusterName(), indexMetadata2);
        assertTrue("The index " + INDEX_NAME_2 + " has not been found",
                iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME_2));

        connector.getMetadataEngine().dropIndex(getClusterName(), indexMetadata);
        connector.getMetadataEngine().dropIndex(getClusterName(), indexMetadata2);

        assertFalse("The index " + INDEX_NAME + " has not been dropped",
                iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));
        assertFalse("The index " + INDEX_NAME_2 + " has not been dropped",
                iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME_2));

        // TODO An exception could to be thrown when dropping a index which does not exist
        try {
            connector.getMetadataEngine().dropIndex(getClusterName(), indexMetadata);
            logger.debug("Dropping a not existing index does not cause an exception");
        } catch (Exception e) {
        }

    }

}