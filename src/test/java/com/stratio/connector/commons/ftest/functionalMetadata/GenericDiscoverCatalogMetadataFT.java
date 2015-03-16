/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.commons.ftest.functionalMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.IndexMetadataBuilder;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;

public abstract class GenericDiscoverCatalogMetadataFT extends GenericConnectorTest {

    protected static final String COLUMN_1 = "id";
    protected static final String COLUMN_2 = "name";
    protected static final String INDEX_NAME = "index1";

    protected static final String SECOND_TABLE = "second_table";
    protected static final String SECOND_TABLE_COLUMN = "column";
    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected TableMetadata tableMetadata = createTableMetadataWithIndex();
    protected TableMetadata tableMetadataSecondary = createSimpleTableMetadata();
    protected CatalogMetadata catalogMetadataProvided;

    @Override
    protected ClusterName getClusterName() {
        return new ClusterName("discoverClusterName");
    }

    /**
     * Tests: provideTableMetadata
     *
     * @throws UnsupportedException
     * @throws ConnectorException
     */
    @Override
    public void setUp() throws ConnectorException {
        super.setUp();

        prepareEnvironment(tableMetadata);
        prepareEnvironment(tableMetadataSecondary);
        catalogMetadataProvided = getConnector().getMetadataEngine().provideCatalogMetadata(getClusterName(),
                new CatalogName(CATALOG));

    }

    @Test
    public void provideCatalogMetadataNamesFT() throws UnsupportedException, ConnectorException {

        assertEquals("The catalog name is not the expected", CATALOG, catalogMetadataProvided.getName().getName());
        assertEquals("There should be 2 tables", 2, catalogMetadataProvided.getTables().size());

        TableMetadata tableWithIndexProvided = catalogMetadataProvided.getTables().get(tableMetadata.getName());
        TableMetadata tableSimpleProvided = catalogMetadataProvided.getTables().get(tableMetadataSecondary.getName());

        // Results verification
        assertTrue("The table name is not the expected",
                tableWithIndexProvided.getName().getName().equals(tableMetadata.getName().getName()));
        assertTrue("The cluster name is not the expected",
                tableWithIndexProvided.getClusterRef().getName().equals(getClusterName().getName()));

        assertTrue("The table name is not the expected",
                tableSimpleProvided.getName().getName().equals(tableMetadataSecondary.getName().getName()));
        assertTrue("The cluster name is not the expected",
                tableSimpleProvided.getClusterRef().getName().equals(getClusterName().getName()));

    }

    @Test
    public void provideIndexMetadataFT() throws UnsupportedException, ConnectorException {

        TableMetadata tableWithIndexProvided = catalogMetadataProvided.getTables().get(tableMetadata.getName());
        TableMetadata tableSimpleProvided = catalogMetadataProvided.getTables().get(tableMetadataSecondary.getName());

        verifyIndexMetadata(tableWithIndexProvided);
        assertEquals("There should be no index", 0, tableSimpleProvided.getIndexes().size());

    }

    private void verifyIndexMetadata(TableMetadata tableMetadataProvided) {

        assertTrue("The index has not been created", iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));
        Map<IndexName, IndexMetadata> indexes = tableMetadataProvided.getIndexes();

        assertTrue("The index has not been recovered", containsIndex(indexes));

        IndexType typeProvided = resolveIndexType(indexes);
        IndexType typeExpected = resolveIndexType(tableMetadata.getIndexes());
        assertEquals("The type is not the expected", typeExpected, typeProvided);

        Set<IndexName> keySet = tableMetadataProvided.getIndexes().keySet();

        IndexMetadata indexMetadata = indexes.get(keySet.iterator().next());

        assertEquals("The index should have 2 columns", 2, indexMetadata.getColumns().keySet().size());
        Iterator<ColumnName> iterator = indexMetadata.getColumns().keySet().iterator();

        assertEquals("First column in composite index is not the expected", COLUMN_1, iterator.next().getName());
        assertEquals("Second column in composite index is not the expected", COLUMN_2, iterator.next().getName());
    }

    @Test
    public void providePrimaryKeyFT() throws UnsupportedException, ConnectorException {

        TableMetadata tableWithIndexProvided = catalogMetadataProvided.getTables().get(tableMetadata.getName());
        TableMetadata tableSimpleProvided = catalogMetadataProvided.getTables().get(tableMetadataSecondary.getName());

        assertEquals("The primary key should be " + COLUMN_1, COLUMN_1, tableWithIndexProvided.getPrimaryKey().get(0)
                .getName());

        assertEquals("The primary key should be " + SECOND_TABLE_COLUMN, SECOND_TABLE_COLUMN, tableSimpleProvided
                .getPrimaryKey().get(0).getName());
    }

    @Test
    public void provideFieldsFT() throws UnsupportedException, ConnectorException {

        TableMetadata tableWithIndexProvided = catalogMetadataProvided.getTables().get(tableMetadata.getName());
        TableMetadata tableSimpleProvided = catalogMetadataProvided.getTables().get(tableMetadataSecondary.getName());

        // verifyAllRowWasInserted table1
        assertEquals("The table must have 2 columns", 2, tableWithIndexProvided.getColumns().size());
        String columnNameProvided_1 = tableWithIndexProvided.getColumns()
                .get(new ColumnName(tableMetadata.getName(), COLUMN_1)).getName().getName();
        assertEquals("The field " + COLUMN_1 + " has not been found", COLUMN_1, columnNameProvided_1);
        String columnNameProvided_2 = tableWithIndexProvided.getColumns()
                .get(new ColumnName(tableMetadata.getName(), COLUMN_2)).getName().getName();
        assertEquals("The field " + COLUMN_2 + " has not been found", COLUMN_2, columnNameProvided_2);

        // verifyAllRowWasInserted table2

        assertEquals("The table must have 1 column", 1, tableSimpleProvided.getColumns().size());
        columnNameProvided_1 = tableSimpleProvided.getColumns()
                .get(new ColumnName(tableMetadataSecondary.getName(), SECOND_TABLE_COLUMN)).getName().getName();
        assertEquals("The field " + SECOND_TABLE_COLUMN + " has not been found", SECOND_TABLE_COLUMN,
                columnNameProvided_1);

    }

    protected boolean containsIndex(Map<IndexName, IndexMetadata> indexes) {

        for (IndexName indexName : indexes.keySet()) {
            if (indexName.getName().equals(INDEX_NAME)) {
                return true;
            }
        }
        return false;
    }

    protected IndexType resolveIndexType(Map<IndexName, IndexMetadata> indexList) {
        IndexType typeProvided = null;
        for (Entry<IndexName, IndexMetadata> indexName : indexList.entrySet()) {
            if (indexName.getKey().getName().equals(INDEX_NAME)) {
                typeProvided = indexName.getValue().getType();
            }
        }
        return typeProvided;
    }

    protected abstract void prepareEnvironment(TableMetadata tableMetadata) throws ConnectorException;

    private TableMetadata createTableMetadataWithIndex() {

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE,
                getClusterName().getName());

        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.INT).addColumn(COLUMN_2,
                ColumnType.TEXT);
        tableMetadataBuilder.withPartitionKey(COLUMN_1);

        IndexMetadataBuilder indexMetadataBuilder = new IndexMetadataBuilder(CATALOG, TABLE, INDEX_NAME,
                IndexType.DEFAULT);
        indexMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR);
        indexMetadataBuilder.addColumn(COLUMN_2, ColumnType.TEXT);

        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        tableMetadataBuilder.addIndex(indexMetadata);

        tableMetadata = tableMetadataBuilder.build();
        return tableMetadata;
    }

    private TableMetadata createSimpleTableMetadata() {

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, SECOND_TABLE, getClusterName()
                .getName());

        tableMetadataBuilder.addColumn(SECOND_TABLE_COLUMN, ColumnType.INT);
        tableMetadataBuilder.withPartitionKey(SECOND_TABLE_COLUMN);

        return tableMetadataBuilder.build();
    }

}
