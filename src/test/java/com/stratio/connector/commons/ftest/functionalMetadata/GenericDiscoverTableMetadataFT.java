/*
 * 
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
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;

public abstract class GenericDiscoverTableMetadataFT extends GenericConnectorTest {

    protected static final String COLUMN_1 = "id";
    protected static final String COLUMN_2 = "name";
    protected static final String INDEX_NAME = "index1";

    protected TableMetadata tableMetadata = createMetadata();
    protected TableMetadata tableMetadataProvided;

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

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
        tableMetadataProvided = getConnector().getMetadataEngine().provideTableMetadata(getClusterName(),
                        tableMetadata.getName());

    }

    @Test
    public void provideTableMetadataNameFT() throws UnsupportedException, ConnectorException {

        // Results verification
        assertTrue(tableMetadataProvided.getName().getName().equals(tableMetadata.getName().getName()));
        assertTrue(tableMetadataProvided.getClusterRef().getName().equals(getClusterName().getName()));

    }

    @Test
    public void provideIndexMetadataFT() throws UnsupportedException, ConnectorException {

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

        assertEquals(COLUMN_1, iterator.next().getName());
        assertEquals(COLUMN_2, iterator.next().getName());
    }

    @Test
    public void providePrimaryKeyFT() throws UnsupportedException, ConnectorException {

        assertEquals("The primary key should be " + COLUMN_1, COLUMN_1, tableMetadataProvided.getPrimaryKey().get(0)
                        .getName());
    }

    @Test
    public void provideFieldsFT() throws UnsupportedException, ConnectorException {

        assertEquals("The table must have 2 columns", 2, tableMetadataProvided.getColumns().size());
        String columnNameProvided_1 = tableMetadataProvided.getColumns()
                        .get(new ColumnName(tableMetadata.getName(), COLUMN_1)).getName().getName();
        assertEquals(COLUMN_1, columnNameProvided_1);
        String columnNameProvided_2 = tableMetadataProvided.getColumns()
                        .get(new ColumnName(tableMetadata.getName(), COLUMN_2)).getName().getName();
        assertEquals(COLUMN_2, columnNameProvided_2);

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

    private TableMetadata createMetadata() {

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE, getClusterName().getName());

        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.INT).addColumn(COLUMN_2, ColumnType.TEXT);
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

}
