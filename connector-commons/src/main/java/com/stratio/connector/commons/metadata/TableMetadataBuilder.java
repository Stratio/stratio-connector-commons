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
package com.stratio.connector.commons.metadata;

import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.metadata.*;
import com.stratio.crossdata.common.statements.structures.Selector;

import java.util.*;

/**
 * Builder for TableMetadata.
 *
 * @author darroyo
 */
public class TableMetadataBuilder {

    /**
     * The table.
     */
    private TableName tableName;
    /**
     * The options.
     */
    private Map<Selector, Selector> options;

    /**
     * The columns.
     */
    private Map<ColumnName, ColumnMetadata> columns;

    /**
     * The index metadata.
     */
    private Map<IndexName, IndexMetadata> indexes;

    /**
     * The partition key.
     */
    private List<ColumnName> partitionKey;

    /**
     * The cluster key.
     */
    private List<ColumnName> clusterKey;

    /**
     * The cluster name.
     */
    private ClusterName clusterName;

    /**
     * @param catalogName the catalog name
     * @param tableName   the table name
     * @deprecated use {@link #TableMetadataBuilder(String, String, String)} instead.
     */
    @Deprecated
    public TableMetadataBuilder(String catalogName, String tableName) {
        this.tableName = new TableName(catalogName, tableName);
        columns = new LinkedHashMap<ColumnName, ColumnMetadata>();
        indexes = new LinkedHashMap<IndexName, IndexMetadata>();
        partitionKey = new LinkedList<ColumnName>();
        clusterKey = new LinkedList<ColumnName>();
        options = null;
    }

    /**
     * Instantiates a new table metadata builder.
     *
     * @param catalogName the catalog name
     * @param tableName   the table name
     * @param clusName    the cluster name
     */
    public TableMetadataBuilder(String catalogName, String tableName, String clusName) {
        this.clusterName = new ClusterName(clusName);
        this.tableName = new TableName(catalogName, tableName);
        columns = new LinkedHashMap<ColumnName, ColumnMetadata>();
        indexes = new LinkedHashMap<IndexName, IndexMetadata>();
        partitionKey = new LinkedList<ColumnName>();
        clusterKey = new LinkedList<ColumnName>();
        options = null;
    }

    /**
     * Set the options. Any options previously created are removed.
     *
     * @param opts the opts
     * @return the table metadata builder
     */
    public TableMetadataBuilder withOptions(Map<Selector, Selector> opts) {
        options = new HashMap<Selector, Selector>(opts);
        return this;
    }

    /**
     * Add new columns. The columns previously created are not removed.
     *
     * @param columnsMetadata the columns metadata
     * @return the table metadata builder
     */
    public TableMetadataBuilder withColumns(List<ColumnMetadata> columnsMetadata) {
        for (ColumnMetadata colMetadata : columnsMetadata) {
            columns.put(colMetadata.getName(), colMetadata);
        }
        return this;
    }

    /**
     * Add column. Parameters in columnMetadata will be null.
     *
     * @param columnName the column name
     * @param colType    the column type
     * @return the table metadata builder
     */
    public TableMetadataBuilder addColumn(String columnName, ColumnType colType) {
        ColumnName colName = new ColumnName(tableName, columnName);
        ColumnMetadata colMetadata = new ColumnMetadata(colName, null, colType);
        columns.put(colName, colMetadata);
        return this;
    }

    /**
     * Add an index. Must be called after including columns because columnMetadata is recovered from the tableMetadata.
     * Options in indexMetadata will be null.
     *
     * @param indType   the index type.
     * @param indexName the index name.
     * @param fields    the columns which define the index.
     * @return the table metadata builder.
     * @throws if an error happens.
     */
    public TableMetadataBuilder addIndex(IndexType indType, String indexName, String... fields) throws ExecutionException {

        IndexName indName = new IndexName(tableName.getName(), tableName.getName(), indexName);

        Map<ColumnName, ColumnMetadata> columnsMetadata = new HashMap<ColumnName, ColumnMetadata>(fields.length);
        // recover the columns from the table metadata
        for (String field : fields) {
            ColumnMetadata cMetadata = columns.get(new ColumnName(tableName, field));
            if (cMetadata == null) {
                throw new ExecutionException("Trying to index a not existing column: " + field);
            }
            columnsMetadata.put(new ColumnName(tableName, field), cMetadata);
        }
        IndexMetadata indMetadata = new IndexMetadata(indName, columnsMetadata, indType, null);
        indexes.put(indName, indMetadata);
        return this;
    }

    /**
     * Add an index.
     *
     * @param indexMetadata the index metadata
     * @return the table metadata builder
     */
    public TableMetadataBuilder addIndex(IndexMetadata indexMetadata) {
        indexes.put(indexMetadata.getName(), indexMetadata);
        return this;
    }

    /**
     * Set the partition key.
     *
     * @param fields the fields
     * @return the table metadata builder
     */
    public TableMetadataBuilder withPartitionKey(String... fields) {
        for (String field : fields) {
            partitionKey.add(new ColumnName(tableName, field));

        }

        return this;
    }

    /**
     * Set the cluster key.
     *
     * @param fields the fields
     * @return the table metadata builder
     */
    public TableMetadataBuilder withClusterKey(String... fields) {
        for (String field : fields) {
            clusterKey.add(new ColumnName(tableName, field));
        }
        return this;
    }

    /**
     * Set the cluster name.
     *
     * @param clusterName the cluster name
     * @return the table metadata builder
     * @deprecated use {@link #TableMetadataBuilder(String, String, String)} instead.
     */
    @Deprecated
    public TableMetadataBuilder withClusterNameRef(ClusterName clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    /**
     * Builds the table metadata.
     *
     * @return the table metadata
     */
    public TableMetadata build() {
        return build(false);
    }

    /**
     * Builds the table metadata.
     *
     * @param isPKRequired whether the pk is required or not
     * @return the table metadata
     */
    public TableMetadata build(boolean isPKRequired) {
        if (isPKRequired && partitionKey.isEmpty()) {
            this.withPartitionKey(columns.keySet().iterator().next().getName());
        }
        return new TableMetadata(tableName, options, new LinkedHashMap<>(columns), indexes, clusterName, partitionKey, clusterKey);
    }

}
