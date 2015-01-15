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
package com.stratio.connector.commons.metadata;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;

/**
 * @author darroyo
 */
public class TableMetadataBuilder {
    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    private TableName tableName;
    private Map<Selector, Selector> options;
    private LinkedHashMap<ColumnName, ColumnMetadata> columns;
    private Map<IndexName, IndexMetadata> indexes;
    private LinkedList<ColumnName> partitionKey;
    private LinkedList<ColumnName> clusterKey;
    private ClusterName clusterName;

    @Deprecated
    public TableMetadataBuilder(String catalogName, String tableName) {
        this.tableName = new TableName(catalogName, tableName);
        columns = new LinkedHashMap<ColumnName, ColumnMetadata>();
        indexes = new LinkedHashMap<IndexName, IndexMetadata>();
        partitionKey = new LinkedList<ColumnName>();
        clusterKey = new LinkedList<ColumnName>();
        options = null;
    }

    public TableMetadataBuilder(String catalogName, String tableName, String clusName) {
        this.clusterName = new ClusterName(clusName);
        this.tableName = new TableName(catalogName, tableName);
        columns = new LinkedHashMap<ColumnName, ColumnMetadata>();
        indexes = new LinkedHashMap<IndexName, IndexMetadata>();
        partitionKey = new LinkedList<ColumnName>();
        clusterKey = new LinkedList<ColumnName>();
        options = null;
    }

    public TableMetadataBuilder withOptions(Map<Selector, Selector> opts) {
        options = new HashMap<Selector, Selector>(opts);
        return this;
    }

    public TableMetadataBuilder withColumns(List<ColumnMetadata> columnsMetadata) {
        for (ColumnMetadata colMetadata : columnsMetadata) {
            columns.put(colMetadata.getName(), colMetadata);
        }
        return this;
    }

    /**
     * parameters in columnMetadata will be null
     *
     * @param columnName
     * @param colType
     * @return
     */
    public TableMetadataBuilder addColumn(String columnName, ColumnType colType) {
        ColumnName colName = new ColumnName(tableName, columnName);
        ColumnMetadata colMetadata = new ColumnMetadata(colName, null, colType);
        columns.put(colName, colMetadata);
        return this;
    }

    /**
     * Must be called after including columns options in indexMetadata will be null TODO same as options?.
     * ColumnMetadata is recovered from the tableMetadata
     *
     * @param indType
     * @param indexName
     * @param fields
     *            the columns which define the index
     * @return
     */
    public TableMetadataBuilder addIndex(IndexType indType, String indexName, String... fields) {

        IndexName indName = new IndexName(tableName.getName(), tableName.getName(), indexName);

        Map<ColumnName, ColumnMetadata> columnsMetadata = new HashMap<ColumnName, ColumnMetadata>(fields.length);
        // recover the columns from the table metadata
        for (String field : fields) {
            ColumnMetadata cMetadata = columns.get(new ColumnName(tableName, field));
            if (cMetadata == null) {
                throw new RuntimeException("Trying to index a not existing column: " + field);
            }
            columnsMetadata.put(new ColumnName(tableName, field), cMetadata);
        }
        IndexMetadata indMetadata = new IndexMetadata(indName, columnsMetadata, indType, null);
        indexes.put(indName, indMetadata);
        return this;
    }

    /**
     * Must be called after including columns options in indexMetadata will be null TODO same as options?.
     * ColumnMetadata is recovered from the tableMetadata
     *
     */
    public TableMetadataBuilder addIndex(IndexMetadata indexMetadata) {
        indexes.put(indexMetadata.getName(), indexMetadata);
        return this;
    }

    // TODO implement the generic
    /*
     * public TableMetadataBuilder withIndexes(){ }
     */

    public TableMetadataBuilder withPartitionKey(String... fields) {
        for (String field : fields) {
            partitionKey.add(new ColumnName(tableName, field));

        }

        return this;
    }

    public TableMetadataBuilder withClusterKey(String... fields) {
        for (String field : fields) {
            clusterKey.add(new ColumnName(tableName, field));
        }
        return this;
    }

    @Deprecated
    public TableMetadataBuilder withClusterNameRef(ClusterName clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public TableMetadata build() {
        return build(false);
    }

    public TableMetadata build(boolean isPKRequired) {
        if (isPKRequired) {
            if (partitionKey.isEmpty()) {
                this.withPartitionKey(columns.keySet().iterator().next().getName());
            }
        }
        return new TableMetadata(tableName, options, columns, indexes, clusterName, partitionKey, clusterKey);
    }

}