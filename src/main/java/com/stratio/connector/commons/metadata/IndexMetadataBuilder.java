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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * Builder for IndexMetadata.
 *
 * @author darroyo
 */
public class IndexMetadataBuilder {

    private IndexName indexName;
    private TableName tableName;
    private Map<Selector, Selector> options = Collections.emptyMap();
    private Map<ColumnName, ColumnMetadata> columns = Collections.emptyMap();
    private IndexType indexType;

    /**
     * Instantiates a new index metadata builder.
     *
     * @param catalogName
     *            the catalog name
     * @param tableName
     *            the table name
     * @param indexName
     *            the index name
     * @param type
     *            the type
     */
    public IndexMetadataBuilder(String catalogName, String tableName, String indexName, IndexType type) {
        this.tableName = new TableName(catalogName, tableName);
        this.indexName = new IndexName(catalogName, tableName, indexName);
        columns = new HashMap<ColumnName, ColumnMetadata>();
        options = null;
        this.indexType = type;
    }

    /**
     * Set the options. Any options previously created are removed.
     *
     * @param opts
     *            the opts
     * @return the index metadata builder
     */
    public IndexMetadataBuilder withOptions(Map<Selector, Selector> opts) {
        options = new HashMap<Selector, Selector>(opts);
        return this;
    }

    /**
     * Add new columns. The columns previously created are not removed.
     *
     * @param columnsMetadata
     *            the columns metadata
     * @return the index metadata builder
     */
    public IndexMetadataBuilder withColumns(List<ColumnMetadata> columnsMetadata) {
        for (ColumnMetadata colMetadata : columnsMetadata) {
            columns.put(colMetadata.getName(), colMetadata);
        }
        return this;
    }

    /**
     * Add column. Parameters in columnMetadata will be null.
     *
     * @param columnName
     *            the column name
     * @param colType
     *            the col type
     * @return the index metadata builder
     */
    public IndexMetadataBuilder addColumn(String columnName, ColumnType colType) {
        ColumnName colName = new ColumnName(tableName, columnName);
        ColumnMetadata colMetadata = new ColumnMetadata(colName, null, colType);
        columns.put(colName, colMetadata);
        return this;
    }

    /**
     * Adds a new string option.
     *
     * @param option
     *            the option
     * @param value
     *            the value
     * @return the index metadata builder
     */
    public IndexMetadataBuilder addOption(String option, String value) {
        if (options == null) {
            options = new HashMap<Selector, Selector>();
        }
        options.put(new StringSelector(option), new StringSelector(value));
        return this;
    }

    /**
     * Adds a new integer option.
     *
     * @param option
     *            the option
     * @param value
     *            the value
     * @return the index metadata builder
     */
    public IndexMetadataBuilder addOption(String option, Integer value) {
        if (options == null) {
            options = new HashMap<Selector, Selector>();
        }
        options.put(new StringSelector(option), new IntegerSelector(value));
        return this;
    }

    /**
     * Adds a new boolean option.
     *
     * @param option
     *            the option
     * @param value
     *            the value
     * @return the index metadata builder
     */
    public IndexMetadataBuilder addOption(String option, Boolean value) {
        if (options == null) {
            options = new HashMap<Selector, Selector>();
        }
        options.put(new StringSelector(option), new BooleanSelector(value));
        return this;
    }

    /**
     * Builds the IndexMetadata.
     *
     * @return the index metadata
     */
    public IndexMetadata build() {
        // TODO logger.debug()
        return new IndexMetadata(indexName, columns, indexType, options);

    }

}
