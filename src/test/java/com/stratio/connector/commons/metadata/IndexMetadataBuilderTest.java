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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

public class IndexMetadataBuilderTest {

    private static final String CATALOG = "catalogName";
    private static final String TABLE = "tableName";
    private static final String INDEX_NAME_DEFAULT = "indexName";

    private static final String INDEX_OPTION_STRING = "indexOpt";
    private static final String INDEX_OPTION_STRING_VALUE = "sparse";

    @Test
    public void createMetadataBasicIndexTest() {

        IndexMetadataBuilder indexMetadataBuilder = new IndexMetadataBuilder(CATALOG, TABLE, INDEX_NAME_DEFAULT,
                        IndexType.DEFAULT);

        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        IndexName indexNameExpected = new IndexName(CATALOG, TABLE, INDEX_NAME_DEFAULT);

        Assert.assertEquals("The indexName is not the expected", indexNameExpected, indexMetadata.getName());
        Assert.assertTrue("There should be no columns", indexMetadata.getColumns().isEmpty());
        Assert.assertNull("There should be no options", indexMetadata.getOptions());
        Assert.assertEquals("The indexType should be DEFAULT", IndexType.DEFAULT, indexMetadata.getType());

    }

    @Test
    public void addIndexOptionsTest() {

        final String INDEX_OPTION_BOOL = "unique";
        final Boolean INDEX_OPTION_BOOL_VALUE = true;
        final String INDEX_OPTION_INT = "ttl";
        final Integer INDEX_OPTION_INT_VALUE = 50;
        IndexMetadataBuilder indexMetadataBuilder = new IndexMetadataBuilder(CATALOG, TABLE, INDEX_NAME_DEFAULT,
                        IndexType.DEFAULT);

        indexMetadataBuilder.withOptions(getIndexOptions()).addOption(INDEX_OPTION_BOOL, INDEX_OPTION_BOOL_VALUE)
                        .addOption(INDEX_OPTION_INT, INDEX_OPTION_INT_VALUE);
        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        assertEquals("The optionsMap size is not the expected", 3, indexMetadata.getOptions().size());
        assertEquals("The string option is not the expected", new StringSelector(INDEX_OPTION_STRING_VALUE),
                        indexMetadata.getOptions().get(new StringSelector(INDEX_OPTION_STRING)));
        assertEquals("The integer option is not the expected", new IntegerSelector(INDEX_OPTION_INT_VALUE),
                        indexMetadata.getOptions().get(new StringSelector(INDEX_OPTION_INT)));
        assertEquals("The boolean option  is not the expected", new BooleanSelector(INDEX_OPTION_BOOL_VALUE),
                        indexMetadata.getOptions().get(new StringSelector(INDEX_OPTION_BOOL)));

    }

    @Test
    public void addColumnsTest() {

        final String COLUMN_1 = "col1";
        final String COLUMN_2 = "col2";
        final String COLUMN_3 = "col3";
        final ColumnType COLUMN_1_TYPE = new ColumnType(DataType.BIGINT);
        final ColumnType COLUMN_2_TYPE =new ColumnType(DataType.FLOAT);
        final ColumnType COLUMN_3_TYPE = new ColumnType(DataType.VARCHAR);
        final ColumnName colName1 = new ColumnName(CATALOG, TABLE, COLUMN_1);
        final ColumnName colName2 = new ColumnName(CATALOG, TABLE, COLUMN_2);
        final ColumnName colName3 = new ColumnName(CATALOG, TABLE, COLUMN_3);

        IndexMetadataBuilder indexMetadataBuilder = new IndexMetadataBuilder(CATALOG, TABLE, INDEX_NAME_DEFAULT,
                        IndexType.DEFAULT);
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        columnMetadataList.add(new ColumnMetadata(new ColumnName(CATALOG, TABLE, COLUMN_3), null, COLUMN_3_TYPE));

        indexMetadataBuilder.addColumn(COLUMN_1, COLUMN_1_TYPE).addColumn(COLUMN_2, COLUMN_2_TYPE)
                        .withColumns(columnMetadataList);

        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        assertEquals("The columns size is not the expected", 3, indexMetadata.getColumns().size());

        assertTrue("The columns map should contain " + COLUMN_1, indexMetadata.getColumns().containsKey(colName1));
        assertEquals("The column " + COLUMN_1 + " type is not the expected", COLUMN_1_TYPE, indexMetadata.getColumns()
                        .get(colName1).getColumnType());

        assertTrue("The columns map should contain " + COLUMN_2, indexMetadata.getColumns().containsKey(colName2));
        assertEquals("The column " + COLUMN_2 + " type is not the expected", COLUMN_2_TYPE, indexMetadata.getColumns()
                        .get(colName2).getColumnType());

        assertTrue("The columns map should contain " + COLUMN_3, indexMetadata.getColumns().containsKey(colName3));
        assertEquals("The column " + COLUMN_3 + " type is not the expected", COLUMN_3_TYPE, indexMetadata.getColumns()
                        .get(colName3).getColumnType());

    }

    private Map<Selector, Selector> getIndexOptions() {
        Map<Selector, Selector> options = new HashMap<>();
        options.put(new StringSelector(INDEX_OPTION_STRING), new StringSelector(INDEX_OPTION_STRING_VALUE));
        return options;
    }
}
