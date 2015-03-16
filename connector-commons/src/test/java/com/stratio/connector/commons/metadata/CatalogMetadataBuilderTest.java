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

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

public class CatalogMetadataBuilderTest {

    private static final String CATALOG = "catalog";

    @Test
    public void basicCatalogTest() {

        CatalogMetadataBuilder catalogMetadataBuilder = new CatalogMetadataBuilder(CATALOG);

        CatalogMetadata catalogMetadata = catalogMetadataBuilder.build();

        Assert.assertEquals("Catalog name is not the expected", CATALOG, catalogMetadata.getName().getName());
        Assert.assertTrue("The catalog should not have tables", catalogMetadata.getTables().isEmpty());

    }

    @Test
    public void catalogWithOptionsTest() {

        CatalogMetadataBuilder catalogMetadataBuilder = new CatalogMetadataBuilder(CATALOG);
        Selector selector = new StringSelector("key");
        Selector selectorValue = new StringSelector("value");
        Map<Selector, Selector> options = new HashMap<Selector, Selector>();
        options.put(selector, selectorValue);

        CatalogMetadata catalogMetadata = catalogMetadataBuilder.withOptions(options).build();

        Assert.assertEquals("Catalog name is not the expected", CATALOG, catalogMetadata.getName().getName());
        Assert.assertEquals("The options are not the expected", selectorValue,
                catalogMetadata.getOptions().get(selector));
    }

    @Test
    public void catalogWithTableMetadataTest() {

        CatalogMetadataBuilder catalogMetadataBuilder = new CatalogMetadataBuilder(CATALOG);
        TableName tableName = new TableName(CATALOG, "table");
        TableMetadata tableMetadata = new TableMetadata(tableName, null, null, null, null, null, null);

        CatalogMetadata catalogMetadata = catalogMetadataBuilder.withTables(tableMetadata).build();

        Assert.assertEquals("Catalog name is not the expected", CATALOG, catalogMetadata.getName().getName());
        Assert.assertEquals("There should be 1 table", 1, catalogMetadata.getTables().size());
        Assert.assertTrue("The table " + tableName.getQualifiedName() + " has not been found", catalogMetadata
                .getTables().containsKey(tableName));

    }

}