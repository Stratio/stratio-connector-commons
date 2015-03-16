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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

public abstract class GenericMetadataCreateFT extends GenericConnectorTest {

    private static final String NEW_CATALOG = "new_catalog";
    private static final String INDEX = "index1";

    @Test
    public void createCatalogTest() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));
            fail("When I try to delete a catalog that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }

        connector.getMetadataEngine().createCatalog(getClusterName(),
                new CatalogMetadata(new CatalogName(NEW_CATALOG), null, null));

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));

        } catch (Throwable t) {
            fail("Now I drop a catalog that exist. The operation must be correct.");
        }

    }

    @Test
    public void createCatalogWithOptionsTest() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));
            fail("When I try to delete a catalog that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }

        Map<Selector, Selector> options = new HashMap<>();
        options.put(new StringSelector("option1"), new StringSelector("value1"));
        options.put(new StringSelector("option2"), new IntegerSelector(new Integer(3)));
        options.put(new StringSelector("option3"), new BooleanSelector(false));
        connector.getMetadataEngine().createCatalog(getClusterName(),
                new CatalogMetadata(new CatalogName(NEW_CATALOG), options, Collections.EMPTY_MAP));

        Map<String, Object> recoveredSettings = getConnectorHelper().recoveredCatalogSettings(NEW_CATALOG);

        Set<String> keys = recoveredSettings.keySet();
        assertTrue("The option1 exists", keys.contains("option1"));
        assertTrue("The option2 exists", keys.contains("option2"));
        assertTrue("The option3 exists", keys.contains("option3"));

        assertEquals("The value of option1 is correct", recoveredSettings.get("option1"), "value1");
        assertEquals("The value of option2 is correct", recoveredSettings.get("option2"), "3");
        assertEquals("The value of option2 is correct", recoveredSettings.get("option3"), "false");

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));

        } catch (Throwable t) {
            fail("Now I drop a catalog that exist. The operation must be correct.");
        }

    }

    @Test
    public void createCatalogExceptionCreateTwoCatalogTest() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));
            fail("When I try to delete a catalog that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }

        connector.getMetadataEngine()
                .createCatalog(getClusterName(),
                        new CatalogMetadata(new CatalogName(NEW_CATALOG), Collections.EMPTY_MAP,
                                Collections.EMPTY_MAP));
        try {
            connector.getMetadataEngine().createCatalog(
                    getClusterName(),
                    new CatalogMetadata(new CatalogName(NEW_CATALOG), Collections.EMPTY_MAP,
                            Collections.EMPTY_MAP));
            fail("I try to create a second catalog with the same identification. Any type of exception must be thrown. It may be a runtime excepcion");
        } catch (Throwable t) {

        }

        connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));

    }

    @Test
    public void createTableTest() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        TableName tableName = new TableName(CATALOG, TABLE);
        Map<Selector, Selector> options = Collections.EMPTY_MAP;

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        int i = 1;
        Collection<ColumnType> allSupportedColumnType = getConnectorHelper().getAllSupportedColumnType();
        for (ColumnType columnType : allSupportedColumnType) {
            ColumnName columnName = new ColumnName(CATALOG, TABLE, "columnName_" + i);
            columns.put(columnName, new ColumnMetadata(columnName, null, columnType));
            i++;
        }

        Map indexex = Collections.EMPTY_MAP;
        ClusterName clusterRef = getClusterName();
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        LinkedList<ColumnName> clusterKey = new LinkedList<>();

        if (getConnectorHelper().isCatalogMandatory()) {
            connector.getMetadataEngine()
                    .createCatalog(getClusterName(),
                            new CatalogMetadata(new CatalogName(CATALOG), Collections.EMPTY_MAP,
                                    Collections.EMPTY_MAP));
        }
        try {
            connector.getMetadataEngine().dropTable(getClusterName(), tableName);
            fail("When I try to delete a table that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }

        connector.getMetadataEngine().createTable(getClusterName(),
                new TableMetadata(tableName, options, columns, indexex, clusterRef, partitionKey, clusterKey));
        try {
            connector.getMetadataEngine().dropTable(getClusterName(), tableName);
        } catch (Throwable t) {
            fail("Now I drop a catalog that exist. The operation must be correct.");
        }
        if (getConnectorHelper().isCatalogMandatory()) {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(CATALOG));
        }

    }

    @Test
    public void createTableWithoutTableTest() throws ConnectorException {
        ClusterName clusterName = getClusterName();

        TableName tableName = new TableName(CATALOG, TABLE);
        Map<Selector, Selector> options = Collections.EMPTY_MAP;

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        Map indexex = Collections.EMPTY_MAP;
        ClusterName clusterRef = getClusterName();
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        LinkedList<ColumnName> clusterKey = new LinkedList<>();

        if (getConnectorHelper().isCatalogMandatory()) {
            connector.getMetadataEngine()
                    .createCatalog(getClusterName(),
                            new CatalogMetadata(new CatalogName(CATALOG), Collections.EMPTY_MAP,
                                    Collections.EMPTY_MAP));
        }
        try {
            connector.getMetadataEngine().dropTable(getClusterName(), tableName);
            fail("When I try to delete a table that not exists any type of exception must be thrown. It may be a runtime exception.");
        } catch (Throwable t) {
        }

        connector.getMetadataEngine().createTable(getClusterName(),
                new TableMetadata(tableName, options, columns, indexex, clusterRef, partitionKey, clusterKey));
        try {
            connector.getMetadataEngine().dropTable(getClusterName(), tableName);
        } catch (Throwable t) {
            fail("Now I drop a catalog that exist. The operation must be correct.");
        }

        connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(CATALOG));
    }

}