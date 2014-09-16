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

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;


public abstract class GenericMetadataCreateTest extends GenericConnectorTest {


    private static final String NEW_CATALOG = "new_catalog";

    @Test
    public void createCatalogTest() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createCatalogTest " + clusterName.getName() + " ***********************************");

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));
            fail("When I try to delete a catalog that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }

        connector.getMetadataEngine().createCatalog(getClusterName(), new CatalogMetadata(new CatalogName(NEW_CATALOG), Collections.EMPTY_MAP, Collections.EMPTY_MAP));

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));

        } catch (Throwable t) {
            fail("Now I drop a catalog that exist. The operation must be correct.");
        }

    }


    @Test
    public void createCatalogWithOptionsTest() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createCatalogTest " + clusterName.getName() + " ***********************************");

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));
            fail("When I try to delete a catalog that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }

        Map<String, Object> options = new HashMap<>();
        options.put("option1", "value1");
        options.put("option2", new Integer(3));
        options.put("option3", false);
        connector.getMetadataEngine().createCatalog(getClusterName(), new CatalogMetadata(new CatalogName(NEW_CATALOG), options, Collections.EMPTY_MAP));


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
    public void createCatalogExceptionCreateTwoCatalogTest() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createCatalogTest " + clusterName.getName() + " ***********************************");

        try {
            connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));
            fail("When I try to delete a catalog that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }

        connector.getMetadataEngine().createCatalog(getClusterName(), new CatalogMetadata(new CatalogName(NEW_CATALOG), Collections.EMPTY_MAP, Collections.EMPTY_MAP));
        try {
            connector.getMetadataEngine().createCatalog(getClusterName(), new CatalogMetadata(new CatalogName(NEW_CATALOG), Collections.EMPTY_MAP, Collections.EMPTY_MAP));
            fail("I try to create a second catalog with the same identification. Any type of exception must be throws. It may be a runtime excepcion");
        } catch (Throwable t) {

        }

        connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(NEW_CATALOG));


    }

    @Test
    public void createTableWithoutTableTest() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createTableTest ***********************************");

        TableName tableName = new TableName(CATALOG, TABLE);
        Map<String, Object> options = Collections.EMPTY_MAP;

        Map<ColumnName, ColumnMetadata> columns = Collections.EMPTY_MAP;
        Map indexex = columns = Collections.EMPTY_MAP;
        ClusterName clusterRef = getClusterName();
        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey = Collections.EMPTY_LIST;

        //We must create the catalog firs
        connector.getMetadataEngine().createCatalog(getClusterName(), new CatalogMetadata(new CatalogName(CATALOG), Collections.EMPTY_MAP, Collections.EMPTY_MAP));

        try {
            connector.getMetadataEngine().dropTable(getClusterName(), tableName);
            fail("When I try to delete a table that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }


        connector.getMetadataEngine().createTable(getClusterName(), new TableMetadata(tableName, options, columns, indexex, clusterRef, partitionKey, clusterKey));
        try {
            connector.getMetadataEngine().dropTable(getClusterName(), tableName);
        } catch (Throwable t) {
            fail("Now I drop a catalog that exist. The operation must be correct.");
        }

        connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(CATALOG));
    }


    @Test
    public void createTableTest() throws UnsupportedException, ExecutionException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createTableTest ***********************************");

        TableName tableName = new TableName(CATALOG, TABLE);
        Map<String, Object> options = Collections.EMPTY_MAP;

        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        int i = 1;
        Collection<ColumnType> allSupportedColumnType = getConnectorHelper().getAllSupportedColumnType();
        for (ColumnType columnType : allSupportedColumnType) {
            ColumnName columnName = new ColumnName(CATALOG, TABLE, "columnName_" + i);
            columns.put(columnName, new ColumnMetadata(columnName, null, columnType));
            i++;
        }
//        ColumnName columnName = new ColumnName(CATALOG, TABLE, "columnName1");
//        columns.put(columnName, new ColumnMetadata(columnName, null, ColumnType.INT));
//        ColumnName columnName2 = new ColumnName(CATALOG, TABLE, "columnName2");
//        columns.put(columnName2, new ColumnMetadata(columnName2, null, ColumnType.BOOLEAN));


        Map indexex = Collections.EMPTY_MAP;
        ClusterName clusterRef = getClusterName();
        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey = Collections.EMPTY_LIST;

        //We must create the catalog firs
        connector.getMetadataEngine().createCatalog(getClusterName(), new CatalogMetadata(new CatalogName(CATALOG), Collections.EMPTY_MAP, Collections.EMPTY_MAP));

        try {
            connector.getMetadataEngine().dropTable(getClusterName(), tableName);
            fail("When I try to delete a table that not exists any type of exception must be throws. It may be a runtime exception.");
        } catch (Throwable t) {
        }


        connector.getMetadataEngine().createTable(getClusterName(), new TableMetadata(tableName, options, columns, indexex, clusterRef, partitionKey, clusterKey));
        try {
            connector.getMetadataEngine().dropTable(getClusterName(), tableName);
        } catch (Throwable t) {
            fail("Now I drop a catalog that exist. The operation must be correct.");
        }

        connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(CATALOG));

    }


}