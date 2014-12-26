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
package com.stratio.connector.commons.ftest;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.security.ICredentials;

public abstract class GenericConnectorTest<T extends IConnector> {

    protected static IConnectorHelper iConnectorHelper;
    public final String TABLE = this.getClass().getSimpleName().toLowerCase();
    /**
     * The Log.
     */
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    public String CATALOG = "catalog_functional_test";
    protected boolean deleteBeteweenTest = true;
    protected T connector;

    protected ClusterName getClusterName() {
        return new ClusterName(CATALOG + "-" + TABLE);
    }

    public void setDeleteBeteweenTest(boolean deleteBeteweenTest) {
        this.deleteBeteweenTest = deleteBeteweenTest;
    }

    protected abstract IConnectorHelper getConnectorHelper();

    @Before
    public void setUp() throws ConnectorException {
        iConnectorHelper = getConnectorHelper();
        connector = getConnector();
        connector.init(getConfiguration());
        connector.connect(getICredentials(), getConnectorClusterConfig());

        dropTable(CATALOG, TABLE);
        deleteCatalog(CATALOG);

    }

    protected void deleteCatalog(String catalog) throws UnsupportedException, ExecutionException {
        try {
            if (deleteBeteweenTest) {
                connector.getMetadataEngine().dropCatalog(getClusterName(), new CatalogName(catalog));
            }
        } catch (Throwable t) {
            logger.debug("Index does not exist");
        }
    }

    /**
     * @param table
     */
    protected void dropTable(String catalog, String table) {
        try {
            if (deleteBeteweenTest) {
                connector.getMetadataEngine().dropTable(getClusterName(), new TableName(catalog, table));
            }
        } catch (Throwable t) {
            logger.debug("Index does not exist");
        }

    }

    protected void refresh(String catalog) {
        iConnectorHelper.refresh(catalog);
    }

    protected T getConnector() {
        return (T) iConnectorHelper.getConnector();
    }

    protected IConfiguration getConfiguration() {
        return iConnectorHelper.getConfiguration();
    }

    protected ConnectorClusterConfig getConnectorClusterConfig() {
        return iConnectorHelper.getConnectorClusterConfig();
    }

    protected ICredentials getICredentials() {
        return iConnectorHelper.getICredentials();
    }

    @After
    public void tearDown() throws ConnectionException, UnsupportedException, ExecutionException {
        // TODO the catalog and the table are not removed from the db when deleting between test.

        if (deleteBeteweenTest) {
            deleteCatalog(CATALOG);
            dropTable(CATALOG, TABLE);
            if (logger.isDebugEnabled()) {
                logger.debug("Delete Catalog: " + CATALOG);

            }

        }
        connector.close(getClusterName());
    }

}
