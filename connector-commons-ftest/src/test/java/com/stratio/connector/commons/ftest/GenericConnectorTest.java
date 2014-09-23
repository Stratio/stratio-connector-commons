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
package com.stratio.connector.commons.ftest;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;

public abstract class GenericConnectorTest<T extends IConnector> {

    protected static IConnectorHelper iConnectorHelper;
    public final String TABLE = this.getClass().getSimpleName();
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
    public void setUp() throws InitializationException, ConnectionException, UnsupportedException, ExecutionException {
        iConnectorHelper = getConnectorHelper();
        connector = getConnector();
        connector.init(getConfiguration());
        connector.connect(getICredentials(), getConnectorClusterConfig());

        deleteCatalog(CATALOG);

        System.out.println(CATALOG + "/" + TABLE);
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

        if (deleteBeteweenTest) {
            deleteCatalog(CATALOG);
            if (logger.isDebugEnabled()) {
                logger.debug("Delete Catalog: " + CATALOG);

                connector.close(getClusterName());
            }

        }
    }
}
