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

package com.stratio.connector.commons.ftest.helper;

import java.util.Collection;
import java.util.Map;

import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.metadata.ColumnType;

/**
 * Created by jmgomez on 4/09/14.
 */
public interface IConnectorHelper {

    void refresh(String catalog);

    IConnector getConnector();

    IConfiguration getConfiguration();

    ConnectorClusterConfig getConnectorClusterConfig();

    ICredentials getICredentials();

    Map<String, Object> recoveredCatalogSettings(String catalog);

    Collection<ColumnType> getAllSupportedColumnType();

    boolean containsIndex(String catalogName, String collectionName,
            String indexName);

    int countIndexes(String catalogName, String collectionName);

}
