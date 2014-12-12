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

package com.stratio.connector.commons.ftest.helper;

import java.util.Collection;
import java.util.Map;

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.security.ICredentials;

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

    boolean isCatalogMandatory();

    boolean isTableMandatory();

    boolean isIndexMandatory();

    boolean isPKMandatory();
}
