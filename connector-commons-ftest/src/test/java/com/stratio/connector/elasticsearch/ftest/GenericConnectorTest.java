/*
 * Stratio Meta
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
package com.stratio.connector.elasticsearch.ftest;

import com.stratio.connector.elasticsearch.ftest.helper.IConnectorHelper;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.junit.After;
import org.junit.Before;


public abstract class GenericConnectorTest<T extends IConnector> {

    protected ClusterName getClusterName(){ return  new ClusterName(SCHEMA+"-"+TABLE); }

    private IConnectorHelper iConnectorHelper;

    protected abstract IConnectorHelper getConnectorHelper();

    protected T connector;


    protected final String TABLE = getClass().getSimpleName();
    protected final String SCHEMA = "functionaltest";



    @Before
    public void setUp() throws InitializationException, ConnectionException {
        iConnectorHelper = getConnectorHelper();
        connector = getConnector();
        connector.init(getConfiguration());
        connector.connect(getICredentials(), getConnectorClusterConfig());

        deleteSet(SCHEMA);

        System.out.println(SCHEMA +"/"+ TABLE);
    }



    protected  void deleteSet(String catalog){
        iConnectorHelper.deleteSet(catalog);
    }
    protected  void refresh(String catalog){
        iConnectorHelper.refresh(catalog);
    }

    protected  T getConnector(){
        return (T)iConnectorHelper.getConnector();
    }


    protected IConfiguration getConfiguration(){
        return iConnectorHelper.getConfiguration();
    }
    protected  ConnectorClusterConfig getConnectorClusterConfig(){
        return iConnectorHelper.getConnectorClusterConfig();
    }
    protected ICredentials getICredentials(){
        return iConnectorHelper.getICredentials();
    }
    
    @After
    public void tearDown() throws ConnectionException {
        deleteSet(SCHEMA);
        connector.close(getClusterName());


    }

}
