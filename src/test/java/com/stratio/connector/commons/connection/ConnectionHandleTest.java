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

package com.stratio.connector.commons.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * ConnectionHandle Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 9, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
public class ConnectionHandleTest {

    public static final String CLUSTER_NAME = "cluster_name";
    @Mock
    ICredentials credentials;
    @Mock
    ConnectorClusterConfig conenectoClusterConfig;
    @Mock
    IConfiguration configuration;
    private StubConnectionHandle stubConnectionHandle;

    @Before
    public void before() throws Exception {

        when(conenectoClusterConfig.getName()).thenReturn(new ClusterName(CLUSTER_NAME));
        stubConnectionHandle = new StubConnectionHandle(configuration);
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: createConnection(ICredentials credentials, ConnectorClusterConfig config)
     */
    @Test
    public void testCreateConnection() throws ExecutionException, ConnectionException {

        connectionNotExist();

        stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);

        assertNotNull("The connection exists", stubConnectionHandle.getConnection(CLUSTER_NAME));

    }

    @Test
    public void testCreateConnectionAlredyCrete() throws ExecutionException,ConnectionException {


        connectionNotExist();

        stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);
        try {
            stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);
            fail("should not get here");
        } catch (ConnectionException e) {
            assertEquals("The message is correct", "The connection [" + CLUSTER_NAME + "] already exists",
                    e.getMessage());
        }

    }

    private void connectionNotExist() throws ExecutionException {
        try {
            stubConnectionHandle.getConnection(CLUSTER_NAME);
            fail("should not get here");

        } catch (ExecutionException e) {
            assertEquals("The message is correct", e.getMessage(),
                    "The connection [" + CLUSTER_NAME + "] does not exist");

        }
    }

    /**
     * Method: closeConnection(String clusterName)
     */
    @Test
    public void testCloseConnection() throws Exception, HandlerConnectionException {

        stubConnectionHandle.closeConnection(CLUSTER_NAME); //Close a not exist connection.

        stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);
        assertNotNull("The connection exists", stubConnectionHandle.getConnection(CLUSTER_NAME));
        stubConnectionHandle.closeConnection(CLUSTER_NAME);
        connectionNotExist();
    }

    /**
     * Method: isConnected(String clusterName)
     */
    @Test
    public void testIsConnected() throws ConnectionException {
        assertFalse("Connection is not connect", stubConnectionHandle.isConnected(CLUSTER_NAME));

        stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);
        StubsConnection theRealConnection = (StubsConnection) stubConnectionHandle.getOnlyOneConnection();

        assertTrue("Connection is connect", stubConnectionHandle.isConnected(CLUSTER_NAME));
        theRealConnection.setConnect(true);

        theRealConnection.setConnect(false);
        assertFalse("And now the connection is not connect", stubConnectionHandle.isConnected(CLUSTER_NAME));

    }

}
