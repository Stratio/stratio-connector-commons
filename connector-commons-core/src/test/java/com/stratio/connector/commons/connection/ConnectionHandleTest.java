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
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;

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
    public void testCreateConnection() throws HandlerConnectionException {

        connectionNotExist();

        stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);

        assertNotNull("The connection exists", stubConnectionHandle.getConnection(CLUSTER_NAME));

    }

    @Test
    public void testCreateConnectionAlredyCrete() throws HandlerConnectionException {

        connectionNotExist();

        stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);
        try {
            stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);
            fail("should not get here");
        } catch (HandlerConnectionException e) {
            assertEquals("The message is correct", "The connection [" + CLUSTER_NAME + "] already exists",
                    e.getMessage());
        }

    }

    private void connectionNotExist() throws HandlerConnectionException {
        try {
            stubConnectionHandle.getConnection(CLUSTER_NAME);
            fail("should not get here");

        } catch (HandlerConnectionException e) {
            assertEquals("The mesahe is coorect", e.getMessage(),
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
    public void testIsConnected() throws HandlerConnectionException {
        assertFalse("Connection is not connect", stubConnectionHandle.isConnected(CLUSTER_NAME));

        stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);
        StubsConnection theRealConnection = (StubsConnection) stubConnectionHandle.getOnlyOneConnection();

        assertTrue("Connection is connect", stubConnectionHandle.isConnected(CLUSTER_NAME));
        theRealConnection.setConnect(true);

        theRealConnection.setConnect(false);
        assertFalse("And now the connection is not connect", stubConnectionHandle.isConnected(CLUSTER_NAME));

    }

}
