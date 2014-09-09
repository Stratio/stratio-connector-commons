package com.stratio.connector.commons.connection;

import com.stratio.connector.commons.connection.exceptions.HandleConnectionException;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/** 
* ConnectionHandle Tester. 
* 
* @author <Authors name> 
* @since <pre>sep 9, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)
public class ConnectionHandleTest {

    public static final String CLUSTER_NAME = "cluster_name";




    private StubConnectionHandle stubConnectionHandle;
    @Mock  ICredentials credentials;
    @Mock  ConnectorClusterConfig conenectoClusterConfig;
    @Mock  IConfiguration configuration;



    @Before
public void before() throws Exception {

        when(conenectoClusterConfig.getName()).thenReturn(new ClusterName(CLUSTER_NAME));
        stubConnectionHandle = new StubConnectionHandle(configuration);
}



    @After
public void after() throws Exception { 
} 

/** 
* 
* Method: createConnection(ICredentials credentials, ConnectorClusterConfig config) 
* 
*/ 
@Test
public void testCreateConnection() throws HandleConnectionException {


    connectionNotExist();

    stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);

    assertNotNull("The connection exists", stubConnectionHandle.getConnection(CLUSTER_NAME));

}

    @Test
    public void testCreateConnectionAlredyCrete() throws HandleConnectionException {


        connectionNotExist();

        stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);
        try {
            stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);
            fail("should not get here");
        }catch(HandleConnectionException e){
            assertEquals("The message is correct","The connection ["+CLUSTER_NAME+"] already exists", e.getMessage());
        }


    }

    private void connectionNotExist() throws HandleConnectionException {
        try {
            stubConnectionHandle.getConnection(CLUSTER_NAME);
            fail("should not get here");

        }catch(HandleConnectionException e){
            assertEquals("The mesahe is coorect",e.getMessage(),"The connection ["+CLUSTER_NAME+"] does not exist");

        }
    }


    /**
* 
* Method: closeConnection(String clusterName) 
* 
*/ 
@Test
public void testCloseConnection() throws Exception, HandleConnectionException {

    stubConnectionHandle.closeConnection(CLUSTER_NAME); //Close a not exist connection.

    stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);
    assertNotNull("The connection exists", stubConnectionHandle.getConnection(CLUSTER_NAME));
    stubConnectionHandle.closeConnection(CLUSTER_NAME);
    connectionNotExist();
} 

/** 
* 
* Method: isConnected(String clusterName) 
* 
*/ 
@Test
public void testIsConnected() throws HandleConnectionException {
    assertFalse("Connection is not connect", stubConnectionHandle.isConnected(CLUSTER_NAME));


    stubConnectionHandle.createConnection(credentials, conenectoClusterConfig);
    StubsConnection theRealConnection = (StubsConnection)stubConnectionHandle.getOnlyOneConnection();

    assertTrue("Connection is connect", stubConnectionHandle.isConnected(CLUSTER_NAME));
    theRealConnection.setConnect(true);

    theRealConnection.setConnect(false);
    assertFalse("And now the connection is not connect", stubConnectionHandle.isConnected(CLUSTER_NAME));

}








}
