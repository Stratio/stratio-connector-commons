package com.stratio.connector.commons.connection; 

import static junit.framework.Assert.assertSame;
import static junit.framework.TestCase.assertNotNull;

import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import com.stratio.connector.commons.connection.dummy.DummyConnector;

/** 
* Connection Tester. 
* 
* @author <Authors name> 
* @since <pre>dic 12, 2014</pre> 
* @version 1.0 
*/ 
public class ConnectionTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: getSessionObject(Class<T> type, String name) 
* 
*/ 
@Test
public void testSession() throws Exception {
 DummyConnector connector = new DummyConnector();
    String identifator = "identifator";
    Integer value = new Integer(5);
    connector.addObjectToSession(identifator, value);
    Object sessionObject = connector.getSessionObject(Integer.class, identifator);
    assertNotNull("The object must be in the session", sessionObject);
    assertSame("The object must be the object inserted before.", value,sessionObject );
}





} 
