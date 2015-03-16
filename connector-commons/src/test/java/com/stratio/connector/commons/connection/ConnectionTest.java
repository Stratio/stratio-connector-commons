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
package com.stratio.connector.commons.connection;

import static junit.framework.Assert.assertSame;
import static junit.framework.TestCase.assertNotNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.commons.connection.dummy.DummyConnector;

/**
 * Connection Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>dic 12, 2014</pre>
 */
public class ConnectionTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: getSessionObject(Class<T> type, String name)
     */
    @Test
    public void testSession() throws Exception {
        DummyConnector connector = new DummyConnector();
        String identifator = "identifator";
        Integer value = new Integer(5);
        connector.addObjectToSession(identifator, value);
        Object sessionObject = connector.getSessionObject(Integer.class, identifator);
        assertNotNull("The object must be in the session", sessionObject);
        assertSame("The object must be the object inserted before.", value, sessionObject);
    }

}
