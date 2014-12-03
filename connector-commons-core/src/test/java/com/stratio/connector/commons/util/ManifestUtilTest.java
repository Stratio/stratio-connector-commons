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
package com.stratio.connector.commons.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * ManifestUtil Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>
 * oct 27, 2014
 * </pre>
 */
public class ManifestUtilTest {

    /**
     * Method: getDatastoreName(String pathManifest)
     */
    @Test
    public void testGetDatastoreName() throws Exception {
        String[] datastoreName = ManifestUtil.getDatastoreName("ExampleConnector.xml");
        assertEquals("The datastore number is correct", 2, datastoreName.length);
        assertEquals("The datastore 1  is correct", "elasticsearch1", datastoreName[0]);
        assertEquals("The datastore 2  is correct", "elasticsearch2", datastoreName[1]);

    }

    /**
     * Method: getConectorName(String pathManifest)
     */
    @Test
    public void testGetConectorName() throws Exception {

        assertEquals("The connectorName is correct", "elasticsearch", ManifestUtil.getConectorName("ExampleConnector"
                + ".xml"));
    }

    /**
     *
     * Method: getResult(Document document, String node)
     *
     */

}
