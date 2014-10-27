package com.stratio.connector.commons.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * ManifestUtil Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>oct 27, 2014</pre>
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

        assertEquals("The connectorName is correct", "elasticsearch", ManifestUtil.getConectorName("ExampleConnector" +
                ".xml"));
    }

    /**
     *
     * Method: getResult(Document document, String node)
     *
     */

} 
