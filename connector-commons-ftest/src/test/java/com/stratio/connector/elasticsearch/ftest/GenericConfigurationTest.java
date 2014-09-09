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

package com.stratio.connector.elasticsearch.ftest;


import org.junit.Test;

import static org.junit.Assert.fail;







public abstract class GenericConfigurationTest extends GenericConnectorTest {



    @Test
    public void supportedOperationsTest() throws Exception {


//    	assertTrue(connector.getSupportededOperations().containsKey(Operations.CREATE_CATALOG));
//    	assertTrue("insert is supported",stratioElasticConnector.getSupportededOperations().get(Operations.INSERT));

        fail("Wait for meta");

    }

    @Test
    public void connectionConfigurationTest() throws Exception {
//    	Set<ConnectionConfiguration> options =  stratioElasticConnector.getConnectionConfiguration();
//    	int numHostOption = 0;
//    	for(ConnectionConfiguration option: options){
//    		if (option.getConnectionOption() == ConnectionOption.HOST_IP) numHostOption++;
//    	}
//    	assertEquals( 1, numHostOption );

        fail("Wait for meta");


    }
 
 
}