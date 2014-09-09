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
package com.stratio.connector.elasticsearch.ftest.functionalMetadata;

import com.stratio.connector.elasticsearch.ftest.GenericConnectorTest;
import com.stratio.meta2.common.data.ClusterName;
import org.junit.Test;

import static org.junit.Assert.fail;


public abstract class GenericMetadataCreateTest extends GenericConnectorTest {


    @Test
	public void createTest()  {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createTest "+ clusterName.getName()+" ***********************************");
            fail("Not Implemented yet");


	}

    @Test
	public void createCollectionTest()  {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createCollectionTest "+ clusterName.getName()+" ***********************************");
        fail("Not Implemented yet");


	}
	
	
}