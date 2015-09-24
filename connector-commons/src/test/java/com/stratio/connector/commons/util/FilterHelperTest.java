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

import com.stratio.crossdata.common.logicalplan.Filter;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static com.stratio.crossdata.common.metadata.Operations.*;

/**
 * ColumnTypeHelper Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>
 * oct 24, 2014
 * </pre>
 */
public class FilterHelperTest {


    @Test
    public void testPkDistinct() throws Exception {
        Set pk = new HashSet<>();
        pk.add(FILTER_PK_NOT_EQ);
        FilterHelper.isPK(new Filter(pk, null));
    }


    @Test
    public void testPkEQ() throws Exception {
        Set pk = new HashSet<>();
        pk.add(FILTER_PK_EQ);
        FilterHelper.isPK(new Filter(pk, null));
    }

    @Test
    public void testPkGET() throws Exception {
        Set pk = new HashSet<>();
        pk.add(FILTER_PK_GET);
        FilterHelper.isPK(new Filter(pk, null));
    }

    @Test
    public void testPkGT() throws Exception {
        Set pk = new HashSet<>();
        pk.add(FILTER_PK_GT);
        FilterHelper.isPK(new Filter(pk, null));
    }

    @Test
    public void testPkLET() throws Exception {
        Set pk = new HashSet<>();
        pk.add(FILTER_PK_LET);
        FilterHelper.isPK(new Filter(pk, null));
    }

    @Test
    public void testPkLT() throws Exception {
        Set pk = new HashSet<>();
        pk.add(FILTER_PK_LT);
        FilterHelper.isPK(new Filter(pk, null));
    }


}
