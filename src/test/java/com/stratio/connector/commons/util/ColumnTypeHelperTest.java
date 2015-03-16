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

import static junit.framework.TestCase.assertEquals;

import org.junit.Test;

import com.stratio.crossdata.common.metadata.ColumnType;

/**
 * ColumnTypeHelper Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>
 * oct 24, 2014
 * </pre>
 */
public class ColumnTypeHelperTest {

    /**
     * Method: getCastingValue(ColumnType columnType, Object value)
     */
    @Test
    public void testGetCastingValueLong() throws Exception {

        assertEquals("The type is correct", Long.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.BIGINT, new Integer(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Long.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.BIGINT, new Long(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Long.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.BIGINT, new Double(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Long.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.BIGINT, new Float(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Long.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.BIGINT, new Short("1")).getClass()
                        .getCanonicalName());
    }

    @Test
    public void testGetCastingValueDouble() throws Exception {

        assertEquals("The type is correct", Double.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.DOUBLE, new Integer(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Double.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.DOUBLE, new Long(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Double.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.DOUBLE, new Double(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Double.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.DOUBLE, new Float(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Double.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.DOUBLE, new Short("1")).getClass()
                        .getCanonicalName());
    }

    @Test
    public void testGetCastingValueFloat() throws Exception {

        assertEquals("The type is correct", Float.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.FLOAT, new Integer(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Float.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.FLOAT, new Long(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Float.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.FLOAT, new Double(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Float.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.FLOAT, new Float(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Float.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.FLOAT, new Short("1")).getClass()
                        .getCanonicalName());
    }

    @Test
    public void testGetCastingValueINT() throws Exception {

        assertEquals("The type is correct", Integer.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.INT, new Integer(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Integer.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.INT, new Long(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Integer.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.INT, new Double(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Integer.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.INT, new Float(5)).getClass()
                        .getCanonicalName());

        assertEquals("The type is correct", Integer.class.getCanonicalName(),
                ColumnTypeHelper.getCastingValue(ColumnType.INT, new Short("1")).getClass()
                        .getCanonicalName());
    }

    @Test
    public void testGetCastingReturnNull() throws Exception {
        assertEquals("The return is null", null,
                ColumnTypeHelper.getCastingValue(ColumnType.INT, null));
        assertEquals("The return is null", null,
                ColumnTypeHelper.getCastingValue(ColumnType.FLOAT, null));
        assertEquals("The return is null", null,
                ColumnTypeHelper.getCastingValue(ColumnType.DOUBLE, null));
        assertEquals("The return is null", null,
                ColumnTypeHelper.getCastingValue(ColumnType.BIGINT, null));

    }

}
