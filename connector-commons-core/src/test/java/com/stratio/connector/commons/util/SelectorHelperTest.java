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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Collections;

import org.junit.Test;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.statements.structures.AsteriskSelector;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.FloatingPointSelector;
import com.stratio.crossdata.common.statements.structures.FunctionSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.RelationSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.SelectorType;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * SelectorHelper Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>
 * oct 2, 2014
 * </pre>
 */
public class SelectorHelperTest {

    /**
     * Method: getValue(Class<T> type, Selector selector)
     */

    @Test
    public void testGetValueInteger() throws Exception {
        Object returnValue = null;

        returnValue = SelectorHelper.getValue(Integer.class, new IntegerSelector(10));
        assertEquals("The class is correct", Integer.class.getCanonicalName(), returnValue.getClass()
                        .getCanonicalName());
        assertEquals("The value  is correct", 10, returnValue);

        returnValue = SelectorHelper.getValue(Integer.class, new StringSelector("10"));
        assertEquals("The class is correct", Integer.class.getCanonicalName(), returnValue.getClass()
                        .getCanonicalName());
        assertEquals("The value  is correct", 10, returnValue);

        try {
            SelectorHelper.getValue(Integer.class, new StringSelector("10999898999989"));
            fail("It must not are here!");
        } catch (ExecutionException e) { // the number is too big
        }

        SelectorHelper.getValue(Integer.class, new FloatingPointSelector(new Double(10)));
        assertEquals("The class is correct", Integer.class.getCanonicalName(), returnValue.getClass()
                        .getCanonicalName());
        assertEquals("The value  is correct", 10, returnValue);

        SelectorHelper.getValue(Integer.class, new FloatingPointSelector(new Double(10.3)));
        assertEquals("The class is correct", Integer.class.getCanonicalName(), returnValue.getClass()
                        .getCanonicalName());
        assertEquals("The value  is correct", 10, returnValue);

        try {
            SelectorHelper.getValue(Integer.class, new BooleanSelector(false));
            fail("It must not are here!");
        } catch (ExecutionException e) {// Is not possible cast a boolean to a int at now
        }

        try {
            returnValue = SelectorHelper.getValue(Integer.class, new StringSelector("LLfe3"));
            fail("It must not are here!");
        } catch (ExecutionException e) {// Is not a number
        }
    }

    @Test
    public void testGetClass() throws ExecutionException {

        Selector[] selector = { new StringSelector(""), new ColumnSelector(mock(ColumnName.class)),
                        new BooleanSelector(true), new FloatingPointSelector("1"), new IntegerSelector(1) };

        Class[] returnClass = { String.class, String.class, Boolean.class, Double.class, Long.class };
        for (int i = 0; i < selector.length; i++) {
            assertEquals("The retur class is correct", returnClass[i], SelectorHelper.getClass(selector[i]));
        }

    }

    @Test
    public void testGetClassException() {

        Selector[] exceptionSelector = { new AsteriskSelector(), new FunctionSelector("", Collections.EMPTY_LIST),
                        new RelationSelector(mock(Relation.class)) };

        for (int i = 0; i < exceptionSelector.length; i++) {

            try {
                SelectorHelper.getClass(exceptionSelector[i]);
                fail("Not must are here");
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testGetValueLong() throws Exception {
        Object returnValue = null;

        returnValue = SelectorHelper.getValue(Long.class, new IntegerSelector(10));
        assertEquals("The class is correct", Long.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct", new Long(10), returnValue);

        returnValue = SelectorHelper.getValue(Long.class, new StringSelector("10"));
        assertEquals("The class is correct", Long.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct", new Long(10), returnValue);

        returnValue = SelectorHelper.getValue(Long.class, new FloatingPointSelector(new Double(10)));
        assertEquals("The class is correct", Long.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct", new Long(10), returnValue);

        returnValue = SelectorHelper.getValue(Long.class, new FloatingPointSelector(new Double(10.5)));
        assertEquals("The class is correct", Long.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct", new Long(10), returnValue);

        try {
            SelectorHelper.getValue(Integer.class, new StringSelector("10999898999989"));
            fail("It must not are here!");
        } catch (ExecutionException e) { // the number is too big
        }

        returnValue = SelectorHelper.getValue(Long.class, new FloatingPointSelector(new Double(10)));
        assertEquals("The class is correct", Long.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct", new Long(10), returnValue);

        try {
            SelectorHelper.getValue(Long.class, new BooleanSelector(false));
            fail("It must not are here!");
        } catch (ExecutionException e) {// Is not possible cast a boolean to a int at now
        }

        try {
            SelectorHelper.getValue(Long.class, new StringSelector("LLfe3"));
            fail("It must not are here!");
        } catch (ExecutionException e) {// Is not a number
        }
    }

    @Test
    public void testGetValueString() throws Exception {
        Object returnValue = null;

        returnValue = SelectorHelper.getValue(String.class, new IntegerSelector(10));
        assertEquals("The class is correct", String.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct", new String("10"), returnValue);

        returnValue = SelectorHelper.getValue(String.class, new StringSelector("10"));
        assertEquals("The class is correct", String.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct", new String("10"), returnValue);

        SelectorHelper.getValue(String.class, new FloatingPointSelector(new Double(10)));
        assertEquals("The class is correct", String.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct", new String("10"), returnValue);

        returnValue = SelectorHelper.getValue(String.class, new BooleanSelector(false));
        assertEquals("The class is correct", String.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct", new String("false"), returnValue);

    }

    @Test
    public void testGetValueDouble() throws Exception {
        Object returnValue = null;

        returnValue = SelectorHelper.getValue(Double.class, new IntegerSelector(10));
        assertEquals("The class is correct", Double.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct", new Double("10"), returnValue);

        returnValue = SelectorHelper.getValue(Double.class, new StringSelector("10"));
        assertEquals("The class is correct", Double.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct", new Double("10"), returnValue);

        returnValue = SelectorHelper.getValue(String.class, new FloatingPointSelector(new Double(10)));
        assertEquals("The class is correct", String.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct", new String("10.0"), returnValue);

        returnValue = SelectorHelper.getValue(String.class, new BooleanSelector(false));
        assertEquals("The class is correct", String.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct", new String("false"), returnValue);

    }

    @Test
    public void testGetValueBoolean() throws Exception {
        Object returnValue = null;

        try {
            SelectorHelper.getValue(Boolean.class, new IntegerSelector(10));
            fail("It must not be here!");
        } catch (ExecutionException e) {
            // Is not boolean
        }

        returnValue = SelectorHelper.getValue(Boolean.class, new StringSelector("true"));
        assertEquals("The class is correct", Boolean.class.getCanonicalName(), returnValue.getClass()
                        .getCanonicalName());
        assertEquals("The value  is correct", new Boolean(true), returnValue);

        try {
            SelectorHelper.getValue(Boolean.class, new FloatingPointSelector(new Double(10)));
            fail("It must not be here!");
        } catch (ExecutionException e) {
            // Is not boolean
        }

        returnValue = SelectorHelper.getValue(Boolean.class, new BooleanSelector(true));
        assertEquals("The class is correct", Boolean.class.getCanonicalName(), returnValue.getClass()
                        .getCanonicalName());
        assertEquals("The value  is correct", new Boolean(true), returnValue);

    }

    @Test(expected = ExecutionException.class)
    public void getIncompatibleValueTest() throws Exception {
        SelectorHelper.getRestrictedValue(new ColumnSelector(
                        new ColumnName(new TableName("catalog", "table"), "column")), SelectorType.BOOLEAN);

    }

    @Test
    public void getRestrictedValueTest() throws Exception {
        String value = (String) SelectorHelper.getRestrictedValue(new ColumnSelector(new ColumnName(new TableName(
                        "catalog", "table"), "column")), SelectorType.COLUMN);
        assertEquals("column", value);
    }
}
