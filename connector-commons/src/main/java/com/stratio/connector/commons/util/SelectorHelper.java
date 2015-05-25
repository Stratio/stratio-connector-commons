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

import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.statements.structures.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.stratio.connector.commons.util.TypeDecisor.*;

/**
 * This class is a helper for the selector crossdata. Created by jmgomez on 17/09/14.
 */
public final class SelectorHelper {

    /**
     * The Log.
     */
    static final transient Logger LOGGER = LoggerFactory.getLogger(SelectorHelper.class);

    /**
     * Private constructor.
     */
    private SelectorHelper() {

    }

    /**
     * Return the selector value represents in the type class.
     *
     * @param type     the type in witch the value will be return.
     * @param selector the selector.
     * @return the type value.
     * @throws ExecutionException if an error happens.
     */
    public static <T> T getValue(Class<T> type, Selector selector) throws ExecutionException {

        return convert(getValue(selector), type);
    }

    /**
     * Return the selector value.
     *
     * @param selector the selector.
     * @return the type value.
     * @throws ExecutionException if an error happens.
     */
    public static Object getValue(Selector selector) throws ExecutionException {
        return getRestrictedValue(selector, null);

    }

    /**
     * Return the selector value only if the type matches with the specified value.
     *
     * @param selector the selector.
     * @param type     the type of the expected selector
     * @return the corresponding value or null if the selector type does not match.
     * @throws ExecutionException if an error happens.
     */
    public static Object getRestrictedValue(Selector selector, SelectorType type) throws ExecutionException {
        Object field = null;

        if (type != null && selector.getType() != type) {
            throw new ExecutionException("The selector type expected is: " + type + " but received: "
                    + selector.getType());
        }

        switch (selector.getType()) {

            case COLUMN:
                field = ((ColumnSelector) selector).getName().getName();
                break;
            case BOOLEAN:
                field = ((BooleanSelector) selector).getValue();
                break;
            case STRING:
                field = ((StringSelector) selector).getValue();
                break;
            case INTEGER:
                field = ((IntegerSelector) selector).getValue();
                break;
            case FLOATING_POINT:
                field = ((FloatingPointSelector) selector).getValue();
                break;

            default:
                throw new ExecutionException("Selector " + selector.getType() + " not supported get value operation.");
        }

        return field;

    }

    /**
     * Return the selector value class.
     *
     * @param selector the selector.
     * @return the selector class.
     * @throws ExecutionException if an error happens.
     */
    public static Class getClass(Selector selector) throws ExecutionException {
        Class returnClass = null;
        switch (selector.getType()) {
            case STRING:
            case COLUMN:
                returnClass = String.class;
                break;
            case BOOLEAN:
                returnClass = Boolean.class;
                break;
            case INTEGER:
                returnClass = Long.class;
                break;
            case FLOATING_POINT:
                returnClass = Double.class;
                break;
            default:
                throw new ExecutionException("Selector " + selector.getType() + " not supported get value operation.");
        }

        return returnClass;
    }

    private static <T> T convert(Object field, Class<T> type) throws ExecutionException {
        T returnValue = null;
        try {
            Object value = field;
            if (isInteger(type)) {
                value = convertInteger(field);
            } else if (isLong(type)) {
                value = convertLong(field);
            } else if (isDouble(type)) {
                value = convertDouble(field);
            } else if (isString(type)) {
                value = field.toString();
            } else if (isBoolean(type)) {
                value = convertBoolean(field);
            }
            returnValue = type.cast(value);

        } catch (ClassCastException | NumberFormatException e) {
            String msg = "Error recovering selector value. " + e.getMessage();
            LOGGER.error(msg);
            throw new ExecutionException(msg, e);
        }
        return returnValue;
    }

    private static Object convertBoolean(Object field) throws ExecutionException {
        Boolean returnValue;
        if (isBoolean(field.getClass())) {
            returnValue = (Boolean) field;
        } else if (isString(field.getClass())) {
            returnValue = Boolean.valueOf((String) field);
        } else {
            throw new ExecutionException("The field " + field + " cannot be parsed");
        }
        return returnValue;
    }

    private static Double convertDouble(Object field) throws ExecutionException {
        Double returnValue;
        if (isString(field.getClass())) {
            returnValue = Double.parseDouble((String) field);
        } else if (isNumber(field.getClass())) {
            returnValue = convertNumeric(Double.class, (Number) field);
        } else {
            returnValue = (Double) field;
        }
        return returnValue;

    }

    private static Long convertLong(Object field) throws ExecutionException {
        Long returnValue;
        if (isString(field.getClass())) {
            returnValue = Long.parseLong((String) field);
        } else if (isNumber(field.getClass())) {
            returnValue = convertNumeric(Long.class, (Number) field);
        } else {
            returnValue = (Long) field;
        }
        return returnValue;
    }

    private static Integer convertInteger(Object field) throws ExecutionException {
        Integer returnValue;
        if (isString(field.getClass())) {
            returnValue = Integer.parseInt((String) field);
        } else if (isNumber(field.getClass())) {
            returnValue = convertNumeric(Integer.class, (Number) field);
        } else {
            returnValue = (Integer) field;
        }
        return returnValue;

    }

    private static <T extends Number> T convertNumeric(Class<T> type, Number field) throws ExecutionException {
        T returnValue;
        if (isInteger(type)) {
            returnValue = (T) Integer.valueOf(field.intValue());
        } else if (isLong(type)) {
            returnValue = (T) Long.valueOf(field.longValue());
        } else if (isDouble(type)) {
            returnValue = (T) Double.valueOf(field.doubleValue());
        } else {
            String msg = "The number " + field + "can not be parse into " + type + ".";
            LOGGER.error(msg);
            throw new ExecutionException(msg);
        }

        return returnValue;
    }


}
