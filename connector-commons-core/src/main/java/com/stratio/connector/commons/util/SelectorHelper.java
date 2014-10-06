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

package com.stratio.connector.commons.util;

import java.util.regex.Pattern;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta2.common.statements.structures.selectors.BooleanSelector;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.FloatingPointSelector;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/**
 * Created by jmgomez on 17/09/14.
 */
public class SelectorHelper {

    /**
     * The Log.
     */
    final transient static Logger logger = LoggerFactory.getLogger(SelectorHelper.class);


    public static <T> T getValue(Class<T> type, Selector selector) throws ExecutionException {
        Object field = null;
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

        return convert(field, type);
    }

    public static Object getValue(Selector selector) throws ExecutionException {
        Object field = null;
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



    private static <T> T convert(Object field, Class<T> type) throws ExecutionException {
        T returnValue = null;
        try {
            Object value = field;
            if (isInteger(type)) {
                value = convertInteger(field);
            } else if (isLong(type)) {
                value = convertLong(field);
            }else if (isDouble(type)){
                value = convertDouble(field);
            }else if (isString(type)){
                value = field.toString();
            }
                returnValue = type.cast(value);

        }catch(ClassCastException | NumberFormatException e) {
            String msg = "Error recovering selector value. "+e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg,e);
        }
        return returnValue;
    }

    private static Double convertDouble(Object field) throws ExecutionException {
        Double returnValue;
        if (isString(field.getClass())){
            returnValue = Double.parseDouble((String) field);
        }else if (isNumber(field.getClass())) {
            returnValue = convertNumeric(Double.class, (Number)field);
        }else {
            returnValue = (Double)field;
        }
        return returnValue;


    }

    private static Long convertLong(Object field) throws ExecutionException {
        Long returnValue;
        if (isString(field.getClass())){
            returnValue = Long.parseLong((String) field);
        }else if (isNumber(field.getClass())) {
            returnValue = convertNumeric(Long.class, (Number) field);
        }else {
            returnValue = (Long)field;
        }
        return returnValue;
    }

    private static Integer convertInteger(Object field) throws ExecutionException {
        Integer returnValue;
        if (isString(field.getClass())){
            returnValue = Integer.parseInt((String)field);
        }else if (isNumber(field.getClass())) {
            returnValue = convertNumeric(Integer.class, (Number) field);
        } else {
            returnValue = (Integer)field;
        }
        return returnValue;


    }




    private static <T extends Number> T convertNumeric(Class<T> type, Number field) throws ExecutionException {
        T returnValue;
        if (isInteger(type)) {
            returnValue = (T)new Integer(field.intValue());
        }else if (isLong(type)){
            returnValue = (T)new Long(field.longValue());
        }else if (isDouble(type)){
            returnValue = (T)new Double(field.doubleValue());
        }else {
            String msg ="The number "+field+"can not be parse into "+type+".";
            logger.error(msg);
            throw  new ExecutionException(msg);
        }

        return returnValue;
    }






    private static boolean isString(Class field) {
        return String.class.getCanonicalName().equals(field.getCanonicalName());

    }

    private static boolean isDouble(Class field) {
        return Double.class.isAssignableFrom(field);
    }


    private static <T> boolean isInteger(Class<T> field) {
        return Integer.class.isAssignableFrom(field);
    }

    private static <T> boolean isLong(Class<T> field) {
        return Long.class.isAssignableFrom(field);
    }

    private static boolean isNumber(Class field) {


        return Number.class.isAssignableFrom(field);
    }




}
