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

/**
 * This class must decide the basic type of a field.
 * Created by jmgomez on 25/03/15.
 */
public class TypeDecisor {

    /**
     * Constructor.
     */
    private TypeDecisor() {
    }

    /**
     * Test if a field is string.
     *
     * @param field the field.
     * @return true  if field is string. False in other case.
     */
    public static boolean isString(Class field) {
        //to compatibility to scala
        return String.class.getSimpleName().equalsIgnoreCase(field.getSimpleName());

    }

    /**
     * Test if a field is Boolean.
     *
     * @param field the field.
     * @return true  if field is Boolean. False in other case.
     */
    public static boolean isBoolean(Class field) {
        //to compatibility to scala
        return Boolean.class.getSimpleName().equalsIgnoreCase(field.getSimpleName());

    }

    /**
     * Test if a field is Double.
     *
     * @param field the field.
     * @return true  if field is Double. False in other case.
     */
    public static boolean isDouble(Class field) {
        //to compatibility to scala
        return Double.class.getSimpleName().equalsIgnoreCase(field.getSimpleName());
    }


    /**
     * Test if a field is Integer.
     *
     * @param field the field.
     * @return true  if field is Integer. False in other case.
     */
    public static <T> boolean isInteger(Class<T> field) {
        //to compatibility to scala
        return Integer.class.getSimpleName().equalsIgnoreCase(field.getSimpleName());
    }

    /**
     * Test if a field is Long.
     *
     * @param field the field.
     * @return true  if field is Long. False in other case.
     */
    public static <T> boolean isLong(Class<T> field) {
        //to compatibility to scala
        return Long.class.getSimpleName().equalsIgnoreCase(field.getSimpleName());
    }

    /**
     * Test if a field is Number.
     *
     * @param field the field.
     * @return true  if field is Number. False in other case.
     */
    public static boolean isNumber(Class field) {

        return Number.class.isAssignableFrom(field);
    }
}
