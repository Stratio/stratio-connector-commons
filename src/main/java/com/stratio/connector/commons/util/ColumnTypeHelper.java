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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.metadata.ColumnType;

/**
 * This class is a helper for CrossData ColumnType Created by jmgomez on 24/10/14.
 */
public final class ColumnTypeHelper {

    /**
     * The Log.
     */
    static final transient Logger LOGGER = LoggerFactory.getLogger(SelectorHelper.class);

    /**
     * Constructor.
     */
    private ColumnTypeHelper() {
    }

    /**
     * Return a casting value adapt to columntType.
     *
     * @param columnType the columnType.
     * @param value      the value.
     * @return the casting value.
     * @throws ExecutionException if the casting is not possible.
     */
    public static Object getCastingValue(ColumnType columnType, Object value) throws ExecutionException {
        validateInput(columnType);
        Object returnValue = null;
        if (value != null) {
            switch (columnType) {

            case BIGINT:
                ensureNumber(value);
                returnValue = ((Number) value).longValue();
                break;
            case DOUBLE:
                ensureNumber(value);
                returnValue = ((Number) value).doubleValue();
                break;
            case FLOAT:
                ensureNumber(value);
                returnValue = ((Number) value).floatValue();
                break;
            case INT:
                ensureNumber(value);
                returnValue = ((Number) value).intValue();
                break;
            default:
                returnValue = value;

            }
        }

        return returnValue;
    }

    /**
     * This method validate the Input.
     * @param columnType the columnType.
     * @throws ExecutionException if a exception happens.
     */
    private static void validateInput(ColumnType columnType) throws ExecutionException {
        if (columnType==null){
            String messagge = "The ColumnType can not be null.";
            LOGGER.error(messagge);
            throw new ExecutionException(messagge);
        }
    }

    /**
     * check if a object is a number.
     *
     * @param value the objet.
     * @return true if is a number false in other case.
     */
    private static boolean isNumber(Object value) {

        return Number.class.isAssignableFrom(value.getClass());
    }

    /**
     * Ensure if a object is a number.
     *
     * @param value the objet.
     * @throws ExecutionException if is not a number.
     */
    private static void ensureNumber(Object value) throws ExecutionException {

        if (!isNumber(value)) {
            String message = value.getClass().getCanonicalName() + " can not cast to a Number.";
            LOGGER.error(message);
            throw new ExecutionException(message);
        }
    }

}
