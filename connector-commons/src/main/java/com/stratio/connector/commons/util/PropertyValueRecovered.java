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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.stratio.connector.commons.util.TypeDecisor.isBoolean;
import static com.stratio.connector.commons.util.TypeDecisor.isString;

/**
 * Created by jmgomez on 25/03/15.
 */
public class PropertyValueRecovered {


    /**
     * The Log.
     */
    private static final transient Logger LOGGER = LoggerFactory.getLogger(PropertyValueRecovered.class);

    /**
     * Constructor.
     */
    private PropertyValueRecovered() {
    }

    /**
     * This method recovered the array of properties.
     *
     * @param properties the string which represents the properties to recovered.
     * @param type       the type to recovered.
     * @return the properties.
     * @throws ExecutionException if an error happens.
     */

    public static <T> T[] recoveredValueASArray(Class<T> type, String properties) throws ExecutionException {

        LOGGER.info(String.format("Recovered propeties [%s] as [%s]", properties, type));
        String[] stringParseProperties = properties.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("]", "").split(",");
        T[] returnValue = null;
        if (isString(type)) {
            returnValue = (T[]) stringParseProperties;
        } else if (isBoolean(type)) {
            Boolean[] tempReturnValue = new Boolean[stringParseProperties.length];
            for (int i = 0; i < stringParseProperties.length; i++) {
                tempReturnValue[i] = Boolean.parseBoolean(stringParseProperties[i]);
            }
            returnValue = (T[]) tempReturnValue;
        } else {
            String message = String.format("The type %s is not supported for conversion.", type);
            LOGGER.error(message);
            throw new ExecutionException(String.format(message));
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("The property as converted to %s", Arrays.deepToString(returnValue)));
        }
        return returnValue;
    }


    /**
     * This method recovered the property.
     *
     * @param properties the string which represents the properties to recovered.
     * @param type       the type to recovered.
     * @return the properties.
     * @Throws ExecutionException if an error happens.
     */
    public static <T> T recoveredValue(Class<T> type, String properties) throws ExecutionException {
        return recoveredValueASArray(type, properties)[0];
    }

}