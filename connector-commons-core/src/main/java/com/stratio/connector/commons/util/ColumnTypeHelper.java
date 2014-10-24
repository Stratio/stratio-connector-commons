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

import java.math.BigInteger;

import com.stratio.crossdata.common.metadata.ColumnType;

/**
 * This class is a helper for CrossData ColumnType
 * Created by jmgomez on 24/10/14.
 */
public final class ColumnTypeHelper {

    /**
     * Constructor.
     */
    private ColumnTypeHelper(){}

    /**
     * Return a casting value adapt to columntType.
     * @param columnType the columnType.
     * @param value the value.
     * @return the casting value.
     */
    public static Object getCastingValue(ColumnType columnType, Object value){
        Object returnValue;
        switch (columnType){

            case BIGINT: returnValue = new BigInteger(value.toString()); //TODO review for other types.
            case DOUBLE:  returnValue = new Double(value.toString());
            case FLOAT: returnValue = new Float(value.toString());
            case INT: returnValue = new Integer(value.toString());
            default: returnValue = value;


        }

        return returnValue;
    }


}
