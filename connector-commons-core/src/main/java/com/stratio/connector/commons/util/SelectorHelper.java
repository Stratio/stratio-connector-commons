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

import com.stratio.meta2.common.statements.structures.selectors.*;

/**
 * Created by jmgomez on 17/09/14.
 */
public class SelectorHelper {

    public String getStringFieldValue(Selector selector) {
        String field = "";
        switch(selector.getType()){
            case COLUMN: field =  ((ColumnSelector)selector).getName().getName(); break;
            case BOOLEAN: field = String.valueOf(((BooleanSelector)selector).getValue()); break;
            case STRING: field =  ((StringSelector)selector).getValue(); break;
            case INTEGER: field = String.valueOf(((IntegerSelector)selector).getValue()); break;
            case FLOATING_POINT: field =((FloatingPointSelector) selector).toString(); break;
            default: throw new RuntimeException("Selector "+selector.getType()+" not supported get value operation.");
        }


        return field;
    }



}
