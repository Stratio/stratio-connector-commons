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

import java.util.HashSet;
import java.util.Set;

import static com.stratio.crossdata.common.metadata.Operations.*;

public final class FilterHelper {

    private static Set pkFilters;

    static {
        pkFilters = new HashSet();
        pkFilters.add(FILTER_PK_NOT_EQ);
        pkFilters.add(FILTER_PK_EQ);
        pkFilters.add(FILTER_PK_GET);
        pkFilters.add(FILTER_PK_GT);
        pkFilters.add(FILTER_PK_LET);
        pkFilters.add(FILTER_PK_LT);
    }

    /**
     * Constructor.
     */
    private FilterHelper() {
    }

    /**
     * This method verify if a filter is PK type.
     *
     * @param filter the filter.
     * @return true if the filter is PK type. False in other case.
     */
    public static boolean isPK(Filter filter) {
        return pkFilters.containsAll(filter.getOperations());


    }

}
