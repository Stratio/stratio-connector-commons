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

package com.stratio.connector.commons.connection;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This interface represents a generic logic connection.
 * Created by jmgomez on 29/08/14.
 */
public abstract class Connection<T> {

    /**
     * The dateFormat.
     */
    private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    private String status;
    /**
     * The last use date.
     */
    private String lastDateInfo;
    /**
     * The work connection state.
     */
    private Boolean workInProgress;

    /**
     * Close the connection.
     */
    public abstract void close();

    /**
     * Ask if the connection is connected.
     *
     * @return true if the connection is connected. False in other case.
     */
    public abstract boolean isConnect();

    /**
     * Return a database native connection.
     *
     * @return the native connection.
     */
    public abstract T getNativeConnection();

    /**
     * Return the  date of last use.
     *
     * @return the date of last use.
     */
    public String getLastDateInfo() {
        return lastDateInfo;
    }

    /**
     * Return the work status.
     *
     * @return the work status.
     */
    public String getStatus() {
        return status;
    }

    /**
     * Check the connection status.
     *
     * @return true if the connections is in use. False in other case.
     */
    public Boolean isWorkInProgress() {
        return workInProgress;
    }

    /**
     * Set the connection status.
     *
     * @param workInProgress the connection work status.
     */
    public void setWorkInProgress(Boolean workInProgress) {
        if (workInProgress) {
            status = "Work in Progress";
        } else {
            status = "Work Finished";
        }
        lastDateInfo = dateFormat.format(new Date());
        this.workInProgress = workInProgress;
    }
}
