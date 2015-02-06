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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * This interface represents a generic logic connection. Created by jmgomez on 29/08/14.
 *
 * @param <T>
 *            the native client
 */
public abstract class Connection<T> {

    /**
     * The session.
     */
    private Map<String, Object> session;

    /**
     * The dateFormat.
     */
    private static final String FILENAME_DATE_PATTERN = "yyyy-MM-dd HH:mm";

    /**
     * The last use date.
     */
    private String lastDateInfo;
    /**
     * The job connection state.
     */
    private Boolean jobInProgress = false;

    /**
     * Recovered a object from the session.
     * 
     * @param type
     *            the object type.
     * @param name
     *            the object name.
     * @param <T>
     *            the object type.
     * @return the object.
     */
    public <T> T getSessionObject(Class<T> type, String name) {
        return (T) session.get(name);
    }

    /**
     * Add a object into the session.
     * 
     * @param name
     *            the object name.
     * @param value
     *            the objecet value.
     */
    public void addObjectToSession(String name, Object value) {
        if (session == null) {
            session = new HashMap<>();
        }

        session.put(name, value);
    }

    /**
     * Close the connection.
     */
    public abstract void close();

    /**
     * Ask if the connection is connected.
     *
     * @return true if the connection is connected. False in other case.
     */
    public abstract boolean isConnected();

    /**
     * Return a database native connection.
     *
     * @return the native connection.
     */
    public abstract T getNativeConnection();

    /**
     * Return the date of last use.
     *
     * @return the date of last use.
     */
    public String getLastDateInfo() {
        return lastDateInfo;
    }

    /**
     * Check the connection status.
     *
     * @return true if the connections is in use. False in other case.
     */
    public Boolean hasPendingJobs() {
        return jobInProgress;
    }

    /**
     * Set the connection status.
     *
     * @param workInProgress
     *            the connection work status.
     */
    public void setJobInProgress(Boolean workInProgress) {
        lastDateInfo = new SimpleDateFormat(FILENAME_DATE_PATTERN).format(new Date());
        this.jobInProgress = workInProgress;
    }
}
