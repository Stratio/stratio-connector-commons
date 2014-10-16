/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.connector.commons.connection.exceptions;

/**
 * This exception represents a exception in the handling of a connection.
 * Created by jmgomez on 9/09/14.
 */
public class CreateNativeConnectionException extends HandlerConnectionException {

    /**
     * Constructor.
     *
     * @param msg the message.
     */
    public CreateNativeConnectionException(String msg) {

        super(msg);
    }

    /**
     * Constructor.
     *
     * @param msg       the message.
     * @param exception the original exception.
     */
    public CreateNativeConnectionException(String msg, Throwable exception) {

        super(msg, exception);
    }

}
