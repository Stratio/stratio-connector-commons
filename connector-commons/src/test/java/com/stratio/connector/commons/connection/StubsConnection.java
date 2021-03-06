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

/**
 * Created by jmgomez on 9/09/14.
 */
public class StubsConnection extends Connection {

    public boolean connect = true;

    @Override
    public void close() {
        connect = false;
    }

    @Override
    public boolean isConnected() {
        return connect;
    }

    /**
     * this method is only for test.
     */
    public void setConnect(boolean connect) {
        this.connect = connect;
    }

    @Override
    public Object getNativeConnection() {
        return "A connection";
    }
}
