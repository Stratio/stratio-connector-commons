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

package com.stratio.connector.commons.connection;

/**
 * Created by jmgomez on 9/09/14.
 */
public class StubsConnection implements Connection {

    @Override
    public void close() {
        connect = false;
    }

    /**
     * this method is only for test.
     */
    public void setConnect(boolean connect) {
        this.connect = connect;
    }

    public boolean connect = true;

    @Override
    public boolean isConnect() {
        return connect;
    }

    @Override
    public Object getNativeConnection() {
        return "A connection";
    }
}
