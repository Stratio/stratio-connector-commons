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
 * This interface represents a generic connection to be handle.
 * Created by jmgomez on 29/08/14.
 */
public interface Connection<T> {


    /**
     * Close the connection.
     */
    public void close();

    /**
     * Ask if the connection is connected.
     *
     * @return true if the connection is connected. False in other case.
     */
    public boolean isConnect();

    /**
     * Return a database native connection.
     *
     * @return the native connection.
     */
    public T getNativeConnection();

}
