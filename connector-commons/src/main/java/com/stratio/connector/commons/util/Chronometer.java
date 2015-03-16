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

import java.util.HashMap;
import java.util.Map;

/**
 * This class must get exexutions time.
 * Created by jmgomez on 24/02/15.
 */
public class Chronometer {

    /**
     * This map store the times.
     */
    private static Map<String,TimeCount> timer = new HashMap<>();

    /**
     * This method initialize the time count.
     * @param id
     */
    public static void  start(String id){
        timer.put(id,new TimeCount());
    }

    /**
     * This method stop the timer.
     * @param id the id.
     */
    public void stop(String id){
        timer.get(id).stop();
    }


    public String getMessage(String id){
        return "TIME - The execute time for ["+id+"] has been ["+timer.get(id).getTime() +" ms]";
    }

    /**
     * The timer count.
     */
    private static class TimeCount{

        /**
         * The start time.
         */
        private Long start;
        /**
         * The end time,.
         */
        private Long end;

        /**
         * Constructor. Initialize the time.
         */
        public TimeCount(){
            start = System.currentTimeMillis();
        }

        /**
         * This method stop the timer.
         */
        public void stop(){
            end = System.currentTimeMillis();
        }

        /**
         * This method return the time between start and end.
         * @return the time.
         */
        public Long getTime(){
            return end - start;
        }

    }
}
