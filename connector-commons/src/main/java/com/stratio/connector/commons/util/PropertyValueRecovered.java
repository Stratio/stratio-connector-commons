package com.stratio.connector.commons.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;

import static com.stratio.connector.commons.util.TypeDecisor.*;
/**
 * Created by jmgomez on 25/03/15.
 */
public class PropertyValueRecovered {



    /**
     * The Log.
     */
    private transient static final Logger logger = LoggerFactory.getLogger(PropertyValueRecovered.class);

    /**
     * This method recovered the array of properties.
     *
     * @param properties the string which represents the properties to recovered.
     * @param type the type to recovered.
     * @return the properties.
     */

    public static <T>  T[] recoveredValueASArray(Class<T> type, String properties) {


        String[] stringParseProperties = properties.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("]", "").split(",");
        T[] returnValue = (T[]) Array.newInstance(type,0); //we ensure not return null values.
        if (isString(type)){
            returnValue = (T[])stringParseProperties;
        } else if (isBoolean(type)){
            Boolean[] tempReturnValue = new Boolean[stringParseProperties.length];
            for (int i=0;i<stringParseProperties.length;i++){
                tempReturnValue[i] = Boolean.parseBoolean(stringParseProperties[i]);
            }
            returnValue = (T[])tempReturnValue;
        }else{
            String message = String.format("The type $s is not supported for conversion.",type);
            logger.error(message);
            throw new RuntimeException(String.format(message));
        }


        return returnValue;
    }


    /**
     * This method recovered the property.
     *
     * @param properties the string which represents the properties to recovered.
     * @param type the type to recovered.
     * @return the properties.
     */
    public static <T>  T recoveredValue(Class<T> type, String properties){
        return recoveredValueASArray(type,properties)[0];
    }

}