package com.stratio.connector.commons.util;

/**
 * Created by jmgomez on 25/03/15.
 */
public class TypeDecisor {

    public static boolean isString(Class field) {
        return String.class.getCanonicalName().equals(field.getCanonicalName());

    }

    public static boolean isBoolean(Class field) {
        return Boolean.class.getCanonicalName().equals(field.getCanonicalName());

    }

    public static boolean isDouble(Class field) {
        return Double.class.isAssignableFrom(field);
    }

    public static <T> boolean isInteger(Class<T> field) {
        return Integer.class.isAssignableFrom(field);
    }

    public static <T> boolean isLong(Class<T> field) {
        return Long.class.isAssignableFrom(field);
    }

    public static boolean isNumber(Class field) {

        return Number.class.isAssignableFrom(field);
    }
}