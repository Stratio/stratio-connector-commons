package com.stratio.connector.commons.util;

/**
 * Created by jmgomez on 25/03/15.
 */
public class TypeDecisor {

    public static boolean isString(Class field) {

        return  String.class.getSimpleName().equalsIgnoreCase(field.getSimpleName()); //to compatibility to scala

    }

    public static boolean isBoolean(Class field) {
        return  Boolean.class.getSimpleName().equalsIgnoreCase(field.getSimpleName()); //to compatibility to scala

    }

    public static boolean isDouble(Class field) {

        return  Double.class.getSimpleName().equalsIgnoreCase(field.getSimpleName()); //to compatibility to scala
    }

    public static <T> boolean isInteger(Class<T> field) {
        return  Integer.class.getSimpleName().equalsIgnoreCase(field.getSimpleName()); //to compatibility to scala
    }

    public static <T> boolean isLong(Class<T> field) {

        return  Long.class.getSimpleName().equalsIgnoreCase(field.getSimpleName()); //to compatibility to scala
    }

    public static boolean isNumber(Class field) {

        return  Number.class.getSimpleName().equalsIgnoreCase(field.getSimpleName()); //to compatibility to scala
    }
}
