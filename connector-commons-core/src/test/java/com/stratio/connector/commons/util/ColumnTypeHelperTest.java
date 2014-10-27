package com.stratio.connector.commons.util;

import static junit.framework.TestCase.assertEquals;

import java.math.BigInteger;

import org.junit.Test;

import com.stratio.crossdata.common.metadata.ColumnType;

/**
 * ColumnTypeHelper Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>oct 24, 2014</pre>
 */
public class ColumnTypeHelperTest {

    /**
     * Method: getCastingValue(ColumnType columnType, Object value)
     */
    @Test
    public void testGetCastingValueBIGINT() throws Exception {

        assertEquals("The type is correct", BigInteger.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.BIGINT, new Integer(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", BigInteger.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.BIGINT, new Long(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", BigInteger.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.BIGINT, new Double(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", BigInteger.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.BIGINT, new Float(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", BigInteger.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.BIGINT, new Short("1")).getClass().getCanonicalName());
    }

    @Test
    public void testGetCastingValueDouble() throws Exception {

        assertEquals("The type is correct", Double.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.DOUBLE, new Integer(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", Double.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.DOUBLE, new Long(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", Double.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.DOUBLE, new Double(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", Double.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.DOUBLE, new Float(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", Double.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.DOUBLE, new Short("1")).getClass().getCanonicalName());
    }

    @Test
    public void testGetCastingValueFloat() throws Exception {

        assertEquals("The type is correct", Float.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.FLOAT, new Integer(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", Float.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.FLOAT, new Long(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", Float.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.FLOAT, new Double(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", Float.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.FLOAT, new Float(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", Float.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.FLOAT, new Short("1")).getClass().getCanonicalName());
    }

    @Test
    public void testGetCastingValueINT() throws Exception {

        assertEquals("The type is correct", Integer.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.INT, new Integer(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", Integer.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.INT, new Long(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", Integer.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.INT, new Double(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", Integer.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.INT, new Float(5)).getClass().getCanonicalName());

        assertEquals("The type is correct", Integer.class.getCanonicalName(), ColumnTypeHelper.getCastingValue
                (ColumnType.INT, new Short("1")).getClass().getCanonicalName());
    }

} 
