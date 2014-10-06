package com.stratio.connector.commons.util; 

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta2.common.statements.structures.selectors.BooleanSelector;
import com.stratio.meta2.common.statements.structures.selectors.FloatingPointSelector;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/** 
* SelectorHelper Tester. 
* 
* @author <Authors name> 
* @since <pre>oct 2, 2014</pre> 
* @version 1.0 
*/ 
public class SelectorHelperTest { 



/** 
* 
* Method: getValue(Class<T> type, Selector selector) 
* 
*/ 
@Test
public void testGetValueInteger() throws Exception {
      Object returnValue = null;

     returnValue = SelectorHelper.getValue(Integer.class, new IntegerSelector(10));
    assertEquals("The class is correct",Integer.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
    assertEquals("The value  is correct",10, returnValue);


  returnValue = SelectorHelper.getValue(Integer.class, new StringSelector("10"));
  assertEquals("The class is correct",Integer.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
  assertEquals("The value  is correct",10, returnValue);

    try {
        SelectorHelper.getValue(Integer.class, new StringSelector("10999898999989"));
        fail("It must not are here!");
    }catch(ExecutionException e) { //the number is too big
    }

    SelectorHelper.getValue(Integer.class, new FloatingPointSelector(new Double(10)));
    assertEquals("The class is correct",Integer.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
    assertEquals("The value  is correct",10, returnValue);


         SelectorHelper.getValue(Integer.class, new FloatingPointSelector(new Double(10.3)));
        assertEquals("The class is correct",Integer.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",10, returnValue);



    try {
     SelectorHelper.getValue(Integer.class, new BooleanSelector(false));
        fail("It must not are here!");
    }catch(ExecutionException e){//Is not possible cast a boolean to a int at now
    }



    try {
    returnValue = SelectorHelper.getValue(Integer.class, new StringSelector("LLfe3"));
        fail("It must not are here!");
    }catch(ExecutionException e){//Is not a number
    }
}

    @Test
    public void testGetValueLong() throws Exception {
        Object returnValue = null;

        returnValue = SelectorHelper.getValue(Long.class, new IntegerSelector(10));
        assertEquals("The class is correct",Long.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",new Long(10), returnValue);


        returnValue = SelectorHelper.getValue(Long.class, new StringSelector("10"));
        assertEquals("The class is correct",Long.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",new Long(10), returnValue);


        returnValue = SelectorHelper.getValue(Long.class, new FloatingPointSelector(new Double(10)));
        assertEquals("The class is correct",Long.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",new Long(10), returnValue);

        returnValue = SelectorHelper.getValue(Long.class, new FloatingPointSelector(new Double(10.5)));
        assertEquals("The class is correct",Long.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",new Long(10), returnValue);


        try {
            SelectorHelper.getValue(Integer.class, new StringSelector("10999898999989"));
            fail("It must not are here!");
        }catch(ExecutionException e) { //the number is too big
        }




        returnValue = SelectorHelper.getValue(Long.class, new FloatingPointSelector(new Double(10)));
            assertEquals("The class is correct",Long.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
            assertEquals("The value  is correct",new Long(10), returnValue);


        try {
            SelectorHelper.getValue(Long.class, new BooleanSelector(false));
            fail("It must not are here!");
        }catch(ExecutionException e){//Is not possible cast a boolean to a int at now
        }



        try {
             SelectorHelper.getValue(Long.class, new StringSelector("LLfe3"));
            fail("It must not are here!");
        }catch(ExecutionException e){//Is not a number
        }
    }



    @Test
    public void testGetValueString() throws Exception {
        Object returnValue = null;

        returnValue = SelectorHelper.getValue(String.class, new IntegerSelector(10));
        assertEquals("The class is correct",String.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",new String("10"), returnValue);


        returnValue = SelectorHelper.getValue(String.class, new StringSelector("10"));
        assertEquals("The class is correct",String.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",new String("10"), returnValue);


            SelectorHelper.getValue(String.class, new FloatingPointSelector(new Double(10)));
        assertEquals("The class is correct",String.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",new String("10"), returnValue);




        returnValue = SelectorHelper.getValue(String.class, new BooleanSelector(false));
        assertEquals("The class is correct",String.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",new String("false"), returnValue);

    }


    @Test
    public void testGetValueDouble() throws Exception {
        Object returnValue = null;

        returnValue = SelectorHelper.getValue(Double.class, new IntegerSelector(10));
        assertEquals("The class is correct",Double.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",new Double("10"), returnValue);


        returnValue = SelectorHelper.getValue(Double.class, new StringSelector("10"));
        assertEquals("The class is correct",Double.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",new Double("10"), returnValue);


        returnValue = SelectorHelper.getValue(String.class, new FloatingPointSelector(new Double(10)));
        assertEquals("The class is correct",String.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",new String("10.0"), returnValue);




        returnValue=     SelectorHelper.getValue(String.class, new BooleanSelector(false));
        assertEquals("The class is correct",String.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",new String("false"), returnValue);


    }


    @Test
    public void testGetValueBoolean() throws Exception {
        Object returnValue = null;

        try {
             SelectorHelper.getValue(Boolean.class, new IntegerSelector(10));
            fail("It must not are here!");
        }catch(ExecutionException e){
            //Is not boolean
        }


        try{
         SelectorHelper.getValue(Boolean.class, new StringSelector("true"));
            fail("It must not are here!");
        }catch(ExecutionException e){
            //Is not boolean
        }

        try{
        SelectorHelper.getValue(Boolean.class, new FloatingPointSelector(new Double(10)));
            fail("It must not are here!");
        }catch(ExecutionException e){
            //Is not boolean
        }




        returnValue = SelectorHelper.getValue(Boolean.class, new BooleanSelector(true));
        assertEquals("The class is correct",Boolean.class.getCanonicalName(), returnValue.getClass().getCanonicalName());
        assertEquals("The value  is correct",new Boolean(true), returnValue);



    }
}


