package com.stratio.connector.commons;

import java.lang.annotation.*;
import java.util.concurrent.TimeUnit;

@Documented
@Target(ElementType.METHOD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface TimerJ {
    String getString() default "";
    boolean print() default true;
}
