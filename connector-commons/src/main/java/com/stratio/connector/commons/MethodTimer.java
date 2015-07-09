package com.stratio.connector.commons;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;

/**
 * Created by ccaballero on 7/9/15.
 */
@Aspect
public class MethodTimer {
    @Around("execution(* *(..)) && @annotation(TimerJ)")
    public Object around(ProceedingJoinPoint point) throws Throwable {
        long start = System.nanoTime();
        Object result = point.proceed();
        long finish = System.nanoTime() - start;
        String methodName = MethodSignature.class.cast(point.getSignature()).getMethod().getName();
        String methodArgs = point.getArgs().toString();
        System.out.println("Method "+methodName+" with args "+ methodArgs +" executed in "+finish+ " ns");
        return result;
    }
}