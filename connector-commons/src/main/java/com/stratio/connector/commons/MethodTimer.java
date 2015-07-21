package com.stratio.connector.commons;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import com.stratio.crossdata.common.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to create the annotation TimerJ using aspectj.
 * Created by ccaballero on 7/9/15.
 */
@Aspect
public class MethodTimer {

    /**
     * The logger
     */
    private final transient Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Function that register metrics.
     * @param point
     * @return
     * @throws Throwable
     */
    @Around("execution(* *(..)) && @annotation(TimerJ)")
    public Object around(ProceedingJoinPoint point) throws Throwable {
        String methodName = MethodSignature.class.cast(point.getSignature()).getMethod().getName();
        String methodArgs = point.getArgs().toString();
        MetricRegistry metricRegistry = Metrics.getRegistry();
        Timer timer;

        String metricName = "Starting method "+methodName + "at "+System.nanoTime();
        synchronized (this) {
            if(metricRegistry.getTimers().containsKey(methodName)){
                timer = metricRegistry.getTimers().get(metricName);
            }
            else{
                timer = metricRegistry.register(metricName, new Timer());
            }
        }
        Timer.Context before = timer.time();
        Object result = point.proceed();
        long timeAfter = before.stop();

        logger.debug("Method " + methodName + " with args " + methodArgs + " executed in " + timeAfter + " ns");

        return result;
    }
}
