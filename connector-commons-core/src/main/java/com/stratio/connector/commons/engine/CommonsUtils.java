package com.stratio.connector.commons.engine;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.meta2.common.data.ClusterName;

/**
 * Created by dgomez on 22/09/14.
 */
public class CommonsUtils {

    /**
     * .
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected void startWork(ClusterName targetCluster, ConnectionHandler connection) {
        Connection conn = null;
        try {
            conn = connection.getConnection(targetCluster.getName());

            conn.setLastDateInfo(new Date().toString());
            conn.setStatus("Work in Progress");
            conn.setWorkInProgress(true);

        } catch (HandlerConnectionException e) {
            String msg = "fail get the Connection. " + e.getMessage();
            logger.error(msg);

        }
    }

    protected void endWork(ClusterName targetCluster, ConnectionHandler connection) {
        Connection conn = null;
        try {
            conn = connection.getConnection(targetCluster.getName());

            conn.setLastDateInfo(new Date().toString());
            conn.setStatus("Work Finished");
            conn.setWorkInProgress(false);

        } catch (HandlerConnectionException e) {
            String msg = "fail getting the Connection. " + e.getMessage();
            logger.error(msg);
        }
    }
}
