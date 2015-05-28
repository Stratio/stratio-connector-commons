package com.stratio.connector.commons.engine;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Created by jmgomez on 27/05/15.
 */
@RunWith(PowerMockRunner.class)
public class CommonsMetadataEngineTest extends TestCase {

    private static final String CLUSTER_NAME = "CLUSTER NAME";
    private static final String CATALOG_NAME = "CATALOG NAME";
    private CatalogMetadata genericCatalogMetadata = new CatalogMetadata(new CatalogName(CATALOG_NAME), Collections.EMPTY_MAP, Collections.EMPTY_MAP);


    @Mock
    private ConnectionHandler connectionHandlerTEst;
    @Mock
    private com.stratio.connector.commons.connection.Connection genericConnection;


    @Override
    @Before
    protected void setUp() throws Exception {
        when(connectionHandlerTEst.getConnection(CLUSTER_NAME)).thenReturn(genericConnection);
        super.setUp();
    }


    @Test
    public void createCatalogTest() throws ExecutionException, UnsupportedException {
        CommonsMetadataEngine cME = new CommonsMetadataEngineAbstract(connectionHandlerTEst);
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);


        cME.createCatalog(clusterName, genericCatalogMetadata);
        InOrder order = inOrder(connectionHandlerTEst);

        order.verify(connectionHandlerTEst).startJob(CLUSTER_NAME);
        order.verify(connectionHandlerTEst).endJob(CLUSTER_NAME);

    }


    class CommonsMetadataEngineAbstract extends CommonsMetadataEngine {

        /**
         * Constructor.
         *
         * @param connectionHandler the connector handler.
         */
        protected CommonsMetadataEngineAbstract(ConnectionHandler connectionHandler) {
            super(connectionHandler);
        }

        @Override
        protected List<CatalogMetadata> provideMetadata(ClusterName targetCluster, Connection connection) throws ConnectorException {
            return null;
        }

        @Override
        protected CatalogMetadata provideCatalogMetadata(CatalogName catalogName, ClusterName targetCluster, Connection connection) throws ConnectorException {
            return null;
        }

        @Override
        protected TableMetadata provideTableMetadata(TableName tableName, ClusterName targetCluster, Connection connection) throws ConnectorException {
            return null;
        }

        @Override
        protected void alterTable(TableName name, AlterOptions alterOptions, Connection connection) throws UnsupportedException, ExecutionException {

        }

        @Override
        protected void createCatalog(CatalogMetadata catalogMetadata, Connection connection) throws UnsupportedException, ExecutionException {
            assertEquals("The catalog metadata must be the same thar has use to invoke", genericCatalogMetadata, catalogMetadata);
            assertEquals("The connections must be the same thar has use in connectionHandler", genericConnection, connection);

        }

        @Override
        protected void createTable(TableMetadata tableMetadata, Connection connection) throws UnsupportedException, ExecutionException {

        }

        @Override
        protected void dropCatalog(CatalogName name, Connection connection) throws UnsupportedException, ExecutionException {

        }

        @Override
        protected void dropTable(TableName name, Connection connection) throws UnsupportedException, ExecutionException {

        }

        @Override
        protected void createIndex(IndexMetadata indexMetadata, Connection connection) throws UnsupportedException, ExecutionException {

        }

        @Override
        protected void dropIndex(IndexMetadata indexMetadata, Connection connection) throws UnsupportedException, ExecutionException {

        }

        @Override
        protected void alterCatalog(CatalogName catalogName, Map options, Connection connection) throws UnsupportedException, ExecutionException {

        }
    }
}


