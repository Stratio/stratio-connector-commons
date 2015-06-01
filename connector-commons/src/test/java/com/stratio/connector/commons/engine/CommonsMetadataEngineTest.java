package com.stratio.connector.commons.engine;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.data.*;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
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
//@PrepareForTest(CommonsMetadataEngine.class)
public class CommonsMetadataEngineTest extends TestCase {

    private static final String CLUSTER_NAME = "CLUSTER NAME";
    private static final String CATALOG_NAME = "CATALOG NAME";
    private static final String TABLE_NAME = "TABLE_NAME";
    private CatalogMetadata genericCatalogMetadata = new CatalogMetadata(new CatalogName(CATALOG_NAME), Collections.EMPTY_MAP, Collections.EMPTY_MAP);
    private TableMetadata genericTableMetadata = new TableMetadata(new TableName(CATALOG_NAME,TABLE_NAME),Collections.EMPTY_MAP,new LinkedHashMap<ColumnName, ColumnMetadata>(), Collections.EMPTY_MAP,new ClusterName(CLUSTER_NAME),Collections.EMPTY_LIST,Collections.EMPTY_LIST);



    @Mock
   private ConnectionHandler connectionHandlerTEst;
    @Mock
    private com.stratio.connector.commons.connection.Connection genericConnection;




    @Before
    public void setUp() throws Exception {
               when(connectionHandlerTEst.getConnection(CLUSTER_NAME)).thenReturn(genericConnection);
        super.setUp();
    }

    @Test
    public void createCatalogTest() throws ExecutionException, UnsupportedException, NoSuchFieldException {

        CommonsMetadataEngine cME = mock(CommonsMetadataEngine.class);

        Whitebox.setInternalState(cME, "connectionHandler", connectionHandlerTEst);
        Whitebox.setInternalState(cME, "logger", LoggerFactory.getLogger(this.getClass()));
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);


        cME.createCatalog(clusterName, genericCatalogMetadata);
        InOrder order = inOrder(connectionHandlerTEst,cME);

        order.verify(connectionHandlerTEst).startJob(CLUSTER_NAME);
        order.verify(cME).createCatalog(genericCatalogMetadata, genericConnection);
        order.verify(connectionHandlerTEst).endJob(CLUSTER_NAME);
    }



    @Test
    public void createTablegTest() throws ExecutionException, UnsupportedException, NoSuchFieldException {

        CommonsMetadataEngine cME = mock(CommonsMetadataEngine.class);

        Whitebox.setInternalState(cME, "connectionHandler", connectionHandlerTEst);
        Whitebox.setInternalState(cME, "logger", LoggerFactory.getLogger(this.getClass()));
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);


        cME.createTable(clusterName, genericTableMetadata);
        InOrder order = inOrder(connectionHandlerTEst,cME);

        order.verify(connectionHandlerTEst).startJob(CLUSTER_NAME);
        order.verify(cME).createTable(genericTableMetadata, genericConnection);
        order.verify(connectionHandlerTEst).endJob(CLUSTER_NAME);

    }





  /*  class CommonsMetadataEngineAbstract extends CommonsMetadataEngine {

        /**
         * Constructor.
         *
         * @param connectionHandler the connector handler.
         * /
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
    }*/
}


