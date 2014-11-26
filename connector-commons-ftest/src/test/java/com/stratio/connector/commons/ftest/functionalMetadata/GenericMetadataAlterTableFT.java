package com.stratio.connector.commons.ftest.functionalMetadata;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.schema.CatalogMetadataBuilder;
import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.data.AlterOperation;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.result.QueryResult;

public abstract class GenericMetadataAlterTableFT extends GenericConnectorTest{

    protected static final String COLUMN_1 = "name1";
    protected static final String COLUMN_2 = "name2";
    
    @Test
    public void addColumnFT() throws ConnectorException {
        
        ClusterName clusterName = getClusterName();
       

        //Create the catalog and the table with COLUMN_1
        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.VARCHAR);
        
        CatalogMetadataBuilder catalogMetadataBuilder = new CatalogMetadataBuilder(CATALOG);
        catalogMetadataBuilder.withTables(tableMetadataBuilder.build(getConnectorHelper()));
        if(iConnectorHelper.isCatalogMandatory()){
            connector.getMetadataEngine().createCatalog(clusterName, catalogMetadataBuilder.build());
        }
        if(iConnectorHelper.isTableMandatory()){
            connector.getMetadataEngine().createTable(clusterName, tableMetadataBuilder.build(getConnectorHelper()));
        }else{

            Row row = new Row();
            Map<String, Cell> cells = new HashMap<>();
            cells.put(COLUMN_1, new Cell("value1"));
            row.setCells(cells);
            connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()), row);
        }
        
        //ADD the column: COLUMN_2 with alterTable
        ColumnMetadata columnMetadata = new ColumnMetadata(new ColumnName(CATALOG, TABLE, COLUMN_2), new Object[0],
                        ColumnType.INT);
        AlterOptions alterOptions = new AlterOptions(AlterOperation.ADD_COLUMN, null, columnMetadata);
        
        refresh(CATALOG);
        
        connector.getMetadataEngine().alterTable(clusterName, new TableName(CATALOG, TABLE), alterOptions);
        
        refresh(CATALOG);
        
        //INSERT a row with column2 != null
        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("value1"));
        cells.put(COLUMN_2, new Cell(25));
        row.setCells(cells);
        
        tableMetadataBuilder.addColumn(COLUMN_2, ColumnType.INT);
        connector.getStorageEngine().insert(clusterName, tableMetadataBuilder.build(getConnectorHelper()), row);

        refresh(CATALOG);
        
        //Verify the proper column field is returned
        QueryResult queryResult = connector.getQueryEngine().execute(new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName()).addColumnName(COLUMN_1).addColumnName(COLUMN_2)
                        .getLogicalWorkflow());
        assertEquals("Table [" + CATALOG + "." + TABLE + "] ", 1, queryResult.getResultSet().size());

        Row row2 = queryResult.getResultSet().getRows().get(0);
        assertEquals(25, row2.getCell(COLUMN_2).getValue());
  
    }
    
    

}

