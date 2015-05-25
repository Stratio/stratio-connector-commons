package com.stratio.connector.commons.ptest.thread;

import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.ptest.util.EficiencyBean;
import com.stratio.connector.commons.ptest.util.TextFileParser;
import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class InsertThread extends Thread {

    public static final String INSERTER_COLUMN_NAME = "inserter";
    /**
     * The Log.
     */
    static final transient Logger LOGGER = LoggerFactory.getLogger(SelectorHelper.class);
    private final IConnector connector;
    private final String catalog;
    private final String table;
    private ClusterName targetCluster;

    private boolean isNotExists;
    private Integer id;

    public InsertThread(Integer id, ClusterName targetCluster, IConnector connector, String catalog, String table,
                        boolean isNotExists) {
        super(InsertThread.class.getName() + "_" + id);
        this.id = id;
        this.targetCluster = targetCluster;
        this.connector = connector;
        this.isNotExists = isNotExists;
        this.catalog = catalog;
        this.table = table;
    }

    public void run() {

        BufferedReader bf = null;
        try {

            String line;
            TableMetadata tableMetadata = createTableMetadata();
            bf = new BufferedReader(new FileReader(TextFileParser.getFilePath(id)));
            IStorageEngine storageEngine = connector.getStorageEngine();
            while ((line = bf.readLine()) != null) {
                EficiencyBean eficiencyBean = new EficiencyBean(line);
                Row row = createRow(eficiencyBean);

                storageEngine.insert(targetCluster, tableMetadata, row,
                        isNotExists);
            }
            insertDefaultLine(tableMetadata, storageEngine);
        } catch (IOException | ConnectorException e) {
            LOGGER.error("Fail insert a row in thread " + id + "." + e);
            e.printStackTrace();
        } finally {
            closeFile(bf);
            LOGGER.debug("Finis inserter thread " + id);
        }

    }

    private void insertDefaultLine(TableMetadata tableMetadata, IStorageEngine storageEngine)
            throws ConnectorException {
        EficiencyBean eficiencyBean = new EficiencyBean(-id, "Jose", false, "Palencia", "En un lugar de la mancha", 33,
                "RandomKEy");
        storageEngine.insert(targetCluster, tableMetadata, createRow(eficiencyBean),
                isNotExists);
    }

    private void closeFile(BufferedReader bf) {
        if (bf != null) {
            try {
                bf.close();
            } catch (IOException e) {
                LOGGER.error("Fail close file " + TextFileParser.getFilePath(id));
                e.printStackTrace();
            }
        }
    }

    private Row createRow(EficiencyBean eficiencyBean) {
        Row row = eficiencyBean.getRow();
        row.addCell(INSERTER_COLUMN_NAME, new Cell("inserter_" + id));
        return row;
    }

    private TableMetadata createTableMetadata() {
        TableMetadataBuilder tableMetadataBuilder = EficiencyBean.getTableMetadata(catalog, targetCluster
                .getName());
        tableMetadataBuilder.addColumn(INSERTER_COLUMN_NAME, new ColumnType(DataType.TEXT));
        return tableMetadataBuilder.build(false);
    }

}
