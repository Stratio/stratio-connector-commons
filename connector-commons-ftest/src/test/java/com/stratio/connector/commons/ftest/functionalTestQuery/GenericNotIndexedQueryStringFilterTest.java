/*
 * Stratio Deep
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */

package com.stratio.connector.commons.ftest.functionalTestQuery;

import static com.stratio.connector.commons.ftest.helper.TextConstant.danteParadise;
import static com.stratio.connector.commons.ftest.helper.TextConstant.names;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * Created by jmgomez on 17/07/14.
 */
public abstract class GenericNotIndexedQueryStringFilterTest extends GenericConnectorTest {

    private static final String COLUMN_TEXT = "column_text";

    LogicalWorkFlowCreator logicalWorkFlowCreator;

    @Before
    public void setUp() throws ConnectionException, ExecutionException, InitializationException, UnsupportedException {
        super.setUp();
        logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName());
    }

    @Test
    public void selectNotIndexedFilterUpperCaseEqual() throws ExecutionException, UnsupportedException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterUpperCaseEqual ***********************************");

        insertRow(names, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_TEXT)
                        .addEqualFilter(COLUMN_TEXT, names[10], false, false).getLogicalWorkflow();

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        assertEquals("The record number is correct", 1, queryResult.getResultSet().size());

    }

    @Test
    public void selectNotIndexedFilterUpperCaseDistinct() throws UnsupportedException, ExecutionException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNoPKFilterDistinct ***********************************");

        insertRow(names, clusterNodeName, false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_TEXT)
                        .addDistinctFilter(COLUMN_TEXT, names[5], false).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(logicalPlan);

        assertEquals("The record number is correct", names.length - 1, queryResult.getResultSet().size());

    }

    @Test
    public void selectNotIndexedFilterLowerCaseEqual() throws ExecutionException, UnsupportedException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterUpperCaseEqual ***********************************");

        insertRow(names, clusterNodeName, true);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_TEXT)
                        .addEqualFilter(COLUMN_TEXT, names[10].toLowerCase(), false, false).getLogicalWorkflow();

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute( logicalPlan);

        assertEquals("The record number is correct", 1, queryResult.getResultSet().size());

    }

    @Test
    public void selectNotIndexedFilterLowerCaseCaseDistinct() throws UnsupportedException, ExecutionException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNoPKFilterDistinct ***********************************");

        insertRow(names, clusterNodeName, true);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_TEXT)
                        .addDistinctFilter(COLUMN_TEXT, names[5].toLowerCase(), false).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute( logicalPlan);

        assertEquals("The record number is correct", names.length - 1, queryResult.getResultSet().size());

    }

    @Test
    public void selectNotIndexedFilterMatch() throws UnsupportedException, ExecutionException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNoPKFilterDistinct ***********************************");

        insertRow(danteParadise, getClusterName(), false);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(COLUMN_TEXT)
                        .addMatchFilter(COLUMN_TEXT, "matter").getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute( logicalPlan);

        ResultSet resultSet = queryResult.getResultSet();
        assertEquals("The record number is correct", 2, resultSet.size());

        String[] result = {
                        "That memory cannot follow.  Nathless all, That in my thoughts I of that sacred realm Could store, shall now be matter of my song",
                        "Yet is it true, That as ofttimes but ill accords the form To the design of art, through sluggishness Of unreplying matter, so this course Is sometimes quitted by the creature, who Hath power, directed thus, to bend elsewhere;," };
        int i = 0;
        for (Row row : resultSet) {
            assertTrue("the return text contains matter",
                            ((String) row.getCell(COLUMN_TEXT).getValue()).contains("matter"));
        }

    }

    private void insertRow(String[] text, ClusterName clusterNodeName, boolean toLowerCase)
                    throws UnsupportedOperationException, ExecutionException, UnsupportedException {

        Collection<Row> rows = new ArrayList();

        for (int i = 0; i < text.length; i++) {
            Map<String, Cell> cells = new HashMap<>();
            String field = text[i];
            if (toLowerCase) {
                field = field.toLowerCase();
            }

            cells.put(COLUMN_TEXT, new Cell(field));
            cells.put("id", new Cell(i));
            Row row = new Row();
            row.setCells(cells);
            rows.add(row);

        }

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadataBuilder.addColumn("id", ColumnType.INT).addColumn(COLUMN_TEXT, ColumnType.VARCHAR);
        TableMetadata targetTable = tableMetadataBuilder.build();

        connector.getStorageEngine().insert(clusterNodeName, targetTable, rows);

    }

}
