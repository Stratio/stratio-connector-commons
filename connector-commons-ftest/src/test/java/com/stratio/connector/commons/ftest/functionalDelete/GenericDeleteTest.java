/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.connector.commons.ftest.functionalDelete;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta2.common.data.ClusterName;

public abstract class GenericDeleteTest extends GenericConnectorTest {

    public static final String COLUMN_TEXT = "text";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";

    private static final int EQUAL_FILTER = 1;

    private static final int NOTEQUAL_BETWEEN = 6;

    @Test
    public void deleteFilterEqualString() throws Exception {

        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST deleteFilterEqualString " + clusterName
                        .getName() + " ***********************************");

        insertRow(1, "text", 10, 20);//row,text,money,age
        insertRow(2, "text", 9, 17);
        insertRow(3, "text", 11, 26);
        insertRow(4, "text", 10, 30);
        insertRow(5, "text", 20, 42);

        refresh(CATALOG);

        Filter[] filterSet = createFilterCollection(EQUAL_FILTER, "text2");

        fail("Not yet generic supported");

    }

    @Test
    public void deleteFilterEqualInt() throws ExecutionException {

        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST deleteFilterEqualInt " + clusterName.getName()
                        + " ***********************************");

        insertRow(1, "text", 10, 20);//row,text,money,age
        insertRow(2, "text", 9, 17);
        insertRow(3, "text", 11, 20);
        insertRow(4, "text", 10, 30);
        insertRow(5, "text", 20, 42);

        refresh(CATALOG);

        Filter[] filterSet = createFilterCollection(EQUAL_FILTER, 20);

        fail("Not yet generic supported");

    }

    @Test
    public void deleteFilterNotEqualInt() throws Exception {

        ClusterName clusterName = getClusterName();
        System.out.println(
                "*********************************** INIT FUNCTIONAL TEST deleteFilterNotEqualInt " + clusterName
                        .getName() + " ***********************************");

        insertRow(1, "text", 10, 20);//row,text,money,age
        insertRow(2, "text", 9, 17);
        insertRow(3, "text", 11, 20);
        insertRow(4, "text", 10, 30);
        insertRow(5, "text", 20, 42);

        refresh(CATALOG);

        //age notequal 20 and money between 9,11
        Filter[] filterSet = createFilterCollection(NOTEQUAL_BETWEEN, 20);

        fail("Not yet generic supported");
    }

    private Filter[] createFilterCollection(int filterType, Object object) {

        ArrayList<Filter> coll = new ArrayList<Filter>();

        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnMetadata> columns = new ArrayList<>();

        switch (filterType) {
        case EQUAL_FILTER:
            coll.add(createEqualsFilter(filterType, object));
            break;
        case NOTEQUAL_BETWEEN:
            coll.add(createNotEqualsFilter(filterType, object));
            coll.add(createBetweenFilter(9, 11));
            break;
        }
        //     if (EQUAL_FILTER==filterType
        //    		 || HIGH_FILTER == filterType
        //    		 || LOW_FILTER == filterType
        //    		 || HIGH_BETWEEN_FILTER == filterType)
        //    	 coll.add(createEqualsFilter(filterType, object));
        //     if (BETWEEN_FILTER==filterType || HIGH_BETWEEN_FILTER==filterType) coll.add(createBetweenFilter());
        Filter[] filArray = new Filter[coll.size()];
        return coll.toArray(filArray);

    }

    private Filter createNotEqualsFilter(int filterType, Object object) {
        //	RelationCompare relCom;
        //	if(object instanceof String) relCom = new RelationCompare(COLUMN_TEXT, "!=", new StringTerm((String)object));
        //	else if(object instanceof Integer) relCom = new RelationCompare(COLUMN_AGE, "<>", new IntegerTerm(String.valueOf(object)));
        //	else throw new Exception("unsupported type"+ object.getClass());
        //
        //	Filter f = new Filter(Operations.SELECT_WHERE_MATCH, RelationType.COMPARE, relCom);
        //	return f;
        return null; //REVIEW por la nueva version de meta

    }

    private Filter createBetweenFilter(int min, int max) {

        //	 Relation relation = new RelationBetween(COLUMN_MONEY);
        //     relation.setType(Relation.TYPE_BETWEEN);
        //     List<Term<?>> terms = new ArrayList<>();
        //     terms.add(new IntegerTerm(String.valueOf(min)));
        //     terms.add(new IntegerTerm(String.valueOf(max)));
        //     relation.setTerms(terms);
        //     Filter f = new Filter(Operations.SELECT_WHERE_BETWEEN, RelationType.BETWEEN, relation);
        //     return f;

        return null; //REVIEW por la nueva version de meta

    }

    private Filter createEqualsFilter(int filterType, Object object) {
        //	RelationCompare relCom;
        //	if(object instanceof String) relCom = new RelationCompare(COLUMN_TEXT, "=", new StringTerm((String)object));
        //	else if(object instanceof Integer) relCom = new RelationCompare(COLUMN_AGE, "=", new IntegerTerm(String.valueOf(object)));
        //	else throw new Exception("unsupported type"+ object.getClass());
        //
        //	Filter f = new Filter(Operations.SELECT_WHERE_MATCH, RelationType.COMPARE, relCom);
        //	return f;
        fail("Not yet generic supported");
        return null; //REVIEW por la nueva version de meta

    }

    private void insertRow(int ikey, String texto, int money, int age)
            throws UnsupportedOperationException, ExecutionException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_TEXT, new Cell(texto + ikey));
        cells.put(COLUMN_MONEY, new Cell(money));
        cells.put(COLUMN_AGE, new Cell(age));
        row.setCells(cells);

        //       ((ElasticsearchStorageEngine) connector.getStorageEngine()).insert(CATALOG, TABLE, row);
        fail("Not yet generic supported");
    }

}