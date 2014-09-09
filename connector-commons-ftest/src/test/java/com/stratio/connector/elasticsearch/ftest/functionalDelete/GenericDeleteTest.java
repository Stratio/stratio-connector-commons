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

package com.stratio.connector.elasticsearch.ftest.functionalDelete;

import com.stratio.connector.elasticsearch.ftest.GenericConnectorTest;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta2.common.data.ClusterName;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;




public abstract class GenericDeleteTest extends GenericConnectorTest {


    public static final String COLUMN_TEXT = "text";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";

    private static final int EQUAL_FILTER =1;
    

    private static final int NOTEQUAL_BETWEEN=6;

    @Test
    public void deleteFilterEqualString() throws Exception {

        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST deleteFilterEqualString "+ clusterName.getName()+" ***********************************");

    	 insertRow(1,"text",10,20);//row,text,money,age
         insertRow(2,"text",9,17);
         insertRow(3,"text",11,26);
         insertRow(4,"text",10,30);
         insertRow(5,"text",20,42);

        refresh(CATALOG);


        Filter[] filterSet = createFilterCollection(EQUAL_FILTER, "text2"); 
//   	 	((ElasticsearchStorageEngine) connector.getStorageEngine()).delete(CATALOG, TABLE, filterSet);
//        refresh(CATALOG);
//
//
//        SearchResponse response = nodeClient.prepareSearch(CATALOG)
//     		.setSearchType(SearchType.QUERY_THEN_FETCH)
//     		.setTypes(COLLECTION)
//     		.execute()
//     		.actionGet();
//
//     SearchHits hits = response.getHits();
//
//     for(SearchHit hit: hits.hits()){
//    	 assertEquals(false, hit.getSource().get(COLUMN_TEXT).equals("text2"));
//     }
//
//     assertEquals(4, hits.getTotalHits());
//

        fail("Not implemented"); //REVIEW debido a cambio de interfaz






    }

    
    @Test
    public void deleteFilterEqualInt() throws ExecutionException {

        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST deleteFilterEqualInt "+ clusterName.getName()+" ***********************************");



    	 insertRow(1,"text",10,20);//row,text,money,age
         insertRow(2,"text",9,17);
         insertRow(3,"text",11,20);
         insertRow(4,"text",10,30);
         insertRow(5,"text",20,42);

        refresh(CATALOG);


        Filter[] filterSet = createFilterCollection(EQUAL_FILTER, 20);    
//   	 	((ElasticsearchStorageEngine) connector.getStorageEngine()).delete(CATALOG, TABLE, filterSet);

//
//        refresh(CATALOG);
//
//
//        SearchResponse response = nodeClient.prepareSearch(CATALOG)
//      		.setSearchType(SearchType.QUERY_THEN_FETCH)
//      		.setTypes(COLLECTION)
//      		.execute()
//      		.actionGet();
//
//      SearchHits hits = response.getHits();
//
//      for(SearchHit hit: hits.hits()){
//     	 assertEquals(false, hit.getSource().get(COLUMN_AGE).equals(20));
//      }
//
//      assertEquals(3, hits.getTotalHits());

        fail("Not implemented"); //REVIEW debido a cambio de interfaz

    }
    
	@Test
	 public void deleteFilterNotEqualInt() throws Exception {


        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST deleteFilterNotEqualInt "+ clusterName.getName()+" ***********************************");

 	 insertRow(1,"text",10,20);//row,text,money,age
	 insertRow(2,"text",9,17);
	 insertRow(3,"text",11,20);
	 insertRow(4,"text",10,30);
	 insertRow(5,"text",20,42);

        refresh(CATALOG);


        //age notequal 20 and money between 9,11
	 Filter[] filterSet = createFilterCollection(NOTEQUAL_BETWEEN, 20);

//	 ((ElasticsearchStorageEngine)connector.getStorageEngine()).delete(CATALOG, TABLE,filterSet);


//        refresh(CATALOG);
//
//        SearchResponse response = nodeClient.prepareSearch(CATALOG)
//	      		.setSearchType(SearchType.QUERY_THEN_FETCH)
//	      		.setTypes(COLLECTION)
//	      		.execute()
//	      		.actionGet();
//
//	      SearchHits hits = response.getHits();
//
//	      for(SearchHit hit: hits.hits()){
//	     	assertEquals(false, hit.getSource().get(COLUMN_AGE).equals(17) ||
//	     			hit.getSource().get(COLUMN_AGE).equals(30));
//	      }
//
//	      assertEquals(3, hits.getTotalHits());

        fail("Not implemented"); //REVIEW debido a cambio de interfaz
	 }
    
    
 private Filter[] createFilterCollection(int filterType, Object object) {
	 
	 ArrayList<Filter> coll = new ArrayList<Filter>();
	
	 
	 List<LogicalStep> stepList = new ArrayList<>();
     List<ColumnMetadata> columns = new ArrayList<>();

     switch (filterType){
     	case EQUAL_FILTER: coll.add(createEqualsFilter(filterType, object)); break;
     	case NOTEQUAL_BETWEEN: coll.add(createNotEqualsFilter(filterType, object)); coll.add(createBetweenFilter(9,11)); break;
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
    fail("Not implemented");
    return null; //REVIEW por la nueva version de meta

}


private void insertRow(int ikey, String texto, int money, int age) throws UnsupportedOperationException, ExecutionException {
     	
	   Row row = new Row();
       Map<String, Cell> cells = new HashMap<>();
       cells.put(COLUMN_TEXT, new Cell(texto+ikey));
       cells.put(COLUMN_MONEY, new Cell(money));
       cells.put(COLUMN_AGE, new Cell(age));
       row.setCells(cells);
        
//       ((ElasticsearchStorageEngine) connector.getStorageEngine()).insert(CATALOG, TABLE, row);
    fail("Not implemented"); //REVIEW debido a cambio de interfaz
    }
 

 
 
 
}