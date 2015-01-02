StorageEngine
------------------

| Group   	 | Operation    	 | State     |  	Class.Method         		| Comments  |
| -------- 	 | -------------	 | --------  | -----------------------------   		| --------- |
| TRUNCATE  	 |  TRUNCATE	         |   DONE    | GenericTruncateFT           		|           |
| DELETE   	 |  PK_EQUAL(String)	 |   DONE    | GenericDeleteFT     	       		|           |
| DELETE   	 |  PK_LT(String)	 |   DONE    | GenericDeleteFT     	      		|           |
| DELETE   	 |  PK_LET(String)	 |   DONE    | GenericDeleteFT     	       		|           |
| DELETE   	 |  PK_GT(String)	 |   DONE    | GenericDeleteFT     	      		|           |
| DELETE   	 |  PK_GET(String)	 |   DONE    | GenericDeleteFT     	      		|           |		
| INSERT   	 |  PK		         |   DONE    | GenericSimpleInsertFT          		|           |
| INSERT   	 |  WITHOUT_PK	         |   DONE    | GenericSimpleInsertFT.testSimpleInsertWithoutPK|     |
| INSERT   	 |  UPSERT(String)	 |   DONE    | GenericSimpleInsertFT.testInsertSamePK	|           |
| INSERT   	 |  COMPOSITE_PK(String) |   DONE    | GenericSimpleInsertFT.testInsertCompositePK|         |
| INSERT   	 |  UPSERT_COMPOSITE_PK  |   DONE    | GenericSimpleInsertFT.testInsertDuplicateCompositePK||
| INSERT   	 |  PK(String)	         |   DONE    | GenericSimpleInsertFT           		|           |
| INSERT   	 |  PK(Integer)	         |   DONE    | GenericSimpleInsertFT           		|           |
| INSERT   	 |  PK(Long)	         |   DONE    | GenericSimpleInsertFT           		|           |
| INSERT   	 |  PK(Boolean)	         |   DONE    | GenericSimpleInsertFT           		|           |
| INSERT   	 |  PK(Date)	         |   DONE    | GenericSimpleInsertFT           		|           |
| INSERT   	 |  PK(Double)	         |   DONE    | GenericSimpleInsertFT           		|           |
| INSERT   	 |  PK(Float)	         |IN PROGRESS| 			           		|           |
| INSERT   	 |  PK(Map)	         |IN PROGRESS| 			          		|           |
| INSERT   	 |  PK(Array)	         |IN PROGRESS| 			           		|           |
| INSERT   	 |  PK(Map)	         |IN PROGRESS| 			           		|           |
| INSERT   	 |  PK(Array)	         |IN PROGRESS| 			           		|           |
| INSERT   	 |  IF_NOT_EXIST         |   DONE    | GenericSimpleInsertIfNotExistFT		|           |
| INSERT_BULK 	 |  PK	   	         |   DONE    | 			           		|           |
| INSERT_BULK  	 |  WITHOUT_PK	         |   DONE    | GenericBulkInsertFT           		|           |
| INSERT_BULK  	 |  IF_NOT_EXIST 	 |   DONE    | GenericBulkInsertIfNotExistFT            |           |
| UPDATE   	 |  SINGLE_ROW		 |   DONE    | GenericSimpleUpdateFT.updateGenericsFieldsFT|        |
| UPDATE   	 |  MULTI_ROW		 |   DONE    | GenericSimpleUpdateFT.multiUpdateGenericsFieldsFT|   |
| UPDATE_FILTER  |  NON_INDEX_GET 	 |   DONE    | GenericSimpleUpdateWithFiltersFT		|           |
| UPDATE_FILTER  |  PK_EQ		 |   DONE    | GenericSimpleUpdateWithFiltersFT         |           |
| UPDATE_RELATION|  ADD_RELATION	 |   DONE    | GenericAggregationUpdateFT   	        |           |
| UPDATE_RELATION|  SUBSTRACT_RELATION	 |   DONE    | GenericAggregationUpdateFT               |           |
| UPDATE_RELATION|MULTIPLICATION_RELATION|   DONE    | GenericAggregationUpdateFT               |           |

MetadataEngine
------------------

| Group   	 | Operation    	 | State     |  	Class.Method         		| Comments  |
| -------- 	 | -------------	 | --------  | -----------------------------   		| --------- |
| ALTER   	 |  ADD_COLUMN		 |   DONE    | GenericMetadataAlterFT     	       	|           |
| ALTER   	 |  DROP_COLUMN		 |   DONE    | GenericMetadataAlterFT     	       	|           |
| CREATE	 |  CREATE_CATALOG	 |   DONE    | GenericMetadataCreateFT     	       	|           |
| CREATE	 |CREATE_CATALOG_OPTIONS |   DONE    | GenericMetadataCreateFT     	       	|           |
| CREATE	 |CREATE_CATALOG_DUPLICATE|   DONE   | GenericMetadataCreateFT.createCatalogExceptionCreateTwoCatalogTest|    |
| CREATE	 |CREATE_TABLE		 |   DONE    | GenericMetadataCreateFT     	       	|           |
| CREATE	 |CREATE_TABLE_DUPLICATE |   DONE    | GenericMetadataCreateFT.createTableWithoutTableTest| |
| CREATE	 |  CREATE_DEFAULT_INDEX |   DONE    | GenericMetadataIndexFT     	       	|           |
| CREATE	 |  CREATE_MULTI_INDEX   |   DONE    | GenericMetadataIndexFT     	       	|           |
| CREATE	 |  CREATE_TEXT_INDEX    |   DONE    | GenericMetadataIndexFT     	       	|           |
| CREATE	 | CREATE_INDEX_DUPLICATE|   DONE    | GenericMetadataIndexFT     	       	|           |
| DROP		 |  DROP_TABLE 		 |   DONE    | GenericMetadataDropFT     	       	|           |
| DROP		 |  DROP_CATALOG	 |   DONE    | GenericMetadataDropFT     	       	|           |
| DROP		 |  DROP_INDEX		 |   DONE    | GenericMetadataIndexFT     	       	|           |


QueryEngine
----------------

| Group   	 | Operation    	 | State     |  	Class.Method         		| Comments  |
| -------- 	 | -------------	 | --------  | -----------------------------   		| --------- |
|  SELECT        | BASIC_SELECT	 	 |   DONE    | ExampleWorkflowsFT     	         	|           |
|  SELECT        | BASIC_SELECT_AST	 |   DONE    | ExampleWorkflowsFT     	         	|           |
|  SELECT        | SELECT_INDEXED_FIELD  |   DONE    | ExampleWorkflowsFT     	         	|           |
|  SELECT        |SELECT_NONINDEXED_FIELD|   DONE    | ExampleWorkflowsFT     	         	|           |
|  SELECT        |SELECT_MIXED_WHERE	 |   DONE    | ExampleWorkflowsFT     	         	|           |
|  SELECT        | SELECT_METADATA	 |   DONE    | GenericQueryFT     	         	|           |
|  SELECT        | SELECT_ALL		 |   DONE    | GenericQueryFT     	         	|           |
|  SELECT        | SELECT_BASIC_FILTER	 |   DONE    | GenericQueryProjectFT     	        |           |
|  SELECT_GROUPBY| BASIC_GROUP_BY	 |   DONE    | GenericGroupByFT     	         	|           |
|  SELECT_LIMIT  | LIMIT		 |   DONE    | GenericLimitFT     	         	|           |
|  SELECT_FILTER | NONINDEXED_EQ(Integer)|   DONE    | GenericNotIndexedQueryIntegerFilterFT    |           |
|  SELECT_FILTER |NONINDEXED_BETWEEN(Int)|IN PROGRESS| GenericNotIndexedQueryIntegerFilterFT    |           |
|  SELECT_FILTER | NONINDEXED_GET(Int)   |   DONE    | GenericNotIndexedQueryIntegerFilterFT    |           |
|  SELECT_FILTER | NONINDEXED_GT(Int)    |   DONE    | GenericNotIndexedQueryIntegerFilterFT    |           |
|  SELECT_FILTER | NONINDEXED_LT(Int)    |   DONE    | GenericNotIndexedQueryIntegerFilterFT    |           |
|  SELECT_FILTER | NONINDEXED_LET(Int)   |   DONE    | GenericNotIndexedQueryIntegerFilterFT    |           |
|  SELECT_FILTER | NONINDEXED_NEQ(Int)   |   DONE    | GenericNotIndexedQueryIntegerFilterFT    |           |
|  SELECT_FILTER | NONINDEXED_NEQ(Int)   |   DONE    | GenericNotIndexedQueryIntegerFilterFT    |           |
|  SELECT_FILTER | PK_EQUAL(Int)	 |   DONE    | GenericPKQueryIntegerFilterFT    	|           |
|  SELECT_FILTER | PK_EQ_DOUBLE(Int)     |   DONE    | GenericPKQueryIntegerFilterFT   		|           |
|  SELECT_FILTER | PK_NEQ_DOUBLE(Int)    |   DONE    | GenericPKQueryIntegerFilterFT    	|           |
|  SELECT_FILTER | PK_GET(Int)    	 |   DONE    | GenericPKQueryIntegerFilterFT    	|           |
|  SELECT_FILTER | INDEXED		 |IN PROGRESS| GenericNotIndexedQueryStringFilterFT     |           |
|  SELECT_ORDERBY| ASC_ORDER_BY		 |   DONE    | GenericOrderByFT			        |           |
|  SELECT_ORDERBY| DESC_ORDER_BY	 |   DONE    | GenericOrderByFT			        |           |
|  SELECT_ORDERBY| MULTIFIELD_ORDER_BY	 |   DONE    | GenericOrderByFT			        |           |

