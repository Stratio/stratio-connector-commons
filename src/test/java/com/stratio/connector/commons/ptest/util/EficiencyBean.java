package com.stratio.connector.commons.ptest.util;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.ColumnType;

public class EficiencyBean implements Serializable{

	public static final String COLUMN_NAME = "name";
	public static final String COLUMN_MARRIED_MARRIED = "married";
	public static final String COLUMN_COUNTRY = "country";
	public static final String COLUMN_DANTE_LINE = "danteLine";
	public static final String COLUMN_AGE = "age";
	public static final String COLUMN_RANDOM_KEY = "randomKey";
	public static final String TABLE_NAME = EficiencyBean.class.getName();
	private final String separator = "%";

	public  Integer primaryKey;

	public String name;
	
	public Boolean married;
	
	public String country;
	
	public String danteLine;
	
	public Integer age;
	
	public String randomKey;

	private static Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();

	private static final String COLUMN_PK = "primaryKey";



	static {
		columns.put(COLUMN_PK,ColumnType.BIGINT);
		columns.put(COLUMN_NAME, ColumnType.TEXT);
		columns.put(COLUMN_MARRIED_MARRIED, ColumnType.BOOLEAN);
		columns.put(COLUMN_COUNTRY, ColumnType.TEXT);
		columns.put(COLUMN_DANTE_LINE, ColumnType.VARCHAR);
		columns.put(COLUMN_AGE, ColumnType.INT);
		columns.put(COLUMN_RANDOM_KEY, ColumnType.VARCHAR);
		
	}
	
	
	
	
	public String toString(){
		return primaryKey+separator+name+separator+married+separator+country+separator+danteLine+separator+age+separator
				+randomKey;
	}
	
	public EficiencyBean(Integer primaryKey, String name, Boolean married,String country, String danteLine, Integer
			age, String
			randomKey){
		this.primaryKey =primaryKey;
		this.name = name;
		this.married = married;
		this.country = country;
		this.danteLine= danteLine;
		this.age = age;
		this.randomKey = randomKey;
		
	}

	public EficiencyBean(String line){
		String values[] = line.split(separator);
		primaryKey = Integer.parseInt(values[0]);
		name= values[1];
		married = Boolean.parseBoolean(values[2]);
		country=values[3];
		danteLine = values[4];
		age = Integer.parseInt(values[5]);
		randomKey = values[6];
	}

	public Row getRow() {
		Row row = new Row();
		Map<String, Cell> cells = new LinkedHashMap();
		cells.put(COLUMN_NAME, new Cell(name));
		cells.put(COLUMN_MARRIED_MARRIED, new Cell(married));
		cells.put(COLUMN_COUNTRY, new Cell(country));
		cells.put(COLUMN_DANTE_LINE, new Cell(danteLine));
		cells.put(COLUMN_AGE, new Cell(age));
		cells.put(COLUMN_RANDOM_KEY, new Cell(randomKey));
		
		row.setCells(cells);
		return row;
	}
	
	public static TableMetadataBuilder getTableMetadata(String catalog, String clusterNAme){
		TableMetadataBuilder tableMetadataBuilder =  new TableMetadataBuilder( catalog, TABLE_NAME,
				clusterNAme);
		for (Entry<String, ColumnType> column: columns.entrySet()){
			tableMetadataBuilder.addColumn(column.getKey(), column.getValue());
		}
		return tableMetadataBuilder;
	}

	public static LogicalWorkflow getLogicalWorkFlowCreatorSelectAll(String catalog, ClusterName
			clusterNAme){

		LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(catalog, TABLE_NAME,clusterNAme)
				.addColumnName(COLUMN_NAME).addColumnName(COLUMN_MARRIED_MARRIED).addColumnName(COLUMN_COUNTRY)
				.addColumnName(COLUMN_DANTE_LINE).addColumnName(COLUMN_AGE).addColumnName(COLUMN_RANDOM_KEY);

		logicalWorkFlowCreator.addSelect(createSelectFieldWithAlias(logicalWorkFlowCreator));

		return logicalWorkFlowCreator.build();
	}

	private static LinkedList<LogicalWorkFlowCreator.ConnectorField> createSelectFieldWithAlias(
			LogicalWorkFlowCreator logicalWorkFlowCreator) {
		LinkedList<LogicalWorkFlowCreator.ConnectorField> selectField = new LinkedList<>();
		for (Entry<String, ColumnType> column: columns.entrySet()){
			selectField.add(logicalWorkFlowCreator.createConnectorField(column.getKey(),column.getKey(),column
					.getValue()));
		}
		return selectField;
	}
}
