package com.stratio.connector.commons.etest.util;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;










import java.util.Map.Entry;

import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;

public class EficiencyBean implements Serializable{

	private final String separator = "%";
	
	public String name;
	
	public Boolean married;
	
	public String country;
	
	public String danteLine;
	
	public Integer age;
	
	public String randomKey;

	private static Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
	static {
		columns.put("name", ColumnType.TEXT);
		columns.put("married", ColumnType.BOOLEAN);
		columns.put("country", ColumnType.TEXT);
		columns.put("danteLine", ColumnType.VARCHAR);
		columns.put("age", ColumnType.INT);
		columns.put("randomKey", ColumnType.VARCHAR);
		
	}
	
	
	
	
	public String toString(){
		return name+separator+married+separator+country+separator+age+separator+randomKey;
	}
	
	public EficiencyBean(String name, Boolean married,String country, String danteLine, Integer age, String randomKey){
		this.name = name;
		this.married = married;
		this.country = country;
		this.danteLine= danteLine;
		this.age = age;
		this.randomKey = randomKey;
		
	}
	public EficiencyBean(String line){
		String values[] = line.split(separator);
		name= values[0];
		married = Boolean.parseBoolean(values[1]);
		country=values[2];
		danteLine = values[3];
		age = Integer.parseInt(values[4]);
		randomKey = values[5];
	}

	public Row getRow() {
		Row row = new Row();
		Map<String, Cell> cells = new LinkedHashMap();
		cells.put("name", new Cell(name));
		cells.put("married", new Cell(married));
		cells.put("country", new Cell(country));
		cells.put("danteLine", new Cell(danteLine));
		cells.put("age", new Cell(age));
		cells.put("randomKey", new Cell(randomKey));
		
		row.setCells(cells);
		return null;
	}
	
	public static TableMetadata getTableMetadata(String catalog, String table, String clusterNAme, Boolean isPkRequired){
		TableMetadataBuilder tableMetadataBuilder =  new TableMetadataBuilder( catalog, table, clusterNAme);
		for (Entry<String, ColumnType> column: columns.entrySet()){
			tableMetadataBuilder.addColumn(column.getKey(), column.getValue());
		}
		return tableMetadataBuilder.build(isPkRequired);
	}
}
