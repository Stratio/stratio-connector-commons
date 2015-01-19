package com.stratio.connector.commons.etest.thread;

import java.io.IOException;

import com.stratio.connector.commons.etest.util.EficiencyBean;
import com.stratio.connector.commons.etest.util.TextFileParser;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.TableMetadata;

public class InsertThread extends Thread{

	
	
	private ClusterName targetCluster;
	private TableMetadata targetTable;
	private boolean isNotExists;
	private Integer id;

	public InsertThread(Integer id,  ClusterName targetCluster, TableMetadata targetTable, boolean isNotExists){
		super(InsertThread.class.getName()+"_"+id);
		this.id=id;
		this.targetCluster = targetCluster;
		this.targetTable = targetTable;
		this.isNotExists = isNotExists;
	}
	
	 public void run(IConnector connector) throws UnsupportedException, IOException, ConnectorException {
		 TextFileParser textFileParser = new TextFileParser();
		 EficiencyBean eficiencyBean;
		 while ((eficiencyBean=textFileParser.getEficiencyBean(id))!=null){
			 connector.getStorageEngine().insert(targetCluster, targetTable, eficiencyBean.getRow(), isNotExists);
		 }
		 
	 }
	
}
