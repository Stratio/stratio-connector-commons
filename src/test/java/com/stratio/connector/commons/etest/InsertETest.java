package com.stratio.connector.commons.etest;

import java.io.FileNotFoundException;

import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.commons.etest.thread.InsertThread;
import com.stratio.connector.commons.etest.util.EficiencyBean;
import com.stratio.connector.commons.etest.util.TextFileParser;
import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.metadata.TableMetadata;

public abstract class InsertETest extends GenericConnectorTest{
	
	

	@Before
    public void setUp() throws ConnectorException  {
		super.setUp();
		if (!TextFileParser.existTestFiles()){
			try {
				TextFileParser.generateFiles();
			} catch (FileNotFoundException e) {
				throw new ConnectorException("Exception generated textFiles.",e);
			}
		}
	}
	
	 @Test
	    public void insetInSameTableWithNotExistsEqualsFalse() throws ConnectorException, InterruptedException {
		 
		 InsertThread insertThread0 = new InsertThread(0, getClusterName(), EficiencyBean.getTableMetadata(CATALOG, TABLE, getClusterName().getName(),isPkRequired() ),false);
		 InsertThread insertThread1 = new InsertThread(1, getClusterName(), EficiencyBean.getTableMetadata(CATALOG, TABLE, getClusterName().getName(),isPkRequired() ),false);
		 InsertThread insertThread2 = new InsertThread(2, getClusterName(), EficiencyBean.getTableMetadata(CATALOG, TABLE, getClusterName().getName(),isPkRequired() ),false);
		 InsertThread insertThread3 = new InsertThread(3, getClusterName(), EficiencyBean.getTableMetadata(CATALOG, TABLE, getClusterName().getName(),isPkRequired() ),false);

		 insertThread0.start();
		 insertThread1.start();
		 insertThread2.start();
		 insertThread3.start();
		 
		 insertThread0.join();
		 insertThread1.join();
		 insertThread2.join();
		 insertThread3.join();
	 }

	protected Boolean isPkRequired(){return false;}
	

}
