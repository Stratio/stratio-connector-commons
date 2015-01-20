package com.stratio.connector.commons.ptest.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.net.URL;
import java.util.Random;

import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.util.TextConstant;

public class TextFileParser {
	
	private static final URL  PATH = Thread.currentThread().getContextClassLoader().getResource(".");
	
	private static final File FOLDER_URL = new File(PATH.getPath()+File.separator+".."+File.separator+".."+File
			.separator+"src"+File.separator+"test"+File.separator+"efficiencyFiles");
	   /**
     * The Log.
     */
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TextFileParser.class);
    
    private static Random random = new Random(System.currentTimeMillis());
    
    private static final String FILE_NAME = "textFile";
    
    private static final String FILE_EXTENSION = ".txt";
    
	public static void generateFiles() throws FileNotFoundException{
		
		if (!existTestFiles()){
			LOGGER.info("The testFiles doesn't exist... We are going to create it. This can take several minutes...");
			FOLDER_URL.mkdirs();
			int primaryKeyCount = 0;
			for (int i=0;i<4;i++){
				PrintStream pS = new PrintStream(new File(getFilePath(i)));
				for (int j=0;j<1000000;j++){
					EficiencyBean eBean = new EficiencyBean(primaryKeyCount++,TextConstant.getRandomName(), random
							.nextBoolean() , TextConstant.getRandomCountry(),  TextConstant.getRandomDanteLine(), random.nextInt(),
							TextConstant.getRandomText(Math.abs(getLimitRandomInt(100,5000) )));
					pS.println(eBean);
				}
				pS.close();
			}
		}else{
			LOGGER.info("The testFiles exists yet... If you want to regenerate it you should delete "+PATH.getPath()+" folder");
		}
	}

	private static Integer getLimitRandomInt(int lowLimit,int higherLimit) {
		Integer randomInteger;
		while ((randomInteger=random.nextInt(higherLimit))<lowLimit);
		return randomInteger;
	}

	public static String getFilePath(int i) {
		return FOLDER_URL+File.separator+FILE_NAME+i+FILE_EXTENSION;
	}


	public static boolean existTestFiles() {
		
		return FOLDER_URL.exists();
	}

}
