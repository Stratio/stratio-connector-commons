package com.stratio.connector.commons.etest.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.Random;

import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.util.TextConstant;

public class TextFileParser {
	
	private static final URL  PATH = Thread.currentThread().getContextClassLoader().getResource("eficiencyFiles");
	
	private static final File FOLDER_URL = new File(PATH.getPath());
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
			for (int i=0;i<4;i++){
				PrintStream pS = new PrintStream(new File(PATH+File.separator+FILE_NAME+i+FILE_EXTENSION));
				for (int j=0;j<1000000;j++){
					EficiencyBean eBean = new EficiencyBean(TextConstant.getRandomName(), random.nextBoolean() , TextConstant.getRandomCountry(),  TextConstant.getRandomDanteLine(), random.nextInt(), 
							TextConstant.getRandomText(Math.abs(random.nextInt()%500)));
					pS.println(eBean);
				}
				pS.close();
			}
		}else{
			LOGGER.info("The testFiles exists yet... If you want to regenerate it you should delete "+PATH.getPath()+" folder");
		}
	}
	
	BufferedReader bf;
	public EficiencyBean getEficiencyBean(int fileNumber) throws IOException{
		EficiencyBean returnEficiencyBean = null;
		if (bf==null){
			bf = new BufferedReader(new  FileReader(PATH+File.separator+FILE_NAME+fileNumber+FILE_EXTENSION));
		}
		String lineRead;
		if ((lineRead= bf.readLine())!=null){
			returnEficiencyBean = new EficiencyBean(lineRead);
		}
		
		return returnEficiencyBean;
		
	}


	public static boolean existTestFiles() {
		
		return FOLDER_URL.exists();
	}

}
