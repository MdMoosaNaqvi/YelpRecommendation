package com.uofm.filesplit;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.Date;
import java.util.regex.Pattern;

public class FileSplit {
	
	public static void main(String []args) {
		String csvFilePath = args[0];
		String splitFilePath = args[1];
		new FileSplit().splitReviewFile(csvFilePath, splitFilePath);
		//new FileSplit().readFolder("D:/split_output_review/");
	}
	
	public void splitFile(String csvFilePath, String splitFilePath) {
		try {
			FileReader fileReader = new FileReader(csvFilePath);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line="";
			int fileSize = 0;
			BufferedWriter fos = new BufferedWriter(new FileWriter(splitFilePath + new Date().getTime()+".csv",true));
			boolean isFirstLine = true;
			String headers = "";
			while((line = bufferedReader.readLine()) != null) {
				if(isFirstLine) {
					headers = line;
					isFirstLine = false;
				}
				if(fileSize + line.getBytes().length > 9.5 * 1024 * 1024){
					fos.flush();
					fos.close();
					fos = new BufferedWriter(new FileWriter(splitFilePath + new Date().getTime()+".csv",true));
					fos.write(headers + "\n");
					fos.write(line+"\n");
					fileSize = line.getBytes().length;
				}else{
					fos.write(line+"\n");
					fileSize += line.getBytes().length;
				}
			}          
			fos.flush();
			fos.close();
			bufferedReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void splitReviewFile(String csvFilePath, String splitFilePath) {
		try {
			FileReader fileReader = new FileReader(csvFilePath);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line="";
			int fileSize = 0;
			BufferedWriter fos = new BufferedWriter(new FileWriter(splitFilePath + new Date().getTime()+".csv",true));
			boolean isFirstLine = true;
			String headers = "";
			String regexe = "^.*\\d,\\d,\\d$";
			//boolean isRegexPattern = false;
			while((line = bufferedReader.readLine()) != null) {
				if(isFirstLine) {
					headers = line;
					isFirstLine = false;
				}
				
				//Check if file size exceded 9.5MB and it is the end of review record 
				if(fileSize + line.getBytes().length > 9.5 * 1024 * 1024){
					//	isRegexPattern = Pattern.matches(regexe, line);
					//System.out.println(line);
					//System.out.println("______________________________________________________________________________________");
					if(Pattern.matches(regexe, line)) {
						fos.write(line+"\n");
						fos.flush();
						fos.close();
						fos = new BufferedWriter(new FileWriter(splitFilePath + new Date().getTime()+".csv",true));
						fos.write(headers + "\n");
						fileSize = line.getBytes().length;
					}
				}else{
					fos.write(line+"\n");
					fileSize += line.getBytes().length;
				}
			}          
			fos.flush();
			fos.close();
			bufferedReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void readFolder(String folderPath) {
		File file = new File(folderPath); 
		String[] fileList = file.list();
		String completeFilePath = "";
		for (String filePath : fileList) {
			completeFilePath = folderPath + "/" + filePath;
			System.out.println(folderPath + "/" + filePath);
			//replaceInFile(completeFilePath);
			replaceInFile(completeFilePath);
		}
	}
	
	public void replaceInFile(String fileName) {
		try {
			File file = new File(fileName);
			File tempFile = File.createTempFile("buffer", ".tmp");
			FileWriter fw = new FileWriter(tempFile);
			
			Reader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			
			while(br.ready()) {
				String modifiedLine = br.readLine().replaceAll("\\\\", "/");
				//System.out.println(modifiedLine);
				fw.write(modifiedLine + "\n");
			}
			
			fw.close();
			br.close();
			fr.close();
			
			// Finally replace the original file.
			String newFileName = fileName.substring(0, fileName.lastIndexOf("_")) + "_" + new Date().getTime() + ".csv";

			System.out.println(tempFile.renameTo(new File(newFileName)));
			System.out.println(file.delete());
			System.out.println("__________________________________________");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}

