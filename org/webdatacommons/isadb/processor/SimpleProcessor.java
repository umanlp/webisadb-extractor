// http://webdatacommons.org/isadb/
// The WebIsADb and the API are licensed under a Creative Commons Attribution-Non Commercial-Share Alike 3.0 License: http://creativecommons.org/licenses/by-nc-sa/3.0/.
// Acknowledgements
// This work was partially funded by the Deutsche Forschungsgemeinschaft within the JOIN-T project (research grant PO 1900/1-1). Part of the computational resources used for this work were provide by an Amazon AWS in Education Grant award.
// this software is meant to be part of the CommonCrawl framework: http://commoncrawl.org/ to re-build a new WebIsADb from fresh CommonCrawl dumps.

package org.webdatacommons.isadb.processor;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.SocketException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Object;
import org.webdatacommons.framework.processor.FileProcessor;
import org.webdatacommons.framework.processor.ProcessingNode;

public class SimpleProcessor extends ProcessingNode implements FileProcessor{

	private static Logger log = Logger.getLogger(SimpleProcessor.class);
	
	
	@Override
	public Map<String, String> process(ReadableByteChannel fileChannel,
			String inputFileKey) {

		
		//Variables for patternMatching
		String line = "";
		long lineCount = 0;
			
		//Declare Debugging Variables before try Block
		WARCRecord record = null;
		File tempOutputFile = null;
		BufferedWriter bw = null;
		BufferedReader br = null;
				
		try
		{
			tempOutputFile = File.createTempFile("cc-isadb-extraction", ".tab.gz");
			tempOutputFile.deleteOnExit();
			bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(tempOutputFile))));			
			final WARCReader reader = (WARCReader) WARCReaderFactory.get(inputFileKey, Channels.newInputStream(fileChannel), true);
		

		// iterate over each record in the stream
		Iterator<ArchiveRecord> readerIt = reader.iterator();
		while (readerIt.hasNext()) {

			record = (WARCRecord) readerIt.next();
			br = new BufferedReader(new InputStreamReader(
					new BufferedInputStream(record)));
			
				while ((line = br.readLine())!=null) {
					lineCount++;
				}
		}
		bw.write(inputFileKey+"\t"+lineCount);
		
		
			}
			catch (SocketException e)
			{
			log.debug("Sock Exc: Lines "+record.getHeader().getUrl());
			}
			catch (EOFException e)
			{
				log.debug("EOF Exc: "+record.getHeader().getUrl());
			}

			catch (Exception ex) 
			{
				log.info("Lines "+record.getHeader().getUrl());
				
				log.error(ex + " in " + inputFileKey + " for record "
						+ record.getHeader().getUrl(), ex.fillInStackTrace());
				ex.printStackTrace();
			}
		
		finally
		{
			try
			{
				br.close();
				bw.close();
			}
			catch(IOException e)
			{
				log.debug("writer close exception");
			}
		}
		
		try
		{
					String outputFileKey = "data/ex_" + inputFileKey.replace("/", "_") + ".isadb.gz";
					S3Object dataFileObject;
					dataFileObject = new S3Object(tempOutputFile);
					dataFileObject.setKey(outputFileKey);
					getStorage().putObject(getOrCry("resultBucket"), dataFileObject);
		}
				catch (S3ServiceException|NoSuchAlgorithmException|IOException e) {
					log.debug("Error in S3 speicher Block");
		}

		// runtime and rate calculation
		//double duration = (System.currentTimeMillis() - processStartTime) / 1000.0;
		//double rate = (matchesTotal * 1.0) / duration;

		// create data file statistics and return
		Map<String, String> dataStats = new HashMap<String, String>();
		dataStats.put("lineCount", Long.toString(lineCount));
	
		return dataStats;
	}							
}	

