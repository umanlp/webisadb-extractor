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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
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
import org.webdatacommons.isadb.util.CustomPattern;

import com.google.common.net.InternetDomainName;

public class WetProcessor extends ProcessingNode implements FileProcessor{

	private static Logger log = Logger.getLogger(WetProcessor.class);
	
	
	@Override
	public Map<String, String> process(ReadableByteChannel fileChannel,
			String inputFileKey) {

		
		//Variables for patternMatching
		String line = "";
		Matcher matcher = null;
		String extractedPattern;
		String[] extractedPatternParts;
		int onset = 0;
		int offset = 0;
		int linesTotal = 0;
		long startMatchingTime = 0;
		
		//PLD variables
		String tmpUrl = "";
		Boolean pldAlreadyExtracted = false;
		InternetDomainName topPrivateDomain;
		
		//Variables and Constants for sentence splitting
		String[] sentencesRaw;
		ArrayList<String> sentencesClean = new ArrayList<String>();
		Boolean connectWithLatterPart = false;
		String[] abbreviations = {"A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z",
			"Adj","Adm","Adv","Asst","Bart","Bldg","Brig","Bros","Capt","Cmdr","Col","Comdr","Con","Corp","Cpl","DR","Dr","Drs","Ens","Gen","Gov","Hon","Hr","Hosp","Insp","Lt","MM",
			"MR","MRS","MS","Maj","Messrs","Mlle","Mme","Mr","Mrs","Ms","Msgr","Op","Ord","Pfc","Ph","Prof","Pvt","Rep","Reps","Res","Rev","Rt","Sen","Sens","Sfc","Sgt","Sr","St",
			"Supt","Surg","v","vs","i.e","rev","e.g","No","Nos","Art","Nr","pp"};
		String surrounderSymbols 	= 	"[\\u0027\\u2018\\u2019\\u201A\\u201B\\u201C\\u201D\\u201E\\u201F\\u0022]?";
		
		//Variables for ill-formed sentences
		int maxSentenceLength = 250;
		int minSentenceLength = 10;
		
		//Variables to store statistics data
		int sentenceLengthExclusions = 0;
		int pldExtractionExceptions = 0;
		int sentenceSplitExceltions = 0;
		int sentencesTotal = 0;
		int matchesTotal = 0;		
		long processStartTime = System.currentTimeMillis();
		int errorTotal = 0;
		int itemsTotal = 0;
		
		//Initialize patterns
		ArrayList<CustomPattern> allPatterns = new ArrayList<CustomPattern>();
		allPatterns.add(new CustomPattern("p1", "\\,?\\sand\\sother\\s", "compact", true));
		allPatterns.add(new CustomPattern("p2", "\\,?\\sespecially\\s", "compact", true));
		allPatterns.add(new CustomPattern("p3a", "\\,?\\sincluding\\s", "compact", true));
		
		//Variables for duplicate sentences
		int duplicateSentenceExclusions = 0;
		ArrayList<HashSet<Integer>> allSentenceUrlHashes = new ArrayList<HashSet<Integer>>();
		for (int i=0; i<=allPatterns.size(); i++)
		{
			allSentenceUrlHashes.add(new HashSet<Integer>());
		}
		
			
		//Declare Debugging Variables before try Block
		WARCRecord record = null;
		File tempOutputFile = null;
		BufferedWriter bw = null;
		BufferedReader br = null;
				
		// create an tmp output file for the extracted data
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
			itemsTotal++;			
				while ((line = br.readLine())!=null) {
					
					linesTotal++;
					line = line.replaceAll("\\t", "");
					line = line.replaceAll("\\s{2,}", " ");
					//
					//Start Sentence Splitter
					//
					sentencesRaw = line.split("(?<=[\\!\\.\\?]"+surrounderSymbols+")\\s(?="+surrounderSymbols+"\\p{Lu})");
					sentencesClean.add(sentencesRaw[0]);
					for (int z=1; z<sentencesRaw.length; z++)
					{
						for (int j=0; j<abbreviations.length; j++)
						{
							if (sentencesClean.get(sentencesClean.size()-1).endsWith(abbreviations[j]+"."))
							{
								connectWithLatterPart=true;
								break;
							}
						}
						
						if (connectWithLatterPart)
						{
							sentencesClean.set(sentencesClean.size()-1, sentencesClean.get(sentencesClean.size()-1)+" "+sentencesRaw[z]);
						}
						else
						{
							sentencesClean.add(sentencesRaw[z]);
						}
						connectWithLatterPart=false;
					}
					//
					//End Sentence Splitter
					//
						
					if (sentencesClean.size()==0) sentencesClean.add(line); // If no valid sentence is found, the line itself is analyzed

					//
					//Start Analyzing Sentences
					//
										
					for (String sentence : sentencesClean)
					{
						sentencesTotal++;
						//Check for ill-formed sentences
						if (sentence.length()>maxSentenceLength || sentence.length()<minSentenceLength) 
						{
							sentenceLengthExclusions++;
							log.info("sentenceLength Exclsuion "+sentence);
							continue;
						}
						
						for (int i=0; i< allPatterns.size(); i++)
						{
							startMatchingTime = System.nanoTime();
							matcher = allPatterns.get(i).pattern.matcher(line);							
							while(matcher.find())
							{
								//
								//Start Extract PLD
								//
								if (!pldAlreadyExtracted)
								{
									try
									{
										tmpUrl = record.getHeader().getUrl().replaceAll("https?://", "");													//remove protocol								
										if (tmpUrl.contains("/"))tmpUrl = tmpUrl.substring(0,tmpUrl.indexOf("/"));											//remove later subsections starting with a slash.
										if (tmpUrl.contains("?"))tmpUrl = tmpUrl.substring(0,tmpUrl.indexOf("?"));											//remove later subsections starting with a question mark.
										
										//Example http://www.fda.govmailto:info@purefit.com/index.php
										if (tmpUrl.contains("@") && tmpUrl.contains(":"))
										{
											tmpUrl = tmpUrl.substring(tmpUrl.indexOf("@")+1);																//Detected a sort of double URL with username and colon. 
										}
										else
										{
											if (tmpUrl.contains(":"))tmpUrl = tmpUrl.substring(0,tmpUrl.indexOf(":"));										//remove later subsections starting with a colon.
											if (tmpUrl.contains("@"))tmpUrl = tmpUrl.substring(tmpUrl.indexOf("@")+1);										//remove usernames in URLs containing @
										}
										
										//If an exception occurrs (e.g. URL is a IP-Adress), leave tmpUrl as it is.										
										topPrivateDomain = InternetDomainName.from(tmpUrl).topPrivateDomain();
										tmpUrl = topPrivateDomain.toString().substring(25, topPrivateDomain.toString().length()-1);
									}
									catch(IllegalStateException e)
									{
										log.info(e + " in " + inputFileKey + " for record "
												+ record.getHeader().getUrl(), e.fillInStackTrace());
										e.printStackTrace();
										pldExtractionExceptions++;
									}
									catch(IllegalArgumentException e)
									{		
										log.info(e + " in " + inputFileKey + " for record "
												+ record.getHeader().getUrl(), e.fillInStackTrace());
										e.printStackTrace();
										pldExtractionExceptions++;
									}
									pldAlreadyExtracted=true;
								}															
								//
								//End Extract PLD
								//
								
								
								//
								//Start storing pattern and statistics
								//
								
								extractedPattern = matcher.group();
								onset = line.indexOf(extractedPattern, offset);
								offset = onset + extractedPattern.length();
								extractedPatternParts = sentence.split(extractedPattern);
								
								if (extractedPatternParts.length==2)
								{
									if (allSentenceUrlHashes.get(i).contains((extractedPatternParts[0]+extractedPatternParts[1]+tmpUrl).hashCode()))
									{
										duplicateSentenceExclusions++;
										continue; //KÃ¶nnte hier nicht auch break stehen?
									}
									else
									{
										bw.write(allPatterns.get(i).pid+
												"\t"+extractedPatternParts[0]+
												"\t"+extractedPattern+
												"\t"+extractedPatternParts[1]+
												"\t"+linesTotal+
												"\t"+onset+
												"\t"+offset+
												"\t"+tmpUrl+
												"\t"+Long.toString(System.nanoTime()-startMatchingTime));
										bw.newLine();
										matchesTotal++;
										allSentenceUrlHashes.get(i).add((extractedPatternParts[0]+extractedPatternParts[1]+tmpUrl).hashCode());
										
									}
								}
								else
								{
									log.info("Sentence Split failed: "+sentence);
									sentenceSplitExceltions++;
								}
								startMatchingTime = System.nanoTime();
								
								//
								//End storing pattern and statistics
								//
													
							}
							
							//
							//Start reset variables for next pattern
							//
							onset=0;
							offset=0;
					
							//							
							//End reset variables for next pattern
							//
							
						}
						//Reset Variables for next sentence
						pldAlreadyExtracted=false;
						
					}					
					sentencesClean.clear();					
				}
			}
		
			log.info("Lines " + linesTotal +
				" || Runtime "+(System.currentTimeMillis()-processStartTime)+
				" || Matches "+matchesTotal+ 
				" || SLExc "+sentenceLengthExclusions+
				" || PLDExc "+pldExtractionExceptions+
				" || DuplSExc "+duplicateSentenceExclusions+
				" || Items: "+itemsTotal);
		
			// Check if at least one pattern was found
		
			}
			catch (SocketException e)
			{
			log.debug("Sock Exc: Lines " + linesTotal +
					" || Runtime "+(System.currentTimeMillis()-processStartTime)+
					" || Matches "+matchesTotal+ 
					" || SLExc "+sentenceLengthExclusions+
					" || PLDExc "+pldExtractionExceptions+
					" || DuplSExc "+duplicateSentenceExclusions+
					" || Items: "+itemsTotal+
					" || Rec: "+record.getHeader().getUrl());
			}
			catch (EOFException e)
			{
				log.debug("EOF Exc: Lines " + linesTotal +
						" || Runtime "+(System.currentTimeMillis()-processStartTime)+
						" || Matches "+matchesTotal+ 
						" || SLExc "+sentenceLengthExclusions+
						" || PLDExc "+pldExtractionExceptions+
						" || DuplSExc "+duplicateSentenceExclusions+
						" || Items: "+itemsTotal+
						" || Rec: "+record.getHeader().getUrl());
			}

			catch (Exception ex) 
			{
				System.out.println(record.getHeader().getUrl()+" "+inputFileKey+" "+ex.toString());
				log.info("Lines " + linesTotal +
						" || Runtime "+(System.currentTimeMillis()-processStartTime)+
						" || Matches "+matchesTotal+ 
						" || SLExc "+sentenceLengthExclusions+
						" || PLDExc "+pldExtractionExceptions+
						" || DuplSExc "+duplicateSentenceExclusions+
						" || Items: "+itemsTotal+
						" || Rec: "+record.getHeader().getUrl());
				
				log.error(ex + " in " + inputFileKey + " for record "
						+ record.getHeader().getUrl(), ex.fillInStackTrace());
				ex.printStackTrace();
				errorTotal++;
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
		
		allSentenceUrlHashes.clear();
		
		if (matchesTotal > 0) {
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
		}

		// runtime and rate calculation
		double duration = (System.currentTimeMillis() - processStartTime) / 1000.0;
		double rate = (matchesTotal * 1.0) / duration;

		// create data file statistics and return
		Map<String, String> dataStats = new HashMap<String, String>();
		dataStats.put("duration", Double.toString(duration));
		dataStats.put("rate", Double.toString(rate));
		dataStats.put("sentencesTotal", Double.toString(sentencesTotal));
		dataStats.put("linesTotal", Double.toString(linesTotal));
		dataStats.put("pldExtractionExceptions", Integer.toString(pldExtractionExceptions));
		dataStats.put("sentenceLengthExclusions", Integer.toString(sentenceLengthExclusions));
		dataStats.put("sentenceSplitExceltions", Integer.toString(sentenceSplitExceltions));
		dataStats.put("errorTotal", Integer.toString(errorTotal));
	
		return dataStats;
	}							
}	

