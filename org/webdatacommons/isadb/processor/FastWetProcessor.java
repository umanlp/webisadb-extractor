// http://webdatacommons.org/isadb/
// The WebIsADb and the API are licensed under a Creative Commons Attribution-Non Commercial-Share Alike 3.0 License: http://creativecommons.org/licenses/by-nc-sa/3.0/.
// Acknowledgements
// This work was partially funded by the Deutsche Forschungsgemeinschaft within the JOIN-T project (research grant PO 1900/1-1). Part of the computational resources used for this work were provide by an Amazon AWS in Education Grant award.
// this software is meant to be part of the CommonCrawl framework: http://commoncrawl.org/ to re-build a new WebIsADb from fresh CommonCrawl dumps.


package org.webdatacommons.isadb.processor;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.jets3t.service.model.S3Object;
import org.webdatacommons.framework.processor.FileProcessor;
import org.webdatacommons.framework.processor.ProcessingNode;
import org.webdatacommons.isadb.util.CustomPattern;
import org.webdatacommons.isadb.util.NounPhrase;

import com.google.common.base.Splitter;
import com.google.common.net.InternetDomainName;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

public class FastWetProcessor extends ProcessingNode implements FileProcessor{

	private static Logger log = Logger.getLogger(FastWetProcessor.class);
	private static int maxNpSize;
	
	@Override
	public Map<String, String> process(ReadableByteChannel fileChannel,
			String inputFileKey) throws Exception {

		String separatorSymbols  	= 	"[\\u002D\\u2010\\u2011\\u2012\\u2013\\u2014\\u2015\\u2043]?"; 
		String surrounderSymbols 	= 	"[\\u0027\\u2018\\u2019\\u201A\\u201B\\u201C\\u201D\\u201E\\u201F\\u0022]?"; // Apostrophee, LEFT SINGLE QUOTATION MARK, RIGHT SINGLE QUOTATION MARK, SINGLE LOW-9 QUOTATION MARK, SINGLE HIGH-REVERSED-9 QUOTATION MARK, LEFT DOUBLE QUOTATION MARK, RIGHT DOUBLE QUOTATION MARK, DOUBLE LOW-9 QUOTATION MARK, DOUBLE HIGH-REVERSED-9 QUOTATION MARK, Quotation Mark
		String endSymbols			=	"[\"\\u0026\\u0027\\u2018\\u2019\\u201A\\u201B\\u201C\\u201D\\u201E\\u201F\\u00A9\\u00AE]?";
		
		//Nounphrase Placholders for Splitted patterns
		String npPlaceholder = "("+surrounderSymbols+""							//Quotation mark could be in front
				+ "(\\p{L}++|\\d++\\p{L}++)"									//Word can start with letters or digits but must contain letters
				+ "("+separatorSymbols+"(\\p{L}++|\\d++))?"						//Can be separated by a hyphen
				+ endSymbols+"\\s)"												//Can be followed by quotation mark
				+ "{1,4}";														//NP can consist of up to 4 words
		
		String npPlaceholderAdjMost = "("+surrounderSymbols+""					//Quotation mark could be in front
				+ "(\\p{L}++|\\d++\\p{L}++)"									//Word can start with letters or digits but must contain letters
				+ "("+separatorSymbols+"(\\p{L}++|\\d++))?"						//Can be separated by a hyphen
				+ endSymbols+"\\s)"												//Can be followed by quotation mark
				+ "{2,5}";														//NP can consist of up to 4 words and 1 mandatory word for the adjective

		// create an tmp output file for the extracted data and write a header line; 
		File tempOutputFile = File.createTempFile("cc-isadb-extraction", ".tab.gz");
		tempOutputFile.deleteOnExit();
		String outputFileKey = "data/ex_" + inputFileKey.replace("/", "_")
				+ ".isadb.gz";
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(tempOutputFile)), "UTF-8"));
		bw.write("PID\tSentence\tInstances\tInstTags\tClasses\tClassTags\tOnset\tOffset\tPLD\tMatchingTime");
		bw.newLine();
		
		//Variables for patternMatching
		String line = "";
		String lineChunk = "";
		ArrayList<String> sentenceBuffer = new ArrayList<String>();
		Matcher patternMatcher = null;
		String extractedPattern;
		int onset = 0;
		int offset = 0;
		long startMatchingTime = 0;
		maxNpSize = 4;
		
		//Tuple Extraction Variables
		MaxentTagger tagger = new MaxentTagger("english-left3words-distsim.tagger");
		List<TaggedWord> taggedWordsBeforePattern;
		List<TaggedWord> taggedWordsAfterPattern;		
		ArrayList<NounPhrase> currentNPsBeforePattern = new ArrayList<NounPhrase>();
		ArrayList<NounPhrase> currentNPsAfterPattern = new ArrayList<NounPhrase>();
		
		List<HasWord> fullWordList;
		List<TaggedWord> fullTaggedList;
		StringBuilder resultLine = new StringBuilder();
		String potentialGenitiveWord;
		
		//PLD variables
		String tmpUrl = "";
		Boolean pldAlreadyExtracted = false;
		InternetDomainName topPrivateDomain;
		
		//Variables and Constants for sentence splitting
		String[] sentencesRaw;
		ArrayList<String> sentencesClean = new ArrayList<String>();
		Boolean connectWithLatterPart = false;
		String[] abbreviations = {"A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z",
			"Adj","Adm","Adv","Asst","Bart","Bldg","Brig","Bros","Capt","Cmdr","Col","Comdr","Con","Corp","Cpl","DR","Dr","Drs","Ens", "Fig", "FIG", "fig", "Gen","Gov","Hon","Hr","Hosp","Insp","Lt","MM",
			"MR","MRS","MS","Maj","Messrs","Mlle","Mme","Mr","Mrs","Ms","Msgr","Op","Ord","Pat","Pfc","Ph","Prof","Pvt","Rep","Reps","Res","Rev","Rt","Sen","Sens","Sfc","Sgt","Sr","St",
			"Supt","Surg","v","vs", "U.S","u.s", "U.K","u.k","i.e","rev","e.g","No","Nos","Art","Nr","pp"};
		
		//Variables and Constants for false-positive detection
		String[] demonstratives 			= {"that","this","these","those"};
		String[] possessives				= {"mine","yours","his","hers","its","ours","theirs"};
		String[] personals					= {"i","you","he","she","it","we","they"};
		String[] questions					= {"where", "who", "when", "what", "why", "whose", "which", "how"};		

		String[] allExclusions = concatAll(demonstratives, possessives, personals, questions);
		String pronounHolder = "";
		Boolean pronounFrontExcluded = false;
		Boolean pronounBackExcluded = false;
						
		//Initialize patterns
		ArrayList<CustomPattern> allPatterns = new ArrayList<CustomPattern>();

		allPatterns.add(new CustomPattern("p8a", "\\,?\\sis\\san?\\s", "compact", "is\\sa", true));
		allPatterns.add(new CustomPattern("p3a", "\\,?\\sincluding\\s", "compact", "including", false));
		allPatterns.add(new CustomPattern("p5", "\\,?\\ssuch\\sas\\s", "compact", "such\\sas", false));
		allPatterns.add(new CustomPattern("p1", "\\,?\\sand\\sother\\s", "compact", "and\\sother", true));
		allPatterns.add(new CustomPattern("p8b", "\\,?\\swas\\san?\\s", "compact", "was\\sa", true));
		allPatterns.add(new CustomPattern("p4", "\\,?\\sor\\sother\\s", "compact", "or\\sother", true));
		allPatterns.add(new CustomPattern("p2", "\\,?\\sespecially\\s", "compact", "especially", false));
		allPatterns.add(new CustomPattern("p8c", "\\,?\\sare\\san?\\s", "compact", "are\\sa", true));		
		allPatterns.add(new CustomPattern("p34", "\\stypes\\s", "compact", "types", false));
		allPatterns.add(new CustomPattern("p25", "\\,?\\sexcept\\s", "compact", "except", false));
		allPatterns.add(new CustomPattern("p23d", "\\,?\\sparticularly\\s", "compact", "particularly", false));
		allPatterns.add(new CustomPattern("p20a", "\\sis\\sthe\\s\\w+est\\s", "compact", "is\\sthe", true));
		allPatterns.add(new CustomPattern("p43", "\\,?\\ssort\\sof\\s", "compact", "sort\\sof", true));
		allPatterns.add(new CustomPattern("p26", "\\,?\\sother\\sthan\\s", "compact", "other\\sthan", false));
		
		allPatterns.add(new CustomPattern("p21a", "\\p{L}+est\\s"+npPlaceholder+"is\\s", "split_noPrefix", "est\\s", "est", "is", false));
		allPatterns.add(new CustomPattern("p21b", "\\p{L}+est\\s"+npPlaceholder+"are\\s", "split_noPrefix", "est\\s", "est", "are", false));
		allPatterns.add(new CustomPattern("p21c", "\\s(M|m)ost\\s"+npPlaceholderAdjMost+"is\\s", "split_noPrefix", "most\\s", "most", "is", false));
		allPatterns.add(new CustomPattern("p21d", "\\s(M|m)ost\\s"+npPlaceholderAdjMost+"are\\s", "split_noPrefix", null, "most", "are", false));
				
		allPatterns.add(new CustomPattern("p23b", "\\,?\\smostly\\s", "compact", "mostly", false));
		allPatterns.add(new CustomPattern("p23a", "\\,?\\smainly\\s", "compact", "mainly", false));
		allPatterns.add(new CustomPattern("p12a", "\\,\\sone\\sof\\sthe\\s", "compact", "one\\sof\\sthe", true));
		allPatterns.add(new CustomPattern("p20c", "\\sis\\sthe\\smost\\s\\w+\\s", "compact", true));
		allPatterns.add(new CustomPattern("p8d", "\\,?\\swere\\san?\\s", "compact", "were\\sa", true));
		allPatterns.add(new CustomPattern("p6", "\\,?\\sand\\sany\\sother\\s", "compact", "and\\sany\\sother", true));
		allPatterns.add(new CustomPattern("p15a", "\\sexamples\\sof\\s", "compact", "examples\\sof", true));
		allPatterns.add(new CustomPattern("p27a", "\\,?\\se\\.g\\.\\s", "compact", "e\\.g\\.", false));
		allPatterns.add(new CustomPattern("p27b", "\\,?\\si\\.e\\.\\s", "compact", "i\\.e\\.", false));
		allPatterns.add(new CustomPattern("p16", "\\,?\\sfor\\sexample\\s", "compact", "for\\sexample", false));
		allPatterns.add(new CustomPattern("p24", "\\,?\\sin\\sparticular\\s", "compact", "in\\sparticular", false));
		allPatterns.add(new CustomPattern("p20b", "\\sare\\sthe\\s\\w+est\\s", "compact", "are\\sthe", true));
		allPatterns.add(new CustomPattern("p20d", "\\sare\\sthe\\smost\\s\\w+\\s", "compact", true));
		allPatterns.add(new CustomPattern("p23c", "\\,?\\snotably\\s", "compact", "notably", false));
		allPatterns.add(new CustomPattern("p39", "\\,?\\samong\\sthem\\s", "compact", "\\samong\\sthem", false));
		allPatterns.add(new CustomPattern("p38", "\\scompared\\sto\\sother\\s", "compact", "compared\\sto", true));
		allPatterns.add(new CustomPattern("p11", "\\,?\\slike\\sother\\s", "compact", "like\\sother", true));
		allPatterns.add(new CustomPattern("p7", "\\,?\\sand\\ssome\\sother\\s", "compact", "and\\some\\sother", true));
		allPatterns.add(new CustomPattern("p23e", "\\,?\\sprincipally\\s", "compact", "principally", false));		
		allPatterns.add(new CustomPattern("p15b", "\\sis\\san\\sexample\\sof\\s", "compact", "is\\san\\sexample\\sof", true));
		allPatterns.add(new CustomPattern("p22a", "\\,?\\swhich\\sis\\scalled\\s", "compact", "which\\sis\\scalled", false));		
		allPatterns.add(new CustomPattern("p28a", "\\,?\\sa\\skind\\sof\\s", "compact", "a\\skind\\sof", true));
		allPatterns.add(new CustomPattern("p12c", "\\,\\sone\\sof\\sthose\\s", "compact", "one\\sof\\sthose", true));
		allPatterns.add(new CustomPattern("p29a", "\\,?\\swhich\\slooks?\\slike\\s", "compact", "which\\slooks?\\slike", false));
		allPatterns.add(new CustomPattern("p28c", "\\,?\\sa\\sform\\sof\\s", "compact", "a\\sform\\sof", true));
		allPatterns.add(new CustomPattern("p30b", "\\,?\\swhich\\sis\\ssimilar\\sto\\s", "compact", "which\\sis\\ssimilar\\sto", false));
		allPatterns.add(new CustomPattern("p12b", "\\,\\sone\\sof\\sthese\\s", "compact", "one\\sof\\sthese", true));
		allPatterns.add(new CustomPattern("p29c", "\\,?\\swhich\\ssounds?\\slike\\s", "compact", "which\\ssounds?\\slike", false));
		allPatterns.add(new CustomPattern("p28d", "\\,?\\sforms\\sof\\s", "compact", "forms\\sof", true));
		allPatterns.add(new CustomPattern("p30a", "\\,?\\swhich\\sare\\ssimilar\\sto\\s", "compact", "which\\sare\\ssimilar\\sto", false));
		allPatterns.add(new CustomPattern("p22b", "\\,?\\swhich\\sis\\snamed\\s", "compact", "which\\sis\\snamed", false));
		allPatterns.add(new CustomPattern("p42", "\\,?\\sor\\sthe\\smany\\s", "compact", "or\\sthe\\smany", true));
		allPatterns.add(new CustomPattern("p31a", "\\,?\\sexample\\sof\\sthis\\sis\\s", "compact", "example\\sof\\sthis\\sis", false));
		allPatterns.add(new CustomPattern("p28b", "\\,?\\skinds\\sof\\s", "compact", "kinds\\sof", true));	
		allPatterns.add(new CustomPattern("p31b", "\\,?\\sexamples\\sof\\sthis\\sare\\s", "compact", "examples\\sof\\sthis\\sare", false));
		
		allPatterns.add(new CustomPattern("p10", "(S|s)uch\\s"+npPlaceholder+"as\\s", "split_noPrefix", "(S|s)uch\\s", "such", "as", false));
		allPatterns.add(new CustomPattern("p13", "(E|e)xample\\sof\\s"+npPlaceholder+"is\\s", "split_noPrefix", "example\\sof", "example of", "is", false));
		allPatterns.add(new CustomPattern("p14", "(E|e)xamples\\sof\\s"+npPlaceholder+"are\\s", "split_noPrefix", null, "examples of", "are", false));
		allPatterns.add(new CustomPattern("p36", "\\swhether\\s"+npPlaceholder+"or\\s", "split", " whether", "whether", "or", false));
		allPatterns.add(new CustomPattern("p37", "(C|c)ompare\\s"+npPlaceholder+"with\\s", "split_noPrefix", "compare\\s", "compare", "with", true));
		
		//Variables to store statistics data
		int sentenceLongExclusions = 0;
		int sentenceShortExclusions = 0;
		int pldExtractionExceptions = 0;
		int pronounFrontExclusions = 0;
		int pronounBackExclusions = 0;
		int sentencesTotal = 0;
		int matchesTotal = 0;		
		long processStartTime = System.currentTimeMillis();
		int errorTotal = 0;
		ArrayList<Long> patternDurations = new ArrayList<Long>();
		ArrayList<Integer> patternMatches = new ArrayList<Integer>();
		long tuplesTotal = 0;
					
		//Variables for processing Limitations
		int maxLineLength = 2500;
		int maxSentenceLength = 400;
		int minSentenceLength = 10;
		String allPreconditions = "";
		
		//Initialize data lists
		for (CustomPattern pat : allPatterns)
		{
			patternDurations.add(new Long(0));
			patternMatches.add(0);
			if (pat.type!=null)
			{
				allPreconditions += pat.preCondition+"|";
			}
		}
		Pattern preCheckPattern = Pattern.compile(allPreconditions.substring(0,allPreconditions.length()-1));
		Matcher preCheckMatcher = null;
		
		//Variables for duplicate sentences
		int duplicateSentenceExclusions = 0;
		ArrayList<HashSet<Integer>> allSentenceUrlHashes = new ArrayList<HashSet<Integer>>();
		for (int i=0; i<=allPatterns.size(); i++)
		{
			allSentenceUrlHashes.add(new HashSet<Integer>());
		}
		
		//Performance Timers
		long sentSplitTimer = 0;
		long sentSplitTimeTotal = 0;
		
		long pldExtractTimer = 0;
		long pldExtractTimeTotal = 0;
		
		long preCheckTimer = 0;
		long preCheckTimeTotal = 0;
		
		long pronounCheckTimer = 0;
		long pronounCheckTimeTotal = 0;
		
		long matchingTimeTotal = 0;				
		
		final WARCReader reader = (WARCReader) WARCReaderFactory.get(inputFileKey, Channels.newInputStream(fileChannel), true);
		WARCRecord record = null;
		BufferedReader br = null;
		
		// iterate over each record in the stream
		Iterator<ArchiveRecord> readerIt = reader.iterator();
		while (readerIt.hasNext()) {
			try 
			{
			record = (WARCRecord) readerIt.next();
			br = new BufferedReader(new InputStreamReader(
					new BufferedInputStream(record)));
			
				while (br.ready()) 
				{					
					line = br.readLine();	
					
					//If line is below min. Sentence-Length Processing is skipped
					if (line.length()<minSentenceLength) continue;
					
					//Line is split into chunks to avoid bad regex performance
					Iterator<String> lineChunks = Splitter.fixedLength(maxLineLength).split(line).iterator();
					
					//Iterate through line chunks
					while (lineChunks.hasNext())
					{					
						lineChunk = lineChunks.next();
						
						//Pre-check the line chunk for Pattern Keywords
						preCheckTimer  = System.nanoTime();
						preCheckMatcher = preCheckPattern.matcher(lineChunk);
						if (!preCheckMatcher.find())
						{
							continue;
						}						
						preCheckTimeTotal += (System.nanoTime()-preCheckTimer)/1000;
						
						//						
						//Start Sentence Splitter
						//						
						sentSplitTimer = System.nanoTime();
						sentencesRaw = lineChunk.split("(?<=[\\!\\.\\?]"+surrounderSymbols+")\\s(?="+surrounderSymbols+"\\p{Lu})");
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
						
						// If no valid sentence is found, the line itself is analyzed
						if (sentencesClean.size()==0) sentencesClean.add(line);
						sentSplitTimeTotal += (System.nanoTime() - sentSplitTimer) /1000;
						//
						//End Sentence Splitter
						//
						
						sentenceBuffer = new ArrayList<String>();
						for (int i=0; i<sentencesClean.size(); i++)
						{
							
							if (sentencesClean.get(i).length()>maxSentenceLength)
							{
								Iterator<String> sentenceChunks = Splitter.fixedLength(maxSentenceLength).split(sentencesClean.get(i)).iterator();
								while (sentenceChunks.hasNext())
								{
									sentenceBuffer.add(sentenceChunks.next());
								}
							}
							else 
							{
								sentenceBuffer.add(sentencesClean.get(i));
							}
						}
						sentencesClean = sentenceBuffer;
												
						//
						//Start Analyzing Sentences
						//
											
						for (String sentence : sentencesClean)
						{
							
							sentencesTotal++;
							//Check for ill-formed sentences
							if (sentence.length()>maxSentenceLength) 
							{
								sentenceLongExclusions++;
								continue;
							}
							if (sentence.length()<minSentenceLength)
							{
								sentenceShortExclusions++;
								continue;
							}
							
							// Help the Pos-Tagger with Apostrophies and QuotationMarks; Replace as many as possible, without losing the meaning;
							sentence = sentence.replaceAll("\\s+", " ");
							sentence = replaceVerbApostrophies(sentence);
							sentence = sentence.replaceAll("(?<!s)[\\u201A\\u201C\\u201D\\u201E\\u201F\\u0022](?!s)","");
							
							for (int i=0; i< allPatterns.size(); i++)
							{
								startMatchingTime = System.nanoTime();
								patternMatcher = allPatterns.get(i).pattern.matcher(sentence);							
								while(patternMatcher.find())
								{
									//
									//Start Extract PLD
									//
									if (!pldAlreadyExtracted)
									{
										pldExtractTimer = System.nanoTime();
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
											tmpUrl = topPrivateDomain.toString().substring(24, topPrivateDomain.toString().length()-1);
										}
										catch(IllegalStateException|IllegalArgumentException e)
										{
											pldExtractionExceptions++;
										}
										pldAlreadyExtracted=true;
										pldExtractTimeTotal += (System.nanoTime()-pldExtractTimer)/1000;
									}															
									//
									//End Extract PLD
									//
																		
									//
									//Start storing pattern and statistics
									//
									extractedPattern = patternMatcher.group();								
									onset  = sentence.indexOf(extractedPattern, offset);
									offset = onset + extractedPattern.length();
									
									// Compact Patterns still contain an Indicator for the leading and the following Nounphrase
									// This indicator has to be removed
									if (allPatterns.get(i).type.equals("compact"))
									{
										onset++;
										offset--;
									}
									
									// Check if a leading pronoun can be excluded
									pronounCheckTimer = System.nanoTime();									
									pronounHolder = sentence.substring(0, onset);
									if (pronounHolder.indexOf(" ")!=-1)
									{
										pronounHolder= pronounHolder.substring(pronounHolder.lastIndexOf(" ")+1).toLowerCase();
									}
									
								 	for (int f=0; f<allExclusions.length && !pronounHolder.equals(""); f++)
								 	{
									 	if(pronounHolder.toLowerCase().equals(allExclusions[f]))
										{
											pronounFrontExclusions++;
											pronounFrontExcluded=true;
											break;
										}
								 	}
								 	
								 	// Check if a following pronoun can be excluded
									pronounHolder = sentence.substring(offset);
									if (pronounHolder.indexOf(" ")!=-1)
									{
										pronounHolder = pronounHolder.substring(0, pronounHolder.indexOf(" ")).toLowerCase();
									}
								 	for (int f=0; f<allExclusions.length && !pronounHolder.equals(""); f++)
								 	{
									 	if(pronounHolder.toLowerCase().equals(allExclusions[f]))
										{
											pronounBackExclusions++;
											pronounBackExcluded=true;
											break;
										}
								 	}
									pronounCheckTimeTotal += (System.nanoTime()- pronounCheckTimer)/1000;
								 	if(pronounBackExcluded || pronounFrontExcluded)
								 	{
								 		pronounBackExcluded=false;
								 		pronounFrontExcluded=false;
								 		continue;
								 	}
								 	
								 	//Check if the sentence has already been processed/stored
									if (allSentenceUrlHashes.get(i).contains((sentence+tmpUrl).hashCode()))
									{
										duplicateSentenceExclusions++;
										break;
									}
									else
									{
										//
										//Start Extracting single Tupels
										//
										try
										{
											fullWordList = Sentence.toWordList(sentence.split(" "));
											fullTaggedList = tagger.tagSentence(fullWordList);
											
											for (TaggedWord tw : fullTaggedList)
											{	
												potentialGenitiveWord = tw.word();
												if (potentialGenitiveWord.length() < tw.word().replaceAll("(?<=s)[\\u201A\\u201C\\u201D\\u201E\\u201F\\u0022]", "").length() || potentialGenitiveWord.length() < tw.word().replaceAll("[\\u201A\\u201C\\u201D\\u201E\\u201F\\u0022](?=s)", "").length())
												{
													tw.setTag("JJ");
												}
											}
											
											if (allPatterns.get(i).type.equals("compact"))
											{
												taggedWordsBeforePattern = getWordListSubset(0, onset+1, fullTaggedList);
												taggedWordsAfterPattern = getWordListSubset(offset, sentence.length(), fullTaggedList);
											}
											else
											{
												taggedWordsBeforePattern = getWordlistBeforeSplittedPattern(allPatterns.get(i), sentence, onset, fullTaggedList);
												taggedWordsAfterPattern = getWordlistAfterSplittedPattern(allPatterns.get(i), sentence, onset, offset, fullTaggedList);
											}
																					
											Collections.reverse(taggedWordsBeforePattern);
											findNextNounPhraseReverse(0, taggedWordsBeforePattern, currentNPsBeforePattern);
											findNextNounPhrase(0, taggedWordsAfterPattern, currentNPsAfterPattern);
											
											if (currentNPsAfterPattern.size()==0 || currentNPsBeforePattern.size()==0)
											{
												fullWordList.clear();
												taggedWordsBeforePattern.clear();
												taggedWordsAfterPattern.clear();
												currentNPsBeforePattern.clear();
												currentNPsAfterPattern.clear();
												pronounFrontExcluded = false;
												pronounBackExcluded = false;
												continue;												
											}
										
										}
										
										catch (StringIndexOutOfBoundsException e)
										{
											log.error("NP-Extraction ERROR: "+sentence+" "+allPatterns.get(i).pid+" "+nounPhraseListToString(currentNPsBeforePattern)+" "+nounPhraseListToString(currentNPsAfterPattern)+" "+onset+" "+offset);
										}
										
										//
										//End Extracting single Tuples
										//
									
										resultLine.setLength(0);
										resultLine.append(allPatterns.get(i).pid).append("\t")
											.append(sentence.replace("\\t", " ")).append("\t");
										
										if (allPatterns.get(i).instanceFirst)
										{
											resultLine.append(nounPhraseListToString(currentNPsBeforePattern)).append("\t")
											.append(nounPhraseListTagsToString(currentNPsBeforePattern)).append("\t")
											.append(nounPhraseListToString(currentNPsAfterPattern)).append("\t")
											.append(nounPhraseListTagsToString(currentNPsAfterPattern)).append("\t");
										}
										else
										{
											resultLine.append(nounPhraseListToString(currentNPsAfterPattern)).append("\t")
											.append(nounPhraseListTagsToString(currentNPsAfterPattern)).append("\t")
											.append(nounPhraseListToString(currentNPsBeforePattern)).append("\t")
											.append(nounPhraseListTagsToString(currentNPsBeforePattern)).append("\t");
										}
											
										resultLine.append(onset).append("\t")
											.append(offset).append("\t")
											.append(tmpUrl).append("\t")
											.append(Long.toString((System.nanoTime()-startMatchingTime)/1000));
										
										bw.write(resultLine.toString());
										patternDurations.set(i, patternDurations.get(i)+(System.nanoTime()-startMatchingTime)/1000);
										patternMatches.set(i, patternMatches.get(i)+1);
										matchingTimeTotal += (System.nanoTime()-startMatchingTime)/1000;
										tuplesTotal+= currentNPsBeforePattern.size() * currentNPsAfterPattern.size();
										bw.newLine();
										currentNPsBeforePattern.clear();
										currentNPsAfterPattern.clear();
										pronounFrontExcluded = false;
										pronounBackExcluded = false;
										matchesTotal++;
										allSentenceUrlHashes.get(i).add((sentence+tmpUrl).hashCode());										
									}																
									startMatchingTime = System.nanoTime();									
									//
									//End storing pattern and statistics
									//	
								}
								onset = 0;
								offset = 0;
							}							
						}					
						sentencesClean.clear();						
					}
				}
			} 
			catch (Exception ex) {
				log.error(ex + " in " + inputFileKey + " for record "
						+ record.getHeader().getUrl(), ex.fillInStackTrace());
				ex.printStackTrace();
				errorTotal++;
			} finally {
				br.close();
			}
			pldAlreadyExtracted=false;
		}
		bw.close();
		
		S3Object dataFileObject;
		dataFileObject = new S3Object(tempOutputFile);
		dataFileObject.setKey(outputFileKey);
		getStorage().putObject(getOrCry("resultBucket"), dataFileObject);
		
		// runtime and rate calculation
		double duration = (System.currentTimeMillis() - processStartTime) / 1000.0;
		
		// create data file statistics and return
		Map<String, String> dataStats = new HashMap<String, String>();
		dataStats.put("duration", Double.toString(duration));
		dataStats.put("matchesTotal", Double.toString(matchesTotal));
		dataStats.put("tuplesTotal", Double.toString(tuplesTotal));
		dataStats.put("sentencesTotal", Double.toString(sentencesTotal));
		dataStats.put("pldExtractionExceptions", Integer.toString(pldExtractionExceptions));
		dataStats.put("duplicateExclusions", Integer.toString(duplicateSentenceExclusions));
		dataStats.put("pronounFrontExclusions", Integer.toString(pronounFrontExclusions));
		dataStats.put("pronounBackExclusions", Integer.toString(pronounBackExclusions));
		dataStats.put("matchesDetail", dataListOutputInt(patternMatches, allPatterns));
		dataStats.put("durationDetail", dataListOutputLong(patternDurations, allPatterns));
		dataStats.put("sentenceLongExclusions", Integer.toString(sentenceLongExclusions));
		dataStats.put("sentenceShortExclusions", Integer.toString(sentenceShortExclusions));
		dataStats.put("matchingTime", Long.toString(matchingTimeTotal));
		dataStats.put("sentSplitTime", Long.toString(sentSplitTimeTotal));
		dataStats.put("preCheckTime", Long.toString(preCheckTimeTotal));
		dataStats.put("pldExtractTime", Long.toString(pldExtractTimeTotal));
		dataStats.put("pronounCheckTime", Long.toString(pronounCheckTimeTotal));
		dataStats.put("errorTotal", Integer.toString(errorTotal));

		return dataStats;
	}	
	
	public static <T> T[] concatAll(T[] first, T[]... rest) {
		  int totalLength = first.length;
		  for (T[] array : rest) {
		    totalLength += array.length;
		  }
		  T[] result = Arrays.copyOf(first, totalLength);
		  int offset = first.length;
		  for (T[] array : rest) {
		    System.arraycopy(array, 0, result, offset, array.length);
		    offset += array.length;
		  }
		  return result;
	}
	
	public static String dataListOutputLong(ArrayList<Long> input, ArrayList<CustomPattern> cps)
	{
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<cps.size(); i++)
		{
			sb.append(cps.get(i).pid).append(" ").append(input.get(i)).append("|");
		}
		return sb.toString();
	}
	
	public static String dataListOutputInt(ArrayList<Integer> input, ArrayList<CustomPattern> cps)
	{
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<cps.size(); i++)
		{
			sb.append(cps.get(i).pid).append(" ").append(input.get(i)).append("|");
		}
		return sb.toString();
	}
			
	public static String nounPhraseListToString(ArrayList<NounPhrase> nps)
	{
		if (nps.size()==0)
		{
			return "{}";
		}
		StringBuilder result = new StringBuilder();
		result.append("{");
		for (NounPhrase np: nps)
		{
			result.append(np.toString()).append("|");
		}
		result.setLength(result.length()-1);
		result.append("}");
		return result.toString();
	}
	
	public static String nounPhraseListTagsToString(ArrayList<NounPhrase> nps)
	{
		if (nps.size()==0)
		{
			return "{}";
		}
		StringBuilder result = new StringBuilder();
		result.append("{");
		for (NounPhrase np: nps)
		{
			result.append(np.tagsToString()).append("|");
		}
		result.setLength(result.length()-1);
		result.append("}");
		return result.toString();
	}
	
	public static ArrayList<TaggedWord> reducePostModifier(ArrayList<TaggedWord> np)
	{
		if (np.size()==0 || np==null)
		{
			return np;
		}
		while (!(np.get(np.size()-1).tag().startsWith("NN")|np.get(np.size()-1).tag().startsWith("CD")))
		{
			np.remove(np.size()-1);
		}
		return np;
	}
	
	/**
	 * At this stage the post-modifier of NounPHrase is reduced until the Last NN is found; 
	 * The cleaning of NounPhrases with regard to Symbols can be done in the Aggregation step; 
	 * At this stage the risk is too high, that valuable information gets lost / Algorithms can't be too precise at this stage
	 * @param np
	 * @param isReverse
	 * @return
	 */
	public static NounPhrase cleanNounPhrase(NounPhrase np, Boolean isReverse)
	{
		//If dot is inside a NN, the NN is split: Depending if entity was extracted inFront of or afterPattern
		/*
		String[] npPartHolder = np.getNPCore().word().split("(?<=(\\p{L}|\\d))\\s?[\\.\\?\\!\\(\\)\\{\\}\\[\\]]+\\s?(?=\\p{L})");
		
		if (npPartHolder.length>=2)
		{
			if (isReverse)
			{
				np.NPCore.setWord(npPartHolder[npPartHolder.length-1]);
				np.clearPreMod();
			}
			else
			{
				np.NPCore.setWord(npPartHolder[0]);
				np.clearPostMod();
			}
		}*/
		
		//Reduce the Post-Modifier or PreModifier until the last NN is found
		try
		{
			if (!np.getPostModifier().equals(null) && !(np.getPostModifier().size()==0))
			{
				np.getPostModifier().get(np.getPostModifier().size()-1).setWord(np.getPostModifier().get(np.getPostModifier().size()-1).word().trim().replaceAll("(\\.|\\,|\\;|\\:|\\?|\\!)$", ""));
				while(!np.getPostModifier().get(np.getPostModifier().size()-1).tag().startsWith("NN") && !np.getPostModifier().get(np.getPostModifier().size()-1).tag().equals("CD"))
				{
					np.getPostModifier().remove(np.getPostModifier().size()-1);
					if (np.getPostModifier().size()==0)
					{
						break;
					}
				}
			}
			
			if (!np.getPreModifier().equals(null) && !(np.getPreModifier().size()==0))
			{
				np.getPreModifier().get(np.getPreModifier().size()-1).setWord(np.getPreModifier().get(np.getPreModifier().size()-1).word().trim().replaceAll("(\\.|\\,|\\;|\\:|\\?|\\!)$", ""));
				while(!np.getPreModifier().get(0).tag().startsWith("JJ") && !np.getPreModifier().get(0).tag().equals("VBN"))
				{
					np.getPreModifier().remove(0);
					if (np.getPreModifier().size()==0)
					{
						break;
					}
				}
			}
		}
		catch(ArrayIndexOutOfBoundsException e)
		{
			log.debug(np.toString()+" "+e.toString());
		}
		
		//Clean the single word of NounPhrases if there are still symbols in front or behind it; (in case of multiple symbols)
		//Problem with abbreviations or websites => Can be filtered in next steps
		/*
		np.NPCore.setWord(np.NPCore.word().trim().replaceAll("[\\.\\;\\:\\?\\!\\(\\)\\[\\]\\{\\}]+$", "")); // KOmma removed, because it intereferes with Coordinations; (Not in Aggregation step ;-) )
		np.NPCore.setWord(np.NPCore.word().trim().replaceAll("^[\\.\\;\\:\\?\\!\\(\\)\\[\\]\\{\\}]+", ""));
		*/
		return np;
	}
	
	/**
	 * 
	 * @param wordOffset This word offset points to a word in the sentence list and describes the starting point for the NounPhrase search
	 * @param sentence
	 * @param resultNPs
	 * @return
	 */
	public static ArrayList<NounPhrase> findNextNounPhrase(int wordOffset, List<TaggedWord> sentence, ArrayList<NounPhrase> resultNPs)
	{
		NounPhrase currentNP = new NounPhrase(maxNpSize);
		int postOffset=0;
		for (int i=wordOffset; i<sentence.size();i++)
		{
			if (!sentence.get(i).tag().equals("DT") && !sentence.get(i).tag().startsWith("NN") && !sentence.get(i).tag().startsWith("JJ") && !sentence.get(i).tag().equals("VBN") && !sentence.get(i).word().toLowerCase().equals("and") && !sentence.get(i).word().toLowerCase().equals("or") && !sentence.get(i).word().toLowerCase().equals("&") && resultNPs.size()>0)
			{
				return resultNPs;
			}
			
			if (sentence.get(i).tag().startsWith("NN"))
			{
				currentNP.setNPCore(sentence.get(i));
				findPreMod(i,sentence, currentNP);
				if (sentence.get(i).word().endsWith(","))
				{
					resultNPs.add(cleanNounPhrase(currentNP, false));
					findNextNounPhrase(i+1,sentence, resultNPs);
					return resultNPs;
				}
				postOffset = findPostMod(i,sentence, currentNP);
				if (postOffset!=-1)
				{
					resultNPs.add(cleanNounPhrase(currentNP, false));
					findNextNounPhrase(postOffset+1,sentence, resultNPs);
					return resultNPs;
				}
				else
				{
					resultNPs.add(cleanNounPhrase(currentNP, false));
					return resultNPs;
				}
			}
		}
		return resultNPs;
	}
	
	/**
	 * Searches for potential pre-modifying words of the NounPhrase; 
	 * @param nnOffset Is a pointer on the last word analyzed (which has to be a valid Core-NN)
	 * @param sentence
	 * @param currentNP
	 */
	public static void findPreMod(int nnOffset, List<TaggedWord> sentence, NounPhrase currentNP)
	{
		for(int i=nnOffset-1; i>nnOffset-currentNP.getMaxNPLength()&& i>=0; i--)
		{
			if ((sentence.get(i).tag().startsWith("JJ")|sentence.get(i).tag().equals("VBN")) && !sentence.get(i).word().endsWith(","))
			{
				currentNP.addPreModifier(sentence.get(i));
			}
			else
			{
				return;
			}
		}
	}
	
	/**
	 * Searches and stores potential post-modifying words of the NounPhrase, as well as looks for potential coordinations
	 * @param nnOffset Is a pointer on the last word analyzed (which has to be a valid Core-NN)
	 * @param sentence
	 * @param currentNP
	 * @return -1, if no potential coordination was found; word-offset of the last analyzed word (will be used to search for next NounPhrase)
	 */
	public static int findPostMod(int nnOffset, List<TaggedWord> sentence, NounPhrase currentNP)
	{
		for (int i=nnOffset+1; i<nnOffset+currentNP.getMaxNPLength() && i<sentence.size(); i++)
		{
			if(sentence.get(i).tag().startsWith("JJ")|sentence.get(i).tag().equals("VBN")|sentence.get(i).tag().equals("VBG")|sentence.get(i).tag().startsWith("NN")|sentence.get(i).tag().equals("IN")|sentence.get(i).tag().equals("CD")|sentence.get(i).tag().equals("DT"))
			{
				currentNP.addPostModifier(sentence.get(i));
			}
			if(sentence.get(i).word().toLowerCase().equals("and")|sentence.get(i).word().toLowerCase().equals("or")|sentence.get(i).word().toLowerCase().equals("&"))
			{
				return i;
			}
			if (!(sentence.get(i).tag().startsWith("JJ")|sentence.get(i).tag().equals("VBN")|sentence.get(i).tag().equals("VBG")|sentence.get(i).tag().startsWith("NN")|sentence.get(i).tag().equals("IN")|sentence.get(i).tag().equals("CD")|sentence.get(i).tag().equals("DT")|sentence.get(i).word().toLowerCase().equals("and")|sentence.get(i).word().toLowerCase().equals("or")|sentence.get(i).word().toLowerCase().equals("&")))
			{
				return -1;
			}
			if(sentence.get(i).word().endsWith(","))
			{
				return i;
			}
		}
		if (sentence.size()>nnOffset+currentNP.getMaxNPLength())
		{
			if(sentence.get(nnOffset+currentNP.getMaxNPLength()).word().toLowerCase().equals("and")|sentence.get(nnOffset+currentNP.getMaxNPLength()).word().toLowerCase().equals("or")|sentence.get(nnOffset+currentNP.getMaxNPLength()).word().toLowerCase().equals("&"))
			{
			return nnOffset+currentNP.getMaxNPLength();
			}
		}
		return -1;
	}
	
	public static ArrayList<NounPhrase> findNextNounPhraseReverse(int wordOffset, List<TaggedWord> sentence, ArrayList<NounPhrase> resultNPs)
	{
		int status = 0;
		NounPhrase currentNP = new NounPhrase(maxNpSize);
		for (int i=wordOffset; i<sentence.size();i++)
		{
			if (!sentence.get(i).tag().startsWith("NN") && !sentence.get(i).tag().equals("VBG") && !sentence.get(i).tag().equals("IN") && !sentence.get(i).tag().equals("CD") && !sentence.get(i).tag().equals("DT") && resultNPs.size()>0)
			{
				return resultNPs;
			}
				
			if (sentence.get(i).tag().startsWith("NN"))
			{
				currentNP.setNPCore(sentence.get(i));
				if(sentence.get(i).word().endsWith(","))
				{
					status = findPreModReverse(i, sentence, currentNP);
					if (status==-2)
					{
						findNextNounPhraseReverse(i+1,sentence, resultNPs);
						return resultNPs;
					}
					else if (status>0)
					{
						resultNPs.add(cleanNounPhrase(currentNP, true));
						findNextNounPhraseReverse(status+1,sentence, resultNPs);
						return resultNPs;						
					}
					else
					{
						resultNPs.add(cleanNounPhrase(currentNP, true));
						return resultNPs;
					}
				}
				else
				{
					status = findPreModReverse(i, sentence, currentNP);
					if (status==-2)
					{
						findNextNounPhraseReverse(i+1,sentence, resultNPs);
						return resultNPs;
					}
					//Neue NounPhrase Entdeckt y=>ist neues Offset
					if (status>0)
					{						
						findPostModReverse(i,sentence, currentNP);
						resultNPs.add(cleanNounPhrase(currentNP, true));
						findNextNounPhraseReverse(status, sentence, resultNPs);
						return resultNPs;
					}						
					else
					{
						findPostModReverse(i,sentence, currentNP);
						resultNPs.add(cleanNounPhrase(currentNP, true));
						return resultNPs;
					}
				}
			}
		}
		return resultNPs;
	}
	
	public static int findPreModReverse(int nnOffset, List<TaggedWord> sentence, NounPhrase currentNP)
	{
		boolean premodFinished=false;
		for (int i=nnOffset+1;i<nnOffset+currentNP.getMaxNPLength() && i<sentence.size(); i++)
		{
			if ((sentence.get(i).tag().startsWith("JJ")|sentence.get(i).tag().equals("VBN")) && !sentence.get(i).word().endsWith(",") && !premodFinished)
			{
				currentNP.addPreModifier(sentence.get(i));
			}
			else if ((sentence.get(i).tag().equals("VBG")|sentence.get(i).tag().equals("IN")|sentence.get(i).tag().equals("CD")|sentence.get(i).tag().equals("DT")))
			{
				//ignore: It might be that this is pat of a postModifier: if another NN is found;
				premodFinished=true;
			}
			else if (sentence.get(i).tag().startsWith("NN") && !sentence.get(i).word().endsWith(","))
			{
				return -2;
			}
			else if (sentence.get(i).word().endsWith(","))
			{
				return i;
			}
			else if (sentence.get(i).word().toLowerCase().equals("and")|sentence.get(i).word().toLowerCase().equals("or")|sentence.get(i).word().toLowerCase().equals("&"))
			{
				return i+1;
			}
			else
			{
				return -1;
			}
		}
		
		if (sentence.size()>nnOffset+currentNP.getMaxNPLength())
		{
			if (sentence.get(nnOffset+currentNP.getMaxNPLength()).word().toLowerCase().equals("and")|sentence.get(nnOffset+currentNP.getMaxNPLength()).word().toLowerCase().equals("or")|sentence.get(nnOffset+currentNP.getMaxNPLength()).word().toLowerCase().equals("&"))
			{
				return nnOffset+currentNP.getMaxNPLength()+1;
			}
		}	
		return -1;
	}
	
	public static void findPostModReverse(int nnOffset, List<TaggedWord> sentence, NounPhrase currentNP)
	{
		for (int i=nnOffset-1; i>nnOffset-currentNP.getMaxNPLength() && i>=0;i--)
		{
			if (sentence.get(i).word().toLowerCase().equals("and")|sentence.get(i).word().toLowerCase().equals("or")|sentence.get(i).word().toLowerCase().equals("&"))
			{
				return;
			}
			
			if(sentence.get(i).tag().startsWith("JJ")|sentence.get(i).tag().equals("VBN")|sentence.get(i).tag().equals("VBG")|sentence.get(i).tag().startsWith("NN")|sentence.get(i).tag().equals("IN")|sentence.get(i).tag().equals("CD")|sentence.get(i).tag().equals("DT"))
			{
				currentNP.addPostModifier(sentence.get(i));
			}
			
			if(sentence.get(i).word().endsWith(","))
			{
				return;
			}
		}
		return;
	}
	
	/**
	 * Depending on the type of custPat and the Length of its two parts, the onset and offset of the first NounPhrase-Search-Area
	 * @param custPat
	 * @param sentence
	 * @param onset (Of the pattern inside the sentence)
	 * @param offset (Of the pattern inside the sentence)
	 * @param tw
	 * @return
	 */
	public static List<TaggedWord> getWordlistBeforeSplittedPattern(CustomPattern custPat, String sentence, int onset, List<TaggedWord> tw)
	{
		//Beginnt nach First KeyWord und endet vor second KeyWord; 
		if(custPat.type.equals("split_noPrefix"))
		{
			return getWordListSubset(onset+custPat.firstKeyWord.length()+1, sentence.toLowerCase().indexOf(custPat.secondKeyWord, onset), tw);
		}
		//Beginnt vor erstem Keyword
		if (custPat.type.equals("split"))
		{
			return getWordListSubset(0, onset, tw);
		}
		//Onset wird "normal gesplitted in diesem Fall" (wie Compact Patterns)
		if (custPat.type.equals("split_noSuffix"))
		{
			return getWordListSubset(0, onset, tw);
		}
		return new ArrayList<TaggedWord>();
	}
	
	/**
	 * Depending on the type of custPat and the Length of its two parts, the onset and offset of the second NounPhrase-Search-Area
	 * @param custPat
	 * @param sentence
	 * @param onset (Of the pattern inside the sentence)
	 * @param offset (Of the pattern inside the sentence)
	 * @param tw
	 * @return
	 */
	public static List<TaggedWord> getWordlistAfterSplittedPattern(CustomPattern custPat, String sentence, int onset, int offset, List<TaggedWord> tw)
	{
		//Beginnt nach dem zweiten Keyword
		if(custPat.type.equals("split_noPrefix"))
		{
			return getWordListSubset(offset, sentence.length(), tw);
		}
		// Beginnt beim Onset und endet mit secondKeyword
		if (custPat.type.equals("split_noSuffix"))
		{
			return getWordListSubset(onset, sentence.indexOf(custPat.secondKeyWord, onset), tw);
			
		}
		//Beginnt nach dem ersten Keyword
		if (custPat.type.equals("split"))
		{
			return getWordListSubset(onset+custPat.firstKeyWord.length(), sentence.length(), tw);
		}
		return new ArrayList<TaggedWord>();
	}
	
	/**
	 * The method returns a subList of a tagged-Word List (the entire sentence). The Onset and Offset are pointers in the sentence String, which 
	 * restrict the retunred List of words;
	 * @param onset
	 * @param offset
	 * @param taggedWords
	 * @return
	 */
	public static List<TaggedWord> getWordListSubset(int onset, int offset, List<TaggedWord> taggedWords)
	{
		List<TaggedWord> result = new ArrayList<TaggedWord>();
		int charCounter = 0;
		for (TaggedWord tw : taggedWords)
		{
			charCounter+=tw.word().length();
			if (charCounter>=onset && charCounter<=offset)
			{
				result.add(tw);
			}
			charCounter++;
		}
		return result;
	}
	
	/**
	 * Transforms the most common usages of auxiliary-verb apostrophies of the sentence, into the regular form, with the apostrophy replaced;
	 * 4 Types of apostrophies are considered
	 * @param sentence
	 * @return sentence
	 */
	public static String replaceVerbApostrophies(String sentence)
	{
		if (sentence.contains("'")|sentence.contains("’")|sentence.contains("‘")|sentence.contains("‛"))
		{
			//Auxiliary Verb abbreviations
			sentence = sentence.replaceAll("(?i)[\\u0027\\u2018\\u2019\\u201B]d\\s", " would ");
			sentence = sentence.replaceAll("(?i)[\\u0027\\u2018\\u2019\\u201B]re\\s", " are ");
			sentence = sentence.replaceAll("(?i)[\\u0027\\u2018\\u2019\\u201B]ve\\s", " have ");
			sentence = sentence.replaceAll("(?i)[\\u0027\\u2018\\u2019\\u201B]ll\\s", " will ");
			sentence = sentence.replaceAll("(?i)i[\\u0027\\u2018\\u2019\\u201B]m\\s", "I am ");
				
			//Auxiliary Verb 's
			sentence = sentence.replaceAll("(?i)he[\\u0027\\u2018\\u2019\\u201B]s\\s", "he is ");
			sentence = sentence.replaceAll("(?i)she[\\u0027\\u2018\\u2019\\u201B]s\\s", "she is ");
			sentence = sentence.replaceAll("(?i)it[\\u0027\\u2018\\u2019\\u201B]s\\s", "it is ");			
			sentence = sentence.replaceAll("(?i)that[\\u0027\\u2018\\u2019\\u201B]s\\s", "he is ");
			sentence = sentence.replaceAll("(?i)where[\\u0027\\u2018\\u2019\\u201B]s\\s", "she is ");
			sentence = sentence.replaceAll("(?i)who[\\u0027\\u2018\\u2019\\u201B]s\\s", "it is ");
			sentence = sentence.replaceAll("(?i)what[\\u0027\\u2018\\u2019\\u201B]s\\s", "what is ");
			sentence = sentence.replaceAll("(?i)when[\\u0027\\u2018\\u2019\\u201B]s\\s", "when is ");
			sentence = sentence.replaceAll("(?i)why[\\u0027\\u2018\\u2019\\u201B]s\\s", "why is ");
			sentence = sentence.replaceAll("(?i)how[\\u0027\\u2018\\u2019\\u201B]s\\s", "how is ");
			sentence = sentence.replaceAll("(?i)here[\\u0027\\u2018\\u2019\\u201B]s\\s", "here is ");
			sentence = sentence.replaceAll("(?i)there[\\u0027\\u2018\\u2019\\u201B]s\\s", "there is ");
			
			//Negations
			sentence = sentence.replaceAll("(?i)isn[\\u0027\\u2018\\u2019\\u201B]t\\s", "is not ");
			sentence = sentence.replaceAll("(?i)aren[\\u0027\\u2018\\u2019\\u201B]t\\s", "are not ");
			sentence = sentence.replaceAll("(?i)don[\\u0027\\u2018\\u2019\\u201B]t\\s", "do not ");
			sentence = sentence.replaceAll("(?i)doesn[\\u0027\\u2018\\u2019\\u201B]t\\s", "does not "); 
			sentence = sentence.replaceAll("(?i)can[\\u0027\\u2018\\u2019\\u201B]t\\s", "can not "); 
			sentence = sentence.replaceAll("(?i)couldn[\\u0027\\u2018\\u2019\\u201B]t\\s", "could not ");
			sentence = sentence.replaceAll("(?i)shouldn[\\u0027\\u2018\\u2019\\u201B]t\\s", "should not ");
			sentence = sentence.replaceAll("(?i)won[\\u0027\\u2018\\u2019\\u201B]t\\s", " will not ");
			sentence = sentence.replaceAll("(?i)wouldn[\\u0027\\u2018\\u2019\\u201B]t\\s", "would not ");
			sentence = sentence.replaceAll("(?i)haven[\\u0027\\u2018\\u2019\\u201B]t\\s", "have not ");
		}
		return sentence;
	}	
}
