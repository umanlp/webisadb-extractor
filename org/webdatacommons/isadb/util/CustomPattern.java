// http://webdatacommons.org/isadb/
// The WebIsADb and the API are licensed under a Creative Commons Attribution-Non Commercial-Share Alike 3.0 License: http://creativecommons.org/licenses/by-nc-sa/3.0/.
// Acknowledgements
// This work was partially funded by the Deutsche Forschungsgemeinschaft within the JOIN-T project (research grant PO 1900/1-1). Part of the computational resources used for this work were provide by an Amazon AWS in Education Grant award.
// this software is meant to be part of the CommonCrawl framework: http://commoncrawl.org/ to re-build a new WebIsADb from fresh CommonCrawl dumps.

package org.webdatacommons.isadb.util;

import java.util.regex.Pattern;

public class CustomPattern {
	
	public String pid;
	public String regex;
	public String type;
	public Pattern pattern;
	public String preCondition;
	public Boolean excludePronouns;
	public String firstKeyWord;
	public String secondKeyWord;
	public Boolean instanceFirst; 
	
	private String surrounderSymbols 	= 	"[\\u0027\\u2018\\u2019\\u201A\\u201B\\u201C\\u201D\\u201E\\u201F\\u0022]?";
	private String separatorSymbols  	= 	"[\\u002D\\u2010\\u2011\\u2012\\u2013\\u2014\\u2015\\u2043]?";
	private String endSymbols		 	=	"[\"\\u0026\\u0027\\u2018\\u2019\\u201A\\u201B\\u201C\\u201D\\u201E\\u201F\\u00A9\\u00AE]?";	//includes surrounderSymbols as well!
	private String prefix					="(\\p{L}|\\d)"+endSymbols;
	private String suffix					=surrounderSymbols+"(\\p{L}|\\d)";
	
	private String npPlaceholder = "("+surrounderSymbols+""					//Quotation mark could be in front
			+ "(\\p{L}++|\\d++\\p{L}++)"									//Word can start with letters or digits but must contain letters
			+ "("+separatorSymbols+"(\\p{L}++|\\d++))?"						//Can be separated by a hyphen
			+ endSymbols+"\\s)"												//Can be followed by quotation mark
			+ "{1,4}";														//NP can consist of up to 4 words
	
	private String npPlaceholderAdjMost = "("+surrounderSymbols+""			//Quotation mark could be in front
			+ "(\\p{L}++|\\d++\\p{L}++)"									//Word can start with letters or digits but must contain letters
			+ "("+separatorSymbols+"(\\p{L}++|\\d++))?"						//Can be separated by a hyphen
			+ endSymbols+"\\s)"												//Can be followed by quotation mark
			+ "{2,5}";														//NP can consist of up to 4 words and 1 mandatory word for the adjective
	
	private String npPlaceholderComma = "("+surrounderSymbols+""			//Quotation mark could be in front
			+ "(\\p{L}++|\\d++\\p{L}++)"									//Word can start with letters or digits but must contain letters
			+ "("+separatorSymbols+"(\\p{L}++|\\d++))?"						//Can be separated by a hyphen
			+ endSymbols+"\\,?\\s)"											//Can be followed by quotation mark
			+ "{1,4}";														//NP can consist of up to 4 words
	
	private String npPlaceholderBig = "("+surrounderSymbols+""						//Quotation mark could be in front
			+ "((\\p{L}){3,}|\\d++\\p{L})"									//Word can start with letters or digits but must contain at least 3 letters
			+ "("+separatorSymbols+"(\\p{L}++|\\d++))?"						//Can be separated by a hyphen
			+ endSymbols+"\\,?\\s)"											//Can be followed by quotation mark
			+ "{1,4}";														//NP can consist of up to 4 words
	
	public CustomPattern(String pid, String regex, String type, Boolean instanceFirst)
	{
		this.pid = pid;
		this.regex = regex;
		this.type = type;
		this.instanceFirst = instanceFirst;
		
		//Configure the Prefix and suffix of the regex
		if (type.equals("compact") || type.equals("split"))
		{
			this.pattern = Pattern.compile(prefix+regex+suffix);
		}
				
		if (type.equals("split_noPrefix"))
		{
			this.pattern = Pattern.compile("(?>"+regex+suffix+")");
		}
				
		if (type.equals("split_noSuffix"))
		{
			this.pattern = Pattern.compile("(?>"+prefix+regex+")");
		}				
	}
	
	public CustomPattern(String pid, String regex, String type, String preCond, Boolean instanceFirst)
	{
		this.pid = pid;
		this.regex = regex;
		this.type = type;
		this.instanceFirst = instanceFirst;
		
		//Configure the Prefix and suffix of the regex
		if (type.equals("compact") || type.equals("split"))
		{
			this.pattern = Pattern.compile(prefix+regex+suffix);
		}
				
		if (type.equals("split_noPrefix"))
		{
			this.pattern = Pattern.compile("(?>"+regex+suffix+")");
		}
				
		if (type.equals("split_noSuffix"))
		{
			this.pattern = Pattern.compile("(?>"+prefix+regex+")");
		}	
		
		this.preCondition = preCond;
	}	
	
	public CustomPattern(String pid, String regex, String type, String preCond, String fkw, String skw, Boolean instanceFirst)
	{
		this.pid = pid;
		this.regex = regex;
		this.type = type;
		this.firstKeyWord = fkw;
		this.secondKeyWord = skw;
		this.instanceFirst = instanceFirst;
		
		//Configure the Prefix and suffix of the regex
		if (type.equals("compact") || type.equals("split"))
		{
			this.pattern = Pattern.compile(prefix+regex+suffix);
		}
				
		if (type.equals("split_noPrefix"))
		{
			this.pattern = Pattern.compile("(?>"+regex+suffix+")");
		}
				
		if (type.equals("split_noSuffix"))
		{
			this.pattern = Pattern.compile("(?>"+prefix+regex+")");
		}	
		
		this.preCondition = preCond;
	}
}
