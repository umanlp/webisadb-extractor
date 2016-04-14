// http://webdatacommons.org/isadb/
// The WebIsADb and the API are licensed under a Creative Commons Attribution-Non Commercial-Share Alike 3.0 License: http://creativecommons.org/licenses/by-nc-sa/3.0/.
// Acknowledgements
// This work was partially funded by the Deutsche Forschungsgemeinschaft within the JOIN-T project (research grant PO 1900/1-1). Part of the computational resources used for this work were provide by an Amazon AWS in Education Grant award.
// this software is meant to be part of the CommonCrawl framework: http://commoncrawl.org/ to re-build a new WebIsADb from fresh CommonCrawl dumps.


package org.webdatacommons.isadb.util;

import java.util.ArrayList;

import edu.stanford.nlp.ling.TaggedWord;

public class NounPhrase {
	public TaggedWord NPCore;


	private ArrayList<TaggedWord> preModifier, postModifier;
	private boolean isComplete;
	private boolean coreFound;
	public boolean isCoreFound() {
		return coreFound;
	}

	private int maxNPLength;
	
	public NounPhrase(int maxNPLength)
	{
		this.maxNPLength = maxNPLength;
		
		isComplete = false;
		preModifier = new ArrayList<TaggedWord>();
		postModifier = new ArrayList<TaggedWord>();
	}
	
	public void addPreModifier(TaggedWord tw)
	{
		preModifier.add(0,tw);
		if(preModifier.size()==maxNPLength)
		{
			preModifier.remove(preModifier.size()-1);
		}
	}
	
	public void addPostModifier(TaggedWord tw)
	{
		postModifier.add(tw);
		if(postModifier.size()+1+preModifier.size()>maxNPLength)
		{
			if (preModifier.size()>0)
			{
				preModifier.remove(0);
			}
			else
			{
				isComplete = true;
			}
		}
	}
	
	public void NPCoreToPost(TaggedWord tw)
	{
		postModifier.add(NPCore);
		NPCore=tw;
		if (postModifier.size()+1==maxNPLength)
		{
			isComplete=true;
		}
	}
	
	public void clearPreMod()
	{
		preModifier.clear();
	}
	
	public void clearPostMod()
	{
		postModifier.clear();
	}
	
	public void setNPCore(TaggedWord tw)
	{
		NPCore = tw;
		if (postModifier.size()+1==maxNPLength)
		{
			isComplete=true;
		}		
	}
	
	public TaggedWord getNPCore() {
		return NPCore;
	}

	public ArrayList<TaggedWord> getPreModifier() {
		return preModifier;
	}

	public ArrayList<TaggedWord> getPostModifier() {
		return postModifier;
	}
	
	public int getMaxNPLength() {
		return maxNPLength;
	}

	public boolean isComplete() {
		return isComplete;
	}
	
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		for (TaggedWord tw : preModifier)
		{
			sb.append(tw.word()).append(" ");
		}
		sb.append(NPCore.word()).append(" ");
		for (TaggedWord tw : postModifier)
		{
			sb.append(tw.word()).append(" ");
		}
		if (sb.length()>0)
		{
			sb.setLength(sb.length()-1);
		}
		return sb.toString();
	}
	
	public String tagsToString()
	{
		StringBuilder sb = new StringBuilder();		
		for (TaggedWord tw : preModifier)
		{
			sb.append(tw.tag()).append(" ");
		}
		sb.append(NPCore.tag()).append(" ");
		for (TaggedWord tw : postModifier)
		{
			sb.append(tw.tag()).append(" ");
		}
		if (sb.length()>0)
		{
			sb.setLength(sb.length()-1);
		}
		return sb.toString();
	}
}
