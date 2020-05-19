/*
 * Copyright (c) 2006-, IPD Boehm, Universitaet Karlsruhe (TH) / KIT, by Guido Sautter
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Universitaet Karlsruhe (TH) / KIT nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY UNIVERSITAET KARLSRUHE (TH) / KIT AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package de.uka.ipd.idaho.goldenGateServer.ims.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Pattern;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.Attributed;
import de.uka.ipd.idaho.gamta.util.imaging.BoundingBox;
import de.uka.ipd.idaho.gamta.util.imaging.DocumentStyle;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.ImPage;
import de.uka.ipd.idaho.im.ImWord;
import de.uka.ipd.idaho.im.util.ImUtils;
import de.uka.ipd.idaho.plugins.bibRefs.BibRefConstants;
import de.uka.ipd.idaho.plugins.bibRefs.BibRefUtils;
import de.uka.ipd.idaho.plugins.bibRefs.BibRefUtils.RefData;
import de.uka.ipd.idaho.stringUtils.StringUtils;

/**
 * Document style provider that loads a list of documents style templates from
 * some URL, and then loads the templates proper.
 * 
 * @author sautter
 */
public class StandaloneDocumentStyleProvider implements BibRefConstants, DocumentStyle.Provider {
	private Map docStylesByName = Collections.synchronizedMap(new TreeMap());
	
	/** Constructor
	 * @param styleListUrl the URL to load the document style template list from
	 */
	public StandaloneDocumentStyleProvider(String styleListUrl) throws IOException {
		this(styleListUrl, null);
	}
	
	/** Constructor
	 * @param styleListUrl the URL to load the document style template list from
	 * @param styleNamePattern a pattern to filter document style names
	 */
	public StandaloneDocumentStyleProvider(String styleListUrl, String styleNamePattern) throws IOException {
		this(styleListUrl, styleNamePattern, null);
	}
	
	/** Constructor
	 * @param styleFolder the folder document style templates are stored in
	 */
	public StandaloneDocumentStyleProvider(File styleFolder) throws IOException {
		this(null, null, styleFolder);
	}
	
	/** Constructor
	 * @param styleListUrl the URL to load the document style template list from
	 * @param styleNamePattern a pattern to filter document style names
	 * @param styleFolder the folder to store document style templates in
	 */
	public StandaloneDocumentStyleProvider(String styleListUrl, String styleNamePattern, File styleFolder) throws IOException {
		
		//	we work without a local storage folder
		if (styleFolder == null)
			this.loadStylesFromUrl(styleListUrl, styleNamePattern, null);
		
		//	we have a local storage folder
		else {
			
			//	try updating from URL if given
			if (styleListUrl != null) try {
				this.loadStylesFromUrl(styleListUrl, styleNamePattern, styleFolder);
			}
			catch (IOException ioe) {
				ioe.printStackTrace(System.out);
			}
			
			//	load document styles from folder
			File[] styleFiles = styleFolder.listFiles(new FileFilter() {
				public boolean accept(File file) {
					return !file.getName().endsWith(".old");
				}
			});
			for (int s = 0; s < styleFiles.length; s++) {
				Settings dsParamList = Settings.loadSettings(styleFiles[s]);
				if ((dsParamList != null) && (dsParamList.size() != 0)) {
					String dsName = styleFiles[s].getName();
					this.docStylesByName.put(dsName, new DocStyle(dsParamList));
					if (dsName.indexOf('.') != -1)
						dsName = dsName.substring(0, dsName.lastIndexOf('.'));
					dsParamList.setSetting(DocumentStyle.DOCUMENT_STYLE_NAME_ATTRIBUTE, dsName);
				}
			}
		}
		
		//	register as document style provider (but only now that we got all we need)
		DocumentStyle.addProvider(this);
	}
	
	private void loadStylesFromUrl(String styleListUrl, String styleNamePattern, File styleFolder) throws IOException {
		
		//	get base URL
		String dsBaseUrl = styleListUrl.substring(0, (styleListUrl.lastIndexOf('/') + "/".length()));
		
		//	get document style list
		ArrayList dsUrlList = new ArrayList();
		URL dslUrl = new URL(styleListUrl);
		BufferedReader dslBr = new BufferedReader(new InputStreamReader(dslUrl.openStream(), "UTF-8"));
		for (String dslLine; (dslLine = dslBr.readLine()) != null;) {
			if ((styleNamePattern == null) || dslLine.matches(styleNamePattern))
				dsUrlList.add(dsBaseUrl + dslLine);
		}
		
		//	load and index document styles
		for (int s = 0; s < dsUrlList.size(); s++) {
			URL dsUrl = new URL((String) dsUrlList.get(s));
			BufferedReader dsBr = new BufferedReader(new InputStreamReader(dsUrl.openStream(), "UTF-8"));
			Settings dsParamList = Settings.loadSettings(dsBr);
			dsBr.close();
			if ((dsParamList != null) && (dsParamList.size() != 0)) {
				String dsName = ((String) dsUrlList.get(s));
				dsName = dsName.substring(dsName.lastIndexOf('/') + "/".length());
				
				//	store document style in folder if available
				if (styleFolder != null) {
					BufferedWriter dsBw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(styleFolder, dsName))));
					Settings.storeSettingsAsText(dsBw, dsParamList);
					dsBw.flush();
					dsBw.close();
//					Settings.storeSettingsAsText(new File(styleFolder, dsName), dsParamList);
				}
				
				//	load document style
				this.docStylesByName.put(dsName, new DocStyle(dsParamList));
				if (dsName.indexOf('.') != -1)
					dsName = dsName.substring(0, dsName.lastIndexOf('.'));
				dsParamList.setSetting(DocumentStyle.DOCUMENT_STYLE_NAME_ATTRIBUTE, dsName);
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.plugins.docStyle.DocumentStyle.Provider#getStyleFor(de.uka.ipd.idaho.gamta.Attributed)
	 */
	public Properties getStyleFor(Attributed doc) {
		String docStyleName = ((String) doc.getAttribute(DocumentStyle.DOCUMENT_STYLE_NAME_ATTRIBUTE));
		
		//	no style clues at all, have to match
		if (docStyleName == null) {
			
			//	try anchor matches if argument is ImDocument
			if (doc instanceof ImDocument) {
				DocStyle docStyle = this.findStyleFor((ImDocument) doc);
				if (docStyle != null)
					return docStyle.toProperties();
			}
			return null;
		}
		
		//	try loading style directly by name first
//		Settings docStyle = this.loadSettingsResource(docStyleName + ".docStyle");
		DocStyle docStyle = ((DocStyle) this.docStylesByName.get(docStyleName + ".docStyle"));
		if (docStyle != null)
			return docStyle.toProperties();
		
		//	get bibliographic meta data if available
		RefData ref = BibRefUtils.modsAttributesToRefData(doc);
		String docYear = ref.getAttribute(YEAR_ANNOTATION_TYPE);
		if ((docYear != null) && (docStyleName.indexOf(docYear) != -1))
			docYear = null;
		String docType = ref.getAttribute(PUBLICATION_TYPE_ATTRIBUTE);
		if (docType != null)
			docType = docType.replaceAll("[^a-zA-Z]+", "_");
		if ((docType != null) && (docStyleName.indexOf(docType) != -1))
			docType = null;
		
		//	try and match name and year against available styles (sort descending to find closest style on year match)
//		String[] docStyleNames = this.dataProvider.getDataNames();
		String[] docStyleNames = ((String[]) this.docStylesByName.keySet().toArray(new String[this.docStylesByName.size()]));
		Arrays.sort(docStyleNames, Collections.reverseOrder());
		String bestDocStyleName = null;
		for (int n = 0; n < docStyleNames.length; n++) {
			if (!docStyleNames[n].startsWith(docStyleName + "."))
				continue;
			String dsn = docStyleNames[n].substring((docStyleName + ".").length());
			
			if (dsn.matches("[0-9]{4}\\..+")) {
				if ((docYear != null) && (docYear.compareTo(dsn.substring(0, 4)) < 0))
					continue;
				dsn = dsn.substring("0000.".length());
			}
			
			if ((docType != null) && dsn.startsWith(docType + ".")) {
//				docStyle = this.loadSettingsResource(docStyleNames[n]);
				docStyle = ((DocStyle) this.docStylesByName.get(docStyleNames[n]));
				if (docStyle != null)
					return docStyle.toProperties();
			}
			
			if ((bestDocStyleName == null) || (docStyleNames[n].length() < bestDocStyleName.length()))
				bestDocStyleName = docStyleNames[n];
		}
		
		//	do we have a match?
		if (bestDocStyleName != null) {
//			docStyle = this.loadSettingsResource(bestDocStyleName);
			docStyle = ((DocStyle) this.docStylesByName.get(bestDocStyleName));
			if (docStyle != null)
				return docStyle.toProperties();
		}
		
		//	no style found
		return null;
	}
	
	private DocStyle findStyleFor(ImDocument doc) {
		System.out.println("Searching style for document " + doc.docId);
		DocStyle bestDocStyle = null;
		int bestDocStyleAnchorMatchCount = 0;
		float bestDocStyleAnchorMatchScore = 0.66f; // require at least two thirds of anchors to match to prevent erroneous style assignments
		for (Iterator dsnit = this.docStylesByName.keySet().iterator(); dsnit.hasNext();) {
			String dsn = ((String) dsnit.next());
			System.out.println(" - testing style " + dsn);
			DocStyle ds = ((DocStyle) this.docStylesByName.get(dsn));
			if (ds.anchors.length == 0) {
				System.out.println(" ==> no anchors");
				continue;
			}
			System.out.println(" - got " + ds.anchors.length + " anchors");
			int dsAnchorMatchCount = 0;
			for (int a = 0; a < ds.anchors.length; a++) {
				System.out.println("   - testing anchor " + ds.anchors[a].pattern + " at " + ds.anchors[a].area);
				if (anchorMatches(doc, doc.getFirstPageId(), ds.anchors[a].area, ds.anchors[a].minFontSize, ds.anchors[a].maxFontSize, ds.anchors[a].isBold, ds.anchors[a].isItalics, ds.anchors[a].isAllCaps, ds.anchors[a].pattern)) {
					dsAnchorMatchCount++;
					System.out.println("   --> match on first page");
				}
				else if ((ds.anchorPageId != 0) && anchorMatches(doc, ds.anchorPageId, ds.anchors[a].area, ds.anchors[a].minFontSize, ds.anchors[a].maxFontSize, ds.anchors[a].isBold, ds.anchors[a].isItalics, ds.anchors[a].isAllCaps, ds.anchors[a].pattern)) {
					dsAnchorMatchCount++;
					System.out.println("   --> match after cover pages");
				}
				else System.out.println("   --> no match");
			}
			if (dsAnchorMatchCount == 0)
				continue;
			float dsAnchorMatchScore = (((float) dsAnchorMatchCount) / ds.anchors.length);
			System.out.println(" - match score is " + dsAnchorMatchScore);
			if (dsAnchorMatchScore > bestDocStyleAnchorMatchScore) {
				bestDocStyle = ds;
				bestDocStyleAnchorMatchCount = dsAnchorMatchCount;
				bestDocStyleAnchorMatchScore = dsAnchorMatchScore;
				System.out.println(" ==> new best match (" + dsAnchorMatchScore + " for " + dsAnchorMatchCount + " out of " + ds.anchors.length + ")");
			}
			else if ((dsAnchorMatchScore == bestDocStyleAnchorMatchScore) && (bestDocStyleAnchorMatchCount < dsAnchorMatchCount)) {
				bestDocStyle = ds;
				bestDocStyleAnchorMatchCount = dsAnchorMatchCount;
				bestDocStyleAnchorMatchScore = dsAnchorMatchScore;
				System.out.println(" ==> new best-founded match (" + dsAnchorMatchScore + " for " + dsAnchorMatchCount + " out of " + ds.anchors.length + ")");
			}
			else System.out.println(" ==> worse than best match");
		}
		return bestDocStyle;
	}
	
	private static boolean anchorMatches(ImDocument doc, int pageId, BoundingBox area, int minFontSize, int maxFontSize, boolean isBold, boolean isItalics, boolean isAllCaps, String pattern) {
		return anchorMatches(doc, pageId, area, minFontSize, maxFontSize, isBold, isItalics, isAllCaps, pattern, null);
	}
	
	private static boolean anchorMatches(ImDocument doc, int pageId, BoundingBox area, int minFontSize, int maxFontSize, boolean isBold, boolean isItalics, boolean isAllCaps, String pattern, List matchLog) {
		
		//	get page and scale bounding box
		ImPage page = doc.getPage(pageId);
		area = DocumentStyle.scaleBox(area, 72, page.getImageDPI());
		if (matchLog != null)
			matchLog.add(" - area scaled to " + page.getImageDPI() + " DPI: " + area.toString());
		
		//	get words in area
		ImWord[] words = page.getWordsInside(area);
		if (words.length == 0) {
			if (matchLog != null)
				matchLog.add(" ==> no words found in area, mismatch");
			return false;
		}
		if (matchLog != null)
			matchLog.add(" - found " + words.length + " words in area");
		
		//	filter words by font properties
		ArrayList wordList = new ArrayList();
		if (matchLog != null) {
			matchLog.add(" - applying font property filter:");
			if (isBold)
				matchLog.add("   - bold");
			if (isItalics)
				matchLog.add("   - italics");
			if (isAllCaps)
				matchLog.add("   - all-caps");
			matchLog.add("   - font size " + minFontSize + ((maxFontSize == minFontSize) ? "" : ("-" + maxFontSize)));
		}
		for (int w = 0; w < words.length; w++) {
			if (isBold && !words[w].hasAttribute(ImWord.BOLD_ATTRIBUTE))
				continue;
			if (isItalics && !words[w].hasAttribute(ImWord.ITALICS_ATTRIBUTE))
				continue;
			if (isAllCaps && !words[w].getString().equals(words[w].getString().toUpperCase()))
				continue;
			try {
				int wfs = words[w].getFontSize();
				if (wfs < minFontSize)
					continue;
				if (maxFontSize < wfs)
					continue;
			} catch (NumberFormatException nfe) {}
			wordList.add(words[w]);
		}
		if (wordList.isEmpty()) {
			if (matchLog != null)
				matchLog.add(" ==> no words left after font property filter, mismatch");
			return false;
		}
		else if (wordList.size() < words.length)
			words = ((ImWord[]) wordList.toArray(new ImWord[wordList.size()]));
		if (matchLog != null)
			matchLog.add(" - " + words.length + " words left after font property filter");
		
		//	sort words and create normalized string
		ImUtils.sortLeftRightTopDown(words);
		StringBuffer wordStr = new StringBuffer();
		for (int w = 0; w < words.length; w++)
			wordStr.append(normalizeString(words[w].getString()));
		if (matchLog != null)
			matchLog.add(" - word string is '" + wordStr.toString() + "'");
		
		//	test against pattern
		boolean match = Pattern.compile(pattern).matcher(wordStr).matches();
		if (matchLog != null) {
			matchLog.add(" - matching against pattern '" + pattern + "'");
			matchLog.add(" ==> " + (match ? "match" : "mismatch"));
		}
		
		//	finally ...
		return match;
	}
	
	static String normalizeString(String string) {
		StringBuffer nString = new StringBuffer();
		for (int c = 0; c < string.length(); c++) {
			char ch = string.charAt(c);
			if ((ch < 33) || (ch == 160))
				nString.append(" "); // turn all control characters into spaces, along with non-breaking space
			else if (ch < 127)
				nString.append(ch); // no need to normalize basic ASCII characters
			else if ("-\u00AD\u2010\u2011\u2012\u2013\u2014\u2015\u2212".indexOf(ch) != -1)
				nString.append("-"); // normalize dashes right here
			else nString.append(StringUtils.getNormalForm(ch));
		}
		return nString.toString();
	}
	
	private static class DocStyle {
		DocStyleAnchor[] anchors;
		int anchorPageId;
		Settings paramList;
		DocStyle(Settings paramList) {
			this.paramList = paramList;
			this.updateAnchors();
		}
		private void updateAnchors() {
			Settings anchorParamLists = this.paramList.getSubset("anchor");
			String[] anchorNames = anchorParamLists.getSubsetPrefixes();
			ArrayList anchorList = new ArrayList();
			for (int a = 0; a < anchorNames.length; a++) {
				Settings anchorParamList = anchorParamLists.getSubset(anchorNames[a]);
				BoundingBox area = BoundingBox.parse(anchorParamList.getSetting("area"));
				if (area == null)
					continue;
				String pattern = anchorParamList.getSetting("pattern");
				if (pattern == null)
					continue;
				try {
					anchorList.add(new DocStyleAnchor(
							area,
							Integer.parseInt(anchorParamList.getSetting("minFontSize", anchorParamList.getSetting("fontSize", "0"))),
							Integer.parseInt(anchorParamList.getSetting("maxFontSize", anchorParamList.getSetting("fontSize", "72"))),
							"true".equals(anchorParamList.getSetting("isBold")),
							"true".equals(anchorParamList.getSetting("isItalics")),
							"true".equals(anchorParamList.getSetting("isAllCaps")),
							pattern
						));
				} catch (NumberFormatException nfe) {}
			}
			this.anchors = ((DocStyleAnchor[]) anchorList.toArray(new DocStyleAnchor[anchorList.size()]));
			this.anchorPageId = Integer.parseInt(this.paramList.getSetting("layout.coverPageCount", "0"));
		}
		Properties toProperties() {
			return this.paramList.toProperties();
		}
	}
	
	private static class DocStyleAnchor {
		final BoundingBox area;
		final int minFontSize;
		final int maxFontSize;
		final boolean isBold;
		final boolean isItalics;
		final boolean isAllCaps;
		final String pattern;
		DocStyleAnchor(BoundingBox area, int minFontSize, int maxFontSize, boolean isBold, boolean isItalics, boolean isAllCaps, String pattern) {
			this.area = area;
			this.minFontSize = minFontSize;
			this.maxFontSize = maxFontSize;
			this.isBold = isBold;
			this.isItalics = isItalics;
			this.isAllCaps = isAllCaps;
			this.pattern = pattern;
		}
	}
}