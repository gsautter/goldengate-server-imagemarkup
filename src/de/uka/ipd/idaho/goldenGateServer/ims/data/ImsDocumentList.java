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
 *     * Neither the name of the Universität Karlsruhe (TH) nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY UNIVERSITÄT KARLSRUHE (TH) / KIT AND CONTRIBUTORS 
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
package de.uka.ipd.idaho.goldenGateServer.ims.data;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.util.CountingSet;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * List of documents in a GoldenGATE DIO, implemented iterator-style for
 * efficiency.
 * 
 * @author sautter
 */
public abstract class ImsDocumentList implements GoldenGateImsConstants {
	static final String DOCUMENT_LIST_NODE_NAME = "docList";
	static final String DOCUMENT_NODE_NAME = "doc";
	static final String DOCUMENT_LIST_FIELDS_ATTRIBUTE = "listFields";
	
	/**
	 * Constant set containing the names of document attributes for which
	 * document lists do not contain value summaries. This set is immutable, any
	 * modification methods are implemented to simply return false.
	 */
	public static final Set summarylessAttributes = new LinkedHashSet() {
		{
			String[] summarylessAttributeNames = {
				DOCUMENT_ID_ATTRIBUTE,
				DOCUMENT_NAME_ATTRIBUTE,
//				EXTERNAL_IDENTIFIER_ATTRIBUTE,
				DOCUMENT_TITLE_ATTRIBUTE,
//				DOCUMENT_KEYWORDS_ATTRIBUTE,
				CHECKIN_TIME_ATTRIBUTE,
				UPDATE_TIME_ATTRIBUTE,
				CHECKOUT_TIME_ATTRIBUTE,
				DOCUMENT_VERSION_ATTRIBUTE,
			};
			Arrays.sort(summarylessAttributeNames);
			for (int a = 0; a < summarylessAttributeNames.length; a++)
				super.add(summarylessAttributeNames[a]);
		}
		public boolean add(Object o) {
			return false;
		}
		public boolean remove(Object o) {
			return false;
		}
		public void clear() {}
		public Iterator iterator() {
			final Iterator it = super.iterator();
			return new Iterator() {
				public boolean hasNext() {
					return it.hasNext();
				}
				public Object next() {
					return it.next();
				}
				public void remove() {}
			};
		}
		public boolean addAll(Collection coll) {
			return false;
		}
		public boolean removeAll(Collection coll) {
			return false;
		}
		public boolean retainAll(Collection coll) {
			return false;
		}
	};
	
	
	/**
	 * Constant set containing the names of document attributes which can be
	 * used for document list filters. This set is immutable, any modification
	 * methods are implemented to simply return false.
	 */
	public static final Set filterableDataFields = new LinkedHashSet() {
		{
			String[] docTableFields = {
					DOCUMENT_NAME_ATTRIBUTE,
					CHECKIN_USER_ATTRIBUTE,
					CHECKIN_TIME_ATTRIBUTE,
					CHECKOUT_USER_ATTRIBUTE,
					CHECKOUT_TIME_ATTRIBUTE,
					UPDATE_USER_ATTRIBUTE,
					UPDATE_TIME_ATTRIBUTE,
					DOCUMENT_VERSION_ATTRIBUTE,
			};
			Arrays.sort(docTableFields);
			for (int a = 0; a < docTableFields.length; a++)
				super.add(docTableFields[a]);
		}
		public boolean add(Object o) {
			return false;
		}
		public boolean remove(Object o) {
			return false;
		}
		public void clear() {}
		public Iterator iterator() {
			final Iterator it = super.iterator();
			return new Iterator() {
				public boolean hasNext() {
					return it.hasNext();
				}
				public Object next() {
					return it.next();
				}
				public void remove() {}
			};
		}
		public boolean addAll(Collection coll) {
			return false;
		}
		public boolean removeAll(Collection coll) {
			return false;
		}
		public boolean retainAll(Collection coll) {
			return false;
		}
	};
	
	
	/**
	 * Constant set containing the names of numeric document attributes, for
	 * which specific comparison operators can be used for document list
	 * filters. This set is immutable, any modification methods are implemented
	 * to simply return false.
	 */
	public static final Set numericDataAttributes = new LinkedHashSet() {
		{
			String[] numericFieldNames = {
					CHECKIN_TIME_ATTRIBUTE,
					UPDATE_TIME_ATTRIBUTE,
					CHECKOUT_TIME_ATTRIBUTE,
					DOCUMENT_VERSION_ATTRIBUTE,
			};
			Arrays.sort(numericFieldNames);
			for (int a = 0; a < numericFieldNames.length; a++)
				super.add(numericFieldNames[a]);
		}
		public boolean add(Object o) {
			return false;
		}
		public boolean remove(Object o) {
			return false;
		}
		public void clear() {}
		public Iterator iterator() {
			final Iterator it = super.iterator();
			return new Iterator() {
				public boolean hasNext() {
					return it.hasNext();
				}
				public Object next() {
					return it.next();
				}
				public void remove() {}
			};
		}
		public boolean addAll(Collection coll) {
			return false;
		}
		public boolean removeAll(Collection coll) {
			return false;
		}
		public boolean retainAll(Collection coll) {
			return false;
		}
	};
	
	
	/**
	 * Constant set containing the comparison operators that can be used for
	 * numeric document attributes in document list filters. This set is
	 * immutable, any modification methods are implemented to simply return
	 * false.
	 */
	public static final Set numericOperators = new LinkedHashSet() {
		{
			String[] numericOperators = {
					">",
					">=",
					"=",
					"<=",
					"<",
			};
			Arrays.sort(numericOperators);
			for (int a = 0; a < numericOperators.length; a++)
				super.add(numericOperators[a]);
		}
		public boolean add(Object o) {
			return false;
		}
		public boolean remove(Object o) {
			return false;
		}
		public void clear() {}
		public Iterator iterator() {
			final Iterator it = super.iterator();
			return new Iterator() {
				public boolean hasNext() {
					return it.hasNext();
				}
				public Object next() {
					return it.next();
				}
				public void remove() {}
			};
		}
		public boolean addAll(Collection coll) {
			return false;
		}
		public boolean removeAll(Collection coll) {
			return false;
		}
		public boolean retainAll(Collection coll) {
			return false;
		}
	};
	
	/**
	 * A hybrid of set and map, this class contains the distinct values of
	 * document attributes in the database, plus for each attribute value pair
	 * the number of documents having that particular value for the attribute.
	 * This is to help filtering document lists.
	 * 
	 * @author sautter
	 */
	public static class AttributeSummary extends CountingSet {
		
		/** Constructor
		 */
		public AttributeSummary() {
			super(new TreeMap());
		}
	}
	
	/**
	 * the field names for the document list, in the order they should be
	 * displayed
	 */
	public final String[] listFieldNames;

	/**
	 * Constructor for general use
	 * @param listFieldNames the field names for the document list, in the order
	 *            they should be displayed
	 */
	public ImsDocumentList(String[] listFieldNames) {
		this.listFieldNames = listFieldNames;
	}
	
	/**
	 * Constructor for creating wrappers
	 * @param model the document list to wrap
	 */
	public ImsDocumentList(ImsDocumentList model) {
		this(model, null);
	}
	
	/**
	 * Constructor for creating wrappers that add fields
	 * @param model the document list to wrap
	 * @param extensionListFieldNames an array holding additional field names
	 */
	public ImsDocumentList(ImsDocumentList model, String[] extensionListFieldNames) {
		if (extensionListFieldNames == null) {
			this.listFieldNames = new String[model.listFieldNames.length];
			System.arraycopy(model.listFieldNames, 0, this.listFieldNames, 0, model.listFieldNames.length);
		}
		else {
			String[] listFieldNames = new String[model.listFieldNames.length + extensionListFieldNames.length];
			System.arraycopy(model.listFieldNames, 0, listFieldNames, 0, model.listFieldNames.length);
			System.arraycopy(extensionListFieldNames, 0, listFieldNames, model.listFieldNames.length, extensionListFieldNames.length);
			this.listFieldNames = listFieldNames;
		}
		for (int f = 0; f < this.listFieldNames.length; f++)
			this.addListFieldValues(this.listFieldNames[f], model.getListFieldValues(this.listFieldNames[f]));
	}
	
	/**
	 * Retrieve a summary of the values in a list field. The sets returned by
	 * this method are immutable. If there is no summary, this method returns
	 * null, but never an empty set. The set does not contain nulls and is
	 * sorted lexicographically. There are no summaries for document ID,
	 * external identifiers, document titles, or time related attributes, as
	 * these would likely have the same size as the document list itself.
	 * @param listFieldName the name of the field
	 * @return a set containing the summary values
	 */
	public AttributeSummary getListFieldValues(String listFieldName) {
		return this.getListFieldValues(listFieldName, false);
	}
	AttributeSummary getListFieldValues(String listFieldName, boolean create) {
		AttributeSummary das = ((AttributeSummary) this.listFieldValues.get(listFieldName));
		if ((das == null) && create) {
			das = new AttributeSummary();
			this.listFieldValues.put(listFieldName, das);
		}
		return das;
	}
	
	/**
	 * Add a set of values to the value summary of a list field. 
	 * @param listFieldName the name of the field
	 * @param listFieldValues the values to add
	 */
	protected final void addListFieldValues(String listFieldName, AttributeSummary listFieldValues) {
		if (summarylessAttributes.contains(listFieldName))
			return;
		if ((listFieldValues == null) || (listFieldValues.size() == 0))
			return;
		AttributeSummary as = this.getListFieldValues(listFieldName, true);
		for (Iterator vit = listFieldValues.iterator(); vit.hasNext();) {
			String listFieldValue = ((String) vit.next());
			as.add(listFieldValue, listFieldValues.getCount(listFieldValue));
		}
	}
	
	/**
	 * Add a set of values to the value summary of a list field. 
	 * @param listFieldName the name of the field
	 * @param listFieldValues the values to add
	 */
	protected final void addListFieldValues(ImsDocumentListElement dle) {
		if (dle == null)
			return;
		for (int f = 0; f < this.listFieldNames.length; f++) {
			if (summarylessAttributes.contains(this.listFieldNames[f]))
				continue;
			String listFieldValue = ((String) dle.getAttribute(this.listFieldNames[f]));
			if (listFieldValue == null)
				continue;
			this.getListFieldValues(this.listFieldNames[f], true).add(listFieldValue);
		}
	}
	
	private Map listFieldValues = new HashMap();
	
	/**
	 * Check if there is another document in the list.
	 * @return true if there is another document, false otherwise
	 */
	public abstract boolean hasNextDocument();

	/**
	 * Retrieve the next document from the list. If there is no next document, this
	 * method returns null.
	 * @return the next document in the list
	 */
	public abstract ImsDocumentListElement getNextDocument();
	
	/**
	 * Check the total number of documents in the list. If the count is not
	 * available, this method returns -1. Otherwise, the returned value can
	 * either be the exact number of documents remaining, or a conservative
	 * estimate, if the exact number is not available. This default
	 * implementation returns -1 if getRetrievedDocumentCount() returns -1, and
	 * the sum of getRetrievedDocumentCount() and getRemainingDocumentCount()
	 * otherwise. Sub classes are welcome to overwrite it and provide a more
	 * exact estimate. They need to make sure not to use this implementation in
	 * their implementation of getRetrievedDocumentCout() or
	 * getReminingDocumentCount(), however, to prevent delegating back and forth
	 * and causing stack overflows.
	 * @return the number of documents remaining
	 */
	public int getDocumentCount() {
		int retrieved = this.getRetrievedDocumentCount();
		return ((retrieved == -1) ? -1 : (retrieved + this.getRemainingDocumentCount()));
	}
	
	/**
	 * Check the number of documents retrieved so far from the getNextDocument()
	 * method. If the count is not available, this method returns -1. This
	 * default implementation does return -1, sub classes are welcome to
	 * overwrite it and provide a count.
	 * @return the number of documents retrieved so far
	 */
	public int getRetrievedDocumentCount() {
		return -1;
	}
	
	/**
	 * Check the number of documents remaining in the list. If the count is not
	 * available, this method returns -1. Otherwise, the returned value can
	 * either be the exact number of documents remaining, or a conservative
	 * estimate, if the exact number is not available. This default
	 * implementation returns 1 if hasNextDocument() returns true, and 0
	 * otherwise. Sub classes are welcome to overwrite it and provide a more
	 * accurate estimate.
	 * @return the number of documents remaining
	 */
	public int getRemainingDocumentCount() {
		return (this.hasNextDocument() ? 1 : 0);
	}
	
	/**
	 * Write this document list to some writer as XML data.
	 * @param out the Writer to write to
	 * @throws IOException
	 */
	public void writeXml(Writer out) throws IOException {
		
		//	produce writer
		BufferedWriter bw = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
		
		//	write empty data
		if (!this.hasNextDocument()) {
			
			//	write results
			StringVector listFields = new StringVector();
			listFields.addContent(this.listFieldNames);
			bw.write("<" + DOCUMENT_LIST_NODE_NAME + 
					" " + DOCUMENT_LIST_FIELDS_ATTRIBUTE + "=\"" + listFields.concatStrings(" ") + "\"" +
			"/>");
			bw.newLine();
		}
		
		//	write data
		else {
			
			//	get result field names
			StringVector listFields = new StringVector();
			listFields.addContent(this.listFieldNames);
			bw.write("<" + DOCUMENT_LIST_NODE_NAME + 
					" " + DOCUMENT_LIST_FIELDS_ATTRIBUTE + "=\"" + listFields.concatStrings(" ") + "\"" +
			">");
			
			while (this.hasNextDocument()) {
				ImsDocumentListElement dle = this.getNextDocument();
				
				//	write element
				bw.write("  <" + DOCUMENT_NODE_NAME);
				for (int a = 0; a < this.listFieldNames.length; a++) {
					String fieldValue = ((String) dle.getAttribute(this.listFieldNames[a]));
					if ((fieldValue != null) && (fieldValue.length() != 0))
						bw.write(" " + this.listFieldNames[a] + "=\"" + AnnotationUtils.escapeForXml(fieldValue, true) + "\"");
				}
				bw.write("/>");
				bw.newLine();
			}
			
			bw.write("</" + DOCUMENT_LIST_NODE_NAME + ">");
			bw.newLine();
		}
		
		//	flush Writer if it was wrapped
		if (bw != out)
			bw.flush();
	}
	
	/**
	 * Write the documents in this list to a given writer. This method consumes
	 * the list, i.e., it iterates through to the last document list element it
	 * contains.
	 * @param out the writer to write to
	 * @throws IOException
	 */
	public void writeData(Writer out) throws IOException {
		BufferedWriter bw = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
		bw.write("" + this.getDocumentCount());
		bw.newLine();
		for (int f = 0; f < this.listFieldNames.length; f++) {
			if (f != 0)
				bw.write("\t");
			bw.write(this.listFieldNames[f]);
		}
		bw.newLine();
		while (this.hasNextDocument()) {
			bw.write(this.getNextDocument().toTabString(this.listFieldNames));
			bw.newLine();
		}
		for (int f = 0; f < this.listFieldNames.length; f++) {
			AttributeSummary fieldValues = this.getListFieldValues(this.listFieldNames[f]);
			if (fieldValues == null)
				continue;
			bw.write(this.listFieldNames[f]);
			for (Iterator vit = fieldValues.iterator(); vit.hasNext();) {
				String fieldValue = ((String) vit.next());
				bw.write(" " + URLEncoder.encode(fieldValue, "UTF-8") + "=" + fieldValues.getCount(fieldValue));
			}
			bw.newLine();
		}
		if (bw != out)
			bw.flush();
	}
	
	/**
	 * Wrap a document list around a reader, which provides the list's data in
	 * form of a character stream. Do not close the specified reader after this
	 * method returns. The reader is closed by the returned list after the last
	 * document list element is read.
	 * @param in the Reader to read from
	 * @return a document list that makes the data from the specified reader
	 *         available as document list elements
	 * @throws IOException
	 */
	public static ImsDocumentList readDocumentList(Reader in) throws IOException {
		
		//	create buffered reader for document count and field names
		BufferedReader br = ((in instanceof BufferedReader) ? ((BufferedReader) in) : new BufferedReader(in));
		
		//	get total list size
		String docCountString = br.readLine();
		int docCount = -1;
		try {
			docCount = Integer.parseInt(docCountString);
		} catch (NumberFormatException nfe) {}
		
		//	get list fields
		String fieldString = br.readLine();
		String[] fields = fieldString.split("\\t");
		
		//	create document list
		ImsDocumentList dl = new ReaderDocumentList(fields, br, docCount);
		
		/*
		 * This call to hasNextDocument() is necessary to make sure attribute
		 * summaries are loaded even if client does not call hasNextDocument(),
		 * e.g. knowing that it's a list head request only.
		 */
		dl.hasNextDocument();
		
		//	finally ...
		return dl;
	}
	
	private static class ReaderDocumentList extends ImsDocumentList {
		private BufferedReader br;
		private String next = null;
		private int docCount;
		private int docsRetrieved = 0;
		private int charsRead = 0;
		private int charsRetrieved = 0;
		ReaderDocumentList(String[] listFieldNames, Reader in, int docCount) {
			super(listFieldNames);
			this.br = new BufferedReader(new FilterReader(in) {
				public int read() throws IOException {
					int r = super.read();
					if (r != -1)
						charsRead++;
					return r;
				}
				public int read(char[] cbuf, int off, int len) throws IOException {
					int r = super.read(cbuf, off, len);
					if (r != -1)
						charsRead += r;
					return r;
				}
			}, 65536);
			this.docCount = docCount;
		}
		public int getDocumentCount() {
			return this.docCount;
		}
		public int getRetrievedDocumentCount() {
			return this.docsRetrieved;
		}
		public int getRemainingDocumentCount() {
			if (this.docCount == -1) {
				if (this.charsRetrieved == 0)
					return (this.hasNextDocument() ? 1 : 0);
				int docSize = (this.charsRetrieved / ((this.docsRetrieved == 0) ? 1 : this.docsRetrieved));
				int charsRemaining = (charsRead - this.charsRetrieved);
				return Math.round(((float) charsRemaining) / docSize);
			}
			else return (this.docCount - this.docsRetrieved);
		}
		public boolean hasNextDocument() {
			
			//	we're good already here
			if (this.next != null)
				return true;
			
			//	input utterly depleted
			else if (this.br == null)
				return false;
			
			//	read next line
			try {
				this.next = this.br.readLine();
			}
			catch (IOException ioe) {
				ioe.printStackTrace(System.out);
			}
			
			//	input depleted
			if ((this.next == null) || (this.next.trim().length() == 0)) {
				this.closeReader();
				return false;
			}
			
			//	data line, we're good
			if (this.next.indexOf('\t') != -1) {
				this.charsRetrieved += this.next.length();
				return true;
			}
			
			//	read field value summary
			int split = this.next.indexOf(' ');
			if (split != -1) {
				String fieldName = this.next.substring(0, split);
				String[] fieldValues = this.next.substring(split + 1).split("\\s++");
				AttributeSummary as = this.getListFieldValues(fieldName, true);
				for (int v = 0; v < fieldValues.length; v++) try {
					String fieldValue = fieldValues[v];
					int countStart = fieldValue.indexOf('=');
					if (countStart == -1)
						as.add(URLDecoder.decode(fieldValue, "UTF-8"));
					else as.add(URLDecoder.decode(fieldValue.substring(0, countStart), "UTF-8"), Integer.parseInt(fieldValue.substring(countStart + "=".length())));
				} catch (IOException ioe) {}
			}
			
			//	recurse, there might be more
			this.next = null;
			return this.hasNextDocument();
		}
		public ImsDocumentListElement getNextDocument() {
			if (!this.hasNextDocument())
				return null;
			String[] next = this.next.split("\\t");
			this.next = null;
			ImsDocumentListElement dle = new ImsDocumentListElement();
			for (int f = 0; f < this.listFieldNames.length; f++)
				dle.setAttribute(this.listFieldNames[f], ((f < next.length) ? next[f] : ""));
			this.docsRetrieved++;
			return dle;
		}
		private void closeReader() {
			try {
				this.br.close();
			}
			catch (IOException ioe) {
				ioe.printStackTrace(System.out);
			}
			this.br = null;
		}
		protected void finalize() throws Throwable {
			if (this.br != null) {
				this.br.close();
				this.br = null;
			}
		}
	}
	
//	private static String[] parseCsvLine(String line, char delimiter) {
//		char currentChar;
//		char nextChar;
//		
//		boolean quoted = false;
//		boolean escaped = false;
//		
//		StringVector lineParts = new StringVector();
//		StringBuffer linePartAssembler = new StringBuffer();
//		
//		for (int c = 0; c < line.length(); c++) {
//			currentChar = line.charAt(c);
//			nextChar = (((c + 1) == line.length()) ? '\u0000' : line.charAt(c+1));
//			
//			//	escaped character
//			if (escaped) {
//				escaped = false;
//				linePartAssembler.append(currentChar);
//			}
//			
//			//	start or end of quoted value
//			else if (currentChar == delimiter) {
//				if (quoted) {
//					if (nextChar == delimiter) escaped = true;
//					else {
//						quoted = false;
//						lineParts.addElement(linePartAssembler.toString());
//						linePartAssembler = new StringBuffer();
//					}
//				}
//				else quoted = true;
//			}
//			
//			//	in quoted value
//			else if (quoted) linePartAssembler.append(currentChar);
//			
//			//	end of value
//			else if ((currentChar == ',')) {
//				if (linePartAssembler.length() != 0) {
//					lineParts.addElement(linePartAssembler.toString());
//					linePartAssembler = new StringBuffer();
//				}
//			}
//			
//			//	other char
//			else linePartAssembler.append(currentChar);
//		}
//		
//		//	return result
//		return lineParts.toStringArray();
//	}
}