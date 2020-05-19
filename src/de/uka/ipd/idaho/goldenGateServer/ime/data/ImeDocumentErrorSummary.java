///*
// * Copyright (c) 2006-, IPD Boehm, Universitaet Karlsruhe (TH) / KIT, by Guido Sautter
// * All rights reserved.
// *
// * Redistribution and use in source and binary forms, with or without
// * modification, are permitted provided that the following conditions are met:
// *
// *     * Redistributions of source code must retain the above copyright
// *       notice, this list of conditions and the following disclaimer.
// *     * Redistributions in binary form must reproduce the above copyright
// *       notice, this list of conditions and the following disclaimer in the
// *       documentation and/or other materials provided with the distribution.
// *     * Neither the name of the Universitaet Karlsruhe (TH) / KIT nor the
// *       names of its contributors may be used to endorse or promote products
// *       derived from this software without specific prior written permission.
// *
// * THIS SOFTWARE IS PROVIDED BY UNIVERSITAET KARLSRUHE (TH) / KIT AND CONTRIBUTORS 
// * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR ANY
// * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// */
//package de.uka.ipd.idaho.goldenGateServer.ime.data;
//
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.io.OutputStream;
//import java.io.OutputStreamWriter;
//import java.io.Reader;
//import java.io.Writer;
//import java.util.Comparator;
//import java.util.TreeMap;
//import java.util.TreeSet;
//
//import de.uka.ipd.idaho.gamta.Attributed;
//import de.uka.ipd.idaho.gamta.util.CountingSet;
//import de.uka.ipd.idaho.gamta.util.DocumentErrorProtocol;
//
///**
// * Summary of a document error protocol, containing only error categories and
// * types, along with counts of error severities in each. All the various
// *  <code>getErrors()</code> methods return empty arrays.
// * 
// * @author sautter
// */
//public class ImeDocumentErrorSummary extends DocumentErrorProtocol {
//	
//	/** the ID of the document the error summary pertains to */
//	public final String docId;
//	
//	private TreeSet errorCategories = new TreeSet();
//	private TreeSet errorTypes = new TreeSet();
//	private CountingSet errorCounts = new CountingSet(new TreeMap());
//	private CountingSet errorSeverityCounts = new CountingSet(new TreeMap());
//	
//	/** Constructor
//	 * @param docId the ID of the document the error summary pertains to
//	 */
//	public ImeDocumentErrorSummary(String docId) {
//		this.docId = docId;
//	}
//	
//	public int getErrorCategoryCount() {
//		return this.errorCategories.size();
//	}
//	
//	public int getErrorTypeCount() {
//		return this.errorTypes.size();
//	}
//	
//	public int getErrorCount() {
//		return this.errorCounts.getCount("");
//	}
//	
//	public int getErrorSeverityCount(String severity) {
//		return this.errorSeverityCounts.getCount(severity);
//	}
//	
//	public DocumentError[] getErrors() {
//		return new DocumentError[0];
//	}
//	
//	public int getErrorCount(String category) {
//		return ((category == null) ? this.errorCounts.getCount("") : this.errorCounts.getCount(category));
//	}
//	
//	public int getErrorSeverityCount(String category, String severity) {
//		return this.errorSeverityCounts.getCount(category + "." + severity);
//	}
//	
//	public DocumentError[] getErrors(String category) {
//		return new DocumentError[0];
//	}
//	
//	public int getErrorCount(String category, String type) {
//		return ((category == null) ? this.errorCounts.getCount("") : ((type == null) ? this.errorCounts.getCount(category) : this.errorCounts.getCount(category + "." + type)));
//	}
//	
//	public int getErrorSeverityCount(String category, String type, String severity) {
//		return this.errorSeverityCounts.getCount(category + "." + type + "." + severity);
//	}
//	
//	public DocumentError[] getErrors(String category, String type) {
//		return new DocumentError[0];
//	}
//	
//	public void addError(String source, Attributed subject, Attributed parent, String category, String type, String description, String severity) {
//		this.errorCategories.add(category);
//		this.errorTypes.add(category + "." + type);
//		this.errorCounts.add("");
//		this.errorCounts.add(category);
//		this.errorCounts.add(category + "." + type);
//		this.errorSeverityCounts.add(severity);
//		this.errorSeverityCounts.add(category + "." + severity);
//		this.errorSeverityCounts.add(category + "." + type + "." + severity);
//	}
//	
//	void addError(String category, String type, String severity, int count) {
//		this.errorCategories.add(category);
//		this.errorTypes.add(category + "." + type);
//		this.errorCounts.add("", count);
//		this.errorCounts.add(category, count);
//		this.errorCounts.add((category + "." + type), count);
//		this.errorSeverityCounts.add(severity, count);
//		this.errorSeverityCounts.add((category + "." + severity), count);
//		this.errorSeverityCounts.add((category + "." + type + "." + severity), count);;
//	}
//	
//	public void removeError(DocumentError error) {}
//	public Comparator getErrorComparator() {
//		return null;
//	}
//	public Attributed findErrorSubject(Attributed doc, String[] data) {
//		return null;
//	}
//	
//	/**
//	 * Store an error summary to a given output stream.
//	 * @param ides the error summary to store
//	 * @param out the output stream to store the error summary to
//	 * @throws IOException
//	 */
//	public static void storeErrorSummary(ImeDocumentErrorSummary ides, OutputStream out) throws IOException {
//		storeErrorSummary(ides, new OutputStreamWriter(out, "UTF-8"));
//	}
//	
//	/**
//	 * Store an error summary to a given output stream.
//	 * @param ides the error summary to store
//	 * @param out the output stream to store the error protocol to
//	 * @throws IOException
//	 */
//	public static void storeErrorSummary(ImeDocumentErrorSummary ides, Writer out) throws IOException {
//		//	TODOnot consider zipping ==> IMF is zipped anyway, and IMD is huge, so ease of access more important
//		
//		//	persist error protocol
//		BufferedWriter epBw = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
//		String[] categories = ides.getErrorCategories();
//		for (int c = 0; c < categories.length; c++) {
//			
//			//	store category proper
//			epBw.write("CATEGORY");
//			epBw.write("\t" + categories[c]);
//			epBw.write("\t" + ides.getErrorCategoryLabel(categories[c]));
//			epBw.write("\t" + ides.getErrorCategoryDescription(categories[c]));
//			epBw.newLine();
//			
//			//	store error types in current category
//			String[] types = ides.getErrorTypes(categories[c]);
//			for (int t = 0; t < types.length; t++) {
//				
//				//	store type proper
//				epBw.write("TYPE");
//				epBw.write("\t" + types[t]);
//				epBw.write("\t" + ides.getErrorTypeLabel(categories[c], types[t]));
//				epBw.write("\t" + ides.getErrorTypeDescription(categories[c], types[t]));
//				epBw.newLine();
//				
//				//	store error counts
//				storeErrorSeverity(categories[c], types[t], DocumentError.SEVERITY_BLOCKER, ides, epBw);
//				storeErrorSeverity(categories[c], types[t], DocumentError.SEVERITY_CRITICAL, ides, epBw);
//				storeErrorSeverity(categories[c], types[t], DocumentError.SEVERITY_MAJOR, ides, epBw);
//				storeErrorSeverity(categories[c], types[t], DocumentError.SEVERITY_MINOR, ides, epBw);
//			}
//		}
//		epBw.flush();
//	}
//	private static void storeErrorSeverity(String category, String type, String severity, ImeDocumentErrorSummary ides, BufferedWriter epBw) throws IOException {
//		int errors = ides.getErrorSeverityCount(category, type, severity);
//		if (errors == 0)
//			return;
//		epBw.write("ERROR");
//		epBw.write("\t" + severity);
//		epBw.write("\t" + errors);
//		epBw.newLine();
//	}
//	
//	/**
//	 * Fill an error summary with the data provided by a given input stream.
//	 * @param ides the error summary to populate
//	 * @param in the input stream to populate the error summary from
//	 * @throws IOException
//	 */
//	public static void fillErrorSummary(ImeDocumentErrorSummary ides, InputStream in) throws IOException {
//		fillErrorSummary(ides, new InputStreamReader(in, "UTF-8"));
//	}
//	
//	/**
//	 * Fill an error summary with the data provided by a given input stream.
//	 * @param ides the error summary to populate
//	 * @param in the input stream to populate the error summary from
//	 * @throws IOException
//	 */
//	public static void fillErrorSummary(ImeDocumentErrorSummary ides, Reader in) throws IOException {
//		//	TODOnot consider zipping ==> IMF is zipped anyway, and IMD is huge, so ease of access more important
//		
//		//	load error protocol, scoping error categories and types
//		BufferedReader epBr = ((in instanceof BufferedReader) ? ((BufferedReader) in) : new BufferedReader(in));
//		String category = "null";
//		String type = "null";
//		for (String line; (line = epBr.readLine()) != null;) {
//			line = line.trim();
//			if (line.length() == 0)
//				continue;
//			
//			//	parse data
//			String[] data = line.split("\\t");
//			if (data.length < 2)
//				continue;
//			
//			//	read error category
//			if ("CATEGORY".equals(data[0])) {
//				category = data[1];
//				String label = getElement(data, 2, category);
//				String description = getElement(data, 3, category);
//				ides.addErrorCategory(category, label, description);
//				continue;
//			}
//			
//			//	read error type
//			if ("TYPE".equals(data[0])) {
//				type = data[1];
//				String label = getElement(data, 2, type);
//				String description = getElement(data, 3, type);
//				ides.addErrorType(category, type, label, description);
//				continue;
//			}
//			
//			//	read error
//			if ("ERROR".equals(data[0])) {
//				String severity = data[1];
//				String count = data[2];
//				ides.addError(category, type, severity, Integer.parseInt(count));
//			}
//		}
//		epBr.close();
//	}
//	private static String getElement(String[] data, int index, String def) {
//		return ((index < data.length) ? data[index] : def);
//	}
//}
