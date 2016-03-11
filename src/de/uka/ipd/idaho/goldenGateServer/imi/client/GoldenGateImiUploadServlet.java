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
 *     * Neither the name of the Universität Karlsruhe (TH) / KIT nor the
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
package de.uka.ipd.idaho.goldenGateServer.imi.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.uka.ipd.idaho.easyIO.web.FormDataReceiver;
import de.uka.ipd.idaho.easyIO.web.FormDataReceiver.FieldValueInputStream;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.util.SgmlDocumentReader;
import de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;
import de.uka.ipd.idaho.plugins.bibRefs.BibRefConstants;
import de.uka.ipd.idaho.plugins.bibRefs.BibRefEditorFormHandler;
import de.uka.ipd.idaho.plugins.bibRefs.BibRefTypeSystem;
import de.uka.ipd.idaho.plugins.bibRefs.BibRefUtils;
import de.uka.ipd.idaho.plugins.bibRefs.BibRefUtils.RefData;
import de.uka.ipd.idaho.plugins.bibRefs.reFinder.ReFinderClient;
import de.uka.ipd.idaho.plugins.bibRefs.refBank.RefBankClient;
import de.uka.ipd.idaho.plugins.bibRefs.refBank.RefBankClient.BibRef;
import de.uka.ipd.idaho.plugins.bibRefs.refBank.RefBankClient.BibRefIterator;

/**
 * Web based upload facility for GoldenGATE Image Markup document Importer.
 * 
 * @author sautter
 */
public class GoldenGateImiUploadServlet extends GgServerHtmlServlet implements BibRefConstants {
	private static final int uploadMaxLength = (10 * 1024 * 1024); // 10MB for starters
	private static Set fileFieldNames = Collections.synchronizedSet(new HashSet());
	static {
		fileFieldNames.add("uploadDocFile");
	}
	
	private BibRefTypeSystem refTypeSystem = null;
	private String[] refIdTypes;
	private String refIdTypesString;
	
	private Map mimeTypes = new LinkedHashMap();
	
	private String putPassPhrase = Gamta.getAnnotationID(); // initialize to random value to block PUT upload for faulty configuration
	
	private File uploadCacheFolder = null;
	
	private GoldenGateImiClient imiClient = null;
	
	private RefBankClient rbkClient = null;
	private ReFinderClient rfrClient = null;
	
	private Set validUploadIDs = Collections.synchronizedSet(new HashSet());
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerClientServlet#doInit()
	 */
	protected void doInit() throws ServletException {
		super.doInit();
		
		//	prepare for upload caching
		this.uploadCacheFolder = new File(new File(this.webInfFolder, "caches"), "imsUploadData");
		this.uploadCacheFolder.mkdirs();
		
		//	connect to backing BDP
		this.imiClient = new GoldenGateImiClient(this.serverConnection);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.easyIO.web.HtmlServlet#reInit()
	 */
	protected void reInit() throws ServletException {
		super.reInit();
		
		//	read pass phrase from configuration
		this.putPassPhrase = this.getSetting("putPassPhrase", this.putPassPhrase);
		
		//	collect identifier types
		ArrayList idTypes = new ArrayList();
		idTypes.add("Generic");
		idTypes.addAll(Arrays.asList(this.getSetting("refIdTypes", "").trim().split("\\s+")));
		this.refIdTypes = ((String[]) idTypes.toArray(new String[idTypes.size()]));
		StringBuffer refIdTypesString = new StringBuffer();
		for (int i = 1; i < this.refIdTypes.length; i++) {
			if (i != 1)
				refIdTypesString.append(';');
			refIdTypesString.append(this.refIdTypes[i]);
		}
		this.refIdTypesString = refIdTypesString.toString();
		
		
		//	TODO_later load custom reference type system if configured
		this.refTypeSystem = BibRefTypeSystem.getDefaultInstance();
		
		//	load list of MIME types and map to descriptions
		try {
			BufferedReader mtBr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(this.dataFolder, "mimeTypes.txt")), "UTF-8"));
			for (String mtl; (mtl = mtBr.readLine()) != null;) {
				if (mtl.startsWith("//"))
					continue;
				if (mtl.indexOf(' ') == -1)
					continue;
				String mt = mtl.substring(0, mtl.indexOf(' '));
				String mtd = mtl.substring(mtl.indexOf(' ')).trim();
				this.mimeTypes.put(mt, mtd);
			}
			mtBr.close();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			throw new ServletException(("Error loading MIME types: " + ioe.getMessage()), ioe);
		}
		
		//	set up RefBank connection for meta data search
		String rbkUrl = this.getSetting("refBankUrl");
		if (rbkUrl != null)
			this.rbkClient = new RefBankClient(rbkUrl);
		
		//	connect to ReFinder
		String rfrUrl = this.getSetting("reFinderUrl");
		this.rfrClient = ((rfrUrl == null) ? new ReFinderClient() : new ReFinderClient(rfrUrl));
	}
	
	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		//	check if request directed at webapp host
		if (this.webAppHost.handleRequest(request, response))
			return;
		
		//	check authentication
		if (!this.webAppHost.isAuthenticated(request)) {
			//	TODO use form passe-partout as described above
			//	TODO style login page
			this.sendHtmlPage(this.webAppHost.getLoginPageBuilder(this, request, response, "includeBody", null));
			return;
		}
		
		//	get user name
		String userName = this.webAppHost.getUserName(request);
		
		//	check type of request
		String pathInfo = request.getPathInfo();
		if (pathInfo == null) {
			this.sendUploadPage(request, response, userName);
			return;
		}
		
		//	handle calls for background reference search
		if ("/searchRefs.js".equals(pathInfo)) {
			response.setContentType("text/plain");
			response.setCharacterEncoding("UTF-8");
			Writer out = new OutputStreamWriter(response.getOutputStream(), "UTF-8");
			BufferedWriter bw = new BufferedWriter(out);
			bw.write("displaySearchResult([");bw.newLine();
			RefData[] rds = this.searchRefData(request);
			for (int r = 0; r < rds.length; r++) {
				if (r != 0)
					bw.write(",");
				bw.write("{");bw.newLine();
				String refType = this.refTypeSystem.classify(rds[r]);
				if (refType != null) {
					bw.write("  \"" + PUBLICATION_TYPE_ATTRIBUTE + "\": \"" + refType.replaceAll("\\\"", "'") + "\",");bw.newLine();
				}
				
				String[] attributeNames = rds[r].getAttributeNames();
				for (int a = 0; a < attributeNames.length; a++) {
					if (PUBLICATION_TYPE_ATTRIBUTE.equals(attributeNames[a]) || PUBLICATION_IDENTIFIER_ANNOTATION_TYPE.equals(attributeNames[a]))
						continue;
					if (AUTHOR_ANNOTATION_TYPE.equals(attributeNames[a]) || EDITOR_ANNOTATION_TYPE.equals(attributeNames[a])) {
						String[] values = rds[r].getAttributeValues(attributeNames[a]);
						if (values == null)
							continue;
						bw.write("  \"" + attributeNames[a] + "\": \"" + values[0].replaceAll("\\\"", "'"));
						for (int v = 1; v < values.length; v++)
							bw.write(" & " + values[v].replaceAll("\\\"", "'"));
						bw.write("\",");bw.newLine();
					}
					else {
						String value = rds[r].getAttribute(attributeNames[a]);
						if (value != null) {
							bw.write("  \"" + attributeNames[a] + "\": \"" + value.replaceAll("\\\"", "'") + "\",");bw.newLine();
						}
					}
				}
				
				String[] idTypes = rds[r].getIdentifierTypes();
				for (int i = 0; i < idTypes.length; i++) {
					String id = rds[r].getIdentifier(idTypes[i]);
					if (id != null) {
						bw.write("  \"ID-" + idTypes[i] + "\": \"" + id.replaceAll("\\\"", "'") + "\",");bw.newLine();
					}
				}
				
				if (rds[r].hasAttribute("docFormat")) {
					bw.write("  \"docFormat\": \"" + rds[r].getAttribute("docFormat").replaceAll("\\\"", "'") + "\",");bw.newLine();
				}
				if (rds[r].hasAttribute("sourceName")) {
					bw.write("  \"sourceName\": \"" + rds[r].getAttribute("sourceName").replaceAll("\\\"", "'") + "\",");bw.newLine();
				}
				if (rds[r].hasAttribute(ReFinderClient.REFERENCE_DATA_SOURCE_ATTRIBUTE)) {
					bw.write("  \"refSource\": \"" + rds[r].getAttribute(ReFinderClient.REFERENCE_DATA_SOURCE_ATTRIBUTE).replaceAll("\\\"", "'") + "\",");bw.newLine();
				}
				
				bw.write("  \"refString\": \"" + BibRefUtils.toRefString(rds[r]).replaceAll("\\\"", "'").trim() + "\"");bw.newLine();
				bw.write("}");
			}
			bw.write("]);");bw.newLine();
			bw.flush();
			out.flush();
			return;
		}
		
		//	something else, send base page
		this.sendUploadPage(request, response, userName);
	}
	
	private void sendUploadPage(HttpServletRequest request, HttpServletResponse response, final String userName) throws IOException {
		response.setContentType("text/html");
		response.setCharacterEncoding("UTF-8");
		this.sendHtmlPage(new HtmlPageBuilder(this, request, response) {
			protected void include(String type, String tag) throws IOException {
				if ("includeBody".equals(type)) {
					//	TODO make sure this sucker is a child of the page body proper, or at least increase z-index
					webAppHost.writeAccountManagerHtml(this, null);
					//	TODO make sure this sucker is properly wrapped in the required DIV container
					//	TODO likely use sort of a 'formHost.html' file as sort of a passe-partout to do so ...
					//	TODO ... and switch form-building servlets to react to 'includeForm' tag inside it 
					writeUploadForm(this, null);
				}
				else super.include(type, tag);
			}
			protected boolean includeJavaScriptDomHelpers() {
				return true;
			}
		});
	}
	
	private void writeUploadForm(HtmlPageBuilder hpb, String forwardUrl) throws IOException {
		 // TODO style upload form
		
		//	generate upload ID
		String uploadId = Gamta.getAnnotationID();
		this.validUploadIDs.add(uploadId);
		
		//	start form
		hpb.writeLine("<form" +
				" id=\"uploadForm\"" +
				" method=\"POST\"" +
				" action=\"" + hpb.request.getContextPath() + hpb.request.getServletPath() + "/upload/" + uploadId + "\"" +
				" onsubmit=\"return prepareUpload();\"" +
				" target=\"uploadResult\"" +
			">");
		
		//	add forward URL (if any)
		if (forwardUrl != null)
			hpb.writeLine("<input type=\"hidden\" name=\"forwardUrl\" value=\"" + forwardUrl + "\">");
		
		//	add fields
		hpb.writeLine("<table class=\"imiDocUploadTable\">");
		hpb.writeLine("<tr>");
		hpb.writeLine("<td class=\"imiDocUploadTableHeader\">");
		hpb.writeLine("Upload a New Document");
		hpb.writeLine("</td>");
		hpb.writeLine("</tr>");
		hpb.writeLine("</table>");
		
		hpb.writeLine("<div id=\"imiDocUploadRefDataFields\">");
		BibRefEditorFormHandler.createHtmlForm(hpb.asWriter(), false, this.refTypeSystem, this.refIdTypes);
		hpb.writeLine("</div>");
		
		hpb.writeLine("<div id=\"imiDocUploadDataFields\">");
		hpb.writeLine("<table class=\"bibRefEditorTable\">");
		hpb.writeLine("<tr>");
		hpb.writeLine("<td class=\"imiDocUploadFieldLabel\" style=\"text-align: right;\">Document URL:</td>");
		hpb.writeLine("<td colspan=\"3\" class=\"imiDocUploadFieldCell\"><input class=\"imiDocUploadField\" style=\"width: 100%;\" id=\"uploadUrl_field\" name=\"uploadDocUrl\"></td>");
		hpb.writeLine("</tr>");
		hpb.writeLine("<tr>");
		hpb.writeLine("<td class=\"imiDocUploadFieldLabel\" style=\"text-align: right;\">Document File:</td>");
		hpb.writeLine("<td colspan=\"3\" class=\"imiDocUploadFieldCell\"><input type=\"file\" class=\"imiDocUploadField\" style=\"width: 100%;\" id=\"uploadFile_field\" name=\"uploadDocFile\" onchange=\"uploadFileChanged();\"></td>");
		hpb.writeLine("</tr>");
		
		hpb.writeLine("<tr>");
		hpb.writeLine("<td class=\"imiDocUploadFieldLabel\" style=\"text-align: right;\">Document Format:</td>");
		hpb.writeLine("<td colspan=\"3\" class=\"imiDocUploadFieldCell\">");
		hpb.writeLine("<select class=\"imiDocUploadField\" style=\"width: 100%;\" id=\"uploadDocFormat_field\" name=\"uploadDocMimeType\">");
		for (Iterator mtit = this.mimeTypes.keySet().iterator(); mtit.hasNext();) {
			String mt = ((String) mtit.next());
			String mtd = ((String) this.mimeTypes.get(mt));
			hpb.writeLine("<option value=\"" + mt + "\">" + mtd + "</option>");
		}
		hpb.writeLine("</select>");
		hpb.writeLine("</td>");
		hpb.writeLine("</tr>");
		
		hpb.writeLine("</table>");
		hpb.writeLine("</div>");
		
		hpb.writeLine("<div id=\"imiDocUploadButtons\">");
		hpb.writeLine("<input type=\"button\" class=\"imiDocUploadButton\" id=\"searchRefs_button\" value=\"Search References\" onclick=\"searchRefs();\">");
		hpb.writeLine("<input type=\"button\" class=\"imiDocUploadButton\" id=\"checkRef_button\" value=\"Check Reference\" onclick=\"validateRefData();\">");
		hpb.writeLine("<input type=\"submit\" class=\"imiDocUploadButton\" id=\"doUpload_button\" value=\"Upload Document\">");
		hpb.writeLine("</div>");
		
		hpb.writeLine("</form>");
		
		hpb.writeLine("<script type=\"text/javascript\">");
		boolean setRef = false;
		hpb.writeLine("  var ref = new Object();");
		setRef = (setRef | this.writeRefDataAttributeSetter(hpb, AUTHOR_ANNOTATION_TYPE));
		setRef = (setRef | this.writeRefDataAttributeSetter(hpb, YEAR_ANNOTATION_TYPE));
		setRef = (setRef | this.writeRefDataAttributeSetter(hpb, TITLE_ANNOTATION_TYPE));
		setRef = (setRef | this.writeRefDataAttributeSetter(hpb, JOURNAL_NAME_ANNOTATION_TYPE));
		setRef = (setRef | this.writeRefDataAttributeSetter(hpb, PUBLISHER_ANNOTATION_TYPE));
		setRef = (setRef | this.writeRefDataAttributeSetter(hpb, LOCATION_ANNOTATION_TYPE));
		setRef = (setRef | this.writeRefDataAttributeSetter(hpb, EDITOR_ANNOTATION_TYPE));
		setRef = (setRef | this.writeRefDataAttributeSetter(hpb, VOLUME_TITLE_ANNOTATION_TYPE));
		setRef = (setRef | this.writeRefDataAttributeSetter(hpb, PAGINATION_ANNOTATION_TYPE));
		setRef = (setRef | this.writeRefDataAttributeSetter(hpb, VOLUME_DESIGNATOR_ANNOTATION_TYPE));
		setRef = (setRef | this.writeRefDataAttributeSetter(hpb, ISSUE_DESIGNATOR_ANNOTATION_TYPE));
		setRef = (setRef | this.writeRefDataAttributeSetter(hpb, NUMERO_DESIGNATOR_ANNOTATION_TYPE));
		setRef = (setRef | this.writeRefDataAttributeSetter(hpb, PUBLICATION_URL_ANNOTATION_TYPE));
		for (int i = 0; i < this.refIdTypes.length; i++)
			setRef = (setRef | this.writeRefDataAttributeSetter(hpb, ("ID-" + this.refIdTypes[i])));
		if (setRef)
			hpb.writeLine("  bibRefEditor_setRef(ref);");
		hpb.writeLine("  bibRefEditor_refTypeChanged();");
		hpb.writeLine("</script>");
		
		hpb.writeLine("<script id=\"refSearchScript\" type=\"text/javascript\"></script>");
		
		hpb.writeLine("<div id=\"refSearchResult\" style=\"display: none;\">");
		hpb.writeLine("</div>");
	}
	
	private boolean writeRefDataAttributeSetter(HtmlPageBuilder hpb, String attribute) throws IOException {
		String value = hpb.request.getParameter(attribute);
		if (value == null)
			return false;
		value = value.trim();
		if (value.length() == 0)
			return false;
		hpb.writeLine("  ref['" + attribute + "'] = '" + value + "'");
		return true;
	}
	
	private RefData[] searchRefData(HttpServletRequest request) throws IOException {
//		if (this.rbkClient == null)
//			return new RefData[0];
		
		//	get query data
		String extIdType = null;
		String extId = null;
		for (int t = 0; t < this.refIdTypes.length; t++)
			if (request.getParameter("ID-" + this.refIdTypes[t]) != null) {
				extIdType = this.refIdTypes[t];
				extId = request.getParameter("ID-" + this.refIdTypes[t]);
				break;
			}
		int year = -1;
		try {
			year = Integer.parseInt(request.getParameter(YEAR_ANNOTATION_TYPE));
		} catch (Exception e) {}
		
		//	query RefBank
		BibRefIterator brit = this.rbkClient.findRefs(null, request.getParameter(AUTHOR_ANNOTATION_TYPE), request.getParameter(TITLE_ANNOTATION_TYPE), year, this.getOrigin(request), extId, extIdType, -1, true);
		ArrayList rdList = new ArrayList();
		while (brit.hasNextRef()) {
			BibRef br = brit.getNextRef();
			RefData rd = this.getRefData(br);
			if (rd != null)
				rdList.add(rd);
		}
		
		//	query RefFinder
		RefData[] rfrRds = this.rfrClient.find(request.getParameter(AUTHOR_ANNOTATION_TYPE), request.getParameter(TITLE_ANNOTATION_TYPE), year, this.getOrigin(request));
		for (int r = 0; r < rfrRds.length; r++)
			rdList.add(rfrRds[r]);
		
		//	finally ...
		return ((RefData[]) rdList.toArray(new RefData[rdList.size()]));
	}
	
	private String getOrigin(HttpServletRequest request) {
		String origin = request.getParameter(JOURNAL_NAME_ANNOTATION_TYPE);
		if (origin != null) {
			String vd = request.getParameter(VOLUME_DESIGNATOR_ANNOTATION_TYPE);
			if (vd != null) {
				origin += (" " + vd);
				return origin;
			}
			String id = request.getParameter(ISSUE_DESIGNATOR_ANNOTATION_TYPE);
			if (id != null) {
				origin += (" " + id);
				return origin;
			}
			String nd = request.getParameter(NUMERO_DESIGNATOR_ANNOTATION_TYPE);
			if (nd != null) {
				origin += (" " + nd);
				return origin;
			}
		}
		
		origin = request.getParameter(VOLUME_TITLE_ANNOTATION_TYPE);
		if (origin != null)
			return origin;
		
		origin = request.getParameter(PUBLISHER_ANNOTATION_TYPE);
		if (origin != null) {
			String location = request.getParameter(LOCATION_ANNOTATION_TYPE);
			if (location != null)
				origin = (location + ": " + origin);
			return origin;
		}
		
		origin = request.getParameter(LOCATION_ANNOTATION_TYPE);
		if (origin != null)
			return origin;
		
		return null;
	}
	
	private RefData getRefData(BibRef br) throws IOException {
		if (br.getRefParsed() == null)
			return null;
		return BibRefUtils.modsXmlToRefData(SgmlDocumentReader.readDocument(new StringReader(br.getRefParsed())));
	}
	
	private RefData getRefData(FormDataReceiver data) throws IOException {
		RefData rd = new RefData();
		this.addRefDataAttribute(rd, data, AUTHOR_ANNOTATION_TYPE);
		this.addRefDataAttribute(rd, data, YEAR_ANNOTATION_TYPE);
		this.addRefDataAttribute(rd, data, TITLE_ANNOTATION_TYPE);
		this.addRefDataAttribute(rd, data, JOURNAL_NAME_ANNOTATION_TYPE);
		this.addRefDataAttribute(rd, data, PUBLISHER_ANNOTATION_TYPE);
		this.addRefDataAttribute(rd, data, LOCATION_ANNOTATION_TYPE);
		this.addRefDataAttribute(rd, data, JOURNAL_NAME_OR_PUBLISHER_ANNOTATION_TYPE);
		this.addRefDataAttribute(rd, data, EDITOR_ANNOTATION_TYPE);
		this.addRefDataAttribute(rd, data, VOLUME_TITLE_ANNOTATION_TYPE);
		this.addRefDataAttribute(rd, data, PAGINATION_ANNOTATION_TYPE);
		this.addRefDataAttribute(rd, data, VOLUME_DESIGNATOR_ANNOTATION_TYPE);
		this.addRefDataAttribute(rd, data, ISSUE_DESIGNATOR_ANNOTATION_TYPE);
		this.addRefDataAttribute(rd, data, NUMERO_DESIGNATOR_ANNOTATION_TYPE);
		this.addRefDataAttribute(rd, data, PUBLICATION_URL_ANNOTATION_TYPE);
		String type = this.refTypeSystem.classify(rd);
		if (type != null)
			rd.setAttribute(PUBLICATION_TYPE_ATTRIBUTE, type);
		for (int i = 0; i < this.refIdTypes.length; i++)
			this.addRefDataAttribute(rd, data, ("ID-" + this.refIdTypes[i]));
		return rd;
	}
	
	private void addRefDataAttribute(RefData rd, FormDataReceiver data, String attribute) throws IOException {
		String value = data.getFieldValue(attribute);
		if (value == null)
			return;
		value = value.trim();
		if (value.length() == 0)
			return;
		if (AUTHOR_ANNOTATION_TYPE.equals(attribute) || EDITOR_ANNOTATION_TYPE.equals(attribute)) {
			String[] values = value.split("\\s*\\&\\s*");
			for (int v = 0; v < values.length; v++)
				rd.addAttribute(attribute, values[v]);
		}
		else rd.setAttribute(attribute, value);
	}
	
	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		//	check if request directed at webapp host
		if (this.webAppHost.handleRequest(request, response))
			return;
		
		//	check authentication (user name & password via host, pass phrase locally)
		if (!this.webAppHost.isAuthenticated(request)) {
			//	TODO use form passe-partout as described above
			//	TODO style login page
			this.sendHtmlPage(this.webAppHost.getLoginPageBuilder(this, request, response, "includeBody", (request.getContextPath() + request.getServletPath())));
			return;
		}
		
		//	get upload ID and check validity
		String pathInfo = request.getPathInfo();
		if (pathInfo == null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		String uploadId = pathInfo.substring("/upload/".length());
		if (!this.validUploadIDs.contains(uploadId)) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		//	receive upload
		FormDataReceiver data = FormDataReceiver.receive(request, uploadMaxLength, this.uploadCacheFolder, 1024, fileFieldNames);
		
		//	get user name to credit
		String mimeType = data.getFieldValue("uploadDocMimeType");
		if (mimeType == null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "File MIME type missing, use 'mimeType' parameter to specify it.");
			return;
		}
		
		//	get user name to credit
		String user = data.getFieldValue("user");
		if (user == null)
			user = this.webAppHost.getUserName(request);
		if (user == null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "To-credit user name missing, use 'user' parameter to specify it.");
			return;
		}
		
		//	check meta data
		RefData rd = this.getRefData(data);
		String[] rdErrors = this.refTypeSystem.checkType(rd);
		if (rdErrors != null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, ("Invalid bibliographic meta data: " + rdErrors[0]));
			return;
		}
		
		//	get input stream for document proper
		InputStream docDataSource = data.getFieldByteStream("uploadDocFile");
		String docDataName = ((docDataSource instanceof FieldValueInputStream) ? ((FieldValueInputStream) docDataSource).fileName : null);
		int docDataSize = ((docDataSource instanceof FieldValueInputStream) ? ((FieldValueInputStream) docDataSource).fieldLength : -1);
		
		//	try and use document URL if no file uploaded
		if (docDataSource == null) {
			String docDataUrl = data.getFieldValue("uploadDocUrl");
			if (docDataUrl == null) {
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Neither file nor URL specified.");
				return;
			}
			else try {
				docDataSource = (new URL(docDataUrl)).openStream();
				docDataName = docDataUrl.substring(docDataUrl.lastIndexOf('/') + "/".length());
			}
			catch (MalformedURLException mue) {
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, ("Invalid document URL: " + docDataUrl));
				return;
			}
			catch (IOException ioe) {
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, ("Invalid document URL: " + docDataUrl));
				return;
			}
		}
		
		//	invalidate upload ID
		this.validUploadIDs.remove(uploadId);
		
		//	wrap meta data in Properties
		Properties docAttributes = new Properties();
		String[] rdAns = rd.getAttributeNames();
		for (int n = 0; n < rdAns.length; n++)
			docAttributes.setProperty(rdAns[n], rd.getAttributeValueString(rdAns[n], "&"));
		String[] rdIts = rd.getIdentifierTypes();
		for (int t = 0; t < rdIts.length; t++)
			docAttributes.setProperty(("ID-" + rdIts[t]), rd.getIdentifier(rdIts[t]));
		
		//	send upload to backing BDP
		final Properties uDocAttributes = this.imiClient.uploadDocument(docDataSource, uploadId, docDataName, docDataSize, mimeType, docAttributes, user);
		
		//	send upload log array and updated document attributes
		response.setCharacterEncoding("UTF-8");
		response.setContentType("text/html");
		this.sendPopupHtmlPage(new HtmlPageBuilder(this, request, response) {
			protected void include(String type, String tag) throws IOException {
				if ("includeBody".equals(type)) {
					this.writeLine("<b>DOCUMENT ATTRIBUTES:</b>");
					for (Enumeration ane = uDocAttributes.propertyNames(); ane.hasMoreElements();) {
						String an = ((String) ane.nextElement());
						String av = uDocAttributes.getProperty(an);
						this.writeLine("<br/><code>" + an + " = " + ((av == null) ? "null" : av) + "</code>");
					}
					this.writeLine("<button onclick=\"window.close()\">OK</button>");
				}
				else super.include(type, tag);
			}
		});
		
		//	finally ...
		data.dispose();
	}
	
	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doPut(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		//	check authentication (pass phrase header)
		String passPhrase = request.getHeader("Authorization");
		if ((passPhrase == null) || !this.putPassPhrase.equals(passPhrase)) {
			response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid pass phrase");
			return;
		}
		
		//	receive upload
		FormDataReceiver data = FormDataReceiver.receive(request, uploadMaxLength, this.uploadCacheFolder, 1024, fileFieldNames);
		
		//	get upload file MIME type
		String mimeType = data.getFieldValue("mimeType");
		if (mimeType == null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "File MIME type missing, use 'mimeType' parameter to specify it.");
			return;
		}
		
		//	get user name to credit
		String user = data.getFieldValue("user");
		if (user == null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "To-credit user name missing, use 'user' parameter to specify it.");
			return;
		}
		
		//	check meta data
		RefData rd = this.getRefData(data);
		String[] rdErrors = this.refTypeSystem.checkType(rd);
		if (rdErrors != null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, ("Invalid bibliographic meta data: " + rdErrors[0]));
			return;
		}
		
		//	wrap meta data in Properties
		Properties docAttributes = new Properties();
		String[] rdAns = rd.getAttributeNames();
		for (int n = 0; n < rdAns.length; n++)
			docAttributes.setProperty(rdAns[n], rd.getAttributeValueString(rdAns[n], "&"));
		String[] rdIts = rd.getIdentifierTypes();
		for (int t = 0; t < rdIts.length; t++)
			docAttributes.setProperty(("ID-" + rdIts[t]), rd.getIdentifier(rdIts[t]));
		
		//	get input stream for document proper
		FieldValueInputStream docDataSource = data.getFieldByteStream("file");
		
		//	send upload to backing BDP
		Properties uDocAttributes = this.imiClient.uploadDocument(docDataSource, Gamta.getAnnotationID(), docDataSource.fileName, docDataSource.fieldLength, mimeType, docAttributes, user);
		
		//	send JSON object with upload log array and updated document attributes
		response.setCharacterEncoding("UTF-8");
		response.setContentType("application/json");
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), "UTF-8"));
		bw.write("{"); bw.newLine();
		bw.write("  \"attributes\": {");
		for (Enumeration ane = uDocAttributes.propertyNames(); ane.hasMoreElements();) {
			String an = ((String) ane.nextElement());
			String av = uDocAttributes.getProperty(an);
			bw.write("    \"" + an + "\": " + ((av == null) ? "null" : ("\"" + av.replaceAll("\\\"", "'") + "\"")));
			if (ane.hasMoreElements())
				bw.write(",");
			bw.newLine();
		}
		bw.write("  },"); bw.newLine();
		bw.write("  \"user\": \"" + user.replaceAll("\\\"", "'") + "\","); bw.newLine();
		bw.write("  \"mimeType\": \"" + mimeType.replaceAll("\\\"", "'") + "\""); bw.newLine();
		bw.write("}"); bw.newLine();
		bw.flush();
		
		//	finally ...
		data.dispose();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.easyIO.web.HtmlServlet#writePageHeadExtensions(de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder)
	 */
	public void writePageHeadExtensions(HtmlPageBuilder out) throws IOException {
		
		//	write JavaScripts used by reference form
		BibRefEditorFormHandler.writeJavaScripts(out.asWriter(), this.refTypeSystem, this.refIdTypes);
		
		//	write JavaScripts used by reference search
		out.writeLine("<script type=\"text/javascript\">");
		
		out.writeLine("function uploadFileChanged() {");
		out.writeLine("  var uff = $('uploadFile_field');");
		out.writeLine("  if ((uff.value == null) || (uff.value.length == 0))");
		out.writeLine("    return;");
		out.writeLine("  var uuf = $('uploadUrl_field');");
		out.writeLine("  uuf.value = '';");
		out.writeLine("}");
		
		//	search bibliographic metadata
		out.writeLine("function searchRefs() {");
		out.writeLine("  clearSearchResult();");
		out.writeLine("  var ref = bibRefEditor_getRef();");
		out.writeLine("  var refQuery = '';");
		out.writeLine("  for (var attr in ref)");
		out.writeLine("    refQuery = (refQuery + '&' + attr + '=' + encodeURIComponent(ref[attr]));");
		out.writeLine("  if (refQuery == '') {");
		out.writeLine("    alert('Please enter some bibliographic attributes to search for.');");
		out.writeLine("    return;");
		out.writeLine("  }");
		out.writeLine("  var refJs = getById('refSearchScript');");
		out.writeLine("  var refJsp = refJs.parentNode;");
		out.writeLine("  removeElement(refJs);");
		out.writeLine("  refJs = newElement('script', 'refSearchScript', null, null);");
		out.writeLine("  refJs.src = '" + out.request.getContextPath() + out.request.getServletPath() + "/searchRefs.js?time=' + (new Date()).getTime() + refQuery;");
		out.writeLine("  refJsp.appendChild(refJs);");
		out.writeLine("}");
		out.writeLine("function clearSearchResult() {");
		out.writeLine("  var srd = $('refSearchResult');");
		out.writeLine("  while (srd.firstChild)");
		out.writeLine("    srd.removeChild(srd.firstChild);");
		out.writeLine("  srd.style.display = 'none';");
		out.writeLine("}");
		out.writeLine("function displaySearchResult(refs) {");
		out.writeLine("  clearSearchResult();");
		out.writeLine("  var srd = $('refSearchResult');");
		out.writeLine("  if (refs.length == 0)");
		out.writeLine("    srd.appendChild(newElement('p', null, 'refSearchResultElement', 'Your search did not return any references.'));");
		out.writeLine("  else for (var r = 0; r < refs.length; r++)");
		out.writeLine("    displaySearchResultRef(srd, refs[r]);");
		out.writeLine("  srd.style.display = null;");
		out.writeLine("}");
		out.writeLine("function displaySearchResultRef(srd, ref) {");
		out.writeLine("  var rd = newElement('p', null, 'refSearchResultElement', ref.refString);");
		out.writeLine("  rd.onclick = function() {");
		out.writeLine("    selectSearchResult(ref);");
		out.writeLine("  };");
		out.writeLine("  srd.appendChild(rd);");
		out.writeLine("}");
		out.writeLine("function selectSearchResult(ref) {");
		out.writeLine("  if (!ref['ID-" + this.refIdTypes[0] + "'])");
		out.writeLine("    for (var attr in ref) {");
		out.writeLine("      if (attr.indexOf('ID-') != 0)");
		out.writeLine("        continue;");
		out.writeLine("      if ('" + this.refIdTypesString + "'.indexOf(attr.substr(3)) != -1)");
		out.writeLine("        continue;");
		out.writeLine("      ref['ID-" + this.refIdTypes[0] + "'] = ref[attr];");
		out.writeLine("      break;");
		out.writeLine("    }");
		out.writeLine("  if (!ref['ID-" + this.refIdTypes[0] + "'])");
		out.writeLine("    for (var attr in ref) {");
		out.writeLine("      if (attr.indexOf('ID-') != 0)");
		out.writeLine("        continue;");
		out.writeLine("      ref['ID-" + this.refIdTypes[0] + "'] = ref[attr];");
		out.writeLine("      break;");
		out.writeLine("    }");
		out.writeLine("  bibRefEditor_setRef(ref);");
		out.writeLine("  if (ref['" + PUBLICATION_URL_ANNOTATION_TYPE + "']) {");
		out.writeLine("    var uuf = $('uploadUrl_field');");
		out.writeLine("    var uff = $('uploadFile_field');");
		out.writeLine("    if ((uuf != null) && ((uff == null) || (uff.value == null) || (uff.value == '')))");
		out.writeLine("      uuf.value = ref['" + PUBLICATION_URL_ANNOTATION_TYPE + "'];");
		out.writeLine("  }");
		out.writeLine("  if (ref['docFormat']) {");
		out.writeLine("    var udff = $('uploadDocFormat_field');");
		out.writeLine("    if (udff != null)");
		out.writeLine("      udff.value = ref['docFormat'];");
		out.writeLine("  }");
		out.writeLine("  clearSearchResult();");
		out.writeLine("  return false;");
		out.writeLine("}");
		
		//	write JavaScripts for validating document meta data
		out.writeLine("function validateRefData() {");
		out.writeLine("  var refErrors = bibRefEditor_getRefErrors();");
		out.writeLine("  if (refErrors != null) {");
		out.writeLine("    var em = 'The meta data has the following errors:';");
		out.writeLine("    for (var e = 0;; e++) {");
		out.writeLine("      var refError = refErrors[e];");
		out.writeLine("      if (refError == null)");
		out.writeLine("        break;");
		out.writeLine("      em += '\\n - ';");
		out.writeLine("      em += refError;");
		out.writeLine("    }");
		out.writeLine("    alert(em);");
		out.writeLine("  }");
		out.writeLine("  else alert('The metadata is good for import.');");
		out.writeLine("}");
		
		//	write JavaScripts used by document upload requests
		out.writeLine("function prepareUpload() {");
		out.writeLine("  if (checkUploadData()) {");
		out.writeLine("    window.open('Upload Result', 'uploadResult', 'width=200,height=200,scrollbar=yes;scrollbars=yes');");
		out.writeLine("    return true;");
		out.writeLine("  }");
		out.writeLine("  else return false;");
		out.writeLine("}");// TODO use popin.js
		out.writeLine("function checkUploadData() {");
		out.writeLine("  var refErrors = bibRefEditor_getRefErrors();");
		out.writeLine("  if (refErrors != null) {");
		out.writeLine("    var em = 'The import cannot be processed due to incomplete metadata:';");
		out.writeLine("    for (var e = 0;; e++) {");
		out.writeLine("      var refError = refErrors[e];");
		out.writeLine("      if (refError == null)");
		out.writeLine("        break;");
		out.writeLine("      em += '\\n - ';");
		out.writeLine("      em += refError;");
		out.writeLine("    }");
		out.writeLine("    alert(em);");
		out.writeLine("    return false;");
		out.writeLine("  }");
		out.writeLine("  var uf = $('uploadForm');");
		out.writeLine("  if (uf == null)");
		out.writeLine("    return false;");
		out.writeLine("  var uuf = $('uploadUrl_field');");
		out.writeLine("  var uploadUrl = ((uuf == null) ? '' : uuf.value);");
		out.writeLine("  if (uploadUrl != '') {");
		out.writeLine("    uf.enctype = 'application/x-www-form-urlencoded';");
		out.writeLine("    return true;");
		out.writeLine("  }");
		out.writeLine("  var uff = $('uploadFile_field');");
		out.writeLine("  var uploadFile = ((uff == null) ? '' : uff.value);");
		out.writeLine("  if (uploadFile != '') {");
		out.writeLine("    uf.enctype = 'multipart/form-data';");
		out.writeLine("    return true;");
		out.writeLine("  }");
		out.writeLine("  alert('Please specify a URL to retrieve the document from, or select a file from you computer to upload.');");
		out.writeLine("  return false;");
		out.writeLine("}");
		
		out.writeLine("</script>");
	}
}