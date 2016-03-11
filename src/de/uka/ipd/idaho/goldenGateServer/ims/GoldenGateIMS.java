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
package de.uka.ipd.idaho.goldenGateServer.ims;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.AttributeUtils;
import de.uka.ipd.idaho.gamta.Attributed;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerEventService;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent.ImsDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.ims.data.ImsDocumentList;
import de.uka.ipd.idaho.goldenGateServer.ims.data.ImsDocumentListElement;
import de.uka.ipd.idaho.goldenGateServer.uaa.UserAccessAuthority;
import de.uka.ipd.idaho.goldenGateServer.util.BufferedLineInputStream;
import de.uka.ipd.idaho.goldenGateServer.util.BufferedLineOutputStream;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.util.ImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.FolderImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.ImDocumentEntry;
import de.uka.ipd.idaho.im.util.ImDocumentIO;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * GoldenGATE Image Markup Store provides persistent storage, write locking,
 * and IO for Image Markup documents, and generates events to notify other
 * components of updates.
 * 
 * @author sautter
 */
public class GoldenGateIMS extends AbstractGoldenGateServerComponent implements GoldenGateImsConstants {
	
	private File documentStorageRoot;
	
	private IoProvider io;
	
	private UserAccessAuthority uaa;
	
	private static final String DOCUMENT_TABLE_NAME = "GgImsDocuments";
	private static final String DOCUMENT_ATTRIBUTE_TABLE_NAME = "GgImsDocumentAttributes";
	
	private static final String DOCUMENT_ID_HASH_ATTRIBUTE = "docIdHash";
	
	private static final int DOCUMENT_NAME_COLUMN_LENGTH = 127;
	
	private TreeMap documentAttributesByName = new TreeMap(String.CASE_INSENSITIVE_ORDER);
	private static class DocumentAttribute {
		String colName;
		int colLength;
		boolean isInteger;
//		String attribName;
		String[] attribNames;
//		DocumentAttribute(String colName, int colLength, String attribName) {
		DocumentAttribute(String colName, int colLength, String[] attribNames) {
			this.colName = colName;
			this.colLength = colLength;
			this.isInteger = (this.colLength < 1);
//			this.attribName = attribName;
			this.attribNames = attribNames;
		}
		private String getValueFor(Attributed doc) {
			for (int a = 0; a < this.attribNames.length; a++) {
				String value = ((String) doc.getAttribute(this.attribNames[a]));
				if (value != null)
					return value;
			}
			return null;
		}
		String getInsertQueryValue(Attributed doc) {
//			String value = ((String) doc.getAttribute(this.attribName));
			String value = this.getValueFor(doc);
			if (value == null)
				return (this.isInteger ? "0" : "");
			if (this.isInteger) {
				value = value.replaceAll("[^0-9]", "");
				return ((value.length() == 0) ? "0" : value);
			}
			else return EasyIO.sqlEscape(value);
		}
		
		String getUpdateQueryAssignment(Attributed doc) {
//			return this.getUpdateQueryAssignment((String) doc.getAttribute(this.attribName));
			return this.getUpdateQueryAssignment(this.getValueFor(doc));
		}
		String getUpdateQueryAssignment(String value) {
			if (value == null)
				return null;
			if (this.isInteger)
				value = value.replaceAll("[^0-9]", "");
			if (value.length() == 0)
				return null;
			if (this.isInteger)
				return (this.colName + " = " + value);
			if (value.length() > this.colLength)
				value = value.substring(0, this.colLength);
			return (this.colName + " = '" + EasyIO.sqlEscape(value) + "'");
		}
		
		static DocumentAttribute parseDocumentAttribute(String daData) {
			String[] dad = daData.split("\\t", 3);
			try {
//				return new DocumentAttribute(dad[0], Integer.parseInt(dad[1]), dad[2]);
				return new DocumentAttribute(dad[0], Integer.parseInt(dad[1]), dad[2].split("\\s+"));
			}
			catch (RuntimeException re /* number format as well as array length */ ) {
				return null;
			}
		}
	}
	
	private int documentListSizeThreshold = 0;
	
	private Set docIdSet = new HashSet();
	private Map docAttributeValueCache = new HashMap();
	private void cacheDocumentAttributeValue(String fieldName, String fieldValue) {
		if ((fieldValue == null) || ImsDocumentList.summarylessAttributes.contains(fieldValue))
			return;
		this.getListFieldSummary(fieldName, true).add(fieldValue);
	}
	private void cacheDocumentAttributeValues(Attributed values) {
		if (values == null)
			return;
		for (int f = 0; f < documentDataFields.length; f++)
			this.cacheDocumentAttributeValue(documentDataFields[f], ((String) values.getAttribute(documentDataFields[f])));
		for (int f = 0; f < documentDataFieldsAdmin.length; f++)
			this.cacheDocumentAttributeValue(documentDataFieldsAdmin[f], ((String) values.getAttribute(documentDataFields[f])));
	}
	private void uncacheDocumentAttributeValue(String fieldName, String fieldValue) {
		if ((fieldValue == null) || ImsDocumentList.summarylessAttributes.contains(fieldValue))
			return;
		ImsDocumentList.AttributeSummary as = this.getListFieldSummary(fieldName, false);
		if (as != null)
			as.remove(fieldValue);
	}
	private void uncacheDocumentAttributeValues(Attributed values) {
		if (values == null)
			return;
		for (int f = 0; f < documentDataFields.length; f++)
			this.uncacheDocumentAttributeValue(documentDataFields[f], ((String) values.getAttribute(documentDataFields[f])));
		for (int f = 0; f < documentDataFieldsAdmin.length; f++)
			this.uncacheDocumentAttributeValue(documentDataFieldsAdmin[f], ((String) values.getAttribute(documentDataFields[f])));
	}
	private ImsDocumentList.AttributeSummary getListFieldSummary(String fieldName, boolean create) {
		ImsDocumentList.AttributeSummary as = ((ImsDocumentList.AttributeSummary) this.docAttributeValueCache.get(fieldName));
		if ((as == null) && create) {
			as = new ImsDocumentList.AttributeSummary();
			this.docAttributeValueCache.put(fieldName, as);
		}
		return as;
	}
	
	/** Constructor passing 'IMS' as the letter code to super constructor
	 */
	public GoldenGateIMS() {
		super("IMS");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
	 */
	protected void initComponent() {
		
		//	get document storage root folder
		String docFolderName = this.configuration.getSetting("documentFolderName", "Documents");
		while (docFolderName.startsWith("./"))
			docFolderName = docFolderName.substring("./".length());
		this.documentStorageRoot = (((docFolderName.indexOf(":\\") == -1) && (docFolderName.indexOf(":/") == -1) && !docFolderName.startsWith("/")) ? new File(this.dataPath, docFolderName) : new File(docFolderName));
		this.documentStorageRoot.mkdirs();
		
		//	connect to database
		this.io = this.host.getIoProvider();
		if (this.io == null)
			throw new RuntimeException("GoldenGateIMS: Cannot work without database access");
		
		//	load attribute table columns from configuration
		try {
			StringVector daColumnDefs = StringVector.loadList(new File(this.dataPath, "documentAttributes.cnfg"));
			for (int c = 0; c < daColumnDefs.size(); c++) {
				String dacd = daColumnDefs.get(c).trim();
				if ((dacd.length() == 0) || dacd.startsWith("//"))
					continue;
				DocumentAttribute da = DocumentAttribute.parseDocumentAttribute(dacd);
				if (da != null)
					this.documentAttributesByName.put(da.colName, da);
			}
		}
		catch (IOException ioe) {
			throw new RuntimeException("GoldenGateIMS: Document attribute definitions not found");
		}
		
		//	create/update document table
		TableDefinition dtd = new TableDefinition(DOCUMENT_TABLE_NAME);
		dtd.addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		dtd.addColumn(DOCUMENT_ID_HASH_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		dtd.addColumn(DOCUMENT_NAME_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, DOCUMENT_NAME_COLUMN_LENGTH);
		dtd.addColumn(CHECKIN_USER_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, UserAccessAuthority.USER_NAME_MAX_LENGTH);
		dtd.addColumn(CHECKIN_TIME_ATTRIBUTE, TableDefinition.BIGINT_DATATYPE, 0);
		dtd.addColumn(CHECKOUT_USER_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, UserAccessAuthority.USER_NAME_MAX_LENGTH);
		dtd.addColumn(CHECKOUT_TIME_ATTRIBUTE, TableDefinition.BIGINT_DATATYPE, 0);
		dtd.addColumn(UPDATE_USER_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, UserAccessAuthority.USER_NAME_MAX_LENGTH);
		dtd.addColumn(UPDATE_TIME_ATTRIBUTE, TableDefinition.BIGINT_DATATYPE, 0);
		dtd.addColumn(DOCUMENT_VERSION_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		if (!this.io.ensureTable(dtd, true))
			throw new RuntimeException("GoldenGateIMS: Cannot work without database access.");
		
		//	index main table
		this.io.indexColumn(DOCUMENT_TABLE_NAME, DOCUMENT_ID_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, DOCUMENT_ID_HASH_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, CHECKIN_USER_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, UPDATE_USER_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, CHECKOUT_USER_ATTRIBUTE);
		
		//	create document attribute table
		TableDefinition datd = new TableDefinition(DOCUMENT_ATTRIBUTE_TABLE_NAME);
		datd.addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		datd.addColumn(DOCUMENT_ID_HASH_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		for (Iterator dacnit = this.documentAttributesByName.keySet().iterator(); dacnit.hasNext();) {
			DocumentAttribute da = ((DocumentAttribute) this.documentAttributesByName.get(dacnit.next()));
			datd.addColumn(da.colName, (da.isInteger ? TableDefinition.INT_DATATYPE : TableDefinition.VARCHAR_DATATYPE), da.colLength);
		}
		if (!this.io.ensureTable(datd, true))
			throw new RuntimeException("GoldenGateIMS: Cannot work without database access.");
		
		//	index attribute table
		this.io.indexColumn(DOCUMENT_ATTRIBUTE_TABLE_NAME, DOCUMENT_ID_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_ATTRIBUTE_TABLE_NAME, DOCUMENT_ID_HASH_ATTRIBUTE);
		
		//	get maximum document list size for non-admin users
		try {
			this.documentListSizeThreshold = Integer.parseInt(this.configuration.getSetting("documentListSizeThreshold", "0"));
		} catch (NumberFormatException nfe) {}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
	 */
	public void link() {
		
		//	link up to user access authority
		this.uaa = ((UserAccessAuthority) this.host.getServerComponent(UserAccessAuthority.class.getName()));
		
		//	check success
		if (this.uaa == null) throw new RuntimeException(UserAccessAuthority.class.getName());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
	 */
	public void linkInit() {
		
		//	prefill caches
		ImsDocumentList dl = this.getDocumentList(UserAccessAuthority.SUPERUSER_NAME, false, null);
		while (dl.hasNextDocument()) {
			ImsDocumentListElement dle = dl.getNextDocument();
			this.docIdSet.add(dle.getAttribute(DOCUMENT_ID_ATTRIBUTE));
			for (int f = 0; f < dl.listFieldNames.length; f++) {
				if (ImsDocumentList.summarylessAttributes.contains(dl.listFieldNames[f]))
					continue;
				this.cacheDocumentAttributeValue(dl.listFieldNames[f], ((String) dle.getAttribute(dl.listFieldNames[f])));
			}
		}
		
		//	register permissions
		this.uaa.registerPermission(UPLOAD_DOCUMENT_PERMISSION);
		this.uaa.registerPermission(UPDATE_DOCUMENT_PERMISSION);
		this.uaa.registerPermission(DELETE_DOCUMENT_PERMISSION);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		
		//	shut down database connection
		this.io.close();
	}
	
	/*
	 * (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList();
		ComponentAction ca;

		// list documents
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_DOCUMENT_LIST;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					writeLogEntry("Request for invalid session - " + sessionId);
					return;
				}
				
				//	read filter string
				String filterString = input.readLine();
				Properties filter;
				if (filterString.length() == 0)
					filter = null;
				else {
					String[] filters = filterString.split("\\&");
					filter = new Properties();
					for (int f = 0; f < filters.length; f++) {
						String[] pair = filters[f].split("\\=");
						if (pair.length == 2) {
							String name = pair[0].trim();
							String value = URLDecoder.decode(pair[1].trim(), ENCODING).trim();
							
							String existingValue = filter.getProperty(name);
							if (existingValue != null)
								value = existingValue + "\n" + value;
							
							filter.setProperty(name, value);
						}
					}
				}
				
				ImsDocumentList docList = getDocumentList((uaa.getUserNameForSession(sessionId)), false, filter);
				
				output.write(GET_DOCUMENT_LIST);
				output.newLine();

				docList.writeData(output);
				output.newLine();
			}
		};
		cal.add(ca);
		
		// deliver document update protocol
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_UPDATE_PROTOCOL;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				String docId = input.readLine();
				DocumentUpdateProtocol up = ((DocumentUpdateProtocol) updateProtocolsByDocId.get(docId));
				if (up == null) {
					output.write("No recent update for document '" + docId + "'");
					output.newLine();
				}
				else {
					output.write(GET_UPDATE_PROTOCOL);
					output.newLine();
					for (int e = 0; e < up.size(); e++) {
						output.write((String) up.get(e));
						output.newLine();
					}
				}
			}
		};
		cal.add(ca);

		// deliver document through network
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_DOCUMENT;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					writeLogEntry("Request for invalid session - " + sessionId);
					return;
				}
				
				//	get document ID and version
				String docId = input.readLine();
				int version = Integer.parseInt(input.readLine());
				
				//	get document data
				ImsDocumentData docData = getDocumentData(docId, false, false);
				if (docData == null) {
					output.write("Invalid document ID (" + docId + ")");
					output.newLine();
					return;
				}
				
				//	switch to requested version
				docData = docData.cloneForVersion(version);
				if (docData == null) {
					output.write("Invalid document version (" + version + " for " + docId + ")");
					output.newLine();
					return;
				}
				
				//	get and send entry list
				try {
					ImDocumentEntry[] docEntries = docData.getEntries();
					output.write(GET_DOCUMENT);
					output.newLine();
					for (int e = 0; e < docEntries.length; e++) {
						output.write(docEntries[e].toTabString());
						output.newLine();
					}
					output.newLine();
					output.flush();
				}
				catch (IOException ioe) {
					output.write(ioe.getMessage());
					output.newLine();
				}
			}
		};
		cal.add(ca);
		
		// check out a document
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return CHECKOUT_DOCUMENT;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {

				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					writeLogEntry("Request for invalid session - " + sessionId);
					return;
				}
				
				//	check permission
				if (!uaa.hasSessionPermission(sessionId, UPDATE_DOCUMENT_PERMISSION, true)) {
					output.write("Insufficient permissions for checking out a document");
					output.newLine();
					return;
				}

				// get user
				String userName = uaa.getUserNameForSession(sessionId);
				
				//	get document ID and version
				String docId = input.readLine();
				int version = Integer.parseInt(input.readLine());
				
				//	get entry list, obtain lock, and send entry list
				try {
					
					//	checkout document
					ImsDocumentData docData = checkoutDocumentAsData(userName, docId, version);
					
					//	send back document entry list
					ImDocumentEntry[] docEntries = docData.getEntries();
					output.write(CHECKOUT_DOCUMENT);
					output.newLine();
					for (int e = 0; e < docEntries.length; e++) {
						output.write(docEntries[e].toTabString());
						output.newLine();
					}
					output.newLine();
					output.flush();
				}
				
				//	catch concurrent checkout exception separately
				catch (DocumentCheckedOutException dcoe) {
					output.write(dcoe.getMessage());
					output.newLine();
					return;
				}
				
				// release document if sending it fails for any reason
				catch (IOException ioe) {
					releaseDocument(userName, docId);
					throw ioe;
				}
			}
		};
		cal.add(ca);
		
		// deliver document entries through network
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_DOCUMENT_ENTRIES;
			}
			public void performActionNetwork(BufferedLineInputStream input, BufferedLineOutputStream output) throws IOException {
				
				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					writeLogEntry("Request for invalid session - " + sessionId);
					return;
				}
				
				//	get document ID and entry list
				String docId = input.readLine();
				ArrayList docImfEntries = new ArrayList();
				for (String entryString; (entryString = input.readLine()) != null;) {
					if (entryString.length() == 0)
						break;
					String[] entryStringParts = entryString.split("\\t+");
					if (entryStringParts.length == 3)
						docImfEntries.add(new ImDocumentEntry(entryStringParts[0], Long.parseLong(entryStringParts[1]), entryStringParts[2]));
				}
				
				//	get document data
				ImsDocumentData docData = getDocumentData(docId, false, true);
				if (docData == null) {
					output.writeLine("Invalid document ID (" + docId + ")");
					return;
				}
				
				//	announce entries coming
				output.writeLine(GET_DOCUMENT_ENTRIES);
				
				//	send requested entries
				ZipOutputStream zout = new ZipOutputStream(output);
				byte[] buffer = new byte[1024];
				for (int e = 0; e < docImfEntries.size(); e++) {
					ImDocumentEntry entry = ((ImDocumentEntry) docImfEntries.get(e));
					ZipEntry ze = new ZipEntry(entry.getFileName());
					ze.setTime(entry.updateTime);
					zout.putNextEntry(ze);
					InputStream entryIn = docData.getInputStream(entry);
					for (int r; (r = entryIn.read(buffer, 0, buffer.length)) != -1;)
						zout.write(buffer, 0, r);
					entryIn.close();
					zout.closeEntry();
				}
				zout.flush();
			}
		};
		cal.add(ca);

		// update a document, or store a new one
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return UPLOAD_DOCUMENT;
			}
			public void performActionNetwork(final BufferedReader input, BufferedWriter output) throws IOException {

				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					writeLogEntry("Request for invalid session - " + sessionId);
					return;
				}
				
				//	check permission
				if (!uaa.hasSessionPermission(sessionId, UPLOAD_DOCUMENT_PERMISSION, true)) {
					output.write("Insufficient permissions for uploading a document");
					output.newLine();
					return;
				}
				
				//	get session user name
				String sessionUserName = uaa.getUserNameForSession(sessionId);
				
				//	get document ID and user to credit
				String docId = input.readLine();
				String userName = input.readLine();
				if (userName.length() == 0)
					userName = sessionUserName;
				
				//	get document data, and send error if it already exists
				ImsDocumentData docData = getDocumentData(docId, false, false);
				if (docData != null) {
					output.write("Document " + docId + " already exists, use update instead");
					output.newLine();
					return;
				}
				
				//	create document data
				docData = getDocumentData(docId, true, true);
				
				//	get document entry list
				ArrayList docEntryList = new ArrayList();
				HashSet docImDocumentEntryFileNames = new HashSet();
				for (String entryString; (entryString = input.readLine()) != null;) {
					if (entryString.length() == 0)
						break;
					ImDocumentEntry entry = ImDocumentEntry.fromTabString(entryString);
					if (entry != null) {
						docEntryList.add(entry);
						docImDocumentEntryFileNames.add(entry.getFileName());
					}
				}
				
				//	generate update key and cache data
				String updateKey = Gamta.getAnnotationID();
				DocumentUpdate update = new DocumentUpdate(docData, docImDocumentEntryFileNames, null, userName);
				updatesByKey.put(updateKey, update);
				
				//	send back entire entry list
				output.write(UPLOAD_DOCUMENT);
				output.newLine();
				output.write(updateKey);
				output.newLine();
				for (int e = 0; e < docEntryList.size(); e++) {
					output.write(((ImDocumentEntry) docEntryList.get(e)).toTabString());
					output.newLine();
				}
				output.newLine();
				output.flush();
			}
		};
		cal.add(ca);
		
		// update a document, or store a new one
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return UPDATE_DOCUMENT;
			}
			public void performActionNetwork(final BufferedReader input, BufferedWriter output) throws IOException {
				
				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					writeLogEntry("Request for invalid session - " + sessionId);
					return;
				}
				
				//	check permission
				if (!uaa.hasSessionPermission(sessionId, UPDATE_DOCUMENT_PERMISSION, true)) {
					output.write("Insufficient permissions for updating a document");
					output.newLine();
					return;
				}
				
				//	get session user name
				String sessionUserName = uaa.getUserNameForSession(sessionId);
				
				//	get document ID and user to credit
				String docId = input.readLine();
				String userName = input.readLine();
				if (userName.length() == 0)
					userName = sessionUserName;
				
				//	check document checkout state
				if (!mayUpdateDocument(sessionUserName, docId)) {
					output.write("Document checked out by other user, update not possible.");
					output.newLine();
					return;
				}
				
				//	get entry list
				ArrayList docEntryList = new ArrayList();
				for (String entryString; (entryString = input.readLine()) != null;) {
					if (entryString.length() == 0)
						break;
					ImDocumentEntry entry = ImDocumentEntry.fromTabString(entryString);
					if (entry != null)
						docEntryList.add(entry);
				}
				
				//	get document data
				ImsDocumentData docData = getDocumentData(docId, true, true);
				
				//	diff with received entry list
				ArrayList toUpdateDocEntries = new ArrayList();
				HashSet toUploadImDocumentEntryFileNames = new HashSet();
				for (int e = 0; e < docEntryList.size(); e++) {
					ImDocumentEntry entry = ((ImDocumentEntry) docEntryList.get(e));
					if (docData.hasEntryData(entry))
						docData.putEntry(entry);
					else {
						toUpdateDocEntries.add(entry);
						toUploadImDocumentEntryFileNames.add(entry.getFileName());
					}
				}
				
				//	we already have all the entries
				if (toUpdateDocEntries.isEmpty()) {
					
					//	finalize update right away
					DocumentUpdateProtocol dup = new DocumentUpdateProtocol(docId, false);
					finalizeDocumentUpdate(new DocumentUpdate(docData, toUploadImDocumentEntryFileNames, sessionUserName, userName), dup);
					
					//	send back empty update key to indicate protocol is coming right away
					output.write(UPDATE_DOCUMENT);
					output.newLine();
					output.write("");
					output.newLine();
					output.write("Document '" + dup.docName + "' stored as version " + dup.docVersion);
					output.newLine();
					output.flush();
					
					//	we're done here
					return;
				}
				
				//	generate update ID
				String updateKey = Gamta.getAnnotationID();
				DocumentUpdate update = new DocumentUpdate(docData, toUploadImDocumentEntryFileNames, sessionUserName, userName);
				updatesByKey.put(updateKey, update);
				
				//	send back list of to-update entries
				output.write(UPDATE_DOCUMENT);
				output.newLine();
				output.write(updateKey);
				output.newLine();
				for (int e = 0; e < toUpdateDocEntries.size(); e++) {
					output.write(((ImDocumentEntry) toUpdateDocEntries.get(e)).toTabString());
					output.newLine();
				}
				output.flush();
			}
		};
		cal.add(ca);
		
		// deliver document entries through network
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return UPDATE_DOCUMENT_ENTRIES;
			}
			public void performActionNetwork(BufferedLineInputStream input, BufferedLineOutputStream output) throws IOException {
				
				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					writeLogEntry("Request for invalid session - " + sessionId);
					return;
				}
				
				//	get update key and update data
				String updateKey = input.readLine();
				DocumentUpdate du = ((DocumentUpdate) updatesByKey.remove(updateKey));
				if (du == null) {
					output.write("Invalid update key (" + updateKey + ")");
					output.newLine();
					return;
				}
				
				//	receive requested entries
				ZipInputStream zin = new ZipInputStream(input);
				byte[] buffer = new byte[1024];
				for (ZipEntry ze; (ze = zin.getNextEntry()) != null;) {
					if (updateKey.equals(ze.getName()))
						break;
					ImDocumentEntry entry = new ImDocumentEntry(ze);
					OutputStream cacheOut = du.docData.getOutputStream(entry, true);
					for (int r; (r = zin.read(buffer, 0, buffer.length)) != -1;)
						cacheOut.write(buffer, 0, r);
					cacheOut.flush();
					cacheOut.close();
					du.missingDocEntryFileNames.remove(entry.getFileName());
				}
				
				//	finalize update
				DocumentUpdateProtocol dup = new DocumentUpdateProtocol(du.docData.docId, false);
				finalizeDocumentUpdate(du, dup);
				
				//	send out update log
				output.write(UPDATE_DOCUMENT_ENTRIES);
				output.newLine();
				output.write("Document '" + dup.docName + "' stored as version " + dup.docVersion);
				output.newLine();
				output.flush();
			}
		};
		cal.add(ca);
		
		// delete a document
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return DELETE_DOCUMENT;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {

				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					writeLogEntry("Request for invalid session - " + sessionId);
					return;
				}
				
				//	check permission
				if (!uaa.hasSessionPermission(sessionId, DELETE_DOCUMENT_PERMISSION, true)) {
					output.write("Insufficient permissions for deleting a document");
					output.newLine();
					return;
				}
				
				//	get document ID
				String docId = input.readLine();
				
				//	perform deletion
				try {
					DocumentUpdateProtocol up = new DocumentUpdateProtocol(docId, true);
					deleteDocument(uaa.getUserNameForSession(sessionId), docId, up);
					
					output.write(DELETE_DOCUMENT);
					output.newLine();
					for (int e = 0; e < up.size(); e++) {
						output.write((String) up.get(e));
						output.newLine();
					}
				}
				catch (IOException ioe) {
					output.write(ioe.getMessage());
					output.newLine();
				}
			}
		};
		cal.add(ca);

		// release checked out document
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return RELEASE_DOCUMENT;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					writeLogEntry("Request for invalid session - " + sessionId);
					return;
				}
				
				// get user
				String userName = uaa.getUserNameForSession(sessionId);
				
				// get document ID
				String docId = input.readLine();
				
				// release document
				releaseDocument(userName, docId);
				
				// send response
				output.write(RELEASE_DOCUMENT);
				output.newLine();
			}
		};
		cal.add(ca);
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	/**
	 * IMS specific extension of a document data object, with added management
	 * facilities for provenance related attributes. These attributes are not
	 * stored in the document proper, as this would change the data hash of the
	 * respective entry, but rather in a dedicated attribute list provided by
	 * this class.
	 * 
	 * @author sautter
	 */
	public static class ImsDocumentData extends FolderImDocumentData implements Attributed {
		private String docId;
		private File entryDataFolder;
		
		ImsDocumentData(String docId, File entryDataFolder) throws IOException {
			super(entryDataFolder);
			this.docId = docId;
			this.entryDataFolder = entryDataFolder;
		}
		ImsDocumentData(String docId, File entryDataFolder, int version) throws IOException {
			super(entryDataFolder);
			this.docId = docId;
			this.entryDataFolder = entryDataFolder;
			
			if (version != -1) {
				
				//	check document entry file
				File docEntryListFile = new File(this.entryDataFolder, ("entries" + ((version == 0) ? "" : ("." + version)) + ".txt"));
				
				//	read entry list
				if (docEntryListFile.exists()) {
					BufferedReader entryIn = new BufferedReader(new InputStreamReader(new FileInputStream(docEntryListFile), ENCODING));
					
					//	read provenance attributes from first line
					String attributeString = entryIn.readLine();
					ImDocumentIO.setAttributes(this, attributeString);
					
					//	read document entry list
					for (String entryString; (entryString = entryIn.readLine()) != null;) {
						ImDocumentEntry entry = ImDocumentEntry.fromTabString(entryString);
						if (entry != null)
							this.putEntry(entry);
					}
					entryIn.close();
				}
				
				//	report invalid version number only for explicit version (if no current version exists, something might have gone wrong on document creation)
				else if (version != 0)
					throw new IOException("Invalid version number " + version + " for document ID '" + this.docId + "'");
			}
		}
		
		private TreeMap attributes = new TreeMap();
		public void setAttribute(String name) {
			this.setAttribute(name, "true");
		}
		public Object setAttribute(String name, Object value) {
			if (value == null)
				return this.attributes.remove(name);
			else return this.attributes.put(name, value);
		}
		public void copyAttributes(Attributed source) { /* we're not doing this global overwrite */ }
		public Object getAttribute(String name) {
			return this.getAttribute(name, null);
		}
		public Object getAttribute(String name, Object def) {
			Object value = this.attributes.get(name);
			return ((value == null) ? def : value);
		}
		public boolean hasAttribute(String name) {
			return this.attributes.containsKey(name);
		}
		public String[] getAttributeNames() {
			return ((String[]) this.attributes.keySet().toArray(new String[this.attributes.size()]));
		}
		public Object removeAttribute(String name) {
			return this.attributes.remove(name);
		}
		public void clearAttributes() { /* we're not clearing anything */ }
		
		/**
		 * This standard entry list storage facility is disabled, so IMS can
		 * take care of additional concerns (document locking, versioning, and
		 * provenance related issues) internally.
		 */
		public void storeEntryList() throws IOException {}
		
		int storeEntryList(long time) throws IOException {
			
			//	get current version
			int version = this.getCurrentVersion();
			
			//	open entry file
			File docEntryListFileUpdating = new File(this.entryDataFolder, "entries.updating.txt");
			BufferedWriter entryOut = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(docEntryListFileUpdating), ENCODING));
			
			//	store provenance attributes as first line of entry list
			entryOut.write(ImDocumentIO.getAttributesString(this));
			entryOut.newLine();
			
			//	write entry list
			ImDocumentEntry[] entries = this.getEntries();
			Arrays.sort(entries);
			for (int e = 0; e < entries.length; e++) {
				entryOut.write(entries[e].toTabString());
				entryOut.newLine();
			}
			entryOut.flush();
			entryOut.close();
			
			//	switch updating entry list live
			File docEntryListFile = new File(this.entryDataFolder, "entries.txt");
			if (docEntryListFile.exists())
				docEntryListFile.renameTo(new File(this.entryDataFolder, ("entries." + version + ".txt")));
			docEntryListFileUpdating.setLastModified(time);
			docEntryListFileUpdating.renameTo(new File(this.entryDataFolder, "entries.txt"));
			
			//	extrapolate to next version
			return (version + 1);
		}
		
		private int getCurrentVersion() {
			
			//	check if folder even exists
			if (!this.entryDataFolder.exists())
				return 0;
			
			//	get document entry lists
			File[] docEntryListFiles = this.entryDataFolder.listFiles(new FileFilter() {
				public boolean accept(File file) {
					return ((file != null) && file.isFile() && file.getName().startsWith("entries.") && file.getName().endsWith(".txt"));
				}
			});
			
			//	no entry lists at all, current version is 0
			if ((docEntryListFiles == null) || (docEntryListFiles.length == 0))
				return 0;
			
			//	only updating entry list, current version is still 0
			if ((docEntryListFiles.length == 1) && "entries.updating.txt".equals(docEntryListFiles[0].getName()))
				return 0;
			
			//	find most recent version number
			int version = 0;
			for (int f = 0; f < docEntryListFiles.length; f++) {
				String docFileName = docEntryListFiles[f].getName();
				
				//	cut file name and dot
				docFileName = docFileName.substring("entries.".length());
				
				//	make sure there's left more than the 'txt' file extension, which will be the case for the most recent version
				if (docFileName.length() <= 3)
					continue;
				
				//	this one's not live just yet
				if (docFileName.startsWith("updating."))
					continue;
				
				//	cut file extension and get version number
				try {
					version = Math.max(version, Integer.parseInt(docFileName.substring(0, (docFileName.length() - ".txt".length()))));
				} catch (NumberFormatException nfe) {}
			}
			
			//	extrapolate to most recent version
			return (version + 1);
		}
		
		ImsDocumentData cloneForVersion(final int version) throws IOException {
			
			//	compute version if argument relative to current one
			int absoluteVersion = version;
			if (absoluteVersion < 0)
				absoluteVersion = (this.getCurrentVersion() - version);
			
			//	little we can do about this one
			if (absoluteVersion < 0)
				throw new IOException("Invalid version number " + version + " for document ID '" + this.docId + "'");
			
			//	clone for computed version
			return new ImsDocumentData(this.docId, this.entryDataFolder, absoluteVersion);
		}
		
		private boolean readOnly = false;
		void setReadOnly(boolean readOnly) {
			this.readOnly = readOnly;
		}
		
		public boolean canStoreDocument() {
			return (super.canStoreDocument() && !this.readOnly);
		}
		
		public ImDocumentEntry putEntry(ImDocumentEntry entry) {
			if (this.readOnly)
				throw new RuntimeException("Cannot add entry '" + entry.name + "' in read-only mode !!!");
			return super.putEntry(entry);
		}
		
		public OutputStream getOutputStream(String entryName, boolean writeDirectly) throws IOException {
			if (this.readOnly)
				throw new IOException("Cannot write '" + entryName + "' in read-only mode !!!");
			return super.getOutputStream(entryName, writeDirectly);
		}
	}
	
	private Map updatesByKey = Collections.synchronizedMap(new LinkedHashMap(16, 0.9f, false) {
		protected boolean removeEldestEntry(Entry eldest) {
			return (this.size() > 128); // should be OK for starters
		}
	});
	private class DocumentUpdate {
		final ImsDocumentData docData;
		final Set missingDocEntryFileNames;
		final String authUserName;
		final String userName;
		DocumentUpdate(ImsDocumentData docData, Set missingDocEntryFileNames, String authUserName, String userName) {
			this.docData = docData;
			this.missingDocEntryFileNames = missingDocEntryFileNames;
			this.authUserName = authUserName;
			this.userName = userName;
		}
	}
	
	private Map updateProtocolsByDocId = Collections.synchronizedMap(new LinkedHashMap(16, 0.9f, false) {
		protected boolean removeEldestEntry(Entry eldest) {
			return (this.size() > 128); // should be OK for starters
		}
	});
	private class DocumentUpdateProtocol extends ArrayList implements EventLogger {
		final String docId;
		final boolean isDeletion;
		String docName;
		int docVersion;
		DocumentUpdateProtocol(String docId, boolean isDeletion) {
			this.docId = docId;
			this.isDeletion = isDeletion;
			this.add("Document deleted"); // acts as placeholder for head entry in updates, which know their version number only later
			updateProtocolsByDocId.put(this.docId, this);
		}
		void setHead(String docName, int version) {
			this.docName = docName;
			this.docVersion = version;
			this.set(0, (this.isDeletion ? "Document deleted" : ("Document '" + this.docName + "' stored as version " + this.docVersion)));
		}
		public void writeLog(String logEntry) {
			this.add(logEntry);
		}
		void close() {
			this.add(this.isDeletion ? DELETION_COMPLETE : UPDATE_COMPLETE);
		}
	}
	
	private static class DummyEventLogger implements EventLogger {
		public void writeLog(String logEntry) {}
	}
	
	private ImsDocumentData getDocumentData(String docId, boolean create, boolean loadEntries) throws IOException {
		String primaryFolderName = docId.substring(0, 2);
		String secondaryFolderName = docId.substring(2, 4);
		File docFolder = new File(this.documentStorageRoot, (primaryFolderName + "/" + secondaryFolderName + "/" + docId));
		if (docFolder.exists())
			return new ImsDocumentData(docId, docFolder, (loadEntries ? 0 : -1));
		else if (create) {
			docFolder.mkdirs();
			return new ImsDocumentData(docId, docFolder);
		}
		else return null;
	}
	
	/**
	 * Add a document event listener to this GoldenGATE IMS so it receives
	 * notification when a document is checked out, updated, released, or
	 * deleted.
	 * @param del the document event listener to add
	 */
	public void addDocumentEventListener(ImsDocumentEventListener del) {
		GoldenGateServerEventService.addServerEventListener(del);
	}
	
	/**
	 * Remove a document event listener from this GoldenGATE IMS.
	 * @param del the document event listener to remove
	 */
	public void removeDocumentEventListener(ImsDocumentEventListener del) {
		GoldenGateServerEventService.removeServerEventListener(del);
	}
	
	/**
	 * Upload a new document, using its docId attribute as the storage ID (if
	 * the docId attribute is not set, the document's annotationId will be used
	 * instead). In case a document already exists with the same ID as the
	 * argument document's, an exception will be thrown. If not, the document
	 * will be created, but no lock will be granted to the uploading user. If a
	 * lock is required, use the updateDocument() method.
	 * @param userName the name of the user doing the update
	 * @param doc the document to store
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public synchronized int uploadDocument(String userName, ImDocument doc, EventLogger logger) throws IOException {
		
		// get checkout user (must be null if document is new)
		String checkoutUser = this.getCheckoutUser(doc.docId);
		if (checkoutUser != null)
			throw new IOException("Document already exists, upload not possible.");
		
		//	do update
		return this.doDocumentUpdate(userName, null, doc, ((logger == null) ? new DummyEventLogger() : logger));
	}
	
	/**
	 * Update an existing document, or store a new one, using its docId
	 * attribute as the storage ID (if the docId attribute is not set, the
	 * document's annotationId will be used instead). In case of an update, the
	 * updating user must have acquired the lock on the document in question
	 * (via one of the checkoutDocument() methods) prior to the invocation of
	 * this method. Otherwise, an IOException will be thrown. In case of a new
	 * document, the lock is automatically granted to the specified user, and
	 * remains with him until he yields it via the releaseDocument() method. If
	 * a lock is not desired for a new document, use the uploadDocument()
	 * method.
	 * @param userName the name of the user doing the update
	 * @param authUserName the user name holding the checkout lock
	 * @param doc the document to store
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public synchronized int updateDocument(String userName, String authUserName, ImDocument doc, EventLogger logger) throws IOException {
		
		// check if document checked out
		if (!this.mayUpdateDocument(authUserName, doc.docId))
			throw new IOException("Document checked out by other user, update not possible.");
		
		//	do update
		return this.doDocumentUpdate(userName, authUserName, doc, ((logger == null) ? new DummyEventLogger() : logger));
	}
	
	private int doDocumentUpdate(String userName, String authUserName, ImDocument doc, final EventLogger logger) throws IOException {
		
		//	get document data
		ImsDocumentData docData = this.getDocumentData(doc.docId, true, true);
		
		//	store document and get entry list
		ImDocumentEntry[] docEntries = ImDocumentIO.storeDocument(doc, docData, new ProgressMonitor() {
			public void setStep(String step) {
				logger.writeLog(step);
			}
			public void setInfo(String info) {
				logger.writeLog(info);
			}
			public void setBaseProgress(int baseProgress) {}
			public void setMaxProgress(int maxProgress) {}
			public void setProgress(int progress) {}
		});
		Arrays.sort(docEntries);
		
		//	finalize update
		return this.finalizeDocumentUpdate(new DocumentUpdate(docData, new HashSet(), authUserName, userName), logger);
	}
	
	private int finalizeDocumentUpdate(DocumentUpdate docUpdate, EventLogger logger) throws IOException {
		
		//	do we have all we need?
		if (docUpdate.missingDocEntryFileNames.size() != 0)
			throw new IOException("Missing entries: " + docUpdate.missingDocEntryFileNames);
		
		//	do update
		return this.finalizeDocumentUpdate(docUpdate.docData, docUpdate.userName, docUpdate.authUserName, logger);
	}
	
	/**
	 * Upload a document from its underlying data without instantiating it.
	 * This method only works for document data objects retrieved from either
	 * of the <code>getDocumentData()</code> methods. In case a document
	 * already exists with the same ID as the argument document's, an exception
	 * will be thrown. If not, the document will be created, but no lock will
	 * be granted to the uploading user. If a lock is required, use the
	 * updateDocument() method.
	 * @param userName the name of the user doing the update
	 * @param doc the document to store
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public synchronized int uploadDocumentFromData(String userName, ImsDocumentData docData, EventLogger logger) throws IOException {
		
		// get checkout user (must be null if document is new)
		String checkoutUser = this.getCheckoutUser(docData.docId);
		if (checkoutUser != null)
			throw new IOException("Document already exists, upload not possible.");
		
		//	do update
		return this.finalizeDocumentUpdate(docData, userName, null, ((logger == null) ? new DummyEventLogger() : logger));
	}
	
	/**
	 * Update a document from its underlying data without instantiating it.
	 * This method only works for document data objects retrieved from either
	 * of the <code>getDocumentData()</code> methods. If the document does not
	 * exist, it is created; if the argument checkout user is null, it is
	 * released afterward, otherwise the argument checkout user retains the
	 * lock. If the document does exist and the argument checkout user is null
	 * or does not match the current checkout user, the update fails.
	 * @param userName the name of the user doing the update
	 * @param authUserName the user name holding the checkout lock
	 * @param docData the document data object to store
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public int updateDocumentFromData(String userName, String authUserName, ImsDocumentData docData, EventLogger logger) throws IOException {
		
		// check if document checked out
		if (!this.mayUpdateDocument(authUserName, docData.docId))
			throw new IOException("Document checked out by other user, update not possible.");
		
		//	do update
		return this.finalizeDocumentUpdate(docData, userName, authUserName, ((logger == null) ? new DummyEventLogger() : logger));
	}
	
	private int finalizeDocumentUpdate(final ImsDocumentData docData, final String updateUser, final String checkoutUser, final EventLogger logger) throws IOException {
		
		// get timestamp
		final long time = System.currentTimeMillis();
		String timeString = ("" + time);
		
		// do not store checkout user info
		docData.removeAttribute(CHECKOUT_USER_ATTRIBUTE);
		docData.removeAttribute(CHECKOUT_TIME_ATTRIBUTE);
		
		// update meta data
		docData.setAttribute(UPDATE_USER_ATTRIBUTE, updateUser);
		if (!docData.hasAttribute(CHECKIN_USER_ATTRIBUTE))
			docData.setAttribute(CHECKIN_USER_ATTRIBUTE, updateUser);
		
		docData.setAttribute(UPDATE_TIME_ATTRIBUTE, timeString);
		if (!docData.hasAttribute(CHECKIN_TIME_ATTRIBUTE))
			docData.setAttribute(CHECKIN_TIME_ATTRIBUTE, timeString);
		
		//	store updated entry list and get version number
		final int newVersion = docData.storeEntryList(time);
		
		//	get document attributes
		final Attributed docAttributes = ImDocumentIO.loadDocumentAttributes(docData);
		
		//	clear cache
		this.documentMetaDataCache.remove(docData.docId);
		
		//	prepare database update
		StringVector assignments = new StringVector();
		
		// check and (if necessary) truncate name
		String name = ((String) docAttributes.getAttribute(DOCUMENT_NAME_ATTRIBUTE, ""));
		if (name.length() > DOCUMENT_NAME_COLUMN_LENGTH)
			name = name.substring(0, DOCUMENT_NAME_COLUMN_LENGTH);
		assignments.addElement(DOCUMENT_NAME_ATTRIBUTE + " = '" + EasyIO.sqlEscape(name) + "'");
		
		// get update user and authenticated user (might differ if latter is group account)
		String user = updateUser;
		if (user.length() > UserAccessAuthority.USER_NAME_MAX_LENGTH)
			user = user.substring(0, UserAccessAuthority.USER_NAME_MAX_LENGTH);
		assignments.addElement(UPDATE_USER_ATTRIBUTE + " = '" + EasyIO.sqlEscape(user) + "'");
		String authUser = checkoutUser;
		if ((authUser != null) && (authUser.length() > UserAccessAuthority.USER_NAME_MAX_LENGTH))
			authUser = authUser.substring(0, UserAccessAuthority.USER_NAME_MAX_LENGTH);
		
		// set update time
		assignments.addElement(UPDATE_TIME_ATTRIBUTE + " = " + time);
		
		// update version number
		assignments.addElement(DOCUMENT_VERSION_ATTRIBUTE + " = " + newVersion);
		
		// write new values
		String updateQuery = ("UPDATE " + DOCUMENT_TABLE_NAME + 
				" SET " + assignments.concatStrings(", ") + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + docData.docId + "'" +
				" AND " + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docData.docId.hashCode() + "" +
				";");
		
		try {

			// update did not affect any rows ==> new document
			if (this.io.executeUpdateQuery(updateQuery) == 0) {
				
				// gather complete data for creating master table record
				StringBuffer fields = new StringBuffer(DOCUMENT_ID_ATTRIBUTE);
				StringBuffer fieldValues = new StringBuffer("'" + EasyIO.sqlEscape(docData.docId) + "'");
				fields.append(", " + DOCUMENT_ID_HASH_ATTRIBUTE);
				fieldValues.append(", " + docData.docId.hashCode());
				
				// set name
				fields.append(", " + DOCUMENT_NAME_ATTRIBUTE);
				fieldValues.append(", '" + EasyIO.sqlEscape(name) + "'");
				
				// set checkin user
				fields.append(", " + CHECKIN_USER_ATTRIBUTE);
				fieldValues.append(", '" + EasyIO.sqlEscape(user) + "'");
				
				// set checkin/update time
				fields.append(", " + CHECKIN_TIME_ATTRIBUTE);
				fieldValues.append(", " + time);
				
				// set update user
				fields.append(", " + UPDATE_USER_ATTRIBUTE);
				fieldValues.append(", '" + EasyIO.sqlEscape(user) + "'");

				// set checkin/update time
				fields.append(", " + UPDATE_TIME_ATTRIBUTE);
				fieldValues.append(", " + time);

				// update version number
				fields.append(", " + DOCUMENT_VERSION_ATTRIBUTE);
				fieldValues.append(", " + newVersion);
				
				// set lock if requested
				fields.append(", " + CHECKOUT_USER_ATTRIBUTE);
				fieldValues.append(", '" + ((authUser == null) ? "" : EasyIO.sqlEscape(authUser)) + "'");
				fields.append(", " + CHECKOUT_TIME_ATTRIBUTE);
				fieldValues.append(", " + ((authUser == null) ? -1 : time));
				
				// store data in collection main table
				String insertQuery = "INSERT INTO " + DOCUMENT_TABLE_NAME + 
						" (" + fields.toString() + ")" +
						" VALUES" +
						" (" + fieldValues.toString() + ")" +
						";";
				try {
					this.io.executeUpdateQuery(insertQuery);
					this.docIdSet.add(docData.docId);
					this.cacheDocumentAttributeValues(docAttributes);
					if (authUser != null)
						this.cacheDocumentAttributeValue(CHECKOUT_USER_ATTRIBUTE, authUser);
				}
				catch (SQLException sqle) {
					System.out.println("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while storing new document.");
					System.out.println("  query was " + insertQuery);
					throw new IOException(sqle.getMessage());
				}
			}
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while updating existing document.");
			System.out.println("  query was " + updateQuery);
			throw new IOException(sqle.getMessage());
		}
		
		//	prepare attribute database update
		assignments.clear();
		for (Iterator dacnit = this.documentAttributesByName.keySet().iterator(); dacnit.hasNext();) {
			DocumentAttribute da = ((DocumentAttribute) this.documentAttributesByName.get(dacnit.next()));
			String assignment = da.getUpdateQueryAssignment(docAttributes);
			if (assignment != null)
				assignments.addElement(assignment);
		}
		
		//	catch empty assignment list
		if (assignments.size() == 0)
			assignments.addElement(DOCUMENT_ID_ATTRIBUTE + " = '" + docData.docId + "'");
		
		//	write new values to attribute table
		updateQuery = ("UPDATE " + DOCUMENT_ATTRIBUTE_TABLE_NAME + 
				" SET " + assignments.concatStrings(", ") + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + docData.docId + "'" +
				" AND " + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docData.docId.hashCode() + "" +
				";");
		
		try {
			
			// update did not affect any rows ==> new document
			if (this.io.executeUpdateQuery(updateQuery) == 0) {
				
				// gather complete data for creating master table record
				StringBuffer fields = new StringBuffer(DOCUMENT_ID_ATTRIBUTE);
				StringBuffer fieldValues = new StringBuffer("'" + EasyIO.sqlEscape(docData.docId) + "'");
				fields.append(", " + DOCUMENT_ID_HASH_ATTRIBUTE);
				fieldValues.append(", " + docData.docId.hashCode());
				
				//	collect document attributes
				for (Iterator dacnit = this.documentAttributesByName.keySet().iterator(); dacnit.hasNext();) {
					DocumentAttribute da = ((DocumentAttribute) this.documentAttributesByName.get(dacnit.next()));
					fields.append(", " + da.colName);
					if (da.isInteger)
						fieldValues.append(", " + da.getInsertQueryValue(docAttributes));
					else fieldValues.append(", '" + da.getInsertQueryValue(docAttributes) + "'");
				}
				
				// store data in collection main table
				String insertQuery = "INSERT INTO " + DOCUMENT_ATTRIBUTE_TABLE_NAME + 
						" (" + fields.toString() + ")" +
						" VALUES" +
						" (" + fieldValues.toString() + ")" +
						";";
				try {
					this.io.executeUpdateQuery(insertQuery);
					this.cacheDocumentAttributeValues(docAttributes);
				}
				catch (SQLException sqle) {
					System.out.println("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while storing new document.");
					System.out.println("  query was " + insertQuery);
					throw new IOException(sqle.getMessage());
				}
			}
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while updating existing document.");
			System.out.println("  query was " + updateQuery);
			throw new IOException(sqle.getMessage());
		}
		
		//	update coming through network interface, do event notification asynchronously for quick response
		if (logger instanceof DocumentUpdateProtocol) {
			((DocumentUpdateProtocol) logger).setHead(((String) docAttributes.getAttribute(DOCUMENT_NAME_ATTRIBUTE, docData.docId)), newVersion);
			Thread dunThread = new Thread() {
				public void run() {
					try {
						//	load whole document only now, no use having network clients wait
						ImDocument doc = ImDocumentIO.loadDocument(docData, ProgressMonitor.dummy);
						docData.setReadOnly(true);
						//	TODO OK adding provenance attributes to document?
						AttributeUtils.copyAttributes(docData, doc, AttributeUtils.ADD_ATTRIBUTE_COPY_MODE);
						//	issue update event
						GoldenGateServerEventService.notify(new ImsDocumentEvent(updateUser, doc.docId, doc, newVersion, GoldenGateIMS.class.getName(), time, logger));
						//	issue release event if document is not locked and thus free for editing
						if (checkoutUser == null) GoldenGateServerEventService.notify(new ImsDocumentEvent(updateUser, doc.docId, null, -1, ImsDocumentEvent.RELEASE_TYPE, GoldenGateIMS.class.getName(), time, null));
					}
					
					//	little we can do than log the problem ...
					catch (IOException ioe) {
						logger.writeLog("Error loading document for event notification: " + ioe.getMessage());
					}
					
					//	close update protocol
					finally {
						((DocumentUpdateProtocol) logger).close();
					}
				}
			};
			dunThread.start();
		}
		
		//	component API update, we can work synchronously
		else {
			//	load whole document only now, no use having network clients wait
			ImDocument doc = ImDocumentIO.loadDocument(docData, ProgressMonitor.dummy);
			docData.setReadOnly(true);
			//	TODO OK adding provenance attributes to document?
			AttributeUtils.copyAttributes(docData, doc, AttributeUtils.ADD_ATTRIBUTE_COPY_MODE);
			//	issue update event
			GoldenGateServerEventService.notify(new ImsDocumentEvent(updateUser, doc.docId, doc, newVersion, GoldenGateIMS.class.getName(), time, logger));
			//	issue release event if document is not locked and thus free for editing
			if (checkoutUser == null) GoldenGateServerEventService.notify(new ImsDocumentEvent(updateUser, doc.docId, null, -1, ImsDocumentEvent.RELEASE_TYPE, GoldenGateIMS.class.getName(), time, null));
		}
		
		// report new version
		return newVersion;
	}
	
	/**
	 * Delete a document from storage. If the document with the specified ID is
	 * checked out by a user other than the one doing the deletion, the document
	 * cannot be deleted and an IOException will be thrown.
	 * @param userName the user doing the deletion
	 * @param docId the ID of the document to delete
	 * @param logger a logger for obtaining detailed information on the deletion
	 *            process
	 * @throws IOException
	 */
	public synchronized void deleteDocument(String userName, String docId, EventLogger logger) throws IOException {
		String checkoutUser = this.getCheckoutUser(docId);
		
		//	check if document exists
		if (checkoutUser == null)
			throw new IOException("Document does not exist.");
		
		//	check checkout state
		if (!checkoutUser.equals("") && !checkoutUser.equals(userName))
			throw new IOException("Document checked out by other user, delete not possible.");
		
		//	clear cache
		this.documentMetaDataCache.remove(docId);

		// delete meta data
		String deleteQuery = "DELETE FROM " + DOCUMENT_TABLE_NAME + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
				" AND " + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() +
				";";
		try {
			ImsDocumentListElement dle = this.getMetaData(docId);
			this.io.executeUpdateQuery(deleteQuery);
			this.uncacheDocumentAttributeValues(dle);
			this.checkoutUserCache.remove(docId);
			this.docIdSet.remove(docId);
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
			System.out.println("  query was " + deleteQuery);
		}

		// delete document attributes
		deleteQuery = "DELETE FROM " + DOCUMENT_ATTRIBUTE_TABLE_NAME + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
				" AND " + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() +
				";";
		try {
			this.io.executeUpdateQuery(deleteQuery);
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
			System.out.println("  query was " + deleteQuery);
		}
		
		// issue event
		GoldenGateServerEventService.notify(new ImsDocumentEvent(userName, docId, GoldenGateIMS.class.getName(), System.currentTimeMillis(), logger));
		if (logger instanceof DocumentUpdateProtocol)
			((DocumentUpdateProtocol) logger).close();
	}
	
	/**
	 * Retrieve a document data object for a document with a given ID. If no
	 * such document exists, this method returns null.
	 * @param docId the document ID
	 * @return a date object for the document with the argument ID
	 * @throws IOException
	 */
	public ImsDocumentData getDocumentData(String docId) throws IOException {
		return this.getDocumentData(docId, false);
	}
	
	/**
	 * Retrieve a document data object for a document with a given ID. If no
	 * such document exists and the create argument is false, this method
	 * returns null. If the create argument is true, this method returns an
	 * initially empty document data object that can be populated by client
	 * code and then stored via either <code>updateDocumentFromData()</code>
	 * or <code>uploadDocumentFromData()</code>.
	 * @param docId the document ID
	 * @param create create a document data object if no document exists?
	 * @return a date object for the document with the argument ID
	 * @throws IOException
	 */
	public ImsDocumentData getDocumentData(String docId, boolean create) throws IOException {
		return this.getDocumentData(docId, create, true);
	}
	
	/**
	 * Load a document from storage (the most recent version). This method loops
	 * through to the underlying DST, it exists so other components do not have
	 * to deal with two different storage components. The document is not
	 * locked, so any attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @return the document with the specified ID
	 * @throws IOException
	 * @see de.uka.ipd.idaho.goldenGateServer.dst.DocumentStore#loadDocument(String)
	 */
	public ImDocument getDocument(String documentId) throws IOException {
		return this.getDocument(documentId, 0);
	}
	
	/**
	 * Load a specific version of a document from storage. A positive version
	 * number indicates an actual version specifically, while a negative version
	 * number indicates a version backward relative to the most recent version.
	 * Version number 0 always returns the most recent version. The document is
	 * not locked, so any attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @param version the version to load
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public ImDocument getDocument(String documentId, int version) throws IOException {
		
		//	get document data
		ImDocumentData docData = this.getDocumentAsData(documentId, version);
		if (docData == null)
			throw new IOException("Invalid document ID '" + documentId + "'");
		
		//	load document
		return ImDocumentIO.loadDocument(docData, ProgressMonitor.dummy);
	}
	
	/**
	 * Load a document data object from storage (the most recent version). The
	 * document is not locked, so any attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @return the document with the specified ID
	 * @throws IOException
	 * @see de.uka.ipd.idaho.goldenGateServer.dst.DocumentStore#loadDocument(String)
	 */
	public ImsDocumentData getDocumentAsData(String documentId) throws IOException {
		return this.getDocumentAsData(documentId, 0);
	}
	
	/**
	 * Load a specific version of a document from storage. A positive version
	 * number indicates an actual version specifically, while a negative version
	 * number indicates a version backward relative to the most recent version.
	 * Version number 0 always returns the most recent version. The document is
	 * not locked, so any attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @param version the version to load
	 * @return a data object of the document with the specified ID
	 * @throws IOException
	 */
	public ImsDocumentData getDocumentAsData(String documentId, int version) throws IOException {
		
		//	get document data
		ImsDocumentData docData = this.getDocumentData(documentId, false, false);
		if (docData == null)
			throw new IOException("Invalid document ID '" + documentId + "'");
		
		//	get entry list for argument version
		docData = docData.cloneForVersion(version);
		if (docData == null)
			throw new IOException("Invalid version '" + version + "' for document ID '" + documentId + "'");
		
		//	make document read-only, as there is no checkout
		docData.setReadOnly(true);
		
		//	finally ...
		return docData;
	}
	
	/**
	 * Check out a document from IMS. The document will be protected from
	 * editing by other users until it is released through the releaseDocument()
	 * method.
	 * @param userName the user checking out the document
	 * @param documentId the ID of the document to check out
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public ImDocument checkoutDocument(String userName, String documentId) throws IOException {
		return this.checkoutDocument(userName, documentId, 0);
	}
	
	/**
	 * Check out a document from IMS. The document will be protected from
	 * editing by other users until it is released through the releaseDocument()
	 * method.
	 * @param userName the user checking out the document
	 * @param documentId the ID of the document to check out
	 * @param version the version to load
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public synchronized ImDocument checkoutDocument(String userName, String documentId, int version) throws IOException {
		
		//	get document data
		ImDocumentData docData = this.checkoutDocumentAsData(userName, documentId, version);
		if (docData == null)
			throw new IOException("Invalid document ID '" + documentId + "'");
		
		//	load document on top of local cache folder
		try {
			return ImDocumentIO.loadDocument(docData, ProgressMonitor.dummy);
		}
		catch (IOException ioe) {
			this.setCheckoutUser(documentId, "", -1);
			System.out.println("GoldenGateIMS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while loading document " + documentId + ".");
			ioe.printStackTrace(System.out);
			throw ioe;
		}
	}
	
	/**
	 * Check out a document from IMS. The document will be protected from
	 * editing by other users until it is released through the releaseDocument()
	 * method.
	 * @param userName the user checking out the document
	 * @param documentId the ID of the document to check out
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public ImsDocumentData checkoutDocumentAsData(String userName, String documentId) throws IOException {
		return this.checkoutDocumentAsData(userName, documentId, 0);
	}
	
	/**
	 * Check out a document from IMS. The document will be protected from
	 * editing by other users until it is released through the releaseDocument()
	 * method.
	 * @param userName the user checking out the document
	 * @param documentId the ID of the document to check out
	 * @param version the version to load
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public synchronized ImsDocumentData checkoutDocumentAsData(String userName, String documentId, int version) throws IOException {
		String checkoutUser = this.getCheckoutUser(documentId);
		
		//	check if document exists
		if (checkoutUser == null)
			throw new IOException("Document does not exist.");
		
		//	check if checkout possible for user
		if (!checkoutUser.equals("") && !checkoutUser.equals(userName))
			throw new DocumentCheckedOutException();
		
		//	mark document as checked out
		long checkoutTime = System.currentTimeMillis();
		this.setCheckoutUser(documentId, userName, checkoutTime);
		
		//	get document data
		ImsDocumentData docData = this.getDocumentData(documentId, false, false);
		if (docData == null)
			throw new IOException("Invalid document ID '" + documentId + "'");
		
		//	get entry list for argument version
		docData = docData.cloneForVersion(version);
		if (docData == null)
			throw new IOException("Invalid version '" + version + "' for document ID '" + documentId + "'");
		
		//	log checkout and notify listeners
		this.writeLogEntry("document " + documentId + " checked out by '" + userName + "'.");
		GoldenGateServerEventService.notify(new ImsDocumentEvent(userName, documentId, null, -1, ImsDocumentEvent.CHECKOUT_TYPE, GoldenGateIMS.class.getName(), checkoutTime, null));
		
		//	load document on top of local cache folder
		return docData;
	}
	
	private static class DocumentCheckedOutException extends IOException {
		DocumentCheckedOutException() {
			super("Document checked out by other user, checkout not possible.");
		}
	}
	
	private boolean mayUpdateDocument(String userName, String docId) {
		String checkoutUser = this.getCheckoutUser(docId);
		if (checkoutUser == null) return true; // document not known so far
		else return checkoutUser.equals(userName); // document checked out by user in question
	}

	/**
	 * Release a document. The lock on the document is released, so other users
	 * can check it out again.
	 * @param userName the name of the user holding the lock of the document to
	 *            release
	 * @param docId the ID of the document to release
	 */
	public synchronized void releaseDocument(String userName, String docId) {
		String checkoutUser = this.getCheckoutUser(docId);
		
		//	check if document exists
		if (checkoutUser == null)
			return;
		
		//	release document if possible
		if (this.uaa.isAdmin(userName) || checkoutUser.equals(userName)) { // admin user, or user holding the lock
			this.setCheckoutUser(docId, "", -1);
			GoldenGateServerEventService.notify(new ImsDocumentEvent(userName, docId, null, -1, ImsDocumentEvent.RELEASE_TYPE, GoldenGateIMS.class.getName(), System.currentTimeMillis(), null));
		}
	}

	private static final int checkoutUserCacheSize = 256;

	private LinkedHashMap checkoutUserCache = new LinkedHashMap(checkoutUserCacheSize, .9f, true) {
		protected boolean removeEldestEntry(Entry eldest) {
			return this.size() > checkoutUserCacheSize;
		}
	};
	
	/**
	 * Get the name of the user who has checked out a document with a given ID
	 * and therefore holds the lock for that document.
	 * @param docId the ID of the document in question
	 * @return the name of the user who has checked out the document with the
	 *         specified ID, the empty string if the document is currently not
	 *         checked out by any user, and null if there is no document with
	 *         the specified ID
	 */
	public String getCheckoutUser(String docId) {
		
		// do cache lookup
		String checkoutUser = ((String) this.checkoutUserCache.get(docId));

		// cache hit
		if (checkoutUser != null) return checkoutUser;

		// cache miss, prepare loading data
		String query = "SELECT " + CHECKOUT_USER_ATTRIBUTE + 
				" FROM " + DOCUMENT_TABLE_NAME + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
				" AND " + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() +
				";";

		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query);
			if (sqr.next()) {
				checkoutUser = sqr.getString(0);
				if (checkoutUser == null)
					checkoutUser = "";
				this.checkoutUserCache.put(docId, checkoutUser);
				return checkoutUser;
			}
			else return null;
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading document checkout user.");
			System.out.println("  query was " + query);
			return null;
		}
		finally {
			if (sqr != null) sqr.close();
		}
	}
	
	/**
	 * Retrieve the attributes of a document, as stored in the archive. There is
	 * no guarantee with regard to the attributes contained in the returned
	 * properties. If a document with the specified ID does not exist, this
	 * method returns null.
	 * @param docId the ID of the document
	 * @return a Attributed object holding the attributes of the document with
	 *         the specified ID
	 * @throws IOException
	 */
	public Attributed getDocumentAttributes(String documentId) {
		
		//	load and return attributes
		try {
			ImsDocumentData docData = this.getDocumentData(documentId, false, true);
			Attributed docAttributes = ImDocumentIO.loadDocumentAttributes(docData);
			AttributeUtils.copyAttributes(docData, docAttributes, AttributeUtils.ADD_ATTRIBUTE_COPY_MODE);
			return docAttributes;
		}
		catch (IOException ioe) {
			return null;
		}
	}
	
	/**
	 * Retrieve the meta data for a specific document. The attributes of the
	 * returned document list element correspond to those held in a document
	 * list retrieved via one of the getDocumentList() methods for a user
	 * without administrative privileges. If a document with the specified ID
	 * does not exist, this method returns null.
	 * @param docId the ID of the document to retrieve the meta data for
	 * @return the meta data for the document with the specified ID
	 */
	public ImsDocumentListElement getMetaData(String docId) {
		
		//	do cache lookup
		ImsDocumentListElement dle = ((ImsDocumentListElement) this.documentMetaDataCache.get(docId));
		if (dle != null)
			return dle;
		
		// collect field names
		StringVector fieldNames = new StringVector();
		fieldNames.addContent(documentDataFields);
		for (Iterator dacnit = this.documentAttributesByName.keySet().iterator(); dacnit.hasNext();) {
			DocumentAttribute da = ((DocumentAttribute) this.documentAttributesByName.get(dacnit.next()));
			fieldNames.addElementIgnoreDuplicates(da.colName);
		}
		
		// assemble query
		String query = "SELECT " + fieldNames.concatStrings(", ") + 
				" FROM " + DOCUMENT_TABLE_NAME + " dd, " + DOCUMENT_ATTRIBUTE_TABLE_NAME + " da" +
				" WHERE dd." + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
				" AND dd." + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() +
				" AND da." + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
				" AND da." + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() +
				";";
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query.toString());
			if (sqr.next()) {
				dle = new ImsDocumentListElement();
				for (int f = 0; f < fieldNames.size(); f++)
					dle.setAttribute(fieldNames.get(f), sqr.getString(f));
				this.documentMetaDataCache.put(docId, dle);
				return dle;
			}
			else return null;
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while reading meta data for document " + docId + ".");
			System.out.println("  query was " + query);
			return null;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	private static final int documentMetaDataCacheSize = 256;

	private LinkedHashMap documentMetaDataCache = new LinkedHashMap(documentMetaDataCacheSize, .9f, true) {
		protected boolean removeEldestEntry(Entry eldest) {
			return this.size() > documentMetaDataCacheSize;
		}
	};
	
	private void setCheckoutUser(String docId, String checkoutUser, long checkoutTime) {
		StringVector assignments = new StringVector();

		// set checkout user
		checkoutUser = ((checkoutUser == null) ? "" : checkoutUser);
		if (checkoutUser.length() > UserAccessAuthority.USER_NAME_MAX_LENGTH)
			checkoutUser = checkoutUser.substring(0, UserAccessAuthority.USER_NAME_MAX_LENGTH);
		assignments.addElement(CHECKOUT_USER_ATTRIBUTE + " = '" + EasyIO.sqlEscape(checkoutUser) + "'");

		// set checkout time
		assignments.addElement(CHECKOUT_TIME_ATTRIBUTE + " = " + checkoutTime);

		// write new values
		String updateQuery = ("UPDATE " + DOCUMENT_TABLE_NAME + 
				" SET " + assignments.concatStrings(", ") + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
				" AND " + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() +
				";");
		try {
			this.io.executeUpdateQuery(updateQuery);
			this.checkoutUserCache.put(docId, checkoutUser);
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while setting checkout user for document " + docId + ".");
			System.out.println("  query was " + updateQuery);
		}
	}

	private static final String[] documentDataFields = {
			DOCUMENT_ID_ATTRIBUTE,
//			EXTERNAL_IDENTIFIER_ATTRIBUTE,
			DOCUMENT_NAME_ATTRIBUTE,
//			DOCUMENT_AUTHOR_ATTRIBUTE,
//			DOCUMENT_DATE_ATTRIBUTE,
//			DOCUMENT_TITLE_ATTRIBUTE,
//			DOCUMENT_KEYWORDS_ATTRIBUTE,
			CHECKIN_USER_ATTRIBUTE,
			CHECKIN_TIME_ATTRIBUTE,
			UPDATE_USER_ATTRIBUTE,
			UPDATE_TIME_ATTRIBUTE,
			DOCUMENT_VERSION_ATTRIBUTE,
		};

	private static final String[] documentDataFieldsAdmin = {
			CHECKOUT_USER_ATTRIBUTE,
			CHECKOUT_TIME_ATTRIBUTE
		};

	/**
	 * Retrieve a list of meta data for the document available through this IMS.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. The
	 * list includes all available documents, regardless of their checkout
	 * state. That means all of the documents on the list can be retrieved from
	 * the getDocument() method for read access, but none is guaranteed to be
	 * available for checkout and editing.
	 * @return a list of meta data for the document available through this IMS
	 */
	public ImsDocumentList getDocumentListFull() {
		
		// collect field names
		StringVector fieldNames = new StringVector();
		for (int f = 0; f < documentDataFields.length; f++)
			fieldNames.addElement("dd." + documentDataFields[f]);
		for (Iterator dacnit = this.documentAttributesByName.keySet().iterator(); dacnit.hasNext();) {
			DocumentAttribute da = ((DocumentAttribute) this.documentAttributesByName.get(dacnit.next()));
			fieldNames.addElementIgnoreDuplicates("da." + da.colName);
		}
		
		// assemble query
		String query = "SELECT " + fieldNames.concatStrings(", ") + 
				" FROM " + DOCUMENT_TABLE_NAME + " dd, " + DOCUMENT_ATTRIBUTE_TABLE_NAME + " da" +
				" WHERE dd." + DOCUMENT_ID_ATTRIBUTE + " = da." + DOCUMENT_ID_ATTRIBUTE + 
				" AND dd." + DOCUMENT_ID_HASH_ATTRIBUTE + " = da." + DOCUMENT_ID_HASH_ATTRIBUTE + 
				" ORDER BY " + DOCUMENT_NAME_ATTRIBUTE +
				";";
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query.toString());
			for (int f = 0; f < fieldNames.size(); f++) {
				String fn = fieldNames.get(f);
				if (fn.indexOf(".") != -1)
					fieldNames.setElementAt(fn.substring(fn.indexOf(".") + ".".length()), f);
			}
			return new SqrDocumentList(fieldNames.toStringArray(), sqr, this.docIdSet.size());
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents.");
			System.out.println("  query was " + query);
			
			// return dummy list
			return new ImsDocumentList(fieldNames.toStringArray()) {
				public boolean hasNextDocument() {
					return false;
				}
				public ImsDocumentListElement getNextDocument() {
					return null;
				}
			};
		}
	}
	
	/**
	 * Retrieve a list of meta data for the document available through this IMS.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. The
	 * list only includes documents that can be checked out, i.e., ones that are
	 * not checked out. Use getDocumentListFull() for retrieving a comprehensive
	 * list of documents available, regardless of their checkout state.
	 * @return a list of meta data for the document available through this IMS
	 */
	public ImsDocumentList getDocumentList() {
		return this.getDocumentList("", false, null);
	}

	/**
	 * Retrieve a list of meta data for the document available through this IMS.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. The
	 * list only includes documents that can be checked out, i.e., ones that are
	 * not checked out. Use getDocumentListFull() for retrieving a comprehensive
	 * list of documents available, regardless of their checkout state.
	 * @param headOnly if set to true, this method returns an empty list, only
	 *            containing the header data (field names and attribute value
	 *            summaries)
	 * @return a list of meta data for the document available through this IMS
	 */
	public ImsDocumentList getDocumentList(boolean headOnly) {
		return this.getDocumentList("", false, null);
	}

	/**
	 * Retrieve a list of meta data for the document available through this IMS.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. If the
	 * specified user has administrative privileges, checked-out documents are
	 * included in the list, and the list also provides the checkout user and
	 * checkout status of every document. Otherwise, the list only includes
	 * documents that can be checked out, i.e., ones that are not checked out,
	 * and ones checked out by the specified user. Use getDocumentListFull() for
	 * retrieving a comprehensive list of documents available, regardless of
	 * their checkout state.
	 * @param userName the user to retrieve the list for (used for filtering
	 *            based on document's checkout states, has no effect if the
	 *            specified user has administrative privileges)
	 * @return a list of meta data for the document available through this IMS
	 */
	public ImsDocumentList getDocumentList(String userName) {
		return this.getDocumentList(userName, false, null);
	}
	
	/**
	 * Retrieve a list of meta data for the document available through this IMS.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. If the
	 * specified user has administrative privileges, checked-out documents are
	 * included in the list, and the list also provides the checkout user and
	 * checkout status of every document. Otherwise, the list only includes
	 * documents that can be checked out, i.e., ones that are not checked out,
	 * and ones checked out by the specified user. Use getDocumentListFull() for
	 * retrieving a comprehensive list of documents available, regardless of
	 * their checkout state.
	 * @param userName the user to retrieve the list for (used for filtering
	 *            based on document's checkout states, has no effect if the
	 *            specified user has administrative privileges)
	 * @param headOnly if set to true, this method returns an empty list, only
	 *            containing the header data (field names and attribute value
	 *            summaries)
	 * @return a list of meta data for the document available through this IMS
	 */
	public ImsDocumentList getDocumentList(String userName, boolean headOnly) {
		return this.getDocumentList(userName, headOnly, null);
	}
	
	/**
	 * Retrieve a list of meta data for the document available through this IMS.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. The
	 * list only includes documents that can be checked out, i.e., ones that are
	 * not checked out. Use getDocumentListFull() for retrieving a comprehensive
	 * list of documents available, regardless of their checkout state.
	 * @param filter a properties object containing filter predicates for the
	 *            document list
	 * @return a list of meta data for the document available through this IMS
	 */
	public ImsDocumentList getDocumentList(Properties filter) {
		return this.getDocumentList("", false, filter);
	}

	/**
	 * Retrieve a list of meta data for the document available through this IMS.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. The
	 * list only includes documents that can be checked out, i.e., ones that are
	 * not checked out. Use getDocumentListFull() for retrieving a comprehensive
	 * list of documents available, regardless of their checkout state.
	 * @param headOnly if set to true, this method returns an empty list, only
	 *            containing the header data (field names and attribute value
	 *            summaries)
	 * @param filter a properties object containing filter predicates for the
	 *            document list
	 * @return a list of meta data for the document available through this IMS
	 */
	public ImsDocumentList getDocumentList(boolean headOnly, Properties filter) {
		return this.getDocumentList("", false, filter);
	}
	
	/**
	 * Retrieve a list of meta data for the document available through this IMS.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. If the
	 * specified user has administrative privileges, checked-out documents are
	 * included in the list, and the list also provides the checkout user and
	 * checkout status of every document. Otherwise, the list only includes
	 * documents that can be checked out, i.e., ones that are not checked out,
	 * and ones checked out by the specified user. Use getDocumentListFull() for
	 * retrieving a comprehensive list of documents available, regardless of
	 * their checkout state.
	 * @param userName the user to retrieve the list for (used for filtering
	 *            based on document's checkout states, has no effect if the
	 *            specified user has administrative privileges)
	 * @param filter a properties object containing filter predicates for the
	 *            document list
	 * @return a list of meta data for the document available through this IMS
	 */
	public ImsDocumentList getDocumentList(String userName, Properties filter) {
		return this.getDocumentList(userName, false, filter);
	}
	
	/**
	 * Retrieve a list of meta data for the document available through this IMS.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. If the
	 * specified user has administrative privileges, checked-out documents are
	 * included in the list, and the list also provides the checkout user and
	 * checkout status of every document. Otherwise, the list only includes
	 * documents that can be checked out, i.e., ones that are not checked out,
	 * and ones checked out by the specified user. Use getDocumentListFull() for
	 * retrieving a comprehensive list of documents available, regardless of
	 * their checkout state.
	 * @param userName the user to retrieve the list for (used for filtering
	 *            based on document's checkout states, has no effect if the
	 *            specified user has administrative privileges)
	 * @param headOnly if set to true, this method returns an empty list, only
	 *            containing the header data (field names and attribute value
	 *            summaries)
	 * @param filter a properties object containing filter predicates for the
	 *            document list
	 * @return a list of meta data for the document available through this IMS
	 */
	public ImsDocumentList getDocumentList(String userName, boolean headOnly, Properties filter) {
		
		// get user status
		boolean isAdmin = this.uaa.isAdmin(userName);
		
		// estimate list size
		final int selectivity = this.getSelectivity(filter);
		
		// collect field names
		StringVector fieldNames = new StringVector();
		for (int f = 0; f < documentDataFields.length; f++)
			fieldNames.addElement("dd." + documentDataFields[f]);
		if (isAdmin) {
			for (int f = 0; f < documentDataFieldsAdmin.length; f++)
				fieldNames.addElement("dd." + documentDataFieldsAdmin[f]);
		}
		for (Iterator dacnit = this.documentAttributesByName.keySet().iterator(); dacnit.hasNext();) {
			DocumentAttribute da = ((DocumentAttribute) this.documentAttributesByName.get(dacnit.next()));
			fieldNames.addElementIgnoreDuplicates("da." + da.colName);
		}
		
		// head only, or list too large for regular user, return empty list
		if ((headOnly) || (!isAdmin && (0 < this.documentListSizeThreshold) && (this.documentListSizeThreshold < selectivity))) {
			ImsDocumentList dl = new ImsDocumentList(fieldNames.toStringArray()) {
				public boolean hasNextDocument() {
					return false;
				}
				public ImsDocumentListElement getNextDocument() {
					return null;
				}
				public AttributeSummary getListFieldValues(String listFieldName) {
					return GoldenGateIMS.this.getListFieldSummary(listFieldName, true);
				}
			};
			return dl;
		}
		
		// assemble query
		String query = "SELECT " + fieldNames.concatStrings(", ") + 
				" FROM " + DOCUMENT_TABLE_NAME + " dd, " + DOCUMENT_ATTRIBUTE_TABLE_NAME + " da" +
				" WHERE dd." + DOCUMENT_ID_ATTRIBUTE + " = da." + DOCUMENT_ID_ATTRIBUTE + 
				" AND dd." + DOCUMENT_ID_HASH_ATTRIBUTE + " = da." + DOCUMENT_ID_HASH_ATTRIBUTE + 
				" AND " + this.getDocumentFilter(filter) +
				// filter out documents checked out by another user (if not admin)
				(isAdmin ? "" : (" AND ((" + CHECKOUT_TIME_ATTRIBUTE + " = -1) OR (" + CHECKOUT_USER_ATTRIBUTE + " LIKE '" + EasyIO.prepareForLIKE(userName) + "'))")) +
				" ORDER BY " + DOCUMENT_NAME_ATTRIBUTE + 
				";";
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query.toString());
			for (int f = 0; f < fieldNames.size(); f++) {
				String fn = fieldNames.get(f);
				if (fn.indexOf(".") != -1)
					fieldNames.setElementAt(fn.substring(fn.indexOf(".") + ".".length()), f);
			}
			return new SqrDocumentList(fieldNames.toStringArray(), sqr, selectivity);
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents.");
			System.out.println("  query was " + query);
			
			// return dummy list
			return new ImsDocumentList(fieldNames.toStringArray()) {
				public boolean hasNextDocument() {
					return false;
				}
				public ImsDocumentListElement getNextDocument() {
					return null;
				}
			};
		}
	}
	
	private static class SqrDocumentList extends ImsDocumentList {
		private SqlQueryResult sqr;
		private int docCount;
		private ImsDocumentListElement next = null;
		SqrDocumentList(String[] listFieldNames, SqlQueryResult sqr, int docCount) {
			super(listFieldNames);
			this.sqr = sqr;
			this.docCount = docCount;
		}
		public int getDocumentCount() {
			return this.docCount;
		}
		public boolean hasNextDocument() {
			if (this.next != null) return true;
			else if (this.sqr == null) return false;
			else if (this.sqr.next()) {
				this.next = new ImsDocumentListElement();
				for (int f = 0; f < this.listFieldNames.length; f++)
					this.next.setAttribute(this.listFieldNames[f], this.sqr.getString(f));
				this.addListFieldValues(this.next);
				return true;
			}
			else {
				this.sqr.close();
				this.sqr = null;
				return false;
			}
		}
		public ImsDocumentListElement getNextDocument() {
			if (!this.hasNextDocument()) return null;
			ImsDocumentListElement next = this.next;
			this.next = null;
			return next;
		}
	}
	
	private int getSelectivity(Properties filter) {
		if ((filter == null) || filter.isEmpty())
			return this.docIdSet.size();
		
		String predicate = this.getDocumentFilter(filter);
		if ("1=1".equals(predicate))
			return this.docIdSet.size();
		
		boolean dd = (predicate.indexOf(" dd.") != -1);
		boolean da = (predicate.indexOf(" da.") != -1);
		String from;
		String joinWhere = "";
		if (dd && da) {
			from = (DOCUMENT_TABLE_NAME + " dd, " + DOCUMENT_ATTRIBUTE_TABLE_NAME + " da");
			joinWhere = (" AND dd." + DOCUMENT_ID_ATTRIBUTE + " = da." + DOCUMENT_ID_ATTRIBUTE + 
					" AND dd." + DOCUMENT_ID_HASH_ATTRIBUTE + " = da." + DOCUMENT_ID_HASH_ATTRIBUTE);
		}
		else if (dd)
			from = (DOCUMENT_TABLE_NAME + " dd");
		else if (da)
			from = (DOCUMENT_ATTRIBUTE_TABLE_NAME + " da");
		else return this.docIdSet.size();
		
		String query = "SELECT count(*)" +
				" FROM " + from +
				" WHERE " + predicate + joinWhere;
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query);
			return (sqr.next() ? Integer.parseInt(sqr.getString(0)) : 0);
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting document list size.");
			System.out.println("  query was " + query);
			return Integer.MAX_VALUE;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	private String getDocumentFilter(Properties filter) {
		if ((filter == null) || filter.isEmpty())
			return "1=1";
		
		StringBuffer whereString = new StringBuffer("1=1");
		for (Iterator fit = filter.keySet().iterator(); fit.hasNext();) {
			String filterName = ((String) fit.next());
			if (!ImsDocumentList.filterableDataFields.contains(filterName) && !this.documentAttributesByName.containsKey(filterName))
				continue;
			
			String filterValue = filter.getProperty(filterName, "").trim();
			if (filterValue.length() == 0)
				continue;
			
			String[] filterValues = filterValue.split("[\\r\\n]++");
			if (filterValues.length == 0)
				continue;
			
			String tableAlias = (this.documentAttributesByName.containsKey(filterName) ? "da." : "dd.");
			if (filterValues.length == 1) {
				if (this.documentAttributesByName.containsKey(filterName) ? ((DocumentAttribute) this.documentAttributesByName.get(filterName)).isInteger : ImsDocumentList.numericDataAttributes.contains(filterName)) {
					//	this should prevent SQL injection, as numeric fields are the only ones whose value is not auto-escaped
					if (filterValues[0].matches("[0-9]++")) {
						String customOperator = filter.getProperty((filterName + "Operator"), ">");
						whereString.append(" AND " + tableAlias + filterName + " " + (ImsDocumentList.numericOperators.contains(customOperator) ? customOperator : ">") + " " + filterValues[0]);
					}
				}
				else whereString.append(" AND " + tableAlias + filterName + " LIKE '" + EasyIO.prepareForLIKE(filterValues[0]) + "'");
			}
			else if (filterValues.length > 1) {
				whereString.append(" AND (");
				for (int v = 0; v < filterValues.length; v++) {
					filterValue = filterValues[v].trim();
					if (v != 0)
						whereString.append(" OR ");
					whereString.append(tableAlias + filterName + " LIKE '" + EasyIO.prepareForLIKE(filterValues[v]) + "'");
				}
				whereString.append(")");
			}
		}
		return whereString.toString();
	}
	
	// timestamp format for log entries
	private static final String DEFAULT_LOGFILE_DATE_FORMAT = "yyyy.MM.dd HH:mm:ss";
	private static final DateFormat LOGFILE_DATE_FORMATTER = new SimpleDateFormat(DEFAULT_LOGFILE_DATE_FORMAT);
	
	/**
	 * write an entry to the log file of this markup process server
	 * @param entry the entry to write
	 */
	private void writeLogEntry(String entry) {
		System.out.println(LOGFILE_DATE_FORMATTER.format(new Date()) + ": " + entry);
	}
}