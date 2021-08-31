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
package de.uka.ipd.idaho.goldenGateServer.ims;

import java.io.BufferedOutputStream;
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
import java.lang.ref.SoftReference;
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
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
import de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.gamta.util.transfer.DocumentListElement;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerEventService;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent.ImsDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.ims.data.ImsDocumentList;
import de.uka.ipd.idaho.goldenGateServer.uaa.UserAccessAuthority;
import de.uka.ipd.idaho.goldenGateServer.util.BufferedLineInputStream;
import de.uka.ipd.idaho.goldenGateServer.util.BufferedLineOutputStream;
import de.uka.ipd.idaho.goldenGateServer.util.IdentifierKeyedDataObjectStore;
import de.uka.ipd.idaho.goldenGateServer.util.IdentifierKeyedDataObjectStore.DataObjectFolder;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.ImSupplement;
import de.uka.ipd.idaho.im.util.ImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.DataBackedImDocument;
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
	
	private static final String DOCUMENT_TABLE_NAME = "GgImsDocuments";
	private static final String DOCUMENT_ATTRIBUTE_TABLE_NAME = "GgImsDocumentAttributes";
	
	/** the column name for the integer hash of document IDs, for faster filtering on joining */
	public static final String DOCUMENT_ID_HASH_ATTRIBUTE = "docIdHash";
	
	private static final int DOCUMENT_NAME_COLUMN_LENGTH = 127;
	
	private Map documentAttributesByName = Collections.synchronizedMap(new TreeMap(String.CASE_INSENSITIVE_ORDER));
	private static class DocumentAttribute {
		String colName;
		int colLength;
		boolean isInteger;
		String[] attribNames;
		DocumentAttribute(String colName, int colLength, String[] attribNames) {
			this.colName = colName;
			this.colLength = colLength;
			this.isInteger = (this.colLength < 1);
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
			String value = this.getValueFor(doc);
			if (value == null)
				return (this.isInteger ? "0" : "");
			if (this.isInteger) {
				value = value.replaceAll("[^0-9]", "");
				return ((value.length() == 0) ? "0" : value);
			}
			else {
				if (value.length() > this.colLength)
					value = value.substring(0, this.colLength);
				return EasyIO.sqlEscape(value);
			}
		}
		
		String getUpdateQueryAssignment(Attributed doc) {
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
				return new DocumentAttribute(dad[0], Integer.parseInt(dad[1]), dad[2].split("\\s+"));
			}
			catch (RuntimeException re /* number format as well as array length */ ) {
				return null;
			}
		}
	}
	
	private int documentListSizeThreshold = 0;
	
	private Set docIdSet = Collections.synchronizedSet(new HashSet());
	private Map docAttributeValueCache = Collections.synchronizedMap(new HashMap());
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
	
	private IdentifierKeyedDataObjectStore iks;
	private boolean showCheckoutUserOnError = false;
	
	private IoProvider io;
	
	private UserAccessAuthority uaa;
	
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
		File documentStorageRoot = (((docFolderName.indexOf(":\\") == -1) && (docFolderName.indexOf(":/") == -1) && !docFolderName.startsWith("/")) ? new File(this.dataPath, docFolderName) : new File(docFolderName));
		this.iks = new IdentifierKeyedDataObjectStore("ImsDocuments", documentStorageRoot, null, this);
		
		//	are we supposed to indicate _who_ checked out a document in an error message?
		this.showCheckoutUserOnError = "true".equals(this.configuration.getSetting("showCheckoutUserOnError", ("" + this.showCheckoutUserOnError)));
		
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
				if (da == null)
					continue;
				if (!da.colName.matches("[a-zA-Z][a-zA-Z0-9\\_]{0,30}"))
					continue;
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
			this.documentListSizeThreshold = Integer.parseInt(this.configuration.getSetting("documentListSizeThreshold", ("" + this.documentListSizeThreshold)));
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
		
		//	pre-fill caches
		ImsDocumentList dl = this.getDocumentList(UserAccessAuthority.SUPERUSER_NAME, false, null, null);
		while (dl.hasNextDocument()) {
			DocumentListElement dle = dl.getNextDocument();
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
		
		//	shut down document folder manager
		this.iks.shutdown();
	}
	
	private static final String SHOW_VERSION_HISTORY_COMMAND = "showVersions";
	private static final String REVERT_DOCUMENT_COMMAND = "revertDoc";
	private static final String CHECKOUT_STATE_COMMAND = "checkoutState";
	private static final String RELEASE_DOCUMENT_COMMAND = "releaseDoc";
	private static final String EXPORT_SOURCE_COMMAND = "exportSource";
	private static final String ISSUE_EVENT_COMMAND = "issueEvent";
	
	/*
	 * (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList();
		ComponentAction ca;
		
		//	list versions of some document (good for testing that feature, in the first place)
		ca = new ComponentActionConsole() {
			private DateFormat updateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			public String getActionCommand() {
				return SHOW_VERSION_HISTORY_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						SHOW_VERSION_HISTORY_COMMAND + " <documentId>",
						"List the version history of a document:",
						"- <documentId>: The ID of the document to list the versions of"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1) {
					ImsDocumentVersionHistory dvh = getDocumentVersionHistory(arguments[0]);
					if (dvh == null) {
						this.reportError(" Invalid document ID '" + arguments[0] + "'.");
						return;
					}
					ImsDocumentVersion[] dvs = dvh.getVersions();
					this.reportResult("There are currently " + dvs.length + " versions of document '" + arguments[0] + "':");
					for (int v = 0; v < dvs.length; v++)
						this.reportResult("  " + dvs[v].version + ((v == 0) ? " (current)" : "") + " by " + dvs[v].updateUser + ", " + this.updateTimeFormat.format(new Date(dvs[v].updateTime)));
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID as the only argument.");
			}
		};
		cal.add(ca);
		
		//	list versions of some document (good for testing that feature, in the first place)
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return REVERT_DOCUMENT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						REVERT_DOCUMENT_COMMAND + " <documentId> <version>",
						"Revert a document to a previous version:",
						"- <documentId>: The ID of the document to revert",
						("- <version>: The number of versions to revert (positive value: absolute version number as from '" + SHOW_VERSION_HISTORY_COMMAND + "', negative number: relative version number)"),
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 2) {
					ImsDocumentVersionHistory dvh = getDocumentVersionHistory(arguments[0]);
					if (dvh == null) {
						this.reportError(" Invalid document ID '" + arguments[0] + "'.");
						return;
					}
					int version;
					try {
						version = Integer.parseInt(arguments[1]);
					}
					catch (NumberFormatException nfe) {
						this.reportError("Invalid version number " + arguments[1]);
						return;
					}
					if (version < 0)
						version = (dvh.getVersion() + version);
					if (version <= 0) {
						this.reportError("Invalid version number " + arguments[1] + " for doc document '" + arguments[0] + "', there are only " + dvh.getVersion() + " versions.");
						this.reportError("Use " + SHOW_VERSION_HISTORY_COMMAND + " to list the existing versions.");
						return;
					}
					ImsDocumentVersion docVersion = dvh.getVersion(version);
					if (docVersion == null) {
						this.reportError("Invalid version number " + arguments[1] + " for doc document '" + arguments[0] + "'.");
						this.reportError("Use " + SHOW_VERSION_HISTORY_COMMAND + " to list the existing versions.");
						return;
					}
					try {
						ImsDocumentData docData = checkoutDocumentAsData(docVersion.updateUser, arguments[0], docVersion.version);
						this.reportResult("Document checked out in version " + docVersion.version);
						updateDocumentFromData(docVersion.updateUser, arguments[0], docData, new EventLogger() {
							public void writeLog(String logEntry) {
								reportResult(" ==> " + logEntry);
							}
						});
						this.reportResult("Document updated (reverted) to " + docVersion.version);
						releaseDocument(docVersion.updateUser, arguments[0]);
						this.reportResult("Document released");
					}
					catch (IOException ioe) {
						this.reportError("Could not revert document '" + arguments[0] + "': " + ioe.getMessage());
						this.reportError(ioe);
					}
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID and the version number.");
			}
		};
		cal.add(ca);
		
		//	show the checkout state of some document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return CHECKOUT_STATE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						CHECKOUT_STATE_COMMAND + " <documentId>",
						"Show the checkout status of a document:",
						"- <documentId>: The ID of the document to check"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1) {
					String checkoutUser = getCheckoutUser(arguments[0]);
					if (checkoutUser == null)
						this.reportError("Invalid document ID '" + arguments[0] + "'.");
					else if ("".equals(checkoutUser))
						this.reportResult("Document '" + arguments[0] + "' is not checked out by anyone.");
					else this.reportResult("Document '" + arguments[0] + "' is currently checked out by '" + checkoutUser + "'.");
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID as the only argument.");
			}
		};
		cal.add(ca);
		
		//	clear the checkout state some document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return RELEASE_DOCUMENT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						RELEASE_DOCUMENT_COMMAND + " <documentId> <checkoutUser>",
						"Clear the checkout status of a document:",
						"- <documentId>: The ID of the document to release",
						"- <checkoutUser>: The user currently holding the lock on the document with the argument ID",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 2) {
					String checkoutUser = getCheckoutUser(arguments[0]);
					if (checkoutUser == null)
						this.reportError("Invalid document ID '" + arguments[0] + "'.");
					else if ("".equals(checkoutUser))
						this.reportError("Document '" + arguments[0] + "' is not checked out by anyone.");
					else if (!checkoutUser.equals(arguments[1]))
						this.reportError("Document '" + arguments[0] + "' is not checked out by '" + arguments[1] + "'.");
					else {
						releaseDocument(arguments[1], arguments[0]);
						this.reportResult("Document '" + arguments[0] + "' released successfully.");
					}
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID and checkout user as the only argument.");
			}
		};
		cal.add(ca);
		
		//	export source of an Image Markup document stored in this IMS
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return EXPORT_SOURCE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						EXPORT_SOURCE_COMMAND + " <documentId> <destFolder>",
						"Export the binary source of a document:",
						"- <documentId>: The ID of the document to export",
						"- <destFolder>: The absolute path of the folder to export to (file name will be 'docName' attribute)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 2) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID and destination folder as the only arguments.");
					return;
				}
				
				//	check export destination before starting all the other hassle
				File destFolder = new File(arguments[1]);
				if (destFolder.exists() && destFolder.isFile()) {
					this.reportError(" Invalid destination folder '" + arguments[1] + "', specify a folder, not a file.");
					return;
				}
				destFolder.mkdirs();
				
				//	load document data and perform export from that
				ImsDocumentData docData = null;
				try {
					docData = getDocumentData(arguments[0], false, true);
					if (docData == null) {
						this.reportError(" Invalid document ID '" + arguments[0] + "'.");
						return;
					}
					this.exportSourceDoc(docData, destFolder);
				}
				catch (IOException ioe) {
					this.reportError(" Error seeking source name of document ID '" + arguments[0] + "': " + ioe.getMessage());
					this.reportError(ioe);
					return;
				}
				finally {
					if (docData != null)
						docData.dispose();
				}
			}
			
			private void exportSourceDoc(ImsDocumentData docData, File destFolder) {
				
				//	get original document name
				String docName;
				try {
					Attributed docAttributes = ImDocumentIO.loadDocumentAttributes(docData);
					docName = ((String) docAttributes.getAttribute(DOCUMENT_NAME_ATTRIBUTE, ImSupplement.SOURCE_TYPE));
				}
				catch (IOException ioe) {
					this.reportError(" Error seeking source name of document ID '" + docData.docId + "': " + ioe.getMessage());
					this.reportError(ioe);
					return;
				}
				
				//	get source input stream and create export destination file
				InputStream docSourceIn = null;
				File destFile = null;
				try {
					ImDocumentEntry[] docEntries = docData.getEntries();
					for (int e = 0; e < docEntries.length; e++)
						if (docEntries[e].name.startsWith(ImSupplement.SOURCE_TYPE)) {
							docSourceIn = docData.getInputStream(docEntries[e]);
							String destFileNameSuffix = docEntries[e].name.substring(docEntries[e].name.lastIndexOf('.'));
							destFile = new File(destFolder, (docData.docId + "." + docName + (docName.endsWith(destFileNameSuffix) ? "" : destFileNameSuffix)));
							break;
						}
					if (docSourceIn == null) {
						this.reportError(" Could not find source of document ID '" + docData.docId + "'.");
						return;
					}
				}
				catch (IOException ioe) {
					this.reportError(" Error exporting source of document ID '" + docData.docId + "': " + ioe.getMessage());
					this.reportError(ioe);
					return;
				}
				
				//	do export, finally
				try {
					OutputStream docSourceOut = new BufferedOutputStream(new FileOutputStream(destFile));
					byte[] buffer = new byte[1024];
					for (int r; (r = docSourceIn.read(buffer, 0, buffer.length)) != -1;)
						docSourceOut.write(buffer, 0, r);
					docSourceOut.flush();
					docSourceOut.close();
					docSourceIn.close();
					destFile.setLastModified(Long.parseLong((String) docData.getAttribute(CHECKIN_TIME_ATTRIBUTE)));
					this.reportResult(" Source of document ID '" + docData.docId + "' exported to '" + destFile.getAbsolutePath() + "'.");
				}
				catch (IOException ioe) {
					this.reportError(" Error exporting source of document ID '" + docData.docId + "' to '" + destFile.getAbsolutePath() + "': " + ioe.getMessage());
					this.reportError(ioe);
				}
			}
		};
		cal.add(ca);
//		
//		//	add event queue monitoring action
//		cal.add(this.eventNotifier.getQueueSizeAction());
		
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
					logWarning("Request for invalid session - " + sessionId);
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
				
				//	TODO use fix registered extensions
				ImsDocumentList docList = getDocumentList(uaa.getUserNameForSession(sessionId), false, filter, null);
				
				output.write(GET_DOCUMENT_LIST);
				output.newLine();

				docList.writeData(output);
				output.newLine();
			}
		};
		cal.add(ca);
//		
//		// list documents TODOne remove this unless still occurring in server logs
//		ca = new ComponentActionNetwork() {
//			public String getActionCommand() {
//				return GET_DOCUMENT_LIST_SHARED;
//			}
//			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
//				
//				// check authentication
//				String sessionId = input.readLine();
//				if (!uaa.isValidSession(sessionId)) {
//					output.write("Invalid session (" + sessionId + ")");
//					output.newLine();
//					logWarning("Request for invalid session - " + sessionId);
//					return;
//				}
//				
//				//	read filter string
//				String filterString = input.readLine();
//				Properties filter;
//				if (filterString.length() == 0)
//					filter = null;
//				else {
//					String[] filters = filterString.split("\\&");
//					filter = new Properties();
//					for (int f = 0; f < filters.length; f++) {
//						String[] pair = filters[f].split("\\=");
//						if (pair.length == 2) {
//							String name = pair[0].trim();
//							String value = URLDecoder.decode(pair[1].trim(), ENCODING).trim();
//							
//							String existingValue = filter.getProperty(name);
//							if (existingValue != null)
//								value = existingValue + "\n" + value;
//							
//							filter.setProperty(name, value);
//						}
//					}
//				}
//				
//				//	TODO_above use fix registered extensions
//				ImsDocumentList docList = getDocumentList(uaa.getUserNameForSession(sessionId), false, filter, null);
//				
//				output.write(GET_DOCUMENT_LIST_SHARED);
//				output.newLine();
//				
//				docList.writeData(output);
//				output.newLine();
//			}
//		};
//		cal.add(ca);
		
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
					logWarning("Request for invalid session - " + sessionId);
					return;
				}
				
				//	get document ID and version
				String docId = input.readLine();
				int version = Integer.parseInt(input.readLine());
				
				//	get document data
				ImsDocumentData docData = getDocumentData(docId, false, (version == 0));
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
				finally {
					docData.dispose();
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
					logWarning("Request for invalid session - " + sessionId);
					return;
				}
				
				//	check permission
				if (!uaa.hasSessionPermission(sessionId, UPDATE_DOCUMENT_PERMISSION, true)) {
					//	TODO also allow checkout with only upload permission if checkin user same as checkout user (document owner)
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
				ImsDocumentData docData = null;
				try {
					
					//	checkout document
					docData = checkoutDocumentAsData(userName, docId, version);
					
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
				
				//	clean up
				finally {
					if (docData != null)
						docData.dispose();
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
					logWarning("Request for invalid session - " + sessionId);
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
				try {
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
				finally {
					docData.dispose();
				}
			}
		};
		cal.add(ca);

		// update a document, or store a new one
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return UPLOAD_DOCUMENT;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				receiveDocument(input, false, output);
			}
		};
		cal.add(ca);
		
		// update a document, or store a new one
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return UPDATE_DOCUMENT;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				receiveDocument(input, true, output);
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
					logWarning("Request for invalid session - " + sessionId);
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
				boolean updateComplete = false;
				for (ZipEntry ze; (ze = zin.getNextEntry()) != null;) {
					if (updateKey.equals(ze.getName())) {
						updateComplete = true;
						break;
					}
					if (MORE_DOCUMENT_ENTRIES.equals(ze.getName()))
						break;
					ImDocumentEntry entry = new ImDocumentEntry(ze);
					OutputStream cacheOut = du.docData.getOutputStream(entry, true);
					for (int r; (r = zin.read(buffer, 0, buffer.length)) != -1;)
						cacheOut.write(buffer, 0, r);
					cacheOut.flush();
					cacheOut.close();
					du.missingDocEntryFileNames.remove(entry.getFileName());
				}
				
				//	update complete, finalize it and send update log
				if (updateComplete) {
					DocumentUpdateProtocol dup = new DocumentUpdateProtocol(du.docData.docId, false);
					finalizeDocumentUpdate(du, dup);
					du.docData.dispose();
					
					output.write(UPDATE_DOCUMENT_ENTRIES);
					output.newLine();
					output.write("Document '" + dup.docName + "' stored as version " + dup.docVersion);
					output.newLine();
					output.flush();
				}
				
				//	more to come, make update key valid again and acknowledge received part
				else {
					updatesByKey.put(updateKey, du);
					
					output.write(MORE_DOCUMENT_ENTRIES);
					output.newLine();
				}
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
					logWarning("Request for invalid session - " + sessionId);
					return;
				}
				
				//	check permission
				if (!uaa.hasSessionPermission(sessionId, DELETE_DOCUMENT_PERMISSION, true)) {
					//	TODO also allow deletion with only upload permission if checkin user same as delete user (document owner)
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
		
		//	TODO add ERASE_DOCUMENT for non-recoverable deletion (no implicit owner permission, and no API or at least no UI integration for now)
		
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
					logWarning("Request for invalid session - " + sessionId);
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
		
		//	issue update events
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return ISSUE_EVENT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						ISSUE_EVENT_COMMAND + " <docId>",
						"Issue an update event for a specific document ID:",
						"- <docId>: the ID of the document to issue an update event for"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1)
					issueEvent(arguments[0], this);
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID as the only argument.");
			}
		};
		cal.add(ca);
		
		//	get actions from document storage manager
		ComponentAction[] iksActions = this.iks.getActions();
		for (int a = 0; a < iksActions.length; a++)
			cal.add(iksActions[a]);
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	private void issueEvent(String docId, ComponentActionConsole cac) {
		StringBuffer query = new StringBuffer("SELECT " + DOCUMENT_ID_ATTRIBUTE + ", " + UPDATE_USER_ATTRIBUTE + ", " + UPDATE_TIME_ATTRIBUTE);
		query.append(" FROM " + DOCUMENT_TABLE_NAME);
		query.append(" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'");
		query.append(" AND " + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() + "");
		
		SqlQueryResult sqr = null;
		int count = 0;
		try {
			sqr = io.executeSelectQuery(query.toString(), true);
			if (sqr.next()) {
				String updateUser = sqr.getString(1);
				long updateTime = sqr.getLong(2);
				ImsDocumentData docData;
				try {
					docData = this.getDocumentAsData(docId);
					docData.setReadOnly(true);
				}
				catch (IOException ioe) {
					cac.reportError("GoldenGateIMS: error loading data for document '" + docId + "': " + ioe.getMessage());
					cac.reportError(ioe);
					return;
				}
				GoldenGateServerEventService.notify(new ImsDocumentEvent(updateUser, updateUser, docId, docData, docData.getDocumentVersion(), GoldenGateIMS.class.getName(), updateTime, new EventLogger() {
					public void writeLog(String logEntry) {}
				}));
				count++;
			}
		}
		catch (SQLException sqle) {
			cac.reportError("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while issuing document update events.");
			cac.reportError("  query was " + query);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		cac.reportResult("Issued update events for " + count + " documents.");
	}
	
	private void receiveDocument(BufferedReader input, boolean isUpdate, BufferedWriter output) throws IOException {

		// check authentication
		String sessionId = input.readLine();
		if (!this.uaa.isValidSession(sessionId)) {
			output.write("Invalid session (" + sessionId + ")");
			output.newLine();
			this.logWarning("Request for invalid session - " + sessionId);
			return;
		}
		
		//	check permission
		if (!this.uaa.hasSessionPermission(sessionId, (isUpdate ? UPDATE_DOCUMENT_PERMISSION : UPLOAD_DOCUMENT_PERMISSION), true)) {
			//	TODO also allow update with only upload permission if checkin user same as update user (document owner)
			output.write("Insufficient permissions for " + (isUpdate ? "updating" : "uploading") + " a document");
			output.newLine();
			return;
		}
		
		//	get session user name
		String sessionUserName = this.uaa.getUserNameForSession(sessionId);
		
		//	get document ID and user to credit
		String docId = input.readLine();
		String userName = input.readLine();
		if (userName.length() == 0)
			userName = sessionUserName;
		
		//	check update preconditions (if document exists, user must have lock)
		if (isUpdate) {
			
			//	check document checkout state
			if (!this.mayUpdateDocument(sessionUserName, docId)) {
				output.write("Document checked out by other user, update not possible.");
				output.newLine();
				return;
			}
		}
		
		//	check upload preconditions (document must not exist as complete version)
		else {
			
			//	get document data, and send error if it already exists
			ImsDocumentData docData = null;
			try {
				docData = this.getDocumentData(docId, false, false);
				if ((docData != null) && docData.isCompleteVersion()) {
					output.write("Document " + docId + " already exists, use update instead");
					output.newLine();
					return;
				}
			}
			finally {
				if (docData != null)
					docData.dispose();
			}
		}
		
		//	get document entry list
		ArrayList docEntries = new ArrayList();
		for (String entryString; (entryString = input.readLine()) != null;) {
			if (entryString.length() == 0)
				break;
			ImDocumentEntry entry = ImDocumentEntry.fromTabString(entryString);
			if (entry != null)
				docEntries.add(entry);
		}
		
		//	get or create document data
		ImsDocumentData docData = this.getDocumentData(docId, true, isUpdate);
		
		//	diff with received entry list
		ArrayList toUpdateDocEntries = new ArrayList();
		HashSet toUploadImDocumentEntryFileNames = new HashSet();
		for (int e = 0; e < docEntries.size(); e++) {
			ImDocumentEntry entry = ((ImDocumentEntry) docEntries.get(e));
			if (docData.hasEntryData(entry))
				docData.putEntry(entry);
			else {
				toUpdateDocEntries.add(entry);
				toUploadImDocumentEntryFileNames.add(entry.getFileName());
			}
		}
		
		//	we already have all the entries (version revert update, or upload or update broke just before finalization)
		if (toUpdateDocEntries.isEmpty()) {
			
			//	finalize update right away
			DocumentUpdateProtocol dup = new DocumentUpdateProtocol(docId, false);
			finalizeDocumentUpdate(new DocumentUpdate(docData, toUploadImDocumentEntryFileNames, (isUpdate ? sessionUserName : null), userName), dup);
			docData.dispose();
			
			//	send back empty update key to indicate protocol is coming right away
			output.write(isUpdate ? UPDATE_DOCUMENT : UPLOAD_DOCUMENT);
			output.newLine();
			output.write("");
			output.newLine();
			output.write("Document '" + dup.docName + "' stored as version " + dup.docVersion);
			output.newLine();
			output.flush();
			
			//	we're done here
			return;
		}
		
		//	generate update key and cache data
		String updateKey = Gamta.getAnnotationID();
		DocumentUpdate update = new DocumentUpdate(docData, toUploadImDocumentEntryFileNames, (isUpdate ? sessionUserName : null), userName);
		this.updatesByKey.put(updateKey, update);
		
		//	send back entire entry list
		output.write(isUpdate ? UPDATE_DOCUMENT : UPLOAD_DOCUMENT);
		output.newLine();
		output.write(updateKey);
		output.newLine();
		for (int e = 0; e < toUpdateDocEntries.size(); e++) {
			output.write(((ImDocumentEntry) toUpdateDocEntries.get(e)).toTabString());
			output.newLine();
		}
		output.flush();
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
		private int docVersion = -1;
		private DataObjectFolder entryDataFolder;
		private boolean isActiveClone = true;
		ImsDocumentData(String docId, DataObjectFolder entryDataFolder) throws IOException {
			super(entryDataFolder);
			this.docId = docId;
			this.entryDataFolder = entryDataFolder;
			this.loadEntries(0); // deleted document might have been restored on folder re-creation
		}
		ImsDocumentData(String docId, DataObjectFolder entryDataFolder, int version) throws IOException {
			super(entryDataFolder);
			this.docId = docId;
			this.entryDataFolder = entryDataFolder;
			if (version != -1)
				this.loadEntries(version);
		}
		
		private void loadEntries(int version) throws IOException {
			
			//	check document entry file
			File docEntryListFile = new File(this.entryDataFolder, ("entries" + ((version == 0) ? "" : ("." + version)) + ".txt"));
			
			//	read entry list
			if (docEntryListFile.exists()) {
				if (version != 0)
					this.docVersion = version;
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
		
		public String getDocumentId() {
			return this.docId;
		}
		
		/**
		 * Get the version number of the document as represented by this data
		 * object.
		 * @return the document version
		 */
		public int getDocumentVersion() {
			if (this.docVersion == -1)
				this.docVersion = getCurrentVersion(this.entryDataFolder);
			return this.docVersion;
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
		public void copyAttributes(Attributed source) { /* we're not doing this general overwrite */ }
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
			int version = getCurrentVersion(this.entryDataFolder);
			
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
		
		ImsDocumentData cloneForVersion(int version) throws IOException {
			
			//	current version requested (we load the entries in that case)
			if (version == 0)
				return this;
			
			//	compute version if argument relative to current one
			if (version < 0)
				version = (getCurrentVersion(this.entryDataFolder) - version);
			
			//	little we can do about this one
			if (version < 0)
				throw new IOException("Invalid version number " + version + " for document ID '" + this.docId + "'");
			
			//	clone for computed version
			this.isActiveClone = false;
			return new ImsDocumentData(this.docId, this.entryDataFolder, version);
		}
		
		ImsDocumentVersionHistory getVersionHistory() throws IOException {
			
			//	create main version history object
			ImsDocumentVersionHistory dvh = new ImsDocumentVersionHistory(this.docId);
			
			//	check if folder even exists
			if (!this.entryDataFolder.exists())
				return null;
			
			//	get document entry lists
			File[] docEntryListFiles = this.entryDataFolder.listFiles(new FileFilter() {
				public boolean accept(File file) {
					return ((file != null) && file.isFile() && file.getName().startsWith("entries.") && file.getName().endsWith(".txt"));
				}
			});
			
			//	no entry lists at all, current version is 0
			if ((docEntryListFiles == null) || (docEntryListFiles.length == 0))
				return null;
			
			//	only updating entry list, current version is still 0
			if ((docEntryListFiles.length == 1) && "entries.updating.txt".equals(docEntryListFiles[0].getName()))
				return null;
			
			//	add past versions
			int currentVersion = 1;
			Attributed versionAttributes = new AbstractAttributed();
			for (int f = 0; f < docEntryListFiles.length; f++) {
				
				//	cut prefix and extension from file name
				String docFileName = docEntryListFiles[f].getName();
				docFileName = docFileName.substring("entries.".length());
				
				//	make sure there's left more than the 'txt' file extension, which will be the case for the most recent version
				if (docFileName.length() <= "txt".length())
					continue;
				
				//	this one's not live just yet
				if (docFileName.startsWith("updating."))
					continue;
				
				//	read provenance attributes from first line
				BufferedReader attributeLineIn = new BufferedReader(new InputStreamReader(new FileInputStream(docEntryListFiles[f]), ENCODING));
				String attributeString = attributeLineIn.readLine();
				ImDocumentIO.setAttributes(versionAttributes, attributeString);
				attributeLineIn.close();
				
				//	store version attributes
				String updateUser = ((String) versionAttributes.getAttribute(UPDATE_USER_ATTRIBUTE));
				String updateTime = ((String) versionAttributes.getAttribute(UPDATE_TIME_ATTRIBUTE));
				versionAttributes.clearAttributes();
				if ((updateUser == null) || (updateTime == null))
					continue;
				
				//	get version number, update current version number, and store version
				try {
					int version = Integer.parseInt(docFileName.substring(0, (docFileName.length() - ".txt".length())));
					currentVersion = Math.max(currentVersion, (version + 1));
					dvh.addVersion(version, updateUser, Long.parseLong(updateTime));
				} catch (NumberFormatException nfe) {}
			}
			
			//	add current version
			String updateUser = ((String) this.getAttribute(UPDATE_USER_ATTRIBUTE));
			String updateTime = ((String) this.getAttribute(UPDATE_TIME_ATTRIBUTE));
			dvh.addVersion(currentVersion, updateUser, Long.parseLong(updateTime));
			
			//	finally ...
			return dvh;
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
		
		public DataBackedImDocument getDocument(ProgressMonitor pm) throws IOException {
			DataBackedImDocument doc = ((this.docRef == null) ? null : ((DataBackedImDocument) this.docRef.get()));
			if (doc == null) {
				doc = super.getDocument(pm);
				this.docRef = new SoftReference(doc);
			}
			return doc;
		}
		private SoftReference docRef;
		
		boolean isCompleteVersion() {
			File docEntryListFile = new File(this.entryDataFolder, "entries.txt");
			if (docEntryListFile.exists())
				return true; // we have a complete current version
			File prevDocEntryListFile = new File(this.entryDataFolder, "entries.0.txt");
			return prevDocEntryListFile.exists(); // we have at least one complete previous version, must be broken update
		}
		
		/**
		 * Obtain a list of all entry files in the document, including the entry
		 * lists of all present versions. The files represent the physical entry
		 * names, thus expose the raw physical content of the document. This is
		 * not meant for modification (all the returned files are non-writable),
		 * but rather for maintenance applications like backup/restore and delta
		 * based replication that includes provenance data.
		 * @return a list of raw entry files in the document
		 */
		public File[] getEntryFiles() {
			return this.entryDataFolder.listFiles();
			//	TODO encapsulate files in some way to prevent writing
			//	TODO ==> maybe overwrite canWrite() method to simply return false
		}
		
		public void dispose() {
			super.dispose();
//			CANNOT DISPOSE DOCUMENT, AS THAT CAUSES STACK OVERFLOW
//			if (this.docRef != null) {
//				ImDocument doc = ((ImDocument) this.docRef.get());
//				if (doc != null)
//					doc.dispose();
//				this.docRef.clear();
//			}
			if (this.isActiveClone) // make sure to not close data folder if we've been cloned
				this.entryDataFolder.close();
		}
	}
	
	private static int getCurrentVersion(DataObjectFolder docFolder) {
		
		//	check if folder even exists
		if (!docFolder.exists())
			return 0;
		
		//	get document entry lists
		File[] docEntryListFiles = docFolder.listFiles(new FileFilter() {
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
			if (docFileName.length() <= "txt".length())
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
	
	/**
	 * The version history of a document stored in this IMS.
	 * 
	 * @author sautter
	 */
	public static class ImsDocumentVersionHistory {
		
		/** the ID of the document the version history belongs to */
		public final String docId;
		
		private TreeMap versions = new TreeMap();
		
		ImsDocumentVersionHistory(String docId) {
			this.docId = docId;
		}
		
		void addVersion(int version, String updateUser, long updateTime) {
			this.versions.put(new Integer(version), new ImsDocumentVersion(this.docId, version, updateUser, updateTime));
		}
		
		/**
		 * Retrieve the name of the last user to update the document.
		 * @return the name of the last user to update the document
		 */
		public String getUpdateUser() {
			ImsDocumentVersion lastVersion = ((ImsDocumentVersion) this.versions.get(this.versions.lastKey()));
			return lastVersion.updateUser;
		}
		
		/**
		 * Retrieve the timestamp of the last update to the document.
		 * @return the timestamp of the last update to the document
		 */
		public long getUpdateTime() {
			ImsDocumentVersion lastVersion = ((ImsDocumentVersion) this.versions.get(this.versions.lastKey()));
			return lastVersion.updateTime;
		}
		
		/**
		 * Retrieve the current version number of the document.
		 * @return the current version number
		 */
		public int getVersion() {
			Integer lastVersion = ((Integer) this.versions.lastKey());
			return lastVersion.intValue();
		}
		
		/**
		 * Retrieve an individual version of the document.
		 * @param version the version number
		 * @return the version
		 */
		public ImsDocumentVersion getVersion(int version) {
			return ((ImsDocumentVersion) this.versions.get(new Integer(version)));
		}
		
		/**
		 * Retrieve the individual versions of the document. The versions in
		 * the returned array are in reverse chronological order.
		 * @return an array holding the versions
		 */
		public ImsDocumentVersion[] getVersions() {
			LinkedList versions = new LinkedList();
			for (Iterator vit = this.versions.keySet().iterator(); vit.hasNext();) {
				ImsDocumentVersion version = ((ImsDocumentVersion) this.versions.get(vit.next()));
				versions.addFirst(version);
			}
			return ((ImsDocumentVersion[]) versions.toArray(new ImsDocumentVersion[versions.size()]));
		}
	}
	
	/**
	 * The version history of a document stored in this IMS.
	 * 
	 * @author sautter
	 */
	public static class ImsDocumentVersion {
		
		/** the ID of the document the version belongs to */
		public final String docId;
		
		/** the sequential number of the version */
		public final int version;
		
		/** the name of the user who created the version */
		public final String updateUser;
		
		/** the timestamp of the version */
		public final long updateTime;
		
		ImsDocumentVersion(String docId, int version, String updateUser, long updateTime) {
			this.docId = docId;
			this.version = version;
			this.updateUser = updateUser;
			this.updateTime = updateTime;
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
		DataObjectFolder docFolder = this.iks.getDataObjectFolder(docId);
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
	 * Upload a new document. In case a document already exists, an exception
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
	public int uploadDocument(String userName, ImDocument doc, EventLogger logger) throws IOException {
		
		// get checkout user (must be null if document is new)
		String checkoutUser = this.getCheckoutUser(doc.docId);
		if (checkoutUser != null)
			throw new IOException("Document already exists, upload not possible.");
		
		//	do update
		return this.doUpdateDocument(userName, null, doc, ((logger == null) ? new DummyEventLogger() : logger));
	}
	
	/**
	 * Update an existing document, or store a new one. In case of an update,
	 * the updating authentication user must have acquired the lock on the
	 * document in question (via one of the checkoutDocument() methods) prior
	 * to the invocation of this method. Otherwise, an IOException will be
	 * thrown. In case of a new document, the lock is automatically granted to
	 * the specified user, and remains with him until he yields it via the
	 * releaseDocument() method. If a lock is not desired for a new document,
	 * use the uploadDocument() method.
	 * @param authUserName the user name holding the checkout lock
	 * @param doc the document to store
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public int updateDocument(String authUserName, ImDocument doc, EventLogger logger) throws IOException {
		return this.updateDocument(authUserName, authUserName, doc, logger);
	}
	
	/**
	 * Update an existing document, or store a new one. In case of an update,
	 * the updating authentication user must have acquired the lock on the
	 * document in question (via one of the checkoutDocument() methods) prior
	 * to the invocation of this method. Otherwise, an IOException will be
	 * thrown. In case of a new document, the lock is automatically granted to
	 * the specified user, and remains with him until he yields it via the
	 * releaseDocument() method. If a lock is not desired for a new document,
	 * use the uploadDocument() method.
	 * @param userName the name of the user doing the update
	 * @param authUserName the user name holding the checkout lock
	 * @param doc the document to store
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public int updateDocument(String userName, String authUserName, ImDocument doc, EventLogger logger) throws IOException {
		
		// check if document checked out
		if (!this.mayUpdateDocument(authUserName, doc.docId))
			throw new IOException("Document checked out by other user, update not possible.");
		
		//	do update
		return this.doUpdateDocument(userName, authUserName, doc, ((logger == null) ? new DummyEventLogger() : logger));
	}
	
	private int doUpdateDocument(String userName, String authUserName, ImDocument doc, final EventLogger logger) throws IOException {
		
		//	get document data
		ImsDocumentData docData = this.storeDocumentData(doc, logger);
		
		//	finalize update
		try {
			return this.finalizeDocumentUpdate(new DocumentUpdate(docData, new HashSet(), authUserName, userName), logger);
		}
		finally {
			if (!(doc instanceof DataBackedImDocument) || (((DataBackedImDocument) doc).getDocumentData() != docData))
				docData.dispose(); // dispose data only if document not bound to it
		}
	}
	
	private synchronized ImsDocumentData storeDocumentData(ImDocument doc, final EventLogger logger) throws IOException {
		
		//	get document data
		ImsDocumentData docData = this.getDocumentData(doc.docId, true, true);
		
		//	store document
		ImDocumentIO.storeDocument(doc, docData, new ProgressMonitor() {
			public void setStep(String step) {
				logger.writeLog(step);
			}
			public void setInfo(String info) {
//				logger.writeLog(info);
			}
			public void setBaseProgress(int baseProgress) {}
			public void setMaxProgress(int maxProgress) {}
			public void setProgress(int progress) {}
		});
		
		//	return document data
		return docData;
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
	public int uploadDocumentFromData(String userName, ImsDocumentData docData, EventLogger logger) throws IOException {
		
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
	 * @param authUserName the user name holding the checkout lock
	 * @param docData the document data object to store
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public int updateDocumentFromData(String authUserName, ImsDocumentData docData, EventLogger logger) throws IOException {
		return this.updateDocumentFromData(authUserName, authUserName, docData, logger);
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
		final long time = System.currentTimeMillis();
		
		int version = this.doFinalizeDocumentUpdate(docData, updateUser, checkoutUser, logger, time);
		
		docData.setReadOnly(true);
		GoldenGateServerEventService.notify(new ImsDocumentEvent(updateUser, checkoutUser, docData.docId, docData, version, GoldenGateIMS.class.getName(), time, logger) {
			public void notificationComplete() {
				if (logger instanceof DocumentUpdateProtocol)
					((DocumentUpdateProtocol) logger).close();
			}
		});
		if (checkoutUser == null) // issue release event if document is not locked and thus free for editing
			GoldenGateServerEventService.notify(new ImsDocumentEvent(updateUser, checkoutUser, docData.docId, null, -1, ImsDocumentEvent.RELEASE_TYPE, GoldenGateIMS.class.getName(), time, null));
		
		return version;
	}
	
	private synchronized int doFinalizeDocumentUpdate(ImsDocumentData docData, String updateUser, String checkoutUser, EventLogger logger, long time) throws IOException {
		
		// get timestamp
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
		int newVersion = docData.storeEntryList(time);
		
		//	get document attributes
		Attributed docAttributes = ImDocumentIO.loadDocumentAttributes(docData);
		
		//	clear cache
		this.documentMetaDataCache.remove(docData.docId);
		
		//	prepare database update
		StringVector assignments = new StringVector();
		
		// check and (if necessary) truncate name
		String docName = ((String) docAttributes.getAttribute(DOCUMENT_NAME_ATTRIBUTE, ""));
		if (docName.length() > DOCUMENT_NAME_COLUMN_LENGTH)
			docName = docName.substring(0, DOCUMENT_NAME_COLUMN_LENGTH);
		assignments.addElement(DOCUMENT_NAME_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docName) + "'");
		
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
				fieldValues.append(", '" + EasyIO.sqlEscape(docName) + "'");
				
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
					this.logError("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while storing new document.");
					this.logError("  query was " + insertQuery);
					throw new IOException(sqle.getMessage());
				}
			}
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while updating existing document.");
			this.logError("  query was " + updateQuery);
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
					this.logError("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while storing new document.");
					this.logError("  query was " + insertQuery);
					throw new IOException(sqle.getMessage());
				}
			}
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while updating existing document.");
			this.logError("  query was " + updateQuery);
			throw new IOException(sqle.getMessage());
		}
		
		//	initialize update protocol
		if (logger instanceof DocumentUpdateProtocol)
			((DocumentUpdateProtocol) logger).setHead(((String) docAttributes.getAttribute(DOCUMENT_NAME_ATTRIBUTE, docData.docId)), newVersion);
		
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
	public void deleteDocument(String userName, String docId, final EventLogger logger) throws IOException {
		if (this.doDeleteDocument(userName, docId, logger))
			GoldenGateServerEventService.notify(new ImsDocumentEvent(userName, docId, GoldenGateIMS.class.getName(), System.currentTimeMillis(), logger) {
				public void notificationComplete() {
					if (logger instanceof DocumentUpdateProtocol)
						((DocumentUpdateProtocol) logger).close();
				}
			});
	}
	private synchronized boolean doDeleteDocument(String userName, String docId, final EventLogger logger) throws IOException {
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
			DocumentListElement dle = this.getMetaData(docId);
			this.io.executeUpdateQuery(deleteQuery);
			this.uncacheDocumentAttributeValues(dle);
			this.checkoutUserCache.remove(docId);
			this.docIdSet.remove(docId);
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
			this.logError("  query was " + deleteQuery);
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
			this.logError("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
			this.logError("  query was " + deleteQuery);
		}
		
		//	(reversibly) delete document data
		this.iks.deleteDataObject(docId);
		
		//	indicate success
		return true;
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
		return ImDocumentIO.loadDocument(docData, new ProgressMonitor() {
			public void setStep(String step) {
				logInfo(step);
			}
			public void setInfo(String info) {
				logDebug(info);
			}
			public void setBaseProgress(int baseProgress) {}
			public void setMaxProgress(int maxProgress) {}
			public void setProgress(int progress) {}
		});
	}
	
	/**
	 * Load a document data object from storage (the most recent version). The
	 * document is not locked, so any attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @return the document with the specified ID
	 * @throws IOException
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
		ImsDocumentData docData = this.getDocumentData(documentId, false, (version == 0));
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
	public ImDocument checkoutDocument(String userName, String documentId, int version) throws IOException {
		
		//	get document data
		ImDocumentData docData = this.checkoutDocumentAsData(userName, documentId, version);
		if (docData == null)
			throw new IOException("Invalid document ID '" + documentId + "'");
		
		//	load document on top of local cache folder
		try {
			return ImDocumentIO.loadDocument(docData, new ProgressMonitor() {
				public void setStep(String step) {
					logInfo(step);
				}
				public void setInfo(String info) {
					logDebug(info);
				}
				public void setBaseProgress(int baseProgress) {}
				public void setMaxProgress(int maxProgress) {}
				public void setProgress(int progress) {}
			});
		}
		catch (IOException ioe) {
			this.logError("GoldenGateIMS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while loading document " + documentId + ".");
			this.logError(ioe);
			this.setCheckoutUser(documentId, "", -1);
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
	public ImsDocumentData checkoutDocumentAsData(String userName, String documentId, int version) throws IOException {
		String checkoutUser = this.getCheckoutUser(documentId);
		
		//	check if document exists
		if (checkoutUser == null)
			throw new IOException("Document does not exist.");
		
		//	check if checkout possible for user
		if (!checkoutUser.equals("") && !checkoutUser.equals(userName))
			throw new DocumentCheckedOutException(this.showCheckoutUserOnError ? checkoutUser : null);
		
		//	check out document
		long checkoutTime = System.currentTimeMillis();
		ImsDocumentData docData = this.doCheckoutDocumentAsData(userName, documentId, version, checkoutTime);
		
		//	log checkout and notify listeners
		this.logInfo("document " + documentId + " checked out by '" + userName + "'.");
		GoldenGateServerEventService.notify(new ImsDocumentEvent(userName, documentId, ImsDocumentEvent.CHECKOUT_TYPE, GoldenGateIMS.class.getName(), checkoutTime));
		
		//	load document on top of local cache folder
		return docData;
	}
	private synchronized ImsDocumentData doCheckoutDocumentAsData(String userName, String documentId, int version, long checkoutTime) throws IOException {
		
		//	mark document as checked out
		checkoutTime = System.currentTimeMillis();
		if (!this.setCheckoutUser(documentId, userName, checkoutTime))
			throw new IOException("Could not acquire checkout lock on document '" + documentId + "'.");
		
		//	get document data
		ImsDocumentData docData = this.getDocumentData(documentId, false, (version == 0));
		if (docData == null)
			throw new IOException("Invalid document ID '" + documentId + "'.");
		
		//	get entry list for argument version
		docData = docData.cloneForVersion(version);
		if (docData == null)
			throw new IOException("Invalid version '" + version + "' for document ID '" + documentId + "'");
		
		//	finally ...
		return docData;
	}
	
	private static class DocumentCheckedOutException extends IOException {
		DocumentCheckedOutException(String checkoutUser) {
			super("Document checked out by " + ((checkoutUser == null) ? "other user" : ("user '" + checkoutUser + "'")) + ", checkout not possible.");
		}
	}
	
	private boolean mayUpdateDocument(String userName, String docId) {
		String checkoutUser = this.getCheckoutUser(docId);
		
		//	document not known so far
		if (checkoutUser == null)
			return true;
		
		//	document checked out by user in question
		if (checkoutUser.equals(userName))
			return true;
		
		//	document checked out by other user
		if (!"".equals(checkoutUser))
			return false;
		
		//	try and check out document for current user
		return this.setCheckoutUser(docId, userName, System.currentTimeMillis());
	}
	
	/**
	 * Release a document. The lock on the document is released, so other users
	 * can check it out again.
	 * @param userName the name of the user holding the lock of the document to
	 *            release
	 * @param docId the ID of the document to release
	 */
	public void releaseDocument(String userName, String docId) {
		String checkoutUser = this.getCheckoutUser(docId);
		
		//	check if document exists
		if (checkoutUser == null)
			return;
		
		//	release document if possible
		if (this.uaa.isAdmin(userName) || checkoutUser.equals(userName)) { // admin user, or user holding the lock
			this.setCheckoutUser(docId, "", -1);
			GoldenGateServerEventService.notify(new ImsDocumentEvent(userName, docId, ImsDocumentEvent.RELEASE_TYPE, GoldenGateIMS.class.getName(), System.currentTimeMillis()));
		}
	}
	
	private static final int checkoutUserCacheSize = 256;
	private Map checkoutUserCache = Collections.synchronizedMap(new LinkedHashMap(checkoutUserCacheSize, .9f, true) {
		protected boolean removeEldestEntry(Entry eldest) {
			return this.size() > checkoutUserCacheSize;
		}
	});
	
	/**
	 * Check if a document with a given ID exists.
	 * @param docId the ID of the document to check
	 * @return true if the document with the specified ID exists
	 */
	public boolean isDocumentAvailable(String docId) {
		return this.iks.isDataObjectAvailable(docId);
	}
	
	/**
	 * Check if a document with a given ID exists and is free for checkout and
	 * update. A document is also editable for a user who already holds the
	 * checkout lock.
	 * @param docId the ID of the document to check
	 * @param userName the user name intending to edit the document
	 * @return true if the document with the specified ID exists and is free
	 *            for editing
	 */
	public boolean isDocumentEditable(String docId, String userName) {
		String checkoutUser = this.getCheckoutUser(docId);
		return ("".equals(checkoutUser) || ((userName != null) && userName.equals(checkoutUser)));
	}
	
	/**
	 * Get the name of the user who has checked out a document with a given ID
	 * and therefore holds the lock for that document.
	 * @param docId the ID of the document in question
	 * @return the name of the user who has checked out the document with the
	 *         specified ID, the empty string if the document is currently not
	 *         checked out by any user, and null if there is no document with
	 *         the specified ID
	 */
	public synchronized String getCheckoutUser(String docId) {
		
		// do cache lookup
		String checkoutUser = ((String) this.checkoutUserCache.get(docId));
		
		// cache hit
		if (checkoutUser != null)
			return checkoutUser;
		
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
			this.logError("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading document checkout user.");
			this.logError("  query was " + query);
			return null;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	/**
	 * Retrieve the attributes of a document, as stored in the archive. There
	 * is no guarantee with regard to the attributes contained in the returned
	 * object. If a document with the specified ID does not exist, this method
	 * returns null.
	 * @param docId the ID of the document
	 * @return an Attributed object holding the attributes of the document with
	 *         the specified ID
	 * @throws IOException
	 */
	public Attributed getDocumentAttributes(String documentId) {
		
		//	load and return attributes
		ImsDocumentData docData = null;
		try {
			docData = this.getDocumentData(documentId, false, true);
			if (docData == null)
				return null;
			Attributed docAttributes = ImDocumentIO.loadDocumentAttributes(docData);
			AttributeUtils.copyAttributes(docData, docAttributes, AttributeUtils.ADD_ATTRIBUTE_COPY_MODE);
			return docAttributes;
		}
		catch (IOException ioe) {
			return null;
		}
		finally {
			if (docData != null)
				docData.dispose();
		}
	}
	
	/**
	 * Retrieve the version history of a document. The returned object contains
	 * the update user and timestamp for each present and past version of the
	 * document. If a document with the specified ID does not exist, this
	 * method returns null.
	 * @param docId the ID of the document
	 * @return an holding the version history of the document with the
	 *         specified ID
	 * @throws IOException
	 */
	public ImsDocumentVersionHistory getDocumentVersionHistory(String documentId) {
		
		//	load and return update users
		ImsDocumentData docData = null;
		try {
			docData = this.getDocumentData(documentId, false, true);
			if (docData == null)
				return null;
			return docData.getVersionHistory();
		}
		catch (IOException ioe) {
			return null;
		}
		finally {
			if (docData != null)
				docData.dispose();
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
	public DocumentListElement getMetaData(String docId) {
		
		//	do cache lookup
		DocumentListElement dle = ((DocumentListElement) this.documentMetaDataCache.get(docId));
		if (dle != null)
			return dle;
		
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
				" WHERE dd." + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
				" AND dd." + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() +
				" AND da." + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
				" AND da." + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() +
				";";
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query.toString());
			if (sqr.next()) {
				dle = new DocumentListElement();
				for (int f = 0; f < fieldNames.size(); f++)
					dle.setAttribute(fieldNames.get(f), sqr.getString(f));
				this.documentMetaDataCache.put(docId, dle);
				return dle;
			}
			else return null;
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while reading meta data for document " + docId + ".");
			this.logError("  query was " + query);
			return null;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	private static final int documentMetaDataCacheSize = 256;

	private Map documentMetaDataCache = Collections.synchronizedMap(new LinkedHashMap(documentMetaDataCacheSize, .9f, true) {
		protected boolean removeEldestEntry(Entry eldest) {
			return this.size() > documentMetaDataCacheSize;
		}
	});
	
	private synchronized boolean setCheckoutUser(String docId, String checkoutUser, long checkoutTime) {
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
			return true;
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while setting checkout user for document " + docId + ".");
			this.logError("  query was " + updateQuery);
			return false;
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
			return new SqrDocumentList(fieldNames.toStringArray(), sqr, this.docIdSet.size(), null);
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents.");
			this.logError("  query was " + query);
			
			// return dummy list
			return new ImsDocumentList(fieldNames.toStringArray()) {
				public boolean hasNextDocument() {
					return false;
				}
				public DocumentListElement getNextDocument() {
					return null;
				}
			};
		}
	}
	
	/**
	 * An extension to the document list, specifying a table to join in, which
	 * way to join it (inner or left join), which attributes to load from the
	 * additional table. Joins will always be equi-joins on the document ID and
	 * ID hash columns, which the extension table is mandated to provide.
	 * 
	 * @author sautter
	 */
	public static abstract class DocumentListExtension {
		//	TODO allow for fixed registration of left join extensions (most likely from linkInit())
		final String tableName;
		final boolean innerJoin;
		final String[] columnNames;
		
		/** Constructor
		 * @param tableName the name of the extension table to join
		 * @param innerJoin use an inner join that will filter the list?
		 * @param columnNames the names of the columns / attributes to add
		 */
		protected DocumentListExtension(String tableName, boolean innerJoin, String[] columnNames) {
			this.tableName = tableName;
			this.innerJoin = innerJoin;
			this.columnNames = columnNames;
		}
		
		/**
		 * Obtain the (estimated) number of result rows. This is relevant only
		 * for inner join extensions that naturally act as filters to the
		 * document list.
		 * @return the (estimated) number of result rows 
		 */
		public abstract int getSelectivity();
		
		/**
		 * Indicate whether or not to create a summary of the values of a given
		 * field in the document list. This method is checked disjunctively
		 * across all extensions, so implementations should make sure to return
		 * true only for the fields they add themselves.
		 * @param columnName the name of the list fields
		 * @return true if values of the argument list fields should not be summarized
		 */
		public abstract boolean hasNoSummary(String columnName);
		
		/**
		 * Indicate whether or not the values of a given list fields are numeric.
		 * This method is checked disjunctively across all extensions, so
		 * implementations should make sure to return true only for the fields
		 * they add themselves.
		 * @param columnName the name of the list fields
		 * @return true if values of the argument list fields are numeric
		 */
		public abstract boolean isNumeric(String columnName);
		
		/**
		 * Indicate whether or not a given list fields can be used to filter in
		 * a downstream request. This method is checked disjunctively across
		 * all extensions, so implementations should make sure to return true
		 * only for the fields they add themselves.
		 * @param columnName the name of the list fields
		 * @return true if the argument list fields is suitable for filtering
		 */
		public abstract boolean isFilterable(String columnName);
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
		return this.getDocumentList("", false, null, null);
	}
	
	/**
	 * Retrieve a list of meta data for the document available through this IMS.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. The
	 * list only includes documents that can be checked out, i.e., ones that are
	 * not checked out. Use getDocumentListFull() for retrieving a comprehensive
	 * list of documents available, regardless of their checkout state.
	 * @param extensions an array of extensions to join into the document list
	 * @return a list of meta data for the document available through this IMS
	 */
	public ImsDocumentList getDocumentList(DocumentListExtension[] extensions) {
		return this.getDocumentList("", false, null, extensions);
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
		return this.getDocumentList("", false, null, null);
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
		return this.getDocumentList(userName, false, null, null);
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
		return this.getDocumentList(userName, headOnly, null, null);
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
		return this.getDocumentList("", false, filter, null);
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
		return this.getDocumentList("", false, filter, null);
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
		return this.getDocumentList(userName, false, filter, null);
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
		return this.getDocumentList(userName, headOnly, filter, null);
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
	 * @param extensions an array of extensions to join into the document list
	 * @return a list of meta data for the document available through this IMS
	 */
	public ImsDocumentList getDocumentList(String userName, boolean headOnly, Properties filter, DocumentListExtension[] extensions) {
		
		//	check extensions
		if (extensions == null)
			extensions = new DocumentListExtension[0];
		
		//	get user status
		boolean isAdmin = this.uaa.isAdmin(userName);
		
		//	reduce filter
		if (filter != null) {
			Properties rFilter = new Properties();
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
				if (filterValues.length == 1) {
					if (this.documentAttributesByName.containsKey(filterName) ? ((DocumentAttribute) this.documentAttributesByName.get(filterName)).isInteger : ImsDocumentList.numericDataAttributes.contains(filterName)) {
						//	this should prevent SQL injection, as numeric fields are the only ones whose value is not auto-escaped
						if (filterValues[0].matches("[0-9]++")) {
							rFilter.setProperty(filterName, filterValue);
							String customOperator = filter.getProperty((filterName + "Operator"));
							if ((customOperator != null) && ImsDocumentList.numericOperators.contains(customOperator))
								rFilter.setProperty((filterName + "Operator"), customOperator);
							else rFilter.setProperty((filterName + "Operator"), ">");
						}
					}
					else rFilter.setProperty(filterName, filterValue);
				}
				else if (filterValues.length > 1)
					rFilter.setProperty(filterName, filterValue);
			}
			filter = (rFilter.isEmpty() ? null : rFilter);
		}
		
		// estimate list size
		final int selectivity = this.getSelectivity(filter, extensions);
		
		//	set up collecting table names and join conditions
		StringBuffer tableNames = new StringBuffer(DOCUMENT_TABLE_NAME + " dd");
		TreeSet allTableNames = new TreeSet(String.CASE_INSENSITIVE_ORDER);
		allTableNames.add(DOCUMENT_TABLE_NAME);
		StringBuffer joinWhere = new StringBuffer("1=1");
		
		//	collect field names
		StringVector fieldNames = new StringVector();
		TreeSet allFieldNames = new TreeSet(String.CASE_INSENSITIVE_ORDER);
		for (int f = 0; f < documentDataFields.length; f++) {
			if (allFieldNames.add(documentDataFields[f]))
				fieldNames.addElement("dd." + documentDataFields[f]);
		}
		if (isAdmin)
			for (int f = 0; f < documentDataFieldsAdmin.length; f++) {
				if (allFieldNames.add(documentDataFieldsAdmin[f]))
					fieldNames.addElement("dd." + documentDataFieldsAdmin[f]);
			}
		if (this.documentAttributesByName.size() != 0) {
			tableNames.append(", " + DOCUMENT_ATTRIBUTE_TABLE_NAME + " da");
			allTableNames.add(DOCUMENT_ATTRIBUTE_TABLE_NAME);
			joinWhere.append(" AND dd." + DOCUMENT_ID_HASH_ATTRIBUTE + " = da." + DOCUMENT_ID_HASH_ATTRIBUTE);
			joinWhere.append(" AND dd." + DOCUMENT_ID_ATTRIBUTE + " = da." + DOCUMENT_ID_ATTRIBUTE);
			for (Iterator dacnit = this.documentAttributesByName.keySet().iterator(); dacnit.hasNext();) {
				DocumentAttribute da = ((DocumentAttribute) this.documentAttributesByName.get(dacnit.next()));
				if (allFieldNames.add(da.colName))
					fieldNames.addElementIgnoreDuplicates("da." + da.colName);
			}
		}
		for (int e = 0; e < extensions.length; e++) {
			if (!extensions[e].tableName.matches("[a-zA-Z][a-zA-Z0-9\\_]{0,30}"))
				continue;
			if (!allTableNames.add(extensions[e].tableName))
				continue;
			boolean extensionEmpty = true;
			for (int c = 0; c < extensions[e].columnNames.length; c++) {
				if (!extensions[e].columnNames[c].matches("[a-zA-Z][a-zA-Z0-9\\_]{0,30}"))
					continue;
				if (allFieldNames.add(extensions[e].columnNames[c])) {
					fieldNames.addElementIgnoreDuplicates("ext" + e + "." + extensions[e].columnNames[c]);
					extensionEmpty = false;
				}
			}
			if (extensionEmpty)
				continue; // no actual fields we'd need the table for
			tableNames.append(", " + extensions[e].tableName + " ext" + e);
			if (extensions[e].innerJoin) {
				joinWhere.append(" AND dd." + DOCUMENT_ID_HASH_ATTRIBUTE + " = ext" + e + "." + DOCUMENT_ID_HASH_ATTRIBUTE);
				joinWhere.append(" AND dd." + DOCUMENT_ID_ATTRIBUTE + " = ext" + e + "." + DOCUMENT_ID_ATTRIBUTE);
			}
			else {
				joinWhere.append(" AND ((dd." + DOCUMENT_ID_HASH_ATTRIBUTE + " = ext" + e + "." + DOCUMENT_ID_HASH_ATTRIBUTE);
				joinWhere.append(" AND dd." + DOCUMENT_ID_ATTRIBUTE + " = ext" + e + "." + DOCUMENT_ID_ATTRIBUTE + ")");
				joinWhere.append(" OR ext" + e + "." + DOCUMENT_ID_ATTRIBUTE + " IS NULL)");
			}
		}
		
		//	head only, or list too large for regular user, return empty list
		if ((headOnly) || (!isAdmin && (0 < this.documentListSizeThreshold) && (this.documentListSizeThreshold < selectivity))) {
			for (int f = 0; f < fieldNames.size(); f++) {
				String fn = fieldNames.get(f);
				if (fn.indexOf(".") != -1)
					fieldNames.setElementAt(fn.substring(fn.indexOf(".") + ".".length()), f);
			}
			final DocumentListExtension[] fExtensions = extensions;
			ImsDocumentList dl = new ImsDocumentList(fieldNames.toStringArray()) {
				public boolean hasNextDocument() {
					return false;
				}
				public DocumentListElement getNextDocument() {
					return null;
				}
				public AttributeSummary getListFieldValues(String listFieldName) {
					return GoldenGateIMS.this.getListFieldSummary(listFieldName, true);
				}
				public boolean hasNoSummary(String listFieldName) {
					if (super.hasNoSummary(listFieldName))
						return true;
					for (int e = 0; e < fExtensions.length; e++) {
						if (fExtensions[e].hasNoSummary(listFieldName))
							return true;
					}
					return false;
				}
				public boolean isNumeric(String listFieldName) {
					if (super.isNumeric(listFieldName))
						return true;
					for (int e = 0; e < fExtensions.length; e++) {
						if (fExtensions[e].isNumeric(listFieldName))
							return true;
					}
					return false;
				}
				public boolean isFilterable(String listFieldName) {
					if (super.isFilterable(listFieldName))
						return true;
					for (int e = 0; e < fExtensions.length; e++) {
						if (fExtensions[e].isFilterable(listFieldName))
							return true;
					}
					return false;
				}
			};
			return dl;
		}
		
		//	assemble query
//		String query = "SELECT " + fieldNames.concatStrings(", ") + 
//				" FROM " + DOCUMENT_TABLE_NAME + " dd, " + DOCUMENT_ATTRIBUTE_TABLE_NAME + " da" +
//				" WHERE dd." + DOCUMENT_ID_ATTRIBUTE + " = da." + DOCUMENT_ID_ATTRIBUTE + 
//				" AND dd." + DOCUMENT_ID_HASH_ATTRIBUTE + " = da." + DOCUMENT_ID_HASH_ATTRIBUTE + 
//				" AND " + this.getDocumentFilter(filter) +
//				// filter out documents checked out by other user (if not admin)
//				(isAdmin ? "" : (" AND ((" + CHECKOUT_TIME_ATTRIBUTE + " = -1) OR (" + CHECKOUT_USER_ATTRIBUTE + " LIKE '" + EasyIO.prepareForLIKE(userName) + "'))")) +
//				" ORDER BY " + DOCUMENT_NAME_ATTRIBUTE + 
//				";";
		String query = "SELECT " + fieldNames.concatStrings(", ") + 
				" FROM " + tableNames.toString() +
				" WHERE " + joinWhere.toString() + 
				" AND " + this.getDocumentFilter(filter) +
				// filter out documents checked out by other user (if not admin)
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
			return new SqrDocumentList(fieldNames.toStringArray(), sqr, selectivity, extensions);
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents.");
			this.logError("  query was " + query);
			
			//	return dummy list
			return new ImsDocumentList(fieldNames.toStringArray()) {
				public boolean hasNextDocument() {
					return false;
				}
				public DocumentListElement getNextDocument() {
					return null;
				}
			};
		}
	}
	
	private static class SqrDocumentList extends ImsDocumentList {
		private SqlQueryResult sqr;
		private int docCount;
		private DocumentListElement next = null;
		private DocumentListExtension[] extensions;
		SqrDocumentList(String[] listFieldNames, SqlQueryResult sqr, int docCount, DocumentListExtension[] extensions) {
			super(listFieldNames);
			this.sqr = sqr;
			this.docCount = docCount;
			this.extensions = extensions;
		}
		public int getDocumentCount() {
			return this.docCount;
		}
		public boolean hasNextDocument() {
			if (this.next != null) return true;
			else if (this.sqr == null) return false;
			else if (this.sqr.next()) {
				this.next = new DocumentListElement();
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
		public DocumentListElement getNextDocument() {
			if (!this.hasNextDocument()) return null;
			DocumentListElement next = this.next;
			this.next = null;
			return next;
		}
		public boolean hasNoSummary(String listFieldName) {
			if (super.hasNoSummary(listFieldName))
				return true;
			if (this.extensions == null)
				return false;
			for (int e = 0; e < this.extensions.length; e++) {
				if (this.extensions[e].hasNoSummary(listFieldName))
					return true;
			}
			return false;
		}
		public boolean isNumeric(String listFieldName) {
			if (super.isNumeric(listFieldName))
				return true;
			if (this.extensions == null)
				return false;
			for (int e = 0; e < this.extensions.length; e++) {
				if (this.extensions[e].isNumeric(listFieldName))
					return true;
			}
			return false;
		}
		public boolean isFilterable(String listFieldName) {
			if (super.isFilterable(listFieldName))
				return true;
			if (this.extensions == null)
				return false;
			for (int e = 0; e < this.extensions.length; e++) {
				if (this.extensions[e].isFilterable(listFieldName))
					return true;
			}
			return false;
		}
	}
	
	private int getSelectivity(Properties filter, DocumentListExtension[] extensions) {
		int extSelectivity = Integer.MAX_VALUE;
		for (int e = 0; e < extensions.length; e++) {
			if (extensions[e].innerJoin)
				extSelectivity = Math.min(extSelectivity, extensions[e].getSelectivity());
		}
		if (extSelectivity < Integer.MAX_VALUE)
			return extSelectivity;
		
		if ((filter == null) || filter.isEmpty())
			return this.docIdSet.size();
		
		if (filter.size() == 1) {
			String filterField = ((String) filter.keySet().iterator().next());
			String filterValue = filter.getProperty(filterField);
			if (this.docAttributeValueCache.containsKey(filterField))
				return this.getListFieldSummary(filterField, false).getCount(filterValue);
		}
		
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
				" WHERE " + predicate + joinWhere +
				";";
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query);
			return (sqr.next() ? Integer.parseInt(sqr.getString(0)) : 0);
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting document list size.");
			this.logError("  query was " + query);
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
}