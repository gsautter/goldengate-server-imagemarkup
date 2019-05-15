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
package de.uka.ipd.idaho.goldenGateServer.ime;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.util.DocumentErrorProtocol;
import de.uka.ipd.idaho.gamta.util.DocumentErrorProtocol.DocumentError;
import de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP;
import de.uka.ipd.idaho.goldenGateServer.ime.data.ImeDocumentErrorSummary;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS.DocumentListExtension;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent.ImsDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.ims.data.ImsDocumentList;
import de.uka.ipd.idaho.goldenGateServer.uaa.UserAccessAuthority;
import de.uka.ipd.idaho.im.ImSupplement;
import de.uka.ipd.idaho.im.util.ImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentErrorProtocol;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * @author sautter
 *
 */
public class GoldenGateIME extends GoldenGateAEP implements GoldenGateImeConstants {
	private static final String DOCUMENT_ERROR_TABLE_NAME = "GgImeDocumentErrors";
	private static final String DOCUMENT_ID_HASH_ATTRIBUTE = "docIdHash";
	
	private IoProvider io;
	
	private GoldenGateIMS ims;
	
	private UserAccessAuthority uaa;
	
	/** Constructor passing 'IME' as the letter code to super constructor
	 */
	public GoldenGateIME() {
		super("IME");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#getEventProcessorName()
	 */
	protected String getEventProcessorName() {
		return "ErrorProtocolExporter";
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#initComponent()
	 */
	protected void initComponent() {
		
		//	initialize super class
		super.initComponent();
		
		//	connect to database
		this.io = this.host.getIoProvider();
		if (this.io == null)
			throw new RuntimeException("GoldenGateIMS: Cannot work without database access");
		
		//	create/update document table
		TableDefinition dtd = new TableDefinition(DOCUMENT_ERROR_TABLE_NAME);
		dtd.addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		dtd.addColumn(DOCUMENT_ID_HASH_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		dtd.addColumn(ERROR_COUNT_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		dtd.addColumn(ERROR_CATEGORY_COUNT_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		dtd.addColumn(ERROR_TYPE_COUNT_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		dtd.addColumn(BLOCKER_ERROR_COUNT_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		dtd.addColumn(CRITICAL_ERROR_COUNT_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		dtd.addColumn(MAJOR_ERROR_COUNT_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		dtd.addColumn(MINOR_ERROR_COUNT_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		if (!this.io.ensureTable(dtd, true))
			throw new RuntimeException("GoldenGateIMS: Cannot work without database access.");
		
		//	index main table
		this.io.indexColumn(DOCUMENT_ERROR_TABLE_NAME, DOCUMENT_ID_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_ERROR_TABLE_NAME, DOCUMENT_ID_HASH_ATTRIBUTE);
	}
	
	/*
Build GoldenGateIME (Image Markup Error Handler):
- store error protocol summary on IMS updates (in database table: document ID, document ID hash, update time, error category, category label, error type, type label, error severity, number of errors in type)
- provide list of documents with errors:
  - total number of errors
  - list of error categories & types (also for filtering)
  - check whether or not we can reuse IMS document list here (at very least, we can use modified copy)
	 */
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
	 */
	public void link() {
		
		//	link up to user access authority
		this.uaa = ((UserAccessAuthority) this.host.getServerComponent(UserAccessAuthority.class.getName()));
		
		//	check success
		if (this.uaa == null) throw new RuntimeException(UserAccessAuthority.class.getName());
		
		//	link up to IMS
		this.ims = ((GoldenGateIMS) this.host.getServerComponent(GoldenGateIMS.class.getName()));
		
		//	check success
		if (this.ims == null) throw new RuntimeException(GoldenGateIMS.class.getName());
	}
//	
//	private Set imsUpdatedDocIDs = Collections.synchronizedSet(new HashSet());
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#linkInit()
	 */
	public void linkInit() {
		
		//	listen on IMS updates
		this.ims.addDocumentEventListener(new ImsDocumentEventListener() {
			public void documentCheckedOut(ImsDocumentEvent ide) {}
			public void documentUpdated(ImsDocumentEvent ide) {
				if (ide.sourceClassName.equals(GoldenGateIMS.class.getName())) {
					try {
						updateDocumentErrors(ide.documentId, ide.documentData);
					}
					catch (IOException ioe) {
						logError("Error updating error data of document '" + ide.documentId + "': " + ioe.getMessage());
						logError(ioe);
					}
					//	TODOne do this synchronously (it's fast, and more accurate for user)
//					if (ide.documentData.hasEntryData(errorProtocolSupplementName))
//						imsUpdatedDocIDs.add(ide.documentId);
//					else imsUpdatedDocIDs.remove(ide.documentId);
				}
			}
			public void documentDeleted(ImsDocumentEvent ide) {
				deleteDocumentErrors(ide.documentId);
//				//	TODOne do this synchronously (it's fast, and more accurate for user)
//				dataDeleted(ide.documentId, ide.user);
			}
			public void documentReleased(ImsDocumentEvent ide) {
//				//	TODOne no need for this any more if we update synchronously (it's fast, and more accurate for user)
//				if (ide.sourceClassName.equals(GoldenGateIMS.class.getName())) {
//					if (imsUpdatedDocIDs.remove(ide.documentId))
//						dataUpdated(ide.documentId, (ide.version == 1), ide.user);
//				}
			}
		});
		
		// 	linked initialize super class
		super.linkInit();
	}
	
	/*
	 * (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(super.getActions()));
		ComponentAction ca;
		
		//	list documents with errors
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
				
				//	send document list
				ImsDocumentList docList = getDocumentList(uaa.getUserNameForSession(sessionId), filter);
				output.write(GET_DOCUMENT_LIST);
				output.newLine();
				docList.writeData(output);
				output.newLine();
			}
		};
		cal.add(ca);

		//	get error summary
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_ERROR_SUMMARY;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					return;
				}
				
				//	get document ID
				String docId = input.readLine();
				
				//	get document data
				ImDocumentData docData;
				try {
					docData = ims.getDocumentAsData(docId);
				}
				catch (IOException ioe) {
					output.write(ioe.getMessage());
					output.newLine();
					return;
				}
				
				//	do we have an error protocol
				ImSupplement eps = docData.getSupplement(ImDocumentErrorProtocol.errorProtocolSupplementName);
				if (eps == null) {
					output.write("Error protocol unavailable");
					output.newLine();
					return;
				}
				
				//	load error protocol into summary
				ImeDocumentErrorSummary ides = new ImeDocumentErrorSummary(docId);
				DocumentErrorProtocol.fillErrorProtocol(ides, null, eps.getInputStream());
				
				//	send error summary
				output.write(GET_ERROR_SUMMARY);
				output.newLine();
				ImeDocumentErrorSummary.storeErrorSummary(ides, output);
				output.flush();
			}
		};
		cal.add(ca);
		
		//	get whole error protocol
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_ERROR_PROTOCOL;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					return;
				}
				
				//	get document ID
				String docId = input.readLine();
				
				//	get document data
				ImDocumentData docData;
				try {
					docData = ims.getDocumentAsData(docId);
				}
				catch (IOException ioe) {
					output.write(ioe.getMessage());
					output.newLine();
					return;
				}
				
				//	do we have an error protocol
				ImSupplement eps = docData.getSupplement(ImDocumentErrorProtocol.errorProtocolSupplementName);
				if (eps == null) {
					output.write("Error protocol unavailable");
					output.newLine();
					return;
				}
				
				//	get error protocol input stream
				BufferedReader epBr = new BufferedReader(new InputStreamReader(eps.getInputStream(), "UTF-8"));
				
				//	send error protocol
				output.write(GET_ERROR_PROTOCOL);
				output.newLine();
				char[] buffer = new char[1024];
				for (int r; (r = epBr.read(buffer, 0, buffer.length)) != -1;)
					output.write(buffer, 0, r);
				output.flush();
			}
		};
		cal.add(ca);
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#doUpdate(java.lang.String, java.lang.String, java.util.Properties, long)
	 */
	protected void doUpdate(String dataId, String user, Properties dataAttributes, long params) throws Exception {
		this.updateDocumentErrors(dataId, this.ims.getDocumentAsData(dataId));
	}
	
	void updateDocumentErrors(String dodId, ImDocumentData docData) throws IOException {
		this.logInfo("GoldenGateIME: Getting error protocol for " + dodId);
		
		//	get error protocol supplement
		ImSupplement eps = docData.getSupplement(ImDocumentErrorProtocol.errorProtocolSupplementName);
		if (eps == null) {
			this.logInfo(" ==> supplement not found, cleaning up");
			this.deleteDocumentErrors(dodId);
			return;
		}
		this.logInfo(" - got supplement");
		
		//	load error protocol
		ImeDocumentErrorSummary ides = new ImeDocumentErrorSummary(dodId);
		InputStream epin = eps.getInputStream();
		DocumentErrorProtocol.fillErrorProtocol(ides, null, epin);
		epin.close();
		this.logInfo(" - protocol summary loaded");
		
		//	no error protocol at all, or errors to indicate, we don't need this one any longer
		if (ides.getErrorCount() == 0) {
			this.logInfo(" ==> no errors in protocol, cleaning up");
			this.deleteDocumentErrors(dodId);
			return;
		}
		this.logInfo(" - protocol contains " + ides.getErrorCount() + " errors");
		
		//	prepare database update
		StringVector assignments = new StringVector();
		
		// get error counts
		assignments.addElement(ERROR_COUNT_ATTRIBUTE + " = " + ides.getErrorCount());
		assignments.addElement(ERROR_CATEGORY_COUNT_ATTRIBUTE + " = " + ides.getErrorCategoryCount());
		assignments.addElement(ERROR_TYPE_COUNT_ATTRIBUTE + " = " + ides.getErrorTypeCount());
		assignments.addElement(BLOCKER_ERROR_COUNT_ATTRIBUTE + " = " + ides.getErrorSeverityCount(DocumentError.SEVERITY_BLOCKER));
		assignments.addElement(CRITICAL_ERROR_COUNT_ATTRIBUTE + " = " + ides.getErrorSeverityCount(DocumentError.SEVERITY_CRITICAL));
		assignments.addElement(MAJOR_ERROR_COUNT_ATTRIBUTE + " = " + ides.getErrorSeverityCount(DocumentError.SEVERITY_MAJOR));
		assignments.addElement(MINOR_ERROR_COUNT_ATTRIBUTE + " = " + ides.getErrorSeverityCount(DocumentError.SEVERITY_MINOR));
		
		// write new values
		String updateQuery = ("UPDATE " + DOCUMENT_ERROR_TABLE_NAME + 
				" SET " + assignments.concatStrings(", ") + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(dodId) + "'" +
				" AND " + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + dodId.hashCode() + "" +
				";");
		try {
			
			// update did not affect any rows ==> new document
			if (this.io.executeUpdateQuery(updateQuery) == 0) {
				
				// gather complete data for creating master table record
				StringBuffer fields = new StringBuffer(DOCUMENT_ID_ATTRIBUTE);
				StringBuffer fieldValues = new StringBuffer("'" + EasyIO.sqlEscape(dodId) + "'");
				fields.append(", " + DOCUMENT_ID_HASH_ATTRIBUTE);
				fieldValues.append(", " + dodId.hashCode());
				
				// set error counts
				fields.append(", " + ERROR_COUNT_ATTRIBUTE);
				fieldValues.append(", " + ides.getErrorCount());
				fields.append(", " + ERROR_CATEGORY_COUNT_ATTRIBUTE);
				fieldValues.append(", " + ides.getErrorCategoryCount());
				fields.append(", " + ERROR_TYPE_COUNT_ATTRIBUTE);
				fieldValues.append(", " + ides.getErrorTypeCount());
				fields.append(", " + BLOCKER_ERROR_COUNT_ATTRIBUTE);
				fieldValues.append(", " + ides.getErrorSeverityCount(DocumentError.SEVERITY_BLOCKER));
				fields.append(", " + CRITICAL_ERROR_COUNT_ATTRIBUTE);
				fieldValues.append(", " + ides.getErrorSeverityCount(DocumentError.SEVERITY_CRITICAL));
				fields.append(", " + MAJOR_ERROR_COUNT_ATTRIBUTE);
				fieldValues.append(", " + ides.getErrorSeverityCount(DocumentError.SEVERITY_MAJOR));
				fields.append(", " + MINOR_ERROR_COUNT_ATTRIBUTE);
				fieldValues.append(", " + ides.getErrorSeverityCount(DocumentError.SEVERITY_MINOR));
				
				// store data in collection main table
				String insertQuery = "INSERT INTO " + DOCUMENT_ERROR_TABLE_NAME + 
						" (" + fields.toString() + ")" +
						" VALUES" +
						" (" + fieldValues.toString() + ")" +
						";";
				try {
					this.io.executeUpdateQuery(insertQuery);
					this.logInfo(" ==> database entry created");
				}
				catch (SQLException sqle) {
					System.out.println("GoldenGateIME: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while storing new document error data.");
					System.out.println("  query was " + insertQuery);
					throw new IOException(sqle.getMessage());
				}
			}
			else this.logInfo(" ==> database entry updated");
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateIME: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while updating existing document error data.");
			System.out.println("  query was " + updateQuery);
			throw new IOException(sqle.getMessage());
		}
		
		//	TODO also convert error protocol to XML and update own stats
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#doDelete(java.lang.String, java.lang.String, java.util.Properties, long)
	 */
	protected void doDelete(String dataId, String user, Properties dataAttributes, long params) throws Exception {
		this.deleteDocumentErrors(dataId);
	}
	
	void deleteDocumentErrors(String docId) {
		this.logInfo("GoldenGateIME: Cleaning up error protocol for " + docId);
		
		// delete error data
		String deleteQuery = "DELETE FROM " + DOCUMENT_ERROR_TABLE_NAME + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
				" AND " + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() +
				";";
		try {
			this.io.executeUpdateQuery(deleteQuery);
			this.logInfo(" ==> database entry deleted");
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateIME: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document error data.");
			System.out.println("  query was " + deleteQuery);
		}
		
		//	TODO also convert error protocol to XML and update own stats
	}
	
	ImsDocumentList getDocumentList(String userName, Properties filter) {
		DocumentListExtension[] imeExtension = {new ImeDocumentListExtension(this.getErrorDocumentCount())};
		return this.ims.getDocumentList(userName, false, filter, imeExtension);
	}
	
	private int getErrorDocumentCount() {
		String query = "SELECT count(*)" +
				" FROM " + DOCUMENT_ERROR_TABLE_NAME +
				";";
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query);
			return (sqr.next() ? Integer.parseInt(sqr.getString(0)) : 0);
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateIMS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting error document count.");
			System.out.println("  query was " + query);
			return Integer.MAX_VALUE;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	private static class ImeDocumentListExtension extends DocumentListExtension {
		private static final String[] imeColumnNames = {
			ERROR_COUNT_ATTRIBUTE,
			ERROR_CATEGORY_COUNT_ATTRIBUTE,
			ERROR_TYPE_COUNT_ATTRIBUTE,
			BLOCKER_ERROR_COUNT_ATTRIBUTE,
			CRITICAL_ERROR_COUNT_ATTRIBUTE,
			MAJOR_ERROR_COUNT_ATTRIBUTE,
			MINOR_ERROR_COUNT_ATTRIBUTE
		};
		private int selectivity;
		ImeDocumentListExtension(int selectivity) {
			super(DOCUMENT_ERROR_TABLE_NAME, true, imeColumnNames);
			this.selectivity = selectivity;
		}
		public int getSelectivity() {
			return this.selectivity;
		}
	}
}
