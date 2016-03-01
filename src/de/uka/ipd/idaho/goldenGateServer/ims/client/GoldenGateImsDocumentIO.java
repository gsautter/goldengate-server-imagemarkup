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
package de.uka.ipd.idaho.goldenGateServer.ims.client;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SpinnerNumberModel;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.DefaultTableColumnModel;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableColumn;
import javax.swing.table.TableModel;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.util.ControllingProgressMonitor;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.gamta.util.swing.DialogFactory;
import de.uka.ipd.idaho.gamta.util.swing.ProgressMonitorDialog;
import de.uka.ipd.idaho.goldenGate.plugins.GoldenGatePluginDataProvider;
import de.uka.ipd.idaho.goldenGate.plugins.PluginDataProviderFileBased;
import de.uka.ipd.idaho.goldenGate.util.DialogPanel;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants;
import de.uka.ipd.idaho.goldenGateServer.ims.client.GoldenGateImsClient.DocumentDataCache;
import de.uka.ipd.idaho.goldenGateServer.ims.data.ImsDocumentList;
import de.uka.ipd.idaho.goldenGateServer.ims.data.ImsDocumentListBuffer;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticatedClient;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticationManager;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticationManagerPlugin;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.imagine.plugins.AbstractGoldenGateImaginePlugin;
import de.uka.ipd.idaho.im.imagine.plugins.GoldenGateImagineDocumentListener;
import de.uka.ipd.idaho.im.imagine.plugins.ImageDocumentIoProvider;
import de.uka.ipd.idaho.im.util.ImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.ImDocumentEntry;
import de.uka.ipd.idaho.im.util.ImDocumentIO;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringRelation;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * DocumentIO plugin for GoldenGATE Imaginer supporting document IO with
 * GoldenGATE Image Markup Document Store. If the backing IMS is unreachable,
 * this client will save documents in a local cache and upload then the next
 * time the the user successfully logs in to the backing IMS. However, if the
 * IMS login fails due to invalid authentication data rather than network
 * problems, documents will not be cached.
 * 
 * @author sautter
 */
public class GoldenGateImsDocumentIO extends AbstractGoldenGateImaginePlugin implements ImageDocumentIoProvider, GoldenGateImagineDocumentListener, GoldenGateImsConstants {
	
	/** zero-argument constructor for class loading */
	public GoldenGateImsDocumentIO() {}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGate.plugins.AbstractGoldenGatePlugin#getPluginName()
	 */
	public String getPluginName() {
		return "IMS Document IO Provider";
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.im.imagine.plugins.ImageDocumentIoProvider#getLoadSourceName()
	 */
	public String getLoadSourceName() {
		return "GoldenGATE Server";
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.im.imagine.plugins.ImageDocumentIoProvider#getSaveDestinationName()
	 */
	public String getSaveDestinationName() {
		return "GoldenGATE Server";
	}
	
	private AuthenticationManagerPlugin authManager = null;
	private AuthenticatedClient authClient = null;
	
	private GoldenGateImsClient imsClient = null;
	private DocumentCache cache = null;
	
	private HashSet openDocumentIDs = new HashSet();
	
	private static final String[] listSortKeys = {DOCUMENT_NAME_ATTRIBUTE};
	
	private StringVector listFieldOrder = new StringVector();
	private StringVector listFields = new StringVector();
	
	private Properties listFieldLabels = new Properties();
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGate.plugins.AbstractGoldenGatePlugin#init()
	 */
	public void init() {
		
		//	get authentication manager
		this.authManager = ((AuthenticationManagerPlugin) this.parent.getPlugin(AuthenticationManagerPlugin.class.getName()));
		
		//	read display configuration
		try {
			InputStream is = this.dataProvider.getInputStream("config.cnfg");
			Settings set = Settings.loadSettings(is);
			is.close();
			
			this.listFieldOrder.parseAndAddElements(set.getSetting("listFieldOrder"), " ");
			this.listFields.parseAndAddElements(set.getSetting("listFields"), " ");
			
			Settings listFieldLabels = set.getSubset("listFieldLabel");
			String[] listFieldNames = listFieldLabels.getKeys();
			for (int f = 0; f < listFieldNames.length; f++)
				this.listFieldLabels.setProperty(listFieldNames[f], listFieldLabels.getSetting(listFieldNames[f], listFieldNames[f]));
		}
		catch (IOException ioe) {
			System.out.println(ioe.getClass() + " (" + ioe.getMessage() + ") while initializing ImsDocumentIO.");
			ioe.printStackTrace(System.out);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGate.plugins.AbstractGoldenGatePlugin#exit()
	 */
	public void exit() {
		this.logout();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.im.imagine.plugins.GoldenGateImagineDocumentListener#documentOpened(de.uka.ipd.idaho.im.ImDocument)
	 */
	public void documentOpened(ImDocument doc) {}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.im.imagine.plugins.GoldenGateImagineDocumentListener#documentSaving(de.uka.ipd.idaho.im.ImDocument)
	 */
	public void documentSaving(ImDocument doc) {}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.im.imagine.plugins.GoldenGateImagineDocumentListener#documentSaved(de.uka.ipd.idaho.im.ImDocument)
	 */
	public void documentSaved(ImDocument doc) {}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.im.imagine.plugins.GoldenGateImagineDocumentListener#documentClosed(java.lang.String)
	 */
	public void documentClosed(String docId) {
		
		//	we did not load this one
		if (!this.openDocumentIDs.remove(docId))
			return;
		
		//	we've been logged out
		if (this.imsClient == null)
			return;
		
		//	release document if checked out explicitly
		if ((this.cache == null) || !this.cache.isExplicitCheckout(docId)) try {
			this.imsClient.releaseDocument(docId);
			if (this.cache != null)
				this.cache.unstoreDocument(docId);
		}
		catch (IOException ioe) {
			ioe.printStackTrace(System.out);
			DialogFactory.alert(("An error occurred while releasing the document in the backing the GoldenGATE Server at\n" + this.authManager.getHost() + ":" + this.authManager.getPort() + "\n" + ioe.getMessage()), "Error Releasing Document", JOptionPane.ERROR_MESSAGE);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.im.imagine.plugins.ImageDocumentIoProvider#loadDocument(de.uka.ipd.idaho.gamta.util.ProgressMonitor)
	 */
	public ImDocument loadDocument(final ProgressMonitor pm) {
		System.out.println("ImsDocumentIO (" + this.getClass().getName() + "): loading document");
		pm.setInfo("Checking authentication at GoldenGATE Server ...");
		if (pm instanceof ControllingProgressMonitor) {
			((ControllingProgressMonitor) pm).setPauseResumeEnabled(true);
			((ControllingProgressMonitor) pm).setAbortEnabled(true);
			((ControllingProgressMonitor) pm).setAbortExceptionMessage("ABORTED BY USER");
		}
		this.ensureLoggedIn(pm);
		
		//	get list of documents
		ImsDocumentListBuffer documentList;
		boolean documentListEmpty = true;
		
		//	got server connection, load document list
		if (this.imsClient != null) {
			pm.setInfo("Connected, getting document list from GoldenGATE Server ...");
			
			//	load document list
			try {
				ImsDocumentList dl = this.imsClient.getDocumentList(pm);
				for (int f = 0; f < dl.listFieldNames.length; f++) {
					ImsDocumentList.AttributeSummary das = dl.getListFieldValues(dl.listFieldNames[f]);
					if ((das != null) && (das.size() != 0)) {
						documentListEmpty = false;
						f = dl.listFieldNames.length;
					}
				}
				pm.setInfo("Got document list, caching content ...");
				documentList = new ImsDocumentListBuffer(dl, pm);
				
				if (pm instanceof ControllingProgressMonitor) {
					((ControllingProgressMonitor) pm).setPauseResumeEnabled(false);
					((ControllingProgressMonitor) pm).setAbortEnabled(false);
				}
				
				if (documentList.isEmpty() && documentListEmpty) {
					DialogFactory.alert(("Currently, there are no documents available from the GoldenGATE Server at\n" + this.authManager.getHost() + ":" + this.authManager.getPort()), "No Documents To Load", JOptionPane.INFORMATION_MESSAGE);
					return null;
				}
			}
			catch (RuntimeException re) {
				if (!"ABORTED BY USER".equals(re.getMessage())) {
					re.printStackTrace(System.out);
					DialogFactory.alert(("An error occurred while loading the document list from the GoldenGATE Server at\n" + this.authManager.getHost() + ":" + this.authManager.getPort() + "\n" + re.getMessage()), "Error Getting Document List", JOptionPane.ERROR_MESSAGE);
				}
				return null;
			}
			catch (IOException ioe) {
				ioe.printStackTrace(System.out);
				DialogFactory.alert(("An error occurred while loading the document list from the GoldenGATE Server at\n" + this.authManager.getHost() + ":" + this.authManager.getPort() + "\n" + ioe.getMessage()), "Error Getting Document List", JOptionPane.ERROR_MESSAGE);
				return null;
			}
		}
		
		//	server unreachable, list cached documents
		else if (this.cache != null) {
			pm.setInfo("Server unreachable, getting document list from local cache ...");
			documentList = this.cache.getDocumentList();
		}
		
		//	not connected to server and no cache, indicate error
		else documentList = null;
		
		
		//	check success
		if (documentList == null) {
			DialogFactory.alert("Cannot open a document from GoldenGATE Server without authentication.", "Error Getting Document List", JOptionPane.ERROR_MESSAGE);
			return null;
		}
		
		//	check if documents to open
		else if (documentList.isEmpty() && documentListEmpty) {
			DialogFactory.alert(("Currently, there are no documents available from the GoldenGATE Server at\n" + this.authManager.getHost()), "No Documents To Load", JOptionPane.INFORMATION_MESSAGE);
			return null;
		}
		
		pm.setInfo("Document list loaded, opening selector dialog ...");
		try {
			//	display document list
			DocumentListDialog dld = new DocumentListDialog("Select Document", documentList, this.authManager.getUser(), ((this.authClient != null) && this.authClient.isAdmin()));
			dld.setLocationRelativeTo(DialogPanel.getTopWindow());
			dld.setVisible(true);
			
			//	get selected document
			ImDocumentData docData = dld.getDocumentData();
			if (docData == null)
				return null;
			
			//	load selected document
			ImDocument doc = ImDocumentIO.loadDocument(docData, pm);
			
			//	remember opening document, so we can release it when it's closed
			this.openDocumentIDs.add(doc.docId);
			
			//	finally ...
			return doc;
		}
		catch (Throwable t) {
			t.printStackTrace(System.out);
			return null;
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.im.imagine.plugins.ImageDocumentIoProvider#saveDocument(de.uka.ipd.idaho.im.ImDocument, java.lang.String, de.uka.ipd.idaho.gamta.util.ProgressMonitor)
	 */
	public String saveDocument(final ImDocument doc, final String docName, ProgressMonitor pm) {
		this.ensureLoggedIn(pm);
		
		//	check connection
		if ((this.imsClient == null) && (this.cache == null)) {
			DialogFactory.alert(("Cannot save a document to GoldenGATE Server without authentication."), ("Cannot Save Document"), JOptionPane.ERROR_MESSAGE);
			return null;
		}
		
		//	cache document if possible
		boolean docCached;
		if (this.cache != null) {
			if (pm != null)
				pm.setInfo("Caching document ...");
			docCached = this.cache.storeDocument(doc, docName);
		}
		else docCached = false;
		
		//	server unreachable, we're done here
		if (this.imsClient == null) {
			DialogFactory.alert(("Could not upload '" + docName + "' to GoldenGATE Server at " + authManager.getHost() + "\nbecause this server is unreachable at the moment." + (docCached ? "\n\nThe document has been stored to the local cache\nand will be uploaded when you log in next time." : "")), ("Server Unreachable" + (docCached ? " - Document Cached" : "")), JOptionPane.INFORMATION_MESSAGE);
			return (docCached ? docName : null);
		}
		
		try {
			if (pm instanceof ControllingProgressMonitor) {
				((ControllingProgressMonitor) pm).setPauseResumeEnabled(true);
				((ControllingProgressMonitor) pm).setAbortEnabled(true);
				((ControllingProgressMonitor) pm).setAbortExceptionMessage("ABORTED BY USER");
			}
			String[] uploadProtocol = this.imsClient.updateDocument(doc, pm);
			if (this.cache != null) {
				this.cache.markNotDirty(doc.docId);
				if (pm != null)
					pm.setInfo("Cache updated.");
			}
			final UploadProtocolDialog upDialog = new UploadProtocolDialog("Document Upload Protocol", ("Document '" + docName + "' successfully uploaded to GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\nDetails:"), uploadProtocol);
			Thread upThread = new Thread() {
				public void run() {
					while (imsClient != null) {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException ie) {}
						try {
							String[] up = imsClient.getUpdateProtocol(doc.docId);
							upDialog.setUploadProtocol(up);
							if ((up.length != 0) && (UPDATE_COMPLETE.equals(up[up.length-1]) || DELETION_COMPLETE.equals(up[up.length-1])))
								return;
						}
						catch (IOException ioe) {
							ioe.printStackTrace(System.out);
						}
					}
				}
			};
			upThread.start();
			return docName;
		}
		catch (IOException ioe) {
			DialogFactory.alert(("An error occurred while uploading document '" + docName + "' to the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\n" + ioe.getMessage() + (docCached ? "" : "\n\nThe document has been stored to the local cache\nand will be uploaded when you log in next time.")), ("Error Uploading Document"), JOptionPane.ERROR_MESSAGE);
			return null;
		}
	}
	
	private boolean ensureLoggedIn(ProgressMonitor pm) {
		
		//	test if connection alive
		if (this.authClient != null)
			try {
				//	test if connection alive
				if (this.authClient.ensureLoggedIn())
					return true;
				
				//	connection dead (e.g. a session timeout), make way for re-getting from auth manager
				else {
					this.imsClient = null;
					this.authClient = null;
				}
			}
			
			//	server temporarily unreachable, re-login will be done by auth manager
			catch (IOException ioe) {
				this.imsClient = null;
				this.authClient = null;
				return false;
			}
		
		
		//	check if existing cache valid
		if (this.cache != null) {
			String host = this.authManager.getHost();
			String user = this.authManager.getUser();
			if ((host == null) || (user == null) || !this.cache.belongsTo(host, user) || !this.authManager.isAuthenticated()) {
				this.cache.close();
				this.cache = null;
			}
		}
		
		
		//	got no valid connection at the moment, try and get one
		if (this.authClient == null)
			this.authClient = this.authManager.getAuthenticatedClient();
		
		
		//	create cache if none exists
		if ((this.cache == null) && this.authManager.isAuthenticated()) {
			String host = this.authManager.getHost();
			String user = this.authManager.getUser();
			if ((host != null) && (user != null)) {
				 if (this.dataProvider.isDataEditable())
					 this.cache = new DocumentCache(this.dataProvider, host, user);
				 else this.cache = new DocumentCache(new MemoryWritableDataProvider(this.dataProvider), host, user);
			}
		}
		
		
		//	authentication failed
		if (this.authClient == null)
			return false;
		
		//	got valid connection, flush cache if we got one
		else {
			this.imsClient = new GoldenGateImsClient(this.authClient, ((this.cache == null) ? null : this.cache));
			if (this.cache != null)
				this.cache.flush(this.imsClient, pm);
			return true;
		}
	}
	
	private void logout() {
		try {
			if (this.cache != null) {
				this.cache.close();
				this.cache = null;
			}
			
			this.imsClient = null;
			
			if ((this.authClient != null) && this.authClient.isLoggedIn()) // might have been logged out from elsewhere
				this.authClient.logout();
			this.authClient = null;
		}
		catch (IOException ioe) {
			DialogFactory.alert(("An error occurred while logging out from GoldenGATE Server\n" + ioe.getMessage()), ("Error on Logout"), JOptionPane.ERROR_MESSAGE);
		}
	}
	
	private ImDocumentData checkoutDocumentDataFromServer(String docId, int version, String docName, int readTimeout, ProgressMonitor pm) throws IOException {
		pm.setInfo("Loading document from GoldenGATE IMS ...");
		
		try {
			ImDocumentData docData = this.imsClient.checkoutDocumentAsData(docId, version, pm);
			
			if (pm instanceof ControllingProgressMonitor) {
				((ControllingProgressMonitor) pm).setPauseResumeEnabled(false);
				((ControllingProgressMonitor) pm).setAbortEnabled(false);
			}
			
			if (this.cache != null) {
				pm.setInfo("Document loaded, caching it ...");
				this.cache.markNotDirty(docId);
				pm.setInfo("Document cached.");
			}
			
			return docData;
		}
		catch (RuntimeException re) {
			if ("ABORTED BY USER".equals(re.getMessage())) try {
				this.imsClient.releaseDocument(docId);
			}
			catch (Exception ioe) {
				ioe.printStackTrace(System.out);
			}
			throw re;
		}
//		catch (TimeoutException te) {
//			te.printStackTrace(System.out);
//			try {
//				this.imsClient.releaseDocument(docId);
//			}
//			catch (Exception ioe) {
//				ioe.printStackTrace(System.out);
//			}
//			JOptionPane.showMessageDialog(promptParent, ("Loading document '" + docName + "' from the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + " timed out after " + readTimeout + " seconds.\nUse the 'Read Timeout' field below to increase the timeout."), ("Error Loading Document"), JOptionPane.ERROR_MESSAGE);
//			throw te;
//		}
		catch (IOException ioe) {
			ioe.printStackTrace(System.out);
			DialogFactory.alert(("An error occurred while loading document '" + docName + "' from the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\n" + ioe.getMessage()), ("Error Loading Document"), JOptionPane.ERROR_MESSAGE);
			throw ioe;
		}
	}
//	
//	private class TimeoutReader extends Reader {
//		DocumentReader dr;
//		ProgressMonitor pm;
//		int readTimeout;
//		int readTotal = 0;
//		TimeoutReader(DocumentReader dr, ProgressMonitor pm, int readTimeout) {
//			this.dr = dr;
//			this.pm = pm;
//			this.readTimeout = readTimeout;
//		}
//		public void close() throws IOException {
//			this.pm.setProgress(100);
//			this.dr.close();
//		}
//		public int read(final char[] cbuf, final int off, final int len) throws IOException {
//			final int[] read = {-2};
//			final IOException ioe[] = {null};
//			final Object readLock = new Object();
//			
//			//	create reader
//			Thread reader = new Thread() {
//				public void run() {
//					synchronized (readLock) {
//						readLock.notify();
//					}
//					
//					try {
//						System.out.println("Start timed reading");
//						read[0] = dr.read(cbuf, off, len);
//						System.out.println(" --> read " + read[0] + " chars");
//					}
//					catch (IOException ioex) {
//						ioe[0] = ioex;
//						System.out.println(" --> exception: " + ioex.getMessage());
//					}
//					finally {
//						synchronized (readLock) {
//							readLock.notify();
//						}
//					}
//				}
//			};
//			
//			//	start reader and wait for it
//			synchronized (readLock) {
//				reader.start();
//				try {
//					readLock.wait();
//				} catch (InterruptedException ie) {}
//			}
//			
//			//	wait for reading to succeed, be cancelled, or time out
//			long readDeadline = ((readTimeout < 1) ? Long.MAX_VALUE : (System.currentTimeMillis() + (readTimeout * 1000)));
//			do {
//				
//				//	wait a bit
//				synchronized (readLock) {
//					try {
//						readLock.wait(500);
//					} catch (InterruptedException ie) {}
//				}
//				
//				//	reading exception
//				if (ioe[0] != null) {
//					System.out.println("Reader threw exception: " + ioe[0].getMessage());
//					throw ioe[0];
//				}
//				
//				//	read successful
//				if (read[0] != -2) {
//					System.out.println("Reader read " + read[0] + " chars");
//					this.pm.setProgress((100 * this.readTotal) / this.dr.docLength()); // throws runtime exception if checkout aborted
//					this.readTotal += read[0];
//					return read[0];
//				}
//			}
//			
//			//	check for timeout
//			while (System.currentTimeMillis() < readDeadline);
//			
//			//	read timeout, close asynchronously and throw exception
//			Thread closer = new Thread() {
//				public void run() {
//					try {
//						System.out.println("Start timed closing");
//						dr.close();
//						System.out.println(" --> closed");
//					}
//					catch (IOException ioe) {
//						System.out.println(" --> exception on closing: " + ioe.getMessage());
//					}
//				}
//			};
//			closer.start();
//			
//			throw new TimeoutException("Read timeout");
//		}
//	}
//	
//	private class TimeoutException extends IOException {
//		TimeoutException(String message) {
//			super(message);
//		}
//	}
	
	private final String produceFieldLabel(String fieldName) {
		String listFieldLabel = listFieldLabels.getProperty(fieldName);
		if (listFieldLabel != null)
			return listFieldLabel;
		
		if (fieldName.length() < 2)
			return fieldName;
		
		StringVector parts = new StringVector();
		fieldName = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
		int c = 1;
		while (c < fieldName.length()) {
			if (Character.isUpperCase(fieldName.charAt(c))) {
				parts.addElement(fieldName.substring(0, c));
				fieldName = fieldName.substring(c);
				c = 1;
			} else c++;
		}
		if (fieldName.length() != 0)
			parts.addElement(fieldName);
		
		for (int p = 0; p < (parts.size() - 1);) {
			String part1 = parts.get(p);
			String part2 = parts.get(p + 1);
			if ((part2.length() == 1) && Character.isUpperCase(part1.charAt(part1.length() - 1))) {
				part1 += part2;
				parts.setElementAt(part1, p);
				parts.remove(p+1);
			}
			else p++;
		}
		
		return parts.concatStrings(" ");
	}
	
	/**
	 * Constant set containing the names of document attributes which can be
	 * used for document list filters. This set is immutable, any modification
	 * methods are implemented to simply return false.
	 */
	private static final Set filterableAttributes = Collections.unmodifiableSet(new LinkedHashSet() {
		{
			//	TODO keep this in sync with table definition
			String[] docTableFields = {
					
					//	- identifier data
//					DOCUMENT_ID_ATTRIBUTE, // nobody will filter by a document ID
					DOCUMENT_NAME_ATTRIBUTE,
					
					//	- meta data
					DOCUMENT_AUTHOR_ATTRIBUTE,
					DOCUMENT_DATE_ATTRIBUTE,
					DOCUMENT_TITLE_ATTRIBUTE,
					
					//	- management data
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
	});
	
	/**
	 * Constant set containing the names of numeric document attributes, for
	 * which specific comparison operators can be used for document list
	 * filters. This set is immutable, any modification methods are implemented
	 * to simply return false.
	 */
	private static final Set numericAttributes = Collections.unmodifiableSet(new LinkedHashSet() {
		{
			//	TODO keep this in sync with table definition
			String[] numericFieldNames = {
					DOCUMENT_DATE_ATTRIBUTE,
					CHECKIN_TIME_ATTRIBUTE,
					UPDATE_TIME_ATTRIBUTE,
					CHECKOUT_TIME_ATTRIBUTE,
					DOCUMENT_VERSION_ATTRIBUTE,
			};
			Arrays.sort(numericFieldNames);
			for (int a = 0; a < numericFieldNames.length; a++)
				super.add(numericFieldNames[a]);
		}
	});
	
	/**
	 * Constant set containing the comparison operators that can be used for
	 * numeric document attributes in document list filters. This set is
	 * immutable, any modification methods are implemented to simply return
	 * false.
	 */
	private static final Set numericOperators = Collections.unmodifiableSet(new LinkedHashSet() {
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
	});
	
	private class DocumentFilterPanel extends JPanel {
		
		private abstract class Filter {
			final String fieldName;
			Filter(String fieldName) {
				this.fieldName = fieldName;
			}
			abstract JComponent getOperatorSelector();
			abstract String getOperator();
			abstract JComponent getValueInputField();
			abstract String[] getFilterValues() throws RuntimeException;
		}
		
		private class StringFilter extends Filter {
			private String[] suggestionLabels;
			private Properties suggestionMappings;
			private boolean editable;
			private JTextField valueInput;
			private JComboBox valueSelector;
			StringFilter(String fieldName, ImsDocumentList.AttributeSummary suggestions, boolean editable) {
				super(fieldName);
				if (suggestions == null) 
					this.editable = true;
				else {
					this.editable = editable;
					this.suggestionLabels = new String[suggestions.elementCount()];
					this.suggestionMappings = new Properties();
					for (Iterator sit = suggestions.iterator(); sit.hasNext();) {
						String suggestion = ((String) sit.next());
						if (this.editable) {
							suggestion = suggestion.replaceAll("\\s", "+");
							this.suggestionLabels[this.suggestionMappings.size()] = suggestion;
							this.suggestionMappings.setProperty(suggestion, suggestion);
						}
						else {
							String suggestionLabel = (suggestion + " (" + suggestions.getCount(suggestion) + ")");
							this.suggestionLabels[this.suggestionMappings.size()] = suggestionLabel;
							this.suggestionMappings.setProperty(suggestionLabel, suggestion);
						}
					}
				}
			}
			JComponent getOperatorSelector() {
				return new JLabel("contains (use '+' for spaces)", JLabel.CENTER);
			}
			String getOperator() {
				return null;
			}
			JComponent getValueInputField() {
				if (this.suggestionLabels == null) {
					this.valueInput = new JTextField();
					this.valueInput.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent ae) {
							parent.filterDocumentList();
						}
					});
					return this.valueInput;
				}
				else {
					this.valueSelector = new JComboBox(this.suggestionLabels);
					this.valueSelector.insertItemAt("<do not filter>", 0);
					this.valueSelector.setSelectedItem("<do not filter>");
					this.valueSelector.setEditable(this.editable);
					this.valueSelector.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent ae) {
							parent.filterDocumentList();
						}
					});
					return this.valueSelector;
				}
			}
			String[] getFilterValues() throws RuntimeException {
				String filterValue;
				if (this.suggestionLabels == null)
					filterValue = this.valueInput.getText().trim();
				else {
					filterValue = ((String) this.valueSelector.getSelectedItem()).trim();
					filterValue = this.suggestionMappings.getProperty(filterValue, filterValue);
				}
				
				if ((filterValue.length() == 0) || "<do not filter>".equals(filterValue))
					return null;
				
				if (this.editable) {
					String[] filterValues = filterValue.split("\\s++");
					for (int v = 0; v < filterValues.length; v++)
						filterValues[v] = filterValues[v].replaceAll("\\+", " ").trim();
					return filterValues;
				}
				else {
					String[] filterValues = {filterValue};
					return filterValues;
				}
			}
		}
		
		private class NumberFilter extends Filter {
			private String[] operatorLabels;
			private Properties operatorMappings;
			private JComboBox operatorSelector;
			private String[] suggestionLabels;
			private Properties suggestionMappings;
			private boolean editable;
			private JTextField valueInput;
			private JComboBox valueSelector;
			NumberFilter(String fieldName, ImsDocumentList.AttributeSummary suggestions, boolean editable, boolean isTime) {
				super(fieldName);
				
				this.operatorLabels = new String[numericOperators.size()];
				this.operatorMappings = new Properties();
				for (Iterator oit = numericOperators.iterator(); oit.hasNext();) {
					String operator = ((String) oit.next());
					String operatorLabel;
					if (">".equals(operator))
						operatorLabel = (isTime ? "after" : "more than");
					else if (">=".equals(operator))
						operatorLabel = (isTime ? "the earliest in" : "at least");
					else if ("=".equals(operator))
						operatorLabel = "exactly in";
					else if ("<=".equals(operator))
						operatorLabel = (isTime ? "the latest in" : "at most");
					else if ("<".equals(operator))
						operatorLabel = (isTime ? "before" : "less than");
					else continue;
					this.operatorLabels[this.operatorMappings.size()] = operatorLabel;
					this.operatorMappings.setProperty(operatorLabel, operator);
				}
				
				if (suggestions == null)
					this.editable = true;
				else {
					this.editable = editable;
					this.suggestionLabels = new String[suggestions.elementCount()];
					this.suggestionMappings = new Properties();
					for (Iterator sit = suggestions.iterator(); sit.hasNext();) {
						String suggestion = ((String) sit.next());
						if (this.editable) {
							this.suggestionLabels[this.suggestionMappings.size()] = suggestion;
							this.suggestionMappings.setProperty(suggestion, suggestion);
						}
						else {
							String suggestionLabel = (suggestion + " (" + suggestions.getCount(suggestion) + ")");
							this.suggestionLabels[this.suggestionMappings.size()] = suggestionLabel;
							this.suggestionMappings.setProperty(suggestionLabel, suggestion);
						}
					}
				}
			}
			JComponent getOperatorSelector() {
				this.operatorSelector = new JComboBox(this.operatorLabels);
				this.operatorSelector.setEditable(false);
				return this.operatorSelector;
			}
			String getOperator() {
				String operator = ((String) this.operatorSelector.getSelectedItem()).trim();
				return this.operatorMappings.getProperty(operator, operator);
			}
			JComponent getValueInputField() {
				if (this.suggestionLabels == null) {
					this.valueInput = new JTextField();
					this.valueInput.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent ae) {
							parent.filterDocumentList();
						}
					});
					return this.valueInput;
				}
				else {
					this.valueSelector = new JComboBox(this.suggestionLabels);
					this.valueSelector.insertItemAt("<do not filter>", 0);
					this.valueSelector.setSelectedItem("<do not filter>");
					this.valueSelector.setEditable(this.editable);
					this.valueSelector.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent ae) {
							parent.filterDocumentList();
						}
					});
					return this.valueSelector;
				}
			}
			String[] getFilterValues() throws RuntimeException {
				String filterValue;
				if (this.suggestionLabels == null)
					filterValue = this.valueInput.getText().trim();
				else {
					filterValue = ((String) this.valueSelector.getSelectedItem()).trim();
					filterValue = this.suggestionMappings.getProperty(filterValue, filterValue);
				}
				
				if ((filterValue.length() == 0) || "<do not filter>".equals(filterValue))
					return null;
				
				try {
					Long.parseLong(filterValue);
				}
				catch (NumberFormatException nfe) {
					throw new RuntimeException("'" + filterValue + "' is not a valid value for " + produceFieldLabel(this.fieldName) + ".");
				}
				
				String[] filterValues = {filterValue};
				return filterValues;
			}
		}
		
		private class TimeFilter extends Filter {
			private String[] operatorLabels;
			private Properties operatorMappings;
			private JComboBox operatorSelector;
			private JComboBox valueSelector;
			TimeFilter(String fieldName) {
				super(fieldName);
				
				this.operatorLabels = new String[numericOperators.size()];
				this.operatorMappings = new Properties();
				for (Iterator oit = numericOperators.iterator(); oit.hasNext();) {
					String operator = ((String) oit.next());
					String operatorLabel;
					if (">".equals(operator))
						operatorLabel = "less than";
					else if (">=".equals(operator))
						operatorLabel = "at most";
					else if ("=".equals(operator))
						operatorLabel = "exactly";
					else if ("<=".equals(operator))
						operatorLabel = "at least";
					else if ("<".equals(operator))
						operatorLabel = "more than";
					else continue;
					this.operatorLabels[this.operatorMappings.size()] = operatorLabel;
					this.operatorMappings.setProperty(operatorLabel, operator);
				}
			}
			JComponent getOperatorSelector() {
				this.operatorSelector = new JComboBox(this.operatorLabels);
				this.operatorSelector.setEditable(false);
				return this.operatorSelector;
			}
			String getOperator() {
				String operator = ((String) this.operatorSelector.getSelectedItem()).trim();
				return this.operatorMappings.getProperty(operator, operator);
			}
			JComponent getValueInputField() {
				this.valueSelector = new JComboBox();
				this.valueSelector.addItem("<do not filter>");
				this.valueSelector.addItem("one hour ago");
				this.valueSelector.addItem("one day ago");
				this.valueSelector.addItem("one week ago");
				this.valueSelector.addItem("one month ago");
				this.valueSelector.addItem("three months ago");
				this.valueSelector.addItem("one year ago");
				this.valueSelector.setEditable(false);
				return this.valueSelector;
			}
			String[] getFilterValues() throws RuntimeException {
				String filterValue = ((String) this.valueSelector.getSelectedItem()).trim();
				if ("one hour ago".equals(filterValue))
					filterValue = ("" + (System.currentTimeMillis() - ((long) (1 * 1 * 60 * 60) * 1000)));
				else if ("one day ago".equals(filterValue))
					filterValue = ("" + (System.currentTimeMillis() - ((long) (1 * 24 * 60 * 60) * 1000)));
				else if ("one week ago".equals(filterValue))
					filterValue = ("" + (System.currentTimeMillis() - ((long) (7 * 24 * 60 * 60) * 1000)));
				else if ("one month ago".equals(filterValue))
					filterValue = ("" + (System.currentTimeMillis() - ((long) (30 * 24 * 60 * 60) * 1000)));
				else if ("three months ago".equals(filterValue))
					filterValue = ("" + (System.currentTimeMillis() - ((long) (90 * 24 * 60 * 60) * 1000)));
				else if ("one year ago".equals(filterValue))
					filterValue = ("" + (System.currentTimeMillis() - ((long) (365 * 24 * 60 * 60) * 1000)));
				else return null;
				String[] filterValues = {filterValue};
				return filterValues;
			}
		}
		
		private DocumentListDialog parent;
		private Filter[] filters;
		
		DocumentFilterPanel(ImsDocumentListBuffer docList, DocumentListDialog parent) {
			super(new GridBagLayout(), true);
			this.parent = parent;
			
			ArrayList filterList = new ArrayList();
			for (int f = 0; f < docList.listFieldNames.length; f++) {
				ImsDocumentList.AttributeSummary das = docList.getListFieldValues(docList.listFieldNames[f]);
				if (!filterableAttributes.contains(docList.listFieldNames[f]) && (das == null))
					continue;
				
				Filter filter;
				if (numericAttributes.contains(docList.listFieldNames[f])) {
					if (docList.listFieldNames[f].endsWith("Time"))
						filter = new TimeFilter(docList.listFieldNames[f]);
					else filter = new NumberFilter(docList.listFieldNames[f], das, true, DOCUMENT_DATE_ATTRIBUTE.equals(docList.listFieldNames[f]));
				}
				else filter = new StringFilter(docList.listFieldNames[f], das, !docList.listFieldNames[f].endsWith("User"));
				filterList.add(filter);
			}
			this.filters = ((Filter[]) filterList.toArray(new Filter[filterList.size()]));
			
			GridBagConstraints gbc = new GridBagConstraints();
			gbc.insets.top = 3;
			gbc.insets.bottom = 3;
			gbc.insets.left = 3;
			gbc.insets.right = 3;
			gbc.weighty = 0;
			gbc.gridheight = 1;
			gbc.gridwidth = 1;
			gbc.fill = GridBagConstraints.BOTH;
			gbc.gridy = 0;
			for (int f = 0; f < this.filters.length; f++) {
				gbc.gridx = 0;
				gbc.weightx = 0;
				this.add(new JLabel(produceFieldLabel(this.filters[f].fieldName), JLabel.LEFT), gbc.clone());
				gbc.gridx = 1;
				gbc.weightx = 0;
				this.add(this.filters[f].getOperatorSelector(), gbc.clone());
				gbc.gridx = 2;
				gbc.weightx = 1;
				this.add(this.filters[f].getValueInputField(), gbc.clone());
				gbc.gridy++;
			}
		}
		
		DocumentFilter getFilter() {
			final LinkedList filterList = new LinkedList();
			for (int f = 0; f < this.filters.length; f++) {
				String[] filterValues = this.filters[f].getFilterValues();
				if ((filterValues == null) || (filterValues.length == 0))
					continue;
				
				System.out.println(this.filters[f].fieldName + " filter value is " + this.flattenArray(filterValues));
				
				if (numericAttributes.contains(this.filters[f].fieldName)) {
					final long filterValue = Long.parseLong(filterValues[0]);
					final String operator = this.filters[f].getOperator();
					if ((operator != null) && numericOperators.contains(operator))
						filterList.addFirst(new DocumentFilter(this.filters[f].fieldName) {
							boolean passesFilter(StringTupel docData) {
								String dataValueString = docData.getValue(this.fieldName);
								if (dataValueString == null)
									return false;
								long dataValue;
								try {
									dataValue = Long.parseLong(dataValueString);
								}
								catch (NumberFormatException nfe) {
									return false;
								}
								if (">".equals(operator))
									return (dataValue > filterValue);
								else if (">=".equals(operator))
									return (dataValue >= filterValue);
								else if ("=".equals(operator))
									return (dataValue == filterValue);
								else if ("<=".equals(operator))
									return (dataValue <= filterValue);
								else if ("<".equals(operator))
									return (dataValue < filterValue);
								else return true;
							}
						});
				}
				else {
					final String[] filterStrings = new String[filterValues.length];
					for (int v = 0; v < filterValues.length; v++)
						filterStrings[v] = filterValues[v].replaceAll("\\s++", " ").toLowerCase();
					for (int s = 0; s < filterStrings.length; s++) {
						while (filterStrings[s].startsWith("%"))
							filterStrings[s] = filterStrings[s].substring(1);
						while (filterStrings[s].endsWith("%"))
							filterStrings[s] = filterStrings[s].substring(0, (filterStrings[s].length() - 1));
					}
					filterList.addLast(new DocumentFilter(this.filters[f].fieldName) {
						boolean passesFilter(StringTupel docData) {
							String dataValueString = docData.getValue(this.fieldName);
							if (dataValueString == null)
								return false;
							dataValueString = dataValueString.replaceAll("\\s++", " ").toLowerCase();
							for (int f = 0; f < filterStrings.length; f++) {
								if (dataValueString.indexOf(filterStrings[f]) != -1)
									return true;
							}
							return false;
						}
					});
				}
			}
			
			return (filterList.isEmpty() ? null : new DocumentFilter(null) {
				boolean passesFilter(StringTupel docData) {
					for (Iterator fit = filterList.iterator(); fit.hasNext();) {
						if (!((DocumentFilter) fit.next()).passesFilter(docData))
							return false;
					}
					return true;
				}
			});
		}
		
		Properties getFilterParameters() {
			Properties filter = new Properties();
			for (int f = 0; f < this.filters.length; f++) {
				String filterValue = this.flattenArray(this.filters[f].getFilterValues());
				if (filterValue == null)
					continue;
				filter.setProperty(this.filters[f].fieldName, filterValue);
				if (numericAttributes.contains(this.filters[f].fieldName)) {
					String operator = this.filters[f].getOperator();
					if ((operator != null) && numericOperators.contains(operator))
						filter.setProperty((this.filters[f].fieldName + "Operator"), operator);
				}
			}
			return filter;
		}
		private String flattenArray(String[] filterValues) {
			if ((filterValues == null) || (filterValues.length == 0))
				return null;
			if (filterValues.length == 1)
				return filterValues[0];
			StringBuffer filterValue = new StringBuffer(filterValues[0]);
			for (int v = 1; v < filterValues.length; v++)
				filterValue.append("\n" + filterValues[v]);
			return filterValue.toString();
		}
	}
	
	private abstract class DocumentFilter {
		String fieldName;
		DocumentFilter(String fieldName) {
			this.fieldName = fieldName;
		}
		abstract boolean passesFilter(StringTupel docData);
	}
	
	private static final String[] cacheDocumentListAttributes = {
		DOCUMENT_ID_ATTRIBUTE,
		DOCUMENT_NAME_ATTRIBUTE,
		DOCUMENT_TITLE_ATTRIBUTE,
		CHECKIN_USER_ATTRIBUTE,
		CHECKIN_TIME_ATTRIBUTE,
		UPDATE_USER_ATTRIBUTE,
		UPDATE_TIME_ATTRIBUTE,
		DOCUMENT_VERSION_ATTRIBUTE,
	};
	
	private static final String DEFAULT_TIMESTAMP_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final DateFormat TIMESTAMP_DATE_FORMAT = new SimpleDateFormat(DEFAULT_TIMESTAMP_DATE_FORMAT);
	
	/* TODO
reduce document list loading effort:
- for non-admin users, only transfer document list head with input suggestions for filters
- embed list size in header
  - facilitates displaying list loading progress
  - facilitates detailed message for selectivity of filter
- display message label in place of document list table if list too large
- in ImsDocumentIO, reload list from server when filter button clicked
 */
	private class StringTupelTray {
		final StringTupel data;
		Object[] sortKey = new Object[0];
		StringTupelTray(StringTupel data) {
			this.data = data;
		}
		void updateSortKey(StringVector sortFields) {
			this.sortKey = new Object[sortFields.size()];
			for (int f = 0; f < sortFields.size(); f++)
				this.sortKey[f] = this.data.getValue(sortFields.get(f), "");
		}
	}
	
	private class DocumentListDialog extends DialogPanel {
		private static final String CACHE_STATUS_ATTRIBUTE = "Cache";
		private ImsDocumentListBuffer docList;
		private StringTupelTray[] listData;
		
		private JTable docTable = new JTable();
		private DocumentTableModel docTableModel;
		
		private DocumentFilterPanel filterPanel;
		
		private ImDocumentData loadDocData = null;
		
		private String userName;
		private boolean isAdmin;
		
		private String title;
		
		private SpinnerNumberModel readTimeout = new SpinnerNumberModel(5, 0, 60, 5);
		
		DocumentListDialog(String title, ImsDocumentListBuffer docList, String userName, boolean isAdmin) {
			super(title, true);
			this.title = title;
			this.userName = userName;
			this.isAdmin = isAdmin;
			this.docList = docList;
			
			this.filterPanel = new DocumentFilterPanel(this.docList, this);
			
			final JTableHeader header = this.docTable.getTableHeader();
			if (header != null)
				header.addMouseListener(new MouseAdapter() {
					public void mouseClicked(MouseEvent me) {
						if (docTableModel == null)
							return;
		                int column = header.columnAtPoint(me.getPoint());
		                if (column != -1)
		                	sortList(docTableModel.getFieldName(column));
					}
				});
			
			this.docTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
			this.docTable.addMouseListener(new MouseAdapter() {
				public void mouseClicked(MouseEvent me) {
					int row = docTable.getSelectedRow();
					if (row == -1) return;
					if (me.getClickCount() > 1)
						open(row, 0);
					else if (me.getButton() != MouseEvent.BUTTON1)
						showContextMenu(row, me);
				}
			});
			
			JScrollPane docTableBox = new JScrollPane(this.docTable);
			docTableBox.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
			
			JButton filterButton = new JButton("Filter");
			filterButton.setBorder(BorderFactory.createRaisedBevelBorder());
			filterButton.setPreferredSize(new Dimension(100, 21));
			filterButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					filterDocumentList();
				}
			});
			
			JButton okButton = new JButton("OK");
			okButton.setBorder(BorderFactory.createRaisedBevelBorder());
			okButton.setPreferredSize(new Dimension(100, 21));
			okButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					int row = docTable.getSelectedRow();
					if (row == -1) return;
					open(row, 0);
				}
			});
			
			JButton cancelButton = new JButton("Cancel");
			cancelButton.setBorder(BorderFactory.createRaisedBevelBorder());
			cancelButton.setPreferredSize(new Dimension(100, 21));
			cancelButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					if ((cache != null) && (imsClient != null))
						cache.cleanup(imsClient);
					dispose();
				}
			});
			
			JSpinner readTimeoutSelector = new JSpinner(this.readTimeout);
			JLabel readTimeoutLabel = new JLabel("Read Timeout (in seconds, 0 means no timeout)", JLabel.RIGHT);
			JPanel readTimeoutPanel = new JPanel(new FlowLayout());
			readTimeoutPanel.add(readTimeoutLabel);
			readTimeoutPanel.add(readTimeoutSelector);
			readTimeoutPanel.setBorder(BorderFactory.createEtchedBorder());
			
			JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
			buttonPanel.add(filterButton);
			buttonPanel.add(readTimeoutPanel);
			buttonPanel.add(okButton);
			buttonPanel.add(cancelButton);
			
			this.add(this.filterPanel, BorderLayout.NORTH);
			this.add(docTableBox, BorderLayout.CENTER);
			this.add(buttonPanel, BorderLayout.SOUTH);
			
			this.setSize(new Dimension(800, 800));
			
			this.updateListData(null);
		}
		
		private void setDocumentList(ImsDocumentListBuffer docList) {
			this.docList = docList;
			this.updateListData(null);
		}
		
		private void updateListData(DocumentFilter filter) {
			if (filter == null) {
				this.listData = new StringTupelTray[this.docList.size()];
				for (int d = 0; d < this.docList.size(); d++)
					this.listData[d] = new StringTupelTray(this.docList.get(d));
				this.setTitle(this.title + " (" + this.docList.size() + " documents)");
			}
			else {
				ArrayList listDataList = new ArrayList();
				for (int d = 0; d < this.docList.size(); d++) {
					StringTupel docData = this.docList.get(d);
					if (filter.passesFilter(docData))
						listDataList.add(docData);
				}
				this.listData = new StringTupelTray[listDataList.size()];
				for (int d = 0; d < listDataList.size(); d++)
					this.listData[d] = new StringTupelTray((StringTupel) listDataList.get(d));
				this.setTitle(this.title + " (" + this.listData.length + " of " + this.docList.size() + " documents passing filter)");
			}
			
			StringVector fieldNames = new StringVector();
			if (cache != null) {
				fieldNames.addElement(CACHE_STATUS_ATTRIBUTE);
				for (int d = 0; d < this.listData.length; d++) {
					String docId = this.listData[d].data.getValue(DOCUMENT_ID_ATTRIBUTE);
					if (docId == null)
						continue;
					else if (cache.isExplicitCheckout(docId))
						this.listData[d].data.setValue(CACHE_STATUS_ATTRIBUTE, "Localized");
					else if (cache.containsDocument(docId))
						this.listData[d].data.setValue(CACHE_STATUS_ATTRIBUTE, "Cached");
					else this.listData[d].data.setValue(CACHE_STATUS_ATTRIBUTE, "");
				}
			}
			fieldNames.addContent(listFieldOrder);
			for (int f = 0; f < this.docList.listFieldNames.length; f++) {
				String fieldName = this.docList.listFieldNames[f];
//				if (!DOCUMENT_ID_ATTRIBUTE.equals(fieldName) && (DOCUMENT_TITLE_ATTRIBUTE.equals(fieldName) || !listTitlePatternAttributes.contains(fieldName)))
				if (!DOCUMENT_ID_ATTRIBUTE.equals(fieldName))
					fieldNames.addElementIgnoreDuplicates(fieldName);
			}
			System.out.println("Field names are " + fieldNames.concatStrings(", "));
			
			for (int f = 0; f < fieldNames.size(); f++) {
				String fieldName = fieldNames.get(f);
				if (CACHE_STATUS_ATTRIBUTE.equals(fieldName))
					continue;
				
				if (!listFields.contains(fieldName)) {
					fieldNames.remove(f--);
					continue;
				}
				
				boolean fieldEmpty = true;
//				if (DOCUMENT_TITLE_ATTRIBUTE.equals(fieldName))
//					for (int t = 0; t < listTitlePatternAttributes.size(); t++) {
//						fieldName = listTitlePatternAttributes.get(t);
//						for (int d = 0; d < this.listData.length; d++) {
//							if (!"".equals(this.listData[d].data.getValue(fieldName, ""))) {
//								fieldEmpty = false;
//								d = this.listData.length;
//								t = listTitlePatternAttributes.size();
//							}
//						}
//					}
//				
//				else 
				for (int d = 0; d < this.listData.length; d++)
					if (!"".equals(this.listData[d].data.getValue(fieldName, ""))) {
						fieldEmpty = false;
						d = this.listData.length;
					}
				if (fieldEmpty)
					fieldNames.remove(f--);
			}
			
			this.docTableModel = new DocumentTableModel(fieldNames.toStringArray(), this.listData);
			this.docTable.setColumnModel(new DefaultTableColumnModel() {
				public TableColumn getColumn(int columnIndex) {
					TableColumn tc = super.getColumn(columnIndex);
					String fieldName = docTableModel.getColumnName(columnIndex);
					if (DOCUMENT_TITLE_ATTRIBUTE.equals(fieldName))
						return tc;
					
					if (docTableModel.listData.length == 0) {
						tc.setPreferredWidth(70);
						tc.setMinWidth(70);
					}
					else if (false
							|| CHECKIN_TIME_ATTRIBUTE.equals(fieldName)
							|| UPDATE_TIME_ATTRIBUTE.equals(fieldName)
							|| CHECKOUT_TIME_ATTRIBUTE.equals(fieldName)
							) {
						tc.setPreferredWidth(120);
						tc.setMinWidth(120);
					}
					else if (CACHE_STATUS_ATTRIBUTE.equals(fieldName)) {
						tc.setPreferredWidth(70);
						tc.setMinWidth(70);
					}
					else {
						String test = docTableModel.getValueAt(0, columnIndex).toString().replaceAll("\\<[A-Z\\/]++\\>", "");
						tc.setPreferredWidth(test.matches("[0-9]++") ? 50 : 100);
						tc.setMinWidth(test.matches("[0-9]++") ? 50 : 100);
					}
					
					tc.setResizable(true);
					
					return tc;
				}
			});
			this.docTable.setModel(this.docTableModel);
			
			this.sortList(null);
			
			this.docTable.validate();
			this.docTable.repaint();
		}
		
		private void filterDocumentList() {
			
			//	received list head only so far
			if (this.docList.isEmpty()) {
				ImsDocumentListBuffer documentList;
				boolean documentListEmpty = true;
				Properties filter = this.filterPanel.getFilterParameters();
				try {
					//	TODO use splash screen
					ImsDocumentList dl = imsClient.getDocumentList(filter, ProgressMonitor.dummy);
					for (int f = 0; f < dl.listFieldNames.length; f++) {
						ImsDocumentList.AttributeSummary das = dl.getListFieldValues(dl.listFieldNames[f]);
						if ((das != null) && (das.size() != 0)) {
							documentListEmpty = false;
							f = dl.listFieldNames.length;
						}
					}
					documentList = new ImsDocumentListBuffer(dl);
					if (documentList.isEmpty() && documentListEmpty)
						DialogFactory.alert(("Currently, there are no documents available from the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + ",\nor your filter is too restrictive."), "No Documents To Load", JOptionPane.INFORMATION_MESSAGE);
					this.setDocumentList(documentList);
				}
				catch (IOException ioe) {
					ioe.printStackTrace(System.out);
					DialogFactory.alert(("An error occurred while loading the document list from the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\n" + ioe.getMessage()), "Error Getting Filtered List", JOptionPane.ERROR_MESSAGE);
				}
			}
			
			//	filter local list
			else this.updateListData(this.filterPanel.getFilter());
		}
		
		private void sortList(String sortField) {
			final StringVector sortFields = new StringVector();
//			if (sortField != null) {
//				if (DOCUMENT_TITLE_ATTRIBUTE.equals(sortField) && (listTitlePatternAttributes.size() != 0))
//					sortFields.addContentIgnoreDuplicates(listTitlePatternAttributes);
//				else sortFields.addElement(sortField);
			if (sortField != null)
				sortFields.addElement(sortField);
			sortFields.addContent(listSortKeys);
			
			//	update sort keys, and check which fields are numeric
			boolean[] isFieldNumeric = new boolean[sortFields.size()];
			Arrays.fill(isFieldNumeric, true);
			for (int d = 0; d < this.listData.length; d++) {
				this.listData[d].updateSortKey(sortFields);
				for (int f = 0; f < isFieldNumeric.length; f++) {
					if (isFieldNumeric[f]) try {
						Integer.parseInt((String) this.listData[d].sortKey[f]);
					}
					catch (NumberFormatException nfe) {
						isFieldNumeric[f] = false;
					}
				}
			}
			
			//	make field values numeric only if they are numeric throughout the list
			for (int d = 0; d < this.listData.length; d++) {
				for (int f = 0; f < isFieldNumeric.length; f++) {
					if (isFieldNumeric[f])
						this.listData[d].sortKey[f] = new Integer((String) this.listData[d].sortKey[f]);
				}
			}
			
			//	sort list (catching comparison errors that can occur in Java 7)
			Arrays.sort(this.listData, new Comparator() {
				public int compare(Object o1, Object o2) {
					StringTupelTray st1 = ((StringTupelTray) o1);
					StringTupelTray st2 = ((StringTupelTray) o2);
					int c = 0;
					for (int f = 0; f < st1.sortKey.length; f++) {
						if (st1.sortKey[f] instanceof Integer)
							c = (((Integer) st1.sortKey[f]).intValue() - ((Integer) st2.sortKey[f]).intValue());
						else c = ((String) st1.sortKey[f]).compareToIgnoreCase((String) st2.sortKey[f]);
						if (c != 0)
							return c;
					}
					return 0;
				}
			});
			
			//	update display
			this.docTableModel.update();
			this.docTable.validate();
			this.docTable.repaint();
		}
		
		private void delete(int row) {
			if ((imsClient == null) || (row == -1)) return;
			
			//	get document data
			String docId = this.listData[row].data.getValue(DOCUMENT_ID_ATTRIBUTE);
			if (docId == null) return;
			
			//	checkout and delete document
			try {
				
				//	do it
				if (cache != null)
					cache.unstoreDocument(docId);
				imsClient.deleteDocument(docId);
				
				//	inform user
				DialogFactory.alert("The document has been deleted.", "Document Deleted", JOptionPane.INFORMATION_MESSAGE);
				
				//	update data
				StringTupelTray[] newListData = new StringTupelTray[this.listData.length - 1];
				System.arraycopy(this.listData, 0, newListData, 0, row);
				System.arraycopy(this.listData, (row+1), newListData, row, (newListData.length - row));
				this.listData = newListData;
				
				//	update display
				this.docTableModel.setListData(this.listData);
				this.docTable.validate();
				this.docTable.repaint();
			}
			catch (IOException ioe) {
				DialogFactory.alert(("Document could not be deleted:\n" + ioe.getMessage()), "Error Deleting Document", JOptionPane.ERROR_MESSAGE);
			}
		}
		
		private void open(int row, final int version) {
			if (row == -1) return;
			
			//	get document data
			final String docId = this.listData[row].data.getValue(DOCUMENT_ID_ATTRIBUTE);
			if (docId == null)
				return;
			final String docName = this.listData[row].data.getValue(DOCUMENT_NAME_ATTRIBUTE);
			
			//	create progress monitor
			final ProgressMonitorDialog pmd = new ProgressMonitorDialog(false, true, DialogPanel.getTopWindow(), "Loading Document from GoldenGATE DIO ...");
			pmd.setAbortExceptionMessage("ABORTED BY USER");
			pmd.setInfoLineLimit(1);
			pmd.getWindow().setSize(400, 100);
			pmd.getWindow().setLocationRelativeTo(pmd.getWindow().getOwner());
			
			//	load in separate thread
			Thread dl = new Thread() {
				public void run() {
					
					//	open document
					try {
						ImDocumentData docData = null;
						
						if ((cache != null) && (cache.containsDocument(docId))) {
							pmd.setInfo("Loading document from cache ...");
							docData = cache.loadDocumentData(docId, (imsClient == null));
							pmd.setInfo("Document loaded from cache.");
							pmd.setProgress(100);
						}
						
						if ((docData == null) && (imsClient != null))
							docData = checkoutDocumentDataFromServer(docId, version, docName, readTimeout.getNumber().intValue(), pmd);
						
						if (docData == null)
							throw new IOException("Cannot open a document from GoldenGATE Server without authentication.");
						
						loadDocData = docData;
						
						if (cache != null) {
							cache.markOpen(docId);
							if (imsClient != null)
								cache.cleanup(imsClient);
						}
						
						dispose();
					}
					catch (RuntimeException re) {
						if (!"ABORTED BY USER".equals(re.getMessage()))
							throw re;
					}
					catch (IOException ioe) {
//						loadException = new Exception(("An error occurred while loading document '" + docName + "' from the DIO at\n" + authManager.getHost() + ":" + authManager.getPort() + "\n" + ioe.getMessage()), ioe);
					}
					finally {
						pmd.close();
					}
				}
			};
			
			dl.start();
			pmd.popUp(true);
		}
		
		private void showContextMenu(final int row, MouseEvent me) {
			if (row == -1) return;
			
			final String docId = this.listData[row].data.getValue(DOCUMENT_ID_ATTRIBUTE);
			if (docId == null) return;
			
			int preVersion = 0;
			try {
				preVersion = Integer.parseInt(this.listData[row].data.getValue(DOCUMENT_VERSION_ATTRIBUTE, "0"));
			} catch (NumberFormatException e) {}
			final int version = preVersion;
			
			String preCheckoutUser = this.listData[row].data.getValue(CHECKOUT_USER_ATTRIBUTE);
			if ((preCheckoutUser != null) && (preCheckoutUser.trim().length() == 0))
				preCheckoutUser = null;
			final String checkoutUser = preCheckoutUser;
			
			JPopupMenu menu = new JPopupMenu();
			JMenuItem mi = null;
			
			//	load document (have to exclude checked-out ones for admins, who can see them)
			if ((checkoutUser == null) || checkoutUser.equals(this.userName)) {
				mi = new JMenuItem("Load Document");
				mi.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent ae) {
						open(row, 0);
					}
				});
				menu.add(mi);
				
				//	load previous version (if available)
				if ((version > 1) && (imsClient != null)) {
					JMenu versionMenu = new JMenu("Load Document Version ...");
					
					for (int v = version; v > Math.max(0, (version - 20)); v--) {
						final int openVersion = v;
						mi = new JMenuItem("Version " + openVersion + ((openVersion == version) ? " (most recent)" : ""));
						mi.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent ae) {
								open(row, ((openVersion == version) ? 0 : openVersion));
							}
						});
						versionMenu.add(mi);
					}
					
					menu.add(versionMenu);
				}
				
				//	delete document
				if (imsClient != null) {
					mi = new JMenuItem("Delete Document");
					mi.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent ae) {
							delete(row);
						}
					});
					menu.add(mi);
				}
				
				//	cache operations
				if ((imsClient != null) && (cache != null)) {
					menu.addSeparator();
					
					//	document cached explicitly, offer release
					if (cache.isExplicitCheckout(docId)) {
						mi = new JMenuItem("Release Document");
						mi.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent ae) {
								releaseDocumentFromCache(docId, listData[row].data);
							}
						});
						menu.add(mi);
					}
					
					//	document cached, offer making checkout explicit
					else if (cache.containsDocument(docId)) {
						mi = new JMenuItem("Cache Document");
						mi.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent ae) {
								cache.markExplicitCheckout(docId);
								listData[row].data.setValue(CACHE_STATUS_ATTRIBUTE, "Localized");
								
								DialogFactory.alert("Document cached successfully.", "Document Cached", JOptionPane.INFORMATION_MESSAGE);
								
								docTableModel.update();
								docTable.validate();
								docTable.repaint();
							}
						});
						menu.add(mi);
					}
					
					//	document not in cache, offer cache checkout
					else {
						mi = new JMenuItem("Cache Document");
						mi.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent ae) {
								checkoutDocumentToCache(docId, listData[row].data);
							}
						});
						menu.add(mi);
					}
				}
			}
			
			//	release document (admin only)
			if (this.isAdmin && (imsClient != null) && (checkoutUser != null)) {
				if (mi != null) menu.addSeparator();
				
				mi = new JMenuItem("Unlock Document");
				mi.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent ae) {
						if ((checkoutUser != null) && (DialogFactory.confirm(("This document is currently locked by " + checkoutUser + ",\nunlocking it may incur that all work done by " + checkoutUser + " is lost.\nDo you really want to unlock this document?"), "Confirm Unlock Document", JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE) == JOptionPane.YES_OPTION)) try {
							if (cache != null)
								cache.unstoreDocument(docId);
							imsClient.releaseDocument(docId);
							listData[row].data.removeValue(CHECKOUT_USER_ATTRIBUTE);
							listData[row].data.removeValue(CHECKOUT_TIME_ATTRIBUTE);
							listData[row].data.removeValue(CACHE_STATUS_ATTRIBUTE);
							
							DialogFactory.alert("Document unlocked successfully.", "Document Unlocked", JOptionPane.INFORMATION_MESSAGE);
							docTableModel.update();
							docTable.validate();
							docTable.repaint();
						}
						catch (IOException ioe) {
							DialogFactory.alert(("An error occurred while unlocking the document.\n" + ioe.getClass().getName() + ": " + ioe.getMessage()), "Error Unlocking Document", JOptionPane.ERROR_MESSAGE);
							
							System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while unlocking document '" + docId + "' at " + authManager.getHost());
							ioe.printStackTrace(System.out);
						}
					}
				});
				menu.add(mi);
			}
			
			menu.show(this.docTable, me.getX(), me.getY());
		}
		
		private void checkoutDocumentToCache(final String docId, final StringTupel docData) {
			
			//	create progress monitor
			final ProgressMonitorDialog pmd = new ProgressMonitorDialog(false, true, DialogPanel.getTopWindow(), "Checking Out Document from GoldenGATE DIO ...");
			pmd.setAbortExceptionMessage("ABORTED BY USER");
			pmd.setInfoLineLimit(1);
			pmd.getWindow().setSize(400, 100);
			pmd.getWindow().setLocationRelativeTo(pmd.getWindow().getOwner());
			
			//	load in separate thread
			Thread dc = new Thread() {
				public void run() {
					try {
						String docName = docData.getValue(DOCUMENT_NAME_ATTRIBUTE);
						checkoutDocumentDataFromServer(docId, 0, docName, readTimeout.getNumber().intValue(), pmd);
						cache.markExplicitCheckout(docId);
						docData.setValue(CACHE_STATUS_ATTRIBUTE, "Localized");
						
						DialogFactory.alert("Document cached successfully.", "Document Cached", JOptionPane.INFORMATION_MESSAGE);
						
						docTableModel.update();
						docTable.validate();
						docTable.repaint();
					}
					catch (RuntimeException re) {
						if (!"ABORTED BY USER".equals(re.getMessage()))
							throw re;
					}
					catch (IOException ioe) {} // we can swallow it here, as user is notified in checkout method
					finally {
						pmd.close();
					}
				}
			};
			
			dc.start();
			pmd.popUp(true);
		}
		
		private void releaseDocumentFromCache(final String docId, final StringTupel docData) {
			
			//	create progress monitor
			final ProgressMonitorDialog pmd = new ProgressMonitorDialog(false, true, DialogPanel.getTopWindow(), "Uploading Document to GoldenGATE DIO ...");
			pmd.setAbortExceptionMessage("ABORTED BY USER");
			pmd.setInfoLineLimit(1);
			pmd.getWindow().setSize(400, 100);
			pmd.getWindow().setLocationRelativeTo(pmd.getWindow().getOwner());
			
			//	load in separate thread
			Thread dr = new Thread() {
				public void run() {
					try {
						
						//	we have uncommitted changes, upload them to server
						if (cache.isDirty(docId)) {
							pmd.setInfo("Loading document from cache ...");
							ImDocumentData cDocData = cache.getDocumentData(docId);
							String docName = docData.getValue(DOCUMENT_NAME_ATTRIBUTE);
							try {
								pmd.setInfo("Uploading document from cache ...");
								String[] uploadProtocol = imsClient.updateDocumentFromData(docId, cDocData, pmd);
								UploadProtocolDialog uploadProtocolDialog = new UploadProtocolDialog("Document Upload Protocol", ("Document '" + docName + "' successfully uploaded to GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\nDetails:"), uploadProtocol);
								uploadProtocolDialog.setVisible(true);
							}
							catch (IOException ioe) {
								if (DialogFactory.confirm(("An error occurred while uploading document '" + docName + "' to the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\n" + ioe.getMessage() + "\nRelease document anyway, discarting all cached changes?"), "Error Uploading Cached Document", JOptionPane.YES_NO_OPTION, JOptionPane.ERROR_MESSAGE) != JOptionPane.YES_OPTION)
									return;
							}
						}
						
						//	clean up
						pmd.setInfo("Cleaning up cache ...");
						cache.unstoreDocument(docId);
						imsClient.releaseDocument(docId);
						docData.removeValue(CACHE_STATUS_ATTRIBUTE);
						
						DialogFactory.alert("Document released successfully.", "Document Released", JOptionPane.INFORMATION_MESSAGE);
						
						docTableModel.update();
						docTable.validate();
						docTable.repaint();
					}
					catch (IOException ioe) {
						DialogFactory.alert(("An error occurred while releasing the document from the local cache.\n" + ioe.getClass().getName() + ": " + ioe.getMessage()), "Error Releasing Document", JOptionPane.ERROR_MESSAGE);
						
						System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while releasing cached document '" + docId + "' from GoldenGATE Server at " + authManager.getHost());
						ioe.printStackTrace(System.out);
					}
					catch (RuntimeException re) {
						if (!"ABORTED BY USER".equals(re.getMessage()))
							throw re;
					}
					finally {
						pmd.close();
					}
				}
			};
			
			dr.start();
			pmd.popUp(true);
		}
		
		ImDocumentData getDocumentData() throws Exception {
			return this.loadDocData;
		}
	}
	
	private class DocumentTableModel implements TableModel {
		
		/** the filed names of the table (raw column names) */
		public final String[] fieldNames;
		
		/** an array holding the string tupels that contain the document data to display */
		protected StringTupelTray[] listData;
		
		/**
		 * @param fieldNames
		 */
		protected DocumentTableModel(String[] fieldNames, StringTupelTray[] listData) {
			this.fieldNames = fieldNames;
			this.listData = listData;
		}
		
		void setListData(StringTupelTray[] listData) {
			this.listData = listData;
			this.update();
		}
		
		private ArrayList listeners = new ArrayList();
		public void addTableModelListener(TableModelListener tml) {
			this.listeners.add(tml);
		}
		public void removeTableModelListener(TableModelListener tml) {
			this.listeners.remove(tml);
		}
		
		/**
		 * Update the table, refreshing the display.
		 */
		public void update() {
			for (int l = 0; l < this.listeners.size(); l++)
				((TableModelListener) this.listeners.get(l)).tableChanged(new TableModelEvent(this));
		}
		
		/**
		 * Retrieve the field name at some index.
		 * @param columnIndex the index of the desired field name
		 * @return the field name at the specified index
		 */
		public String getFieldName(int columnIndex) {
			return this.fieldNames[columnIndex];
		}
		
		public String getColumnName(int columnIndex) {
			return produceFieldLabel(this.fieldNames[columnIndex]);
		}
		public Class getColumnClass(int columnIndex) {
			return String.class;
		}
		public int getColumnCount() {
			return this.fieldNames.length;
		}
		public int getRowCount() {
			return listData.length;
		}
		public boolean isCellEditable(int rowIndex, int columnIndex) {
			return false;
		}
		public void setValueAt(Object aValue, int rowIndex, int columnIndex) {}
		
		public Object getValueAt(int rowIndex, int columnIndex) {
			String fieldName = this.fieldNames[columnIndex];
			String fieldValue = "";
			final StringTupel rowData = listData[rowIndex].data;
			
			//	format title
//			if (DOCUMENT_TITLE_ATTRIBUTE.equals(fieldName)) {
//				fieldValue = createDisplayTitle(new Properties() {
//					public String getProperty(String key, String defaultValue) {
//						return rowData.getValue(key, defaultValue);
//					}
//					public String getProperty(String key) {
//						return rowData.getValue(key);
//					}
//				});
//			}
//			else
			fieldValue = rowData.getValue(this.fieldNames[columnIndex], "");
			
			//	format timestamp
			if (false
					|| CHECKIN_TIME_ATTRIBUTE.equals(fieldName)
					|| UPDATE_TIME_ATTRIBUTE.equals(fieldName)
					|| CHECKOUT_TIME_ATTRIBUTE.equals(fieldName)
					) {
				if (fieldValue.matches("[0-9]++")) try {
					fieldValue = TIMESTAMP_DATE_FORMAT.format(new Date(Long.parseLong(fieldValue)));
				} catch (NumberFormatException e) {}
			}
			
			return (fieldValue);
		}
	}
	
	private class UploadProtocolDialog extends JFrame {
		private String[] uploadProtocol = new String[0];
		private JTable protocolList;
		private JButton closeButton;
		UploadProtocolDialog(String title, String label, String[] up) {
			super(title);
			this.setIconImage(parent.getGoldenGateIcon());
			this.protocolList = new JTable(new TableModel() {
				public int getColumnCount() {
					return 1;
				}
				public int getRowCount() {
					return uploadProtocol.length;
				}
				public String getColumnName(int columnIndex) {
					if (columnIndex == 0) return "";
					return null;
				}
				public Class getColumnClass(int columnIndex) {
					return String.class;
				}
				public boolean isCellEditable(int rowIndex, int columnIndex) {
					return false;
				}
				public Object getValueAt(int rowIndex, int columnIndex) {
					if (columnIndex == 0)
						return uploadProtocol[rowIndex];
					return null;
				}
				public void setValueAt(Object aValue, int rowIndex, int columnIndex) {}
				
				public void addTableModelListener(TableModelListener l) {}
				public void removeTableModelListener(TableModelListener l) {}
			});
			this.protocolList.setShowHorizontalLines(true);
			this.protocolList.setShowVerticalLines(false);
			this.protocolList.setTableHeader(null);
			
			JScrollPane resultListBox = new JScrollPane(this.protocolList);
			resultListBox.setViewportBorder(BorderFactory.createLoweredBevelBorder());
			
			this.closeButton = new JButton("Background");
			this.closeButton.setBorder(BorderFactory.createRaisedBevelBorder());
			this.closeButton.setPreferredSize(new Dimension(100, 21));
			this.closeButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					dispose();
				}
			});
			
			this.getContentPane().setLayout(new BorderLayout());
			if (label != null)
				this.getContentPane().add(new JLabel(label, JLabel.LEFT), BorderLayout.NORTH);
			this.getContentPane().add(resultListBox, BorderLayout.CENTER);
			this.getContentPane().add(this.closeButton, BorderLayout.SOUTH);
			
			this.setSize(500, 600);
			this.setLocationRelativeTo(DialogPanel.getTopWindow());
			
			this.setUploadProtocol(up);
		}
		void setUploadProtocol(String[] up) {
			if (this.uploadProtocol.length == up.length)
				return;
			this.uploadProtocol = up;
			this.protocolList.validate();
			this.protocolList.repaint();
			if ((this.uploadProtocol.length != 0) && (UPDATE_COMPLETE.equals(this.uploadProtocol[this.uploadProtocol.length-1]) || DELETION_COMPLETE.equals(this.uploadProtocol[this.uploadProtocol.length-1]))) {
				this.closeButton.setText("OK");
				this.closeButton.validate();
				this.closeButton.repaint();
			}
			this.setVisible(true);
			this.validate();
			this.repaint();
			this.toFront();
		}
	}
	
	private static class DocumentCache implements DocumentDataCache {
		private static final String EXPLICIT_CHECKOUT = "EC";
		private static final String DIRTY = "D";
		private StringVector metaDataStorageKeys = new StringVector();
		
		private GoldenGatePluginDataProvider dataProvider;
		private String host;
		private String user;
		
		private String cachePrefix;
		private TreeMap cacheMetaData = new TreeMap();
		private TreeSet openDocuments = new TreeSet();
		
		DocumentCache(GoldenGatePluginDataProvider dataProvider, String host, String user) {
			this.dataProvider = dataProvider;
			this.host = host;
			this.user = user;
			
			//	compute data prefix
			this.cachePrefix = ("cache/" + (this.host + "." + this.user).hashCode() + "/");
			
			//	load meta data
			try {
				Reader cmdIn = new InputStreamReader(this.dataProvider.getInputStream(this.cachePrefix + "MetaData.cnfg"));
				StringRelation cmd = StringRelation.readCsvData(cmdIn, '"');
				cmdIn.close();
				for (int d = 0; d < cmd.size(); d++) {
					StringTupel dmd = cmd.get(d);
					String cDocId = dmd.getValue(DOCUMENT_ID_ATTRIBUTE);
					if (cDocId != null)
						this.cacheMetaData.put(cDocId, dmd);
					
					//	learn keys
					this.metaDataStorageKeys.addContentIgnoreDuplicates(dmd.getKeys());
				}
			}
			catch (IOException ioe) {
				System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while loading document cache meta data for document cache '" + this.cachePrefix + "'");
				ioe.printStackTrace(System.out);
				this.metaDataStorageKeys.addContentIgnoreDuplicates(cacheDocumentListAttributes);
			}
			
			//	make sure local attributes are stored
			this.metaDataStorageKeys.addElementIgnoreDuplicates(EXPLICIT_CHECKOUT);
			this.metaDataStorageKeys.addElementIgnoreDuplicates(DIRTY);
		}
		
		public ImDocumentData getDocumentData(String docId) throws IOException {
			return new ImsClientCacheDocumentData(docId);
		}
		
		private class ImsClientCacheDocumentData extends ImDocumentData {
			final String docId;
			final String docCachePrefix;
			ImsClientCacheDocumentData(String docId) throws IOException {
				this.docId = docId;
				this.docCachePrefix = (cachePrefix + this.docId + "/");
				
				//	load entry list
				String entryDataName = (this.docCachePrefix + "entries.txt");
				if (dataProvider.isDataAvailable(entryDataName)) {
					BufferedReader entryIn = new BufferedReader(new InputStreamReader(dataProvider.getInputStream(entryDataName), "UTF-8"));
					for (String imfEntryLine; (imfEntryLine = entryIn.readLine()) != null;) {
						ImDocumentEntry entry = ImDocumentEntry.fromTabString(imfEntryLine);
						if (entry != null)
							this.putEntry(entry);
					}
					entryIn.close();
				}
			}
			public boolean hasEntryData(ImDocumentEntry entry) {
				return dataProvider.isDataAvailable(this.docCachePrefix + entry.getFileName());
			}
			public InputStream getInputStream(String entryName) throws IOException {
				ImDocumentEntry entry = this.getEntry(entryName);
				if (entry == null)
					throw new FileNotFoundException(entryName);
				return dataProvider.getInputStream(this.docCachePrefix + entry.getFileName());
			}
			public OutputStream getOutputStream(final String entryName, boolean writeDirectly) throws IOException {
				
				//	split file name from file extension so data hash can be inserted in between
				final String entryDataFileName;
				final String entryDataFileExtension;
				if (entryName.indexOf('.') == -1) {
					entryDataFileName = entryName;
					entryDataFileExtension = "";
				}
				else {
					entryDataFileName = entryName.substring(0, entryName.lastIndexOf('.'));
					entryDataFileExtension = entryName.substring(entryName.lastIndexOf('.'));
				}
				
				//	write to buffer first, as we cannot rename via a data provider
				return new DataHashOutputStream(new ByteArrayOutputStream()) {
					public void close() throws IOException {
						super.flush();
						super.close();
						
						//	write buffer content to persistent storage only if not already there
						String entryDataName = (docCachePrefix + entryDataFileName + "." + this.getDataHash() + entryDataFileExtension);
						if (!dataProvider.isDataAvailable(entryDataName)) {
							BufferedOutputStream out = new BufferedOutputStream(dataProvider.getOutputStream(entryDataName));
							((ByteArrayOutputStream) this.out).writeTo(out);
							out.flush();
							out.close();
						}
						
						//	update entry list
						putEntry(new ImDocumentEntry(entryName, System.currentTimeMillis(), this.getDataHash()));
					}
				};
			}
			public String getDocumentDataId() {
				return this.docCachePrefix;
			}
			public boolean canLoadDocument() {
				return this.hasEntry("document.csv");
			}
			public boolean canStoreDocument() {
				return true;
			}
		}
		
		public void storeEntryList(ImDocumentData docData) throws IOException {
			ImsClientCacheDocumentData iDocData = ((ImsClientCacheDocumentData) docData);
			ImDocumentEntry[] entries = iDocData.getEntries();
			BufferedWriter entryOut = new BufferedWriter(new OutputStreamWriter(dataProvider.getOutputStream(iDocData.docCachePrefix + "entries.txt"), "UTF-8"));
			for (int e = 0; e < entries.length; e++) {
				entryOut.write(entries[e].toTabString());
				entryOut.newLine();
			}
			entryOut.flush();
			entryOut.close();
		}
		
		private void storeMetaData() throws IOException {
			StringRelation cmd = new StringRelation();
			for (Iterator cdit = this.cacheMetaData.values().iterator(); cdit.hasNext();)
				cmd.addElement((StringTupel) cdit.next());
			OutputStreamWriter out = new OutputStreamWriter(this.dataProvider.getOutputStream(this.cachePrefix + "MetaData.cnfg"), ENCODING);
			StringRelation.writeCsvData(out, cmd, '"', this.metaDataStorageKeys);
			out.flush();
			out.close();
		}
		
		synchronized void close() {
			try {
				this.storeMetaData();
				this.cacheMetaData.clear();
			}
			catch (IOException ioe) {
				System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while storing document cache meta data for document cache '" + this.cachePrefix + "'");
				ioe.printStackTrace(System.out);
			}
		}
		
		synchronized boolean belongsTo(String host, String user) {
			return (this.host.equals(host) && this.user.equals(user));
		}
		
		synchronized boolean storeDocument(ImDocument doc, String documentName) {
			
			//	collect meta data
			String time = ("" + System.currentTimeMillis());
			String docId = doc.docId;
			
			String docTitle = ((String) doc.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, documentName));
			
			String checkinUser = ((String) doc.getAttribute(CHECKIN_USER_ATTRIBUTE, this.user));
			String checkinTime = ((String) doc.getAttribute(CHECKIN_TIME_ATTRIBUTE, time));
			
			String docVersion = ((String) doc.getAttribute(DOCUMENT_VERSION_ATTRIBUTE, "-1"));
			
			//	organize meta data
			StringTupel docMetaData = ((StringTupel) this.cacheMetaData.get(docId));
			if (docMetaData == null)
				docMetaData = new StringTupel();
			
			docMetaData.setValue(DOCUMENT_ID_ATTRIBUTE, docId);
			
			docMetaData.setValue(DOCUMENT_NAME_ATTRIBUTE, documentName);
			docMetaData.setValue(DOCUMENT_TITLE_ATTRIBUTE, docTitle);
			
			docMetaData.setValue(CHECKIN_USER_ATTRIBUTE, checkinUser);
			docMetaData.setValue(CHECKIN_TIME_ATTRIBUTE, checkinTime);
			
			docMetaData.setValue(UPDATE_USER_ATTRIBUTE, this.user);
			docMetaData.setValue(UPDATE_TIME_ATTRIBUTE, time);
			
			docMetaData.setValue(DOCUMENT_VERSION_ATTRIBUTE, docVersion);
			
			try {
				
				//	mark as dirty (remember there are cached changes not forwarded to server yet)
				docMetaData.setValue(DIRTY, DIRTY);
				this.cacheMetaData.put(docId, docMetaData);
				this.metaDataStorageKeys.addContentIgnoreDuplicates(docMetaData.getKeys());
				this.storeMetaData();
				
				//	indicate success
				return true;
			}
			catch (IOException ioe) {
				DialogFactory.alert(("An error occurred while storing document '" + documentName + "' to local cache for GoldenGATE Server at " + this.host + "\n" + ioe.getMessage()), ("Error Caching Document"), JOptionPane.ERROR_MESSAGE);
				return false;
			}
		}
		
		synchronized ImDocumentData loadDocumentData(String docId, boolean showError) throws IOException {
			try {
				return this.getDocumentData(docId);
			}
			catch (IOException ioe) {
				System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while loading document '" + docId + "' from cache");
				ioe.printStackTrace(System.out);
				if (showError) {
					DialogFactory.alert(("An error occurred while loading document '" + docId + "' from cache of GoldenGATE Server at '" + this.host + "'\n" + ioe.getMessage()), ("Error Loading Document From Cache"), JOptionPane.ERROR_MESSAGE);
					throw ioe;
				}
				else return null;
			}
		}
		
		synchronized void unstoreDocument(String docId) throws IOException {
			this.openDocuments.remove(docId);
			this.cacheMetaData.remove(docId);
			//	TODO clean up cached data files
			this.storeMetaData();
		}
		
		synchronized ImsDocumentListBuffer getDocumentList() {
			ImsDocumentListBuffer documentList = new ImsDocumentListBuffer(cacheDocumentListAttributes);
			for (Iterator cdit = this.cacheMetaData.values().iterator(); cdit.hasNext();) {
				StringTupel dmd = ((StringTupel) cdit.next());
				StringTupel listDmd = new StringTupel();
				for (int f = 0; f < documentList.listFieldNames.length; f++) {
					String listFieldValue = dmd.getValue(documentList.listFieldNames[f]);
					if (listFieldValue != null)
						listDmd.setValue(documentList.listFieldNames[f], listFieldValue);
				}
				if (listDmd.size() != 0)
					documentList.addElement(listDmd);
			}
			return documentList;
		}
		
		synchronized boolean containsDocument(String docId) {
			return this.cacheMetaData.containsKey(docId);
		}
		
		synchronized void markExplicitCheckout(String docId) {
			StringTupel dmd = ((StringTupel) this.cacheMetaData.get(docId));
			if (dmd != null) try {
				dmd.setValue(EXPLICIT_CHECKOUT, EXPLICIT_CHECKOUT);
				this.storeMetaData();
			}
			catch (IOException ioe) {
				System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while storing document cache meta data for document cache '" + this.cachePrefix + "'");
				ioe.printStackTrace(System.out);
			}
		}
		
		synchronized boolean isExplicitCheckout(String docId) {
			StringTupel dmd = ((StringTupel) this.cacheMetaData.get(docId));
			return ((dmd != null) && EXPLICIT_CHECKOUT.equals(dmd.getValue(EXPLICIT_CHECKOUT)));
		}
		
		synchronized boolean isDirty(String docId) {
			StringTupel dmd = ((StringTupel) this.cacheMetaData.get(docId));
			return ((dmd == null) ? false : (dmd.getValue(DIRTY) != null));
		}
		
		synchronized void markNotDirty(String docId) {
			StringTupel dmd = ((StringTupel) this.cacheMetaData.get(docId));
			if (dmd != null) try {
				dmd.removeValue(DIRTY);
				this.storeMetaData();
			}
			catch (IOException ioe) {
				System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while storing document cache meta data for document cache '" + this.cachePrefix + "'");
				ioe.printStackTrace(System.out);
			}
		}
		
		synchronized void markOpen(String docId) {
			this.openDocuments.add(docId);
		}
		
		synchronized boolean isOpen(String docId) {
			return this.openDocuments.contains(docId);
		}
		
		synchronized void flush(GoldenGateImsClient imsClient, ProgressMonitor pm) {
			
			//	upload current version of all dirty documents
			for (Iterator cdit = (new ArrayList(this.cacheMetaData.values())).iterator(); cdit.hasNext();) {
				StringTupel dmd = ((StringTupel) cdit.next());
				String docId = dmd.getValue(DOCUMENT_ID_ATTRIBUTE);
				String docName = dmd.getValue(DOCUMENT_NAME_ATTRIBUTE);
				
				//	got required meta data
				if ((docId != null) && this.isDirty(docId) && (docName != null)) try {
					
					//	get document data
					ImDocumentData docData = this.getDocumentData(docId);
					String[] uploadLog = imsClient.updateDocumentFromData(docId, docData, ProgressMonitor.dummy);
					System.out.println("Cached document '" + docId + "' uploaded to GoldenGATE Server at " + this.host + ":");
					for (int l = 0; l < uploadLog.length; l++)
						System.out.println(uploadLog[l]);
					this.markNotDirty(docId);
				}
				catch (IOException ioe) {
					System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while uploading cached document '" + docId + "' to GoldenGATE Server at " + this.host);
					ioe.printStackTrace(System.out);
				}
			}
		}
		
		synchronized void cleanup(GoldenGateImsClient imsClient) {
			
			//	upload current version of all dirty documents
			for (Iterator cdit = (new ArrayList(this.cacheMetaData.values())).iterator(); cdit.hasNext();) {
				StringTupel dmd = ((StringTupel) cdit.next());
				String docId = dmd.getValue(DOCUMENT_ID_ATTRIBUTE);
				
				//	got required meta data
				if ((docId != null) && !this.isExplicitCheckout(docId) && !this.isOpen(docId)) {
					try {
						this.unstoreDocument(docId);
						imsClient.releaseDocument(docId);
					}
					catch (IOException ioe) {
						System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while releasing cached document '" + docId + "' at GoldenGATE Server at " + this.host);
						ioe.printStackTrace(System.out);
					}
				}
			}
		}
	}
	
	private static class MemoryWritableDataProvider implements GoldenGatePluginDataProvider {
		private GoldenGatePluginDataProvider dataProvider;
		private HashMap dataCache = new HashMap();
		MemoryWritableDataProvider(GoldenGatePluginDataProvider dataProvider) {
			this.dataProvider = dataProvider;
		}
		public boolean isDataAvailable(String dataName) {
			return (this.dataCache.containsKey(dataName) || this.dataProvider.isDataAvailable(dataName));
		}
		public InputStream getInputStream(String dataName) throws IOException {
			if (this.dataCache.containsKey(dataName))
				return new ByteArrayInputStream((byte[]) this.dataCache.get(dataName));
			else return this.dataProvider.getInputStream(dataName);
		}
		public URL getURL(String dataName) throws IOException {
			return this.dataProvider.getURL(dataName);
		}
		public boolean isDataEditable() {
			return true;
		}
		public boolean isDataEditable(String dataName) {
			return true;
		}
		public OutputStream getOutputStream(final String dataName) throws IOException {
			return new ByteArrayOutputStream() {
				public void close() throws IOException {
					super.close();
					dataCache.put(dataName, this.toByteArray());
				}
			};
		}
		public boolean deleteData(String dataName) {
			return (this.dataCache.remove(dataName) != null);
		}
		public String[] getDataNames() {
			TreeSet dataNames = new TreeSet(Arrays.asList(this.dataProvider.getDataNames()));
			dataNames.addAll(this.dataCache.keySet());
			return ((String[]) dataNames.toArray(new String[dataNames.size()]));
		}
		public boolean allowWebAccess() {
			return this.dataProvider.allowWebAccess();
		}
		public String getAbsolutePath() {
			return this.dataProvider.getAbsolutePath();
		}
		public boolean equals(GoldenGatePluginDataProvider dp) {
			return false;
		}
	}
	
	public static void main(String[] args) throws Exception {
//		ServerConnection sc = ServerConnection.getServerConnection("localhost", 8015);
//		AuthenticatedClient ac = AuthenticatedClient.getAuthenticatedClient(sc);
//		ac.login("Admin", "GG");
		
		AuthenticationManager.setHost("localhost");
		AuthenticationManager.setPort(8015);
		AuthenticationManager.setUser("Admin");
		AuthenticationManager.setPassword("GG");
		AuthenticationManager.getAuthenticatedClient();
		
		AuthenticationManagerPlugin auth = new AuthenticationManagerPlugin();
		auth.setDataProvider(new PluginDataProviderFileBased(new File("E:/GoldenGATEv3/Plugins/AuthManagerPluginData/")));
		auth.init();
		
		GoldenGateImsDocumentIO imsIo = new GoldenGateImsDocumentIO();
		imsIo.setDataProvider(new PluginDataProviderFileBased(new File("E:/GoldenGATEv3/Plugins/ImsDocumentIOData/")));
		imsIo.authManager = auth;
		imsIo.init();
		imsIo.initImagine();
		
		imsIo.loadDocument(ProgressMonitor.dummy);
//		
//		ggic.releaseDocument("FFF1CA60FFCDF655E279E450FFFD2C09");
//		
//		ImDocument doc = ggic.getDocument("FFF1CA60FFCDF655E279E450FFFD2C09", null);
////		OutputStream docOut = new BufferedOutputStream(new FileOutputStream(new File("E:/Testdaten/PdfExtract/zt00872.pdf.resaved.imf")));
////		ImfIO.storeDocument(doc, docOut);
////		docOut.flush();
////		docOut.close();
//		doc.getPage(0).removeAttribute("test");
//		doc.getPage(0).setAttribute("test2");
//		ggic.uploadDocument(doc, "TEST", null);
	}
}