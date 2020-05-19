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
package de.uka.ipd.idaho.goldenGateServer.ims.client;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.event.TableModelListener;
import javax.swing.table.TableModel;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.util.ControllingProgressMonitor;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor.CascadingProgressMonitor;
import de.uka.ipd.idaho.gamta.util.swing.DialogFactory;
import de.uka.ipd.idaho.gamta.util.swing.DocumentListPanel;
import de.uka.ipd.idaho.gamta.util.swing.DocumentListPanel.DocumentFilter;
import de.uka.ipd.idaho.gamta.util.swing.ProgressMonitorDialog;
import de.uka.ipd.idaho.gamta.util.transfer.DocumentListBuffer;
import de.uka.ipd.idaho.gamta.util.transfer.DocumentListElement;
import de.uka.ipd.idaho.goldenGate.plugins.GoldenGatePluginDataProvider;
import de.uka.ipd.idaho.goldenGate.plugins.PluginDataProviderFileBased;
import de.uka.ipd.idaho.goldenGate.util.DialogPanel;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants;
import de.uka.ipd.idaho.goldenGateServer.ims.client.GoldenGateImsClient.DocumentDataCache;
import de.uka.ipd.idaho.goldenGateServer.ims.client.GoldenGateImsClient.FastFetchFilter;
import de.uka.ipd.idaho.goldenGateServer.ims.client.GoldenGateImsClient.ImsClientDocumentData;
import de.uka.ipd.idaho.goldenGateServer.ims.data.ImsDocumentList;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticatedClient;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticationManager;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticationManagerPlugin;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.ImSupplement;
import de.uka.ipd.idaho.im.imagine.plugins.AbstractGoldenGateImaginePlugin;
import de.uka.ipd.idaho.im.imagine.plugins.GoldenGateImagineDocumentListener;
import de.uka.ipd.idaho.im.imagine.plugins.ImageDocumentIoProvider;
import de.uka.ipd.idaho.im.util.ImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.DataBackedImDocument;
import de.uka.ipd.idaho.im.util.ImDocumentData.ImDocumentEntry;
import de.uka.ipd.idaho.im.util.ImDocumentIO;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringRelation;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * DocumentIO plug-in for GoldenGATE Imagine supporting document IO with
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
	private TreeMap cacheDocumentData = new TreeMap();
	
	private HashSet openDocumentIDs = new HashSet();
	private HashMap openProtocolDialogs = new HashMap();
	
	private StringVector listFieldOrder = new StringVector();
	private StringVector listFields = new StringVector();
	
	private Properties listFieldLabels = new Properties();
	
	private int pageImageFetchMode = FastFetchFilter.FETCH_DEFERRED;
	private int supplementFetchMode = FastFetchFilter.FETCH_ON_DEMAND;
	private FastFetchFilter fastFetchFilter = new FastFetchFilter() {
		public int getFetchMode(ImDocumentEntry entry) {
			if (entry.name.startsWith(ImSupplement.SOURCE_TYPE + "."))
				return supplementFetchMode;
			else if (entry.name.startsWith(ImSupplement.FIGURE_TYPE + "@"))
				return supplementFetchMode;
			else if (entry.name.startsWith(ImSupplement.SCAN_TYPE + "@"))
				return supplementFetchMode;
			else if (entry.name.startsWith("page") && entry.name.endsWith(".png")) {
				String pidStr = entry.name;
				pidStr = pidStr.substring("page".length());
				pidStr = pidStr.substring(0, (pidStr.length() - ".png".length()));
				while (pidStr.startsWith("0"))
					pidStr = pidStr.substring("0".length());
				try {
					int pid = Integer.parseInt(pidStr);
					return ((pid < 5) ? FETCH_IMMEDIATELY : pageImageFetchMode);
				}
				catch (NumberFormatException nfe) {
					return FETCH_IMMEDIATELY;
				}
				
			}
			else return FETCH_IMMEDIATELY;
		}
	};
	private String fastFetchFilterLabel = "Page Images: fetch deferred - Supplements: fetch on demand";
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGate.plugins.AbstractGoldenGatePlugin#init()
	 */
	public void init() {
		
		//	get authentication manager
		this.authManager = ((AuthenticationManagerPlugin) this.parent.getPlugin(AuthenticationManagerPlugin.class.getName()));
		
		//	read display and fast-fetch configuration
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
			
			try {
				this.pageImageFetchMode = Integer.parseInt(set.getSetting("pageImageFetchMode", ("" + this.pageImageFetchMode)));
			} catch (NumberFormatException nfe) {}
			try {
				this.supplementFetchMode = Integer.parseInt(set.getSetting("supplementFetchMode", ("" + this.supplementFetchMode)));
			} catch (NumberFormatException nfe) {}
			this.updateFastFetchFilterLabel();
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
		
		//	store fast fetch settings
		if (this.dataProvider.isDataEditable("config.cnfg")) try {
			InputStream is = this.dataProvider.getInputStream("config.cnfg");
			Settings set = Settings.loadSettings(is);
			is.close();
			
			set.setSetting("pageImageFetchMode", ("" + this.pageImageFetchMode));
			set.setSetting("supplementFetchMode", ("" + this.supplementFetchMode));
			
			OutputStream out = this.dataProvider.getOutputStream("config.cnfg");
			set.storeAsText(out);
			out.flush();
			out.close();
		}
		catch (IOException ioe) {
			System.out.println(ioe.getClass() + " (" + ioe.getMessage() + ") while storing ImsDocumentIO settings.");
			ioe.printStackTrace(System.out);
		}
	}
	
	boolean editFastFetchStrategy() {
		JComboBox piFetchMode = new JComboBox(fetchModeOptions);
		piFetchMode.setSelectedIndex(this.pageImageFetchMode);
		piFetchMode.setEditable(false);
		JPanel piFetchModePanel = new JPanel(new BorderLayout());
		piFetchModePanel.add(new JLabel("Page Images: fetch "), BorderLayout.CENTER);
		piFetchModePanel.add(piFetchMode, BorderLayout.EAST);
		
		JComboBox sFetchMode = new JComboBox(fetchModeOptions);
		sFetchMode.setSelectedIndex(this.supplementFetchMode);
		sFetchMode.setEditable(false);
		JPanel sFetchModePanel = new JPanel(new BorderLayout());
		sFetchModePanel.add(new JLabel("Supplements: fetch "), BorderLayout.CENTER);
		sFetchModePanel.add(sFetchMode, BorderLayout.EAST);
		
		JPanel fetchModePanel = new JPanel(new GridLayout(0, 1));
		fetchModePanel.add(piFetchModePanel);
		fetchModePanel.add(sFetchModePanel);
		
		int choice = DialogFactory.confirm(fetchModePanel, "Select Fast Fetch Options", JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE);
		if (choice != JOptionPane.OK_OPTION)
			return false;
		
		this.pageImageFetchMode = ((FetchModeOption) piFetchMode.getSelectedItem()).mode;
		this.supplementFetchMode = ((FetchModeOption) sFetchMode.getSelectedItem()).mode;
		this.updateFastFetchFilterLabel();
		return true;
	}
	
	private void updateFastFetchFilterLabel() {
		this.fastFetchFilterLabel = ("Page Images: fetch " + getFetchModeLabel(this.pageImageFetchMode) + " - Supplements: fetch " + getFetchModeLabel(this.supplementFetchMode));
	}
	
	private static class FetchModeOption {
		final int mode;
		final String label;
		FetchModeOption(int mode) {
			this.mode = mode;
			this.label = getFetchModeLabel(this.mode);
		}
		public String toString() {
			return this.label;
		}
	}
	private static FetchModeOption[] fetchModeOptions = {
		new FetchModeOption(FastFetchFilter.FETCH_IMMEDIATELY),
		new FetchModeOption(FastFetchFilter.FETCH_DEFERRED),
		new FetchModeOption(FastFetchFilter.FETCH_ON_DEMAND)
	};
	private static String getFetchModeLabel(int fm) {
		if (fm == FastFetchFilter.FETCH_ON_DEMAND)
			return "on demand";
		else if (fm == FastFetchFilter.FETCH_DEFERRED)
			return "deferred";
		else return "immediately";
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.im.imagine.plugins.GoldenGateImagineDocumentListener#documentOpened(de.uka.ipd.idaho.im.ImDocument, java.lang.Object, de.uka.ipd.idaho.gamta.util.ProgressMonitor)
	 */
	public void documentOpened(ImDocument doc, Object source, ProgressMonitor pm) {}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.im.imagine.plugins.GoldenGateImagineDocumentListener#documentSelected(de.uka.ipd.idaho.im.ImDocument)
	 */
	public void documentSelected(ImDocument doc) {}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.im.imagine.plugins.GoldenGateImagineDocumentListener#documentSaving(de.uka.ipd.idaho.im.ImDocument, java.lang.Object, de.uka.ipd.idaho.gamta.util.ProgressMonitor)
	 */
	public void documentSaving(ImDocument doc, Object dest, ProgressMonitor pm) {
		
		//	we're storing this document ourselves, we know how to handle any virtual entries
		if (dest == this)
			return;
		
		//	get document data
		ImsClientDocumentData docData = getDocumentData(doc);
		
		//	no way we loaded this one
		if (docData == null)
			return;
		
		//	no virtual entries to handle
		if (!docData.hasVirtualEntries())
			return;
		
		//	throw exception if not logged in
		if (!this.ensureLoggedIn(pm))
			throw new RuntimeException("Cannot fetch virtual entries without connection to IMS.");
		
		//	connect document data to backing IMS
		docData.setImsClient(this.imsClient);
		
		//	fetch all virtual entries before storing to other destination
		try {
			if (pm != null)
				pm.setStep("Fetching vitual entries from backing IMS");
			docData.fetchVirtualEntries(pm);
			docData.setImsClient(null);
		}
		catch (IOException ioe) {
			throw new RuntimeException("Failed to fetch virtual entries from backing IMS.", ioe);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.im.imagine.plugins.GoldenGateImagineDocumentListener#documentSaved(de.uka.ipd.idaho.im.ImDocument, java.lang.Object, de.uka.ipd.idaho.gamta.util.ProgressMonitor)
	 */
	public void documentSaved(ImDocument doc, Object dest, ProgressMonitor pm) {}
	
	private static ImsClientDocumentData getDocumentData(ImDocument doc) {
		if (doc instanceof DataBackedImDocument) {
			ImDocumentData docData = ((DataBackedImDocument) doc).getDocumentData();
			if (docData instanceof ImsClientDocumentData)
				return ((ImsClientDocumentData) docData);
		}
		return null;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.im.imagine.plugins.GoldenGateImagineDocumentListener#documentClosed(java.lang.String)
	 */
	public void documentClosed(String docId) {
		
		//	close any open update status monitor
		UploadProtocolDialog upDialog = ((UploadProtocolDialog) this.openProtocolDialogs.get(docId));
		if (upDialog != null)
			upDialog.dispose();
		
		//	we did not load this one
		if (!this.openDocumentIDs.remove(docId))
			return;
		
		//	clear cached document data object
		for (Iterator cddit = this.cacheDocumentData.keySet().iterator(); cddit.hasNext();) {
			String cddKey = ((String) cddit.next());
			if (cddKey.endsWith(docId)) {
				ImsClientDocumentData imsDocData = ((ImsClientDocumentData) this.cacheDocumentData.get(cddKey));
				if (imsDocData != null)
					imsDocData.dispose();
				cddit.remove();
			}
		}
		
		//	we've been logged out
		if ((this.authClient == null) || (this.imsClient == null))
			return;
		
		//	release document if not checked out explicitly
		if ((this.cache == null) || !this.cache.isExplicitCheckout(docId)) try {
			this.authClient.ensureLoggedIn();
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
		pm.setInfo("Checking authentication at GoldenGATE Server ...");
		if (pm instanceof ControllingProgressMonitor) {
			((ControllingProgressMonitor) pm).setPauseResumeEnabled(true);
			((ControllingProgressMonitor) pm).setAbortEnabled(true);
			((ControllingProgressMonitor) pm).setAbortExceptionMessage("ABORTED BY USER");
		}
		this.ensureLoggedIn(pm);
		
		//	offer fetching document directly by name or ID, without loading whole list
		pm.setInfo("Opening direct loading dialog ...");
		
		//	display document ID input
		DocumentLoadDialog dlod = new DocumentLoadDialog("Open Document Directly");
		dlod.setLocationRelativeTo(DialogPanel.getTopWindow());
		dlod.setVisible(true);
		
		//	get selected document
		ImDocumentData docData = dlod.getDocumentData();
		if (docData == null)
			return null;
		
		//	forwarded to document list
		if (docData == LOAD_DOCUMENT_LIST)
			docData = null;
		
		//	load selected document (unless forwarded to list)
		else try {
			ImDocument doc = ImDocumentIO.loadDocument(docData, pm);
			
			//	remember opening document, so we can release it when it's closed
			this.openDocumentIDs.add(doc.docId);
			
			//	finally ...
			return doc;
		}
		catch (Throwable t) {
			t.printStackTrace(System.out);
		}
		
		
		//	get list of documents
		DocumentListBuffer documentList;
		boolean documentListEmpty = true;
		
		//	got server connection, load document list
		if (this.imsClient != null) {
			pm.setInfo("Connected, getting document list from GoldenGATE Server ...");
			
			//	load document list
			try {
				ImsDocumentList dl = this.imsClient.getDocumentList(pm);
				if (this.cache != null)
					dl = this.addCacheStatus(dl);
				for (int f = 0; f < dl.listFieldNames.length; f++) {
					ImsDocumentList.AttributeSummary das = dl.getListFieldValues(dl.listFieldNames[f]);
					if ((das != null) && (das.size() != 0)) {
						documentListEmpty = false;
						break;
					}
				}
				pm.setInfo("Got document list, caching content ...");
				documentList = new DocumentListBuffer(dl, pm);
				
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
			DocumentListDialog dlid = new DocumentListDialog("Select Document", documentList, this.authManager.getUser(), ((this.authClient != null) && this.authClient.isAdmin()));
			dlid.setLocationRelativeTo(DialogPanel.getTopWindow());
			dlid.setVisible(true);
			
			//	get selected document
			docData = dlid.getDocumentData();
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
	
	private class DocumentLoadDialog extends DialogPanel {
		
		private JTextField docIdField = new JTextField();
		
		private ImDocumentData loadDocData = null;
		
		DocumentLoadDialog(String title) {
			super(title, true);
			
			JPanel docIdPanel = new JPanel(new BorderLayout(), true);
			docIdPanel.add(new JLabel("Document ID: ", JLabel.LEFT), BorderLayout.WEST);
			docIdPanel.add(this.docIdField, BorderLayout.CENTER);
			this.docIdField.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					openDoc();
				}
			});
			
			JPanel contentPanel = new JPanel(new BorderLayout(), true);
			contentPanel.add(new JLabel("Enter document ID to load a specific document directly, or proceed to document list.", JLabel.CENTER), BorderLayout.CENTER);
			contentPanel.add(docIdPanel, BorderLayout.SOUTH);
			
			JButton listButton = new JButton("Show List");
			listButton.setBorder(BorderFactory.createRaisedBevelBorder());
			listButton.setPreferredSize(new Dimension(100, 21));
			listButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					loadDocData = LOAD_DOCUMENT_LIST;
					dispose();
				}
			});
			
			JButton loadButton = new JButton("Load");
			loadButton.setBorder(BorderFactory.createRaisedBevelBorder());
			loadButton.setPreferredSize(new Dimension(100, 21));
			loadButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					openDoc();
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
			
			final JButton fetchModeButton = new JButton("Fetch Mode");
			fetchModeButton.setBorder(BorderFactory.createRaisedBevelBorder());
			fetchModeButton.setPreferredSize(new Dimension(100, 21));
			fetchModeButton.setToolTipText(fastFetchFilterLabel);
			fetchModeButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					if (editFastFetchStrategy())
						fetchModeButton.setToolTipText(fastFetchFilterLabel);
				}
			});
			
			JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
			buttonPanel.add(listButton);
			buttonPanel.add(loadButton);
			buttonPanel.add(cancelButton);
			buttonPanel.add(fetchModeButton);
			
			this.add(contentPanel, BorderLayout.CENTER);
			this.add(buttonPanel, BorderLayout.SOUTH);
			
			this.setSize(new Dimension(500, 120));
		}
		
		private void openDoc() {
			String docId = this.docIdField.getText().trim();
			docId = docId.replaceAll("\\-", "");
			if (docId.matches("[0-9A-Fa-f]{32}"))
				this.openDoc(docId);
			else DialogFactory.alert("Please enter a valid document ID for direct loading.", "Invalid Document ID", JOptionPane.ERROR_MESSAGE);
		}
		
		private void openDoc(final String docId) {
			
			//	create progress monitor
			final ProgressMonitorDialog pmd = new ProgressMonitorDialog(false, true, DialogPanel.getTopWindow(), "Loading Document from GoldenGATE IMS ...");
			pmd.setAbortExceptionMessage("ABORTED BY USER");
			pmd.setInfoLineLimit(1);
			pmd.getWindow().setSize(400, 150);
			pmd.getWindow().setLocationRelativeTo(pmd.getWindow().getOwner());
			
			//	load in separate thread
			Thread dl = new Thread() {
				public void run() {
					
					//	wait for progress monitor to show
					while (!pmd.getWindow().isVisible()) try {
						sleep(10);
					} catch (InterruptedException ie) {}
					
					//	open document
					try {
						ImsClientDocumentData docData = null;
						
						if ((cache != null) && (cache.containsDocument(docId))) {
							pmd.setInfo("Loading document from cache ...");
							docData = cache.loadDocumentData(docId, (imsClient == null));
							if (docData.hasVirtualEntries())
								docData.setImsClient(imsClient);
							pmd.setInfo("Document loaded from cache.");
							pmd.setProgress(100);
						}
						
						if ((docData == null) && (imsClient != null))
							docData = checkoutDocumentDataFromServer(docId, 0, docId, fastFetchFilter, pmd);
						
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
		
		ImDocumentData getDocumentData() {
			return this.loadDocData;
		}
	}
	
	private static final ImDocumentData LOAD_DOCUMENT_LIST = new ImDocumentData() {
		public boolean hasEntryData(ImDocumentEntry entry) {
			return false;
		}
		public InputStream getInputStream(String entryName) throws IOException {
			return null;
		}
		public OutputStream getOutputStream(String entryName, boolean writeDirectly) throws IOException {
			return null;
		}
		public String getDocumentId() {
			return null;
		}
		public String getDocumentDataId() {
			return null;
		}
		public boolean canLoadDocument() {
			return false;
		}
		public boolean canStoreDocument() {
			return false;
		}
	};
	
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
		
		//	make sure to indicate steady progress
		if (pm == null)
			pm = ProgressMonitor.dummy;
		CascadingProgressMonitor cpm = null;
		if ((pm != null) && (this.cache != null))
			cpm = new CascadingProgressMonitor(pm);
		
		//	cache document if possible
		ImsClientDocumentData cachedDocData = null;
		if (this.cache != null) {
			pm.setInfo("Caching document ...");
			if (cpm != null) {
				pm.setBaseProgress(0);
				pm.setMaxProgress(50);
			}
			cachedDocData = this.cache.storeDocument(doc, docName, ((cpm == null) ? pm : cpm));
			if (cpm != null)
				pm.setBaseProgress(50);
		}
		
		//	server unreachable, we're done here
		if (this.imsClient == null) {
			DialogFactory.alert(("Could not upload '" + docName + "' to GoldenGATE Server at " + authManager.getHost() + "\nbecause this server is unreachable at the moment." + ((cachedDocData == null) ? "" : "\n\nThe document has been stored to the local cache\nand will be uploaded when you log in next time.")), ("Server Unreachable" + ((cachedDocData == null) ? "" : " - Document Cached")), JOptionPane.INFORMATION_MESSAGE);
			return ((cachedDocData == null) ? null : docName);
		}
		
		try {
			
			//	facilitate aborting upload
			if (pm instanceof ControllingProgressMonitor) {
				((ControllingProgressMonitor) pm).setPauseResumeEnabled(true);
				((ControllingProgressMonitor) pm).setAbortEnabled(true);
				((ControllingProgressMonitor) pm).setAbortExceptionMessage("ABORTED BY USER");
			}
			
			//	upload document
			if (cpm != null) {
				pm.setBaseProgress(50);
				pm.setMaxProgress(100);
			}
			String[] uploadProtocol;
			if (cachedDocData == null)
				uploadProtocol = this.imsClient.updateDocument(doc, ((cpm == null) ? pm : cpm));
			else uploadProtocol = this.imsClient.updateDocumentFromData(doc.docId, cachedDocData, ((cpm == null) ? pm : cpm));
			if (cpm != null)
				pm.setBaseProgress(100);
			
			//	mark cache as clean
			if (this.cache != null) {
				this.cache.markNotDirty(doc.docId);
				pm.setInfo("Cache updated.");
			}
			
			//	remember document open on server, so we can release it when it's closed
			this.openDocumentIDs.add(doc.docId);
			
			//	incrementally get upload protocol
			final UploadProtocolDialog upDialog = new UploadProtocolDialog(doc.docId, "Document Upload Protocol", ("Document '" + docName + "' successfully uploaded to GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\nDetails:"), uploadProtocol);
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
			DialogFactory.alert(("An error occurred while uploading document '" + docName + "' to the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\n" + ioe.getMessage() + ((cachedDocData == null) ? "" : "\n\nThe document has been stored to the local cache\nand will be uploaded when you log in next time.")), ("Error Uploading Document"), JOptionPane.ERROR_MESSAGE);
			return null;
		}
		finally {
			if ((doc instanceof DataBackedImDocument) && (((DataBackedImDocument) doc).getDocumentData() == cachedDocData))
				cachedDocData = null; // make sure not to dispose document data still backing document
			if (cachedDocData != null)
				cachedDocData.dispose();
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
	
	ImsClientDocumentData checkoutDocumentDataFromServer(String docId, int version, String docName, FastFetchFilter fff, ProgressMonitor pm) throws IOException {
		pm.setInfo("Loading document from GoldenGATE IMS ...");
		
		try {
			ImsClientDocumentData docData = this.imsClient.checkoutDocumentAsData(docId, version, fff, pm);
			
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
		catch (IOException ioe) {
			ioe.printStackTrace(System.out);
			DialogFactory.alert(("An error occurred while loading document '" + docName + "' from the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\n" + ioe.getMessage()), ("Error Loading Document"), JOptionPane.ERROR_MESSAGE);
			throw ioe;
		}
	}
	
	static final String CACHE_STATUS_ATTRIBUTE = "Cache";
	ImsDocumentList addCacheStatus(final ImsDocumentList dl) {
		String[] listFieldNames = new String[dl.listFieldNames.length + 1];
		listFieldNames[0] = CACHE_STATUS_ATTRIBUTE;
		System.arraycopy(dl.listFieldNames, 0, listFieldNames, 1, dl.listFieldNames.length);
		return new ImsDocumentList(listFieldNames) {
			public boolean hasNoSummary(String listFieldName) {
				return dl.hasNoSummary(listFieldName);
			}
			public boolean isNumeric(String listFieldName) {
				return dl.isNumeric(listFieldName);
			}
			public boolean isFilterable(String listFieldName) {
				return dl.isFilterable(listFieldName);
			}
			public boolean hasNextDocument() {
				return dl.hasNextDocument();
			}
			public DocumentListElement getNextDocument() {
				DocumentListElement dle = dl.getNextDocument();
				String docId = ((String) dle.getAttribute(DOCUMENT_ID_ATTRIBUTE));
				if (docId == null) {}
				else if (cache.isExplicitCheckout(docId))
					dle.setAttribute(CACHE_STATUS_ATTRIBUTE, "Localized");
				else if (cache.containsDocument(docId))
					dle.setAttribute(CACHE_STATUS_ATTRIBUTE, "Cached");
				else dle.setAttribute(CACHE_STATUS_ATTRIBUTE, "");
				return dle;
			}
			public AttributeSummary getListFieldValues(String listFieldName) {
				return dl.getListFieldValues(listFieldName);
			}
			public int getDocumentCount() {
				return dl.getDocumentCount();
			}
			public int getRetrievedDocumentCount() {
				return dl.getRetrievedDocumentCount();
			}
			public int getRemainingDocumentCount() {
				return dl.getRemainingDocumentCount();
			}
		};
	}
	
	private class DocumentListDialog extends DialogPanel {
		private DocumentListBuffer docList;
		
		private DocumentListPanel docListPanel;
		
		private ImsClientDocumentData loadDocData = null;
		
		private String userName;
		private boolean isAdmin;
		
		private String title;
		
		DocumentListDialog(String title, DocumentListBuffer docList, String userName, boolean isAdmin) {
			super(title, true);
			this.title = title;
			this.userName = userName;
			this.isAdmin = isAdmin;
			this.docList = docList;
			
			this.docListPanel = new DocumentListPanel(docList, true) {
				protected boolean applyDocumentFilter(DocumentFilter filter) {
					return loadFilterDocumentList(filter);
				}
				protected boolean displayField(String fieldName, boolean isEmpty) {
					return (isEmpty ? CACHE_STATUS_ATTRIBUTE.equals(fieldName) : (!DOCUMENT_ID_ATTRIBUTE.equals(fieldName)));
				}
				public String getListFieldLabel(String fieldName) {
					if (listFieldLabels.containsKey(fieldName))
						return listFieldLabels.getProperty(fieldName);
					else return super.getListFieldLabel(fieldName);
				}
				protected boolean isUtcTimeField(String fieldName) {
					return (false
							|| CHECKIN_TIME_ATTRIBUTE.equals(fieldName)
							|| UPDATE_TIME_ATTRIBUTE.equals(fieldName)
							|| CHECKOUT_TIME_ATTRIBUTE.equals(fieldName)
						);
				}
				protected void documentSelected(StringTupel docData, boolean doubleClick) {
					if (doubleClick)
						open(docData, 0);
				}
				protected JPopupMenu getContextMenu(StringTupel docData, MouseEvent me) {
					return buildContextMenu(docData, me);
				}
				protected void documentListChanged() {
					adjustTitle();
				}
			};
			if (cache == null) {
				this.docListPanel.setListFields(listFields);
				this.docListPanel.setListFieldOrder(listFieldOrder);
			}
			else {
				StringVector cListFields = new StringVector();
				cListFields.addElement(CACHE_STATUS_ATTRIBUTE);
				cListFields.addContent(listFields);
				this.docListPanel.setListFields(cListFields);
				StringVector cListFieldOrder = new StringVector();
				cListFieldOrder.addElement(CACHE_STATUS_ATTRIBUTE);
				cListFieldOrder.addContent(listFieldOrder);
				this.docListPanel.setListFieldOrder(cListFieldOrder);
			}
			this.docListPanel.refreshDocumentList(); // need to make configuration show
			
			JButton filterButton = new JButton("Filter");
			filterButton.setBorder(BorderFactory.createRaisedBevelBorder());
			filterButton.setPreferredSize(new Dimension(100, 21));
			filterButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					docListPanel.filterDocumentList();
				}
			});
			
			JButton okButton = new JButton("OK");
			okButton.setBorder(BorderFactory.createRaisedBevelBorder());
			okButton.setPreferredSize(new Dimension(100, 21));
			okButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					StringTupel docData = docListPanel.getSelectedDocument();
					if (docData != null)
						open(docData, 0);
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
			
			final JButton fetchModeButton = new JButton("Fetch Mode");
			fetchModeButton.setBorder(BorderFactory.createRaisedBevelBorder());
			fetchModeButton.setPreferredSize(new Dimension(100, 21));
			fetchModeButton.setToolTipText(fastFetchFilterLabel);
			fetchModeButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					if (editFastFetchStrategy())
						fetchModeButton.setToolTipText(fastFetchFilterLabel);
				}
			});
			
			JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
			buttonPanel.add(filterButton);
			buttonPanel.add(okButton);
			buttonPanel.add(cancelButton);
			buttonPanel.add(fetchModeButton);
			
			this.add(this.docListPanel, BorderLayout.CENTER);
			this.add(buttonPanel, BorderLayout.SOUTH);
			this.adjustTitle();
			
			this.setSize(new Dimension(800, 800));
		}
		
		void adjustTitle() {
			if (this.docListPanel == null)
				return; // happens when call comes from constructor
			int docCount = this.docListPanel.getDocumentCount();
			int vDocCount = this.docListPanel.getVisibleDocumentCount();
			if (vDocCount == docCount)
				this.setTitle(this.title + " (" + docCount + " documents)");
			else this.setTitle(this.title + " (" + vDocCount + " of " + docCount + " documents passing filter)");
		}
		
		private void setDocumentList(DocumentListBuffer docList) {
			this.docList = docList;
			this.docListPanel.setDocumentList(this.docList);
		}
		
		boolean loadFilterDocumentList(DocumentFilter filter) {
			
			//	we have our data, we're all set
			if (this.docList.size() != 0)
				return false;
			
			//	received list head only so far
			DocumentListBuffer documentList;
			boolean documentListEmpty = true;
			try {
				//	TODO use splash screen
				ImsDocumentList dl = imsClient.getDocumentList(filter.toProperties(), ProgressMonitor.dummy);
				if (cache != null)
					dl = addCacheStatus(dl);
				for (int f = 0; f < dl.listFieldNames.length; f++) {
					ImsDocumentList.AttributeSummary das = dl.getListFieldValues(dl.listFieldNames[f]);
					if ((das != null) && (das.size() != 0)) {
						documentListEmpty = false;
						f = dl.listFieldNames.length;
					}
				}
				documentList = new DocumentListBuffer(dl);
				if (documentList.isEmpty() && documentListEmpty)
					DialogFactory.alert(("Currently, there are no documents available from the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + ",\nor your filter is too restrictive."), "No Documents To Load", JOptionPane.INFORMATION_MESSAGE);
				this.setDocumentList(documentList);
			}
			catch (IOException ioe) {
				ioe.printStackTrace(System.out);
				DialogFactory.alert(("An error occurred while loading the document list from the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\n" + ioe.getMessage()), "Error Getting Filtered List", JOptionPane.ERROR_MESSAGE);
			}
			return true;
		}
		
		void delete(StringTupel docData) {
			if ((imsClient == null) || (docData == null))
				return;
			
			//	get document data
			String docId = docData.getValue(DOCUMENT_ID_ATTRIBUTE);
			if (docId == null)
				return;
			
			//	checkout and delete document
			try {
				
				//	do it
				if (cache != null)
					cache.unstoreDocument(docId);
				imsClient.deleteDocument(docId);
				
				//	inform user
				DialogFactory.alert("The document has been deleted.", "Document Deleted", JOptionPane.INFORMATION_MESSAGE);
				
				//	update data
				this.docList.remove(docData);
				this.docListPanel.refreshDocumentList();
			}
			catch (IOException ioe) {
				DialogFactory.alert(("Document could not be deleted:\n" + ioe.getMessage()), "Error Deleting Document", JOptionPane.ERROR_MESSAGE);
			}
		}
		
		void open(StringTupel docData, final int version) {
			if (docData == null)
				return;
			
			//	get document data
			final String docId = docData.getValue(DOCUMENT_ID_ATTRIBUTE);
			if (docId == null)
				return;
			final String docName = docData.getValue(DOCUMENT_NAME_ATTRIBUTE);
			
			//	create progress monitor
			final ProgressMonitorDialog pmd = new ProgressMonitorDialog(false, true, DialogPanel.getTopWindow(), "Loading Document from GoldenGATE DIO ...");
			pmd.setAbortExceptionMessage("ABORTED BY USER");
			pmd.setInfoLineLimit(1);
			pmd.getWindow().setSize(400, 100);
			pmd.getWindow().setLocationRelativeTo(pmd.getWindow().getOwner());
			
			//	load in separate thread
			Thread dl = new Thread() {
				public void run() {
					
					//	wait for progress monitor to show
					while (!pmd.getWindow().isVisible()) try {
						sleep(10);
					} catch (InterruptedException ie) {}
					
					//	open document
					try {
						ImsClientDocumentData docData = null;
						
						if ((cache != null) && (cache.containsDocument(docId))) {
							pmd.setInfo("Loading document from cache ...");
							docData = cache.loadDocumentData(docId, (imsClient == null));
							if (docData.hasVirtualEntries())
								docData.setImsClient(imsClient);
							pmd.setInfo("Document loaded from cache.");
							pmd.setProgress(100);
						}
						
						if ((docData == null) && (imsClient != null))
							docData = checkoutDocumentDataFromServer(docId, version, docName, fastFetchFilter, pmd);
						
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
		
		private JPopupMenu buildContextMenu(final StringTupel docData, MouseEvent me) {
			if (docData == null)
				return null;
			
			final String docId = docData.getValue(DOCUMENT_ID_ATTRIBUTE);
			if (docId == null)
				return null;
			
			int preVersion = 0;
			try {
				preVersion = Integer.parseInt(docData.getValue(DOCUMENT_VERSION_ATTRIBUTE, "0"));
			} catch (NumberFormatException e) {}
			final int version = preVersion;
			
			String preCheckoutUser = docData.getValue(CHECKOUT_USER_ATTRIBUTE);
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
						open(docData, 0);
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
								open(docData, ((openVersion == version) ? 0 : openVersion));
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
							delete(docData);
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
								releaseDocumentFromCache(docId, docData);
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
								docData.setValue(CACHE_STATUS_ATTRIBUTE, "Localized");
								
								DialogFactory.alert("Document cached successfully.", "Document Cached", JOptionPane.INFORMATION_MESSAGE);
								
								docListPanel.refreshDocumentList();
							}
						});
						menu.add(mi);
					}
					
					//	document not in cache, offer cache checkout
					else {
						mi = new JMenuItem("Cache Document");
						mi.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent ae) {
								checkoutDocumentToCache(docId, docData);
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
							docData.removeValue(CHECKOUT_USER_ATTRIBUTE);
							docData.removeValue(CHECKOUT_TIME_ATTRIBUTE);
							docData.removeValue(CACHE_STATUS_ATTRIBUTE);
							
							DialogFactory.alert("Document unlocked successfully.", "Document Unlocked", JOptionPane.INFORMATION_MESSAGE);
							
							docListPanel.refreshDocumentList();
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
			
			return menu;
		}
		
		void checkoutDocumentToCache(final String docId, final StringTupel docData) {
			
			//	create progress monitor
			final ProgressMonitorDialog pmd = new ProgressMonitorDialog(false, true, DialogPanel.getTopWindow(), "Checking Out Document from GoldenGATE IMS ...");
			pmd.setAbortExceptionMessage("ABORTED BY USER");
			pmd.setInfoLineLimit(1);
			pmd.getWindow().setSize(400, 100);
			pmd.getWindow().setLocationRelativeTo(pmd.getWindow().getOwner());
			
			//	load in separate thread
			Thread dc = new Thread() {
				public void run() {
					
					//	wait for progress monitor to show
					while (!pmd.getWindow().isVisible()) try {
						sleep(10);
					} catch (InterruptedException ie) {}
					
					//	get the document
					try {
						String docName = docData.getValue(DOCUMENT_NAME_ATTRIBUTE);
						checkoutDocumentDataFromServer(docId, 0, docName, null, pmd);
						cache.markExplicitCheckout(docId);
						docData.setValue(CACHE_STATUS_ATTRIBUTE, "Localized");
						
						DialogFactory.alert("Document cached successfully.", "Document Cached", JOptionPane.INFORMATION_MESSAGE);
						
						docListPanel.refreshDocumentList();
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
		
		void releaseDocumentFromCache(final String docId, final StringTupel docData) {
			
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
						
						//	wait for progress monitor to show
						while (!pmd.getWindow().isVisible()) try {
							sleep(10);
						} catch (InterruptedException ie) {}
						
						//	we have uncommitted changes, upload them to server
						if (cache.isDirty(docId)) {
							pmd.setInfo("Loading document from cache ...");
							ImDocumentData cDocData = cache.getDocumentData(docId);
							String docName = docData.getValue(DOCUMENT_NAME_ATTRIBUTE);
							try {
								pmd.setInfo("Uploading document from cache ...");
								String[] uploadProtocol = imsClient.updateDocumentFromData(docId, cDocData, pmd);
								UploadProtocolDialog uploadProtocolDialog = new UploadProtocolDialog(docId, "Document Upload Protocol", ("Document '" + docName + "' successfully uploaded to GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\nDetails:"), uploadProtocol);
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
						
						docListPanel.refreshDocumentList();
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
		
		ImDocumentData getDocumentData() {
			return this.loadDocData;
		}
	}
	
	private class UploadProtocolDialog extends JFrame {
		private String docId;
		private String[] uploadProtocol = new String[0];
		private JTable protocolList;
		private JButton closeButton;
		UploadProtocolDialog(String docId, String title, String label, String[] up) {
			super(title);
			this.docId = docId;
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
			this.protocolList.revalidate();
			this.protocolList.repaint();
			if ((this.uploadProtocol.length != 0) && (UPDATE_COMPLETE.equals(this.uploadProtocol[this.uploadProtocol.length-1]) || DELETION_COMPLETE.equals(this.uploadProtocol[this.uploadProtocol.length-1]))) {
				this.closeButton.setText("OK");
				this.closeButton.validate();
				this.closeButton.repaint();
			}
			this.setVisible(true);
			openProtocolDialogs.put(this.docId, this);
			this.validate();
			this.repaint();
			this.toFront();
		}
		public void dispose() {
			super.dispose();
			openProtocolDialogs.remove(this.docId);
		}
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
	
	private static final String EXPLICIT_CHECKOUT = "EC";
	private static final String DIRTY = "D";
	private class DocumentCache implements DocumentDataCache {
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
		
		public ImsClientDocumentData getDocumentData(String docId) throws IOException {
			
			/* have to make damn sure to reuse any document data object a
			 * document has been instantiated on top of (page images or
			 * supplements might have changed, so re-reading entry list is
			 * not good); caching has to persist beyond intermediate logout,
			 * change of user name, etc. */
			
			String docDataCacheKey = (this.cachePrefix + docId);
			ImsClientDocumentData docData = ((ImsClientDocumentData) cacheDocumentData.get(docDataCacheKey));
			if (docData == null) {
				docData = new ImsClientDocumentData(docId, new CacheImDocumentData(docId));
				cacheDocumentData.put(docDataCacheKey, docData);
			}
			return docData;
		}
		
		private class CacheImDocumentData extends ImDocumentData {
			final String docId;
			final String docCachePrefix;
			CacheImDocumentData(String docId) throws IOException {
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
				
				//	check file name
				checkEntryName(entryName);
				
				//	write to buffer first, as we cannot rename via a data provider
				return new DataHashOutputStream(new ByteArrayOutputStream()) {
					public void close() throws IOException {
						super.flush();
						super.close();
						
						//	create entry
						ImDocumentEntry entry = new ImDocumentEntry(entryName, System.currentTimeMillis(), this.getDataHash());
						
						//	write buffer content to persistent storage only if not already there
						String entryDataName = (docCachePrefix + entry.getFileName());
						if (!dataProvider.isDataAvailable(entryDataName)) {
							BufferedOutputStream out = new BufferedOutputStream(dataProvider.getOutputStream(entryDataName));
							((ByteArrayOutputStream) this.out).writeTo(out);
							out.flush();
							out.close();
						}
						
						//	update entry list
						putEntry(entry);
					}
				};
			}
			public String getDocumentId() {
				return this.docId;
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
		
		public void storeEntryList(ImsClientDocumentData docData) throws IOException {
			CacheImDocumentData cDocData = ((CacheImDocumentData) docData.getLocalDocData());
			ImDocumentEntry[] entries = cDocData.getEntries();
			BufferedWriter entryOut = new BufferedWriter(new OutputStreamWriter(dataProvider.getOutputStream(cDocData.docCachePrefix + "entries.txt"), "UTF-8"));
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
		
		synchronized ImsClientDocumentData storeDocument(ImDocument doc, String documentName, ProgressMonitor pm) {
			
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
			pm.setInfo(" - cache metadata entry created");
			
			try {
				
				//	store document
				ImsClientDocumentData docData = this.getDocumentData(doc.docId);
				pm.setInfo(" - got document cache data");
				ImDocumentIO.storeDocument(doc, docData, pm);
				pm.setInfo(" - document stored to cache data");
				this.storeEntryList(docData);
				pm.setInfo(" - entry list stored");
				
				//	mark as dirty (remember there are cached changes not forwarded to server yet)
				docMetaData.setValue(DIRTY, DIRTY);
				this.cacheMetaData.put(docId, docMetaData);
				this.metaDataStorageKeys.addContentIgnoreDuplicates(docMetaData.getKeys());
				this.storeMetaData();
				pm.setInfo(" - cache metadata stored");
				
				//	indicate success
				return docData;
			}
			catch (IOException ioe) {
				DialogFactory.alert(("An error occurred while storing document '" + documentName + "' to local cache for GoldenGATE Server at " + this.host + "\n" + ioe.getMessage()), ("Error Caching Document"), JOptionPane.ERROR_MESSAGE);
				return null;
			}
		}
		
		synchronized ImsClientDocumentData loadDocumentData(String docId, boolean showError) throws IOException {
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
			//	TODO_not clean up cached data files ==> won't work through data provider anyway (never deletes, only renames)
			this.storeMetaData();
		}
		
		synchronized DocumentListBuffer getDocumentList() {
			DocumentListBuffer documentList = new DocumentListBuffer(cacheDocumentListAttributes);
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
				if (docId == null)
					continue; // WFT
				if (docName == null)
					continue; // WFT
				if (!this.isDirty(docId))
					continue; // nothing to upload
				
				//	load and upload document data
				try {
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
			
			//	release all documents that are not open and not explicitly checked out
			for (Iterator cdit = (new ArrayList(this.cacheMetaData.values())).iterator(); cdit.hasNext();) {
				StringTupel dmd = ((StringTupel) cdit.next());
				String docId = dmd.getValue(DOCUMENT_ID_ATTRIBUTE);
				if (docId == null)
					continue; // WFT
				if (this.isExplicitCheckout(docId))
					continue; // requiring explicit release from user
				if (this.isOpen(docId))
					continue; // we release this one on closing
				
				//	got required meta data
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