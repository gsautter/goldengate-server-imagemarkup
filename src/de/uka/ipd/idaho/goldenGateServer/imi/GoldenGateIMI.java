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
package de.uka.ipd.idaho.goldenGateServer.imi;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;

import de.uka.ipd.idaho.gamta.AttributeUtils;
import de.uka.ipd.idaho.gamta.Attributed;
import de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader.ComponentInitializer;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS.ImsDocumentData;
import de.uka.ipd.idaho.goldenGateServer.util.BufferedLineInputStream;
import de.uka.ipd.idaho.goldenGateServer.util.BufferedLineOutputStream;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.util.ImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.DataHashOutputStream;
import de.uka.ipd.idaho.im.util.ImDocumentData.ImDocumentEntry;

/**
 * GoldenGATE Image Markup Importer provides scheduled import and decoding of
 * binary documents (for instance PDFs) and puts the resulting Image Markup
 * documents in an IMS. Decoding for arbitrary binary formats is implemented by
 * means of plug-ins.
 * 
 * @author sautter
 */
public class GoldenGateIMI extends AbstractGoldenGateServerComponent implements GoldenGateImiConstants {
	
	/**
	 * A single import task, consisting of a source of document data, the MIME
	 * type of said data, and additional document attributes, like for instance
	 * the bibliographic metadata of the document to be imported.
	 * 
	 * @author sautter
	 */
	public class ImiDocumentImport extends AbstractAttributed {
		
		/** the MIME type of the data provided by the file or URL */
		public final String dataMimeType;
		
		private File dataFile;
		private boolean deleteDataFile;
		private URL dataUrl;
		
		/** Constructor for import from local file system
		 * @param dataMimeType
		 * @param dataFile
		 */
		ImiDocumentImport(String dataMimeType, File dataFile) {
			this(dataMimeType, dataFile, false);
		}
		
		/** Constructor for import from local file system
		 * @param dataMimeType
		 * @param dataFile
		 */
		ImiDocumentImport(String dataMimeType, File dataFile, boolean deleteDataFile) {
			this.dataMimeType = dataMimeType;
			this.dataFile = dataFile;
			this.deleteDataFile = deleteDataFile;
			this.dataUrl = null;
		}
		
		/** Constructor for import from remote source
		 * @param dataMimeType
		 * @param dataFile
		 * @param dataUrl
		 */
		ImiDocumentImport(String dataMimeType, URL dataUrl) {
			this.dataMimeType = dataMimeType;
			this.dataFile = null;
			this.deleteDataFile = true;
			this.dataUrl = dataUrl;
		}
		
		/**
		 * Retrieve the data file to import. If the import is from a URL, the
		 * data is first downloaded and cached, and this method returns the
		 * cache file.
		 * @return the data file
		 */
		public File getDataFile() throws IOException {
			if (this.dataFile != null)
				return this.dataFile;
			
			//	download data
			File dataFile = new File(cacheFolder, (this.dataUrl.toString().replaceAll("[^A-Za-z0-9]+", "_") + ".cache"));
			InputStream urlIn = new BufferedInputStream(this.dataUrl.openStream());
			OutputStream dataOut = new BufferedOutputStream(new FileOutputStream(dataFile));
			byte[] buffer = new byte[1024];
			for (int r; (r = urlIn.read(buffer, 0, buffer.length)) != -1;)
				dataOut.write(buffer, 0, r);
			dataOut.flush();
			dataOut.close();
			urlIn.close();
			
			//	switch file live only now
			this.dataFile = dataFile;
			return this.dataFile;
		}
		
		/**
		 * Retrieve the URL of the data to import. If the import is from a
		 * file, this method returns null.
		 * @return the data URL
		 */
		public URL getDataURL() {
			return this.dataUrl;
		}
		
		/**
		 * Store the imported document. An importer should clean up any
		 * persisted files after this method returns. This method adds all
		 * import attributes to the document before storing it, unless they
		 * are already set. After this method has been called, any cached data
		 * is cleaned up.
		 * @param doc the imported document
		 */
		public void setDocument(ImDocument doc) {
			
			//	set document attributes
			AttributeUtils.copyAttributes(this, doc, AttributeUtils.ADD_ATTRIBUTE_COPY_MODE);
			
			//	get user name to credit (if any)
			String userName = ((String) this.getAttribute(GoldenGateIMS.CHECKIN_USER_ATTRIBUTE, defaultImportUserName));
			
			//	store document in IMS
			try {
				ims.uploadDocument(userName, doc, null);
			}
			catch (IOException ioe) {
				System.out.println("Error storing document imported from " + this.toString() + ": " + ioe.getMessage());
				ioe.printStackTrace(System.out);
			}
			
			//	clean up cached data file
			if (this.deleteDataFile && (this.dataFile != null)) {
				this.dataFile.delete();
				this.dataFile = null;
			}
		}
		
		/**
		 * Store the imported document in its data object representation. An
		 * importer should clean up any persisted files after this method
		 * returns. This method does not modify the document data. Client code
		 * using this method for handing over an imported document has to set
		 * the import attributes itself at some point. After this method has
		 * been called, any cached data is cleaned up.
		 * @param docData the data representing the imported document
		 */
		public void setDocumentData(ImDocumentData docData) {
			
			//	get document ID
			String docId = ((String) docData.getDocumentAttributes().getAttribute(GoldenGateIMS.DOCUMENT_ID_ATTRIBUTE));
			if (docId == null)
				docId = ((String) this.getAttribute(GoldenGateIMS.DOCUMENT_ID_ATTRIBUTE));
			
			//	get user name to credit (if any)
			String userName = ((String) this.getAttribute(GoldenGateIMS.CHECKIN_USER_ATTRIBUTE, defaultImportUserName));
			
			//	store document in IMS
			try {
				
				//	get document data from backing IMS
				ImsDocumentData iDocData = ims.getDocumentData(docId, true);
				
				//	copy entries
				ImDocumentEntry[] docEntries = docData.getEntries();
				byte[] buffer = new byte[1024];
				for (int e = 0; e < docEntries.length; e++) {
					InputStream entryIn = docData.getInputStream(docEntries[e]);
					OutputStream entryOut = docData.getOutputStream(docEntries[e]);
					for (int r; (r = entryIn.read(buffer, 0, buffer.length)) != -1;)
						entryOut.write(buffer, 0, r);
					entryOut.flush();
					entryOut.close();
					entryIn.close();
				}
				
				//	finally ...
				ims.uploadDocumentFromData(userName, iDocData, null);
			}
			catch (IOException ioe) {
				System.out.println("Error storing document imported from " + this.toString() + ": " + ioe.getMessage());
				ioe.printStackTrace(System.out);
			}
			
			//	clean up cached data file
			if (this.deleteDataFile && (this.dataFile != null)) {
				this.dataFile.delete();
				this.dataFile = null;
			}
		}
		
		/**
		 * Report an import error. After this method has been called, any
		 * cached data is cleaned up.
		 * @param error some error that prevented the import from succeeding
		 */
		public void setError(Throwable error) {
			
			//	log error
			System.out.println("Error importing document from " + this.toString() + ": " + error.getMessage());
			error.printStackTrace(System.out);

			//	clean up cached data file
			if (this.deleteDataFile && (this.dataFile != null)) {
				this.dataFile.delete();
				this.dataFile = null;
			}
		}
		
		public String toString() {
			if (this.toString == null) {
				if (this.dataUrl == null)
					this.toString = this.dataFile.getAbsolutePath();
				else this.toString = this.dataUrl.toString();
			}
			return this.toString;
		}
		private String toString = null;
		
		public int hashCode() {
			return this.toString().hashCode();
		}
	}
	
	private GoldenGateIMS ims;
	
	private String defaultImportUserName;
	
	private File workingFolder;
	private File cacheFolder;
	
	private ImiDocumentImporter[] importers;
	
	/** Constructor passing 'IMI' as the letter code to super constructor
	 */
	public GoldenGateIMI() {
		super("IMI");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
	 */
	protected void initComponent() {
		
		//	get default import user name
		this.defaultImportUserName = this.configuration.getSetting("defaultImportUserName", "GgIMS");
		
		//	get working folder
		String workingFolderName = this.configuration.getSetting("workingFolderName", "Importers");
		while (workingFolderName.startsWith("./"))
			workingFolderName = workingFolderName.substring("./".length());
		this.workingFolder = (((workingFolderName.indexOf(":\\") == -1) && (workingFolderName.indexOf(":/") == -1) && !workingFolderName.startsWith("/")) ? new File(this.dataPath, workingFolderName) : new File(workingFolderName));
		this.workingFolder.mkdirs();
		
		//	get URL import cache folder (RAM disc !!!)
		String cacheFolderName = this.configuration.getSetting("cacheFolderName", "Cache");
		while (cacheFolderName.startsWith("./"))
			cacheFolderName = cacheFolderName.substring("./".length());
		this.cacheFolder = (((cacheFolderName.indexOf(":\\") == -1) && (cacheFolderName.indexOf(":/") == -1) && !cacheFolderName.startsWith("/")) ? new File(this.dataPath, cacheFolderName) : new File(cacheFolderName));
		this.cacheFolder.mkdirs();
		
		/* TODO keep database of imported documents, including
		 * - document ID
		 * - upload time
		 * - upload user
		 * - MIME type
		 * - document name
		 * - import status
		 * - last update time
		 * - last update user (or pseudo user)
		 * - status (import pending, import error (download error, decoding error, unknown MIME type), imported, other (processed further, etc.))
		 */
		
		//	TODO allow users to see status of "their" documents
		
		//	TODO offer re-triggering non-completed imports (we might have a new decoder, or one with a former error fixed, etc.)
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
	 */
	public void link() {
		
		//	link up to IMS
		this.ims = ((GoldenGateIMS) this.host.getServerComponent(GoldenGateIMS.class.getName()));
		
		//	check success
		if (this.ims == null) throw new RuntimeException(GoldenGateIMS.class.getName());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
	 */
	public void linkInit() {
		
		//	load importers
		this.loadImporters(false);
		
		//	start import handler thread
		Thread importerThread = new ImporterThread();
		importerThread.start();
	}
	
	private synchronized void loadImporters(boolean isReload) {
		
		//	if reload, shut down importers
		if (isReload)
			this.shutdownImporters();
		
		//	load importers
		System.out.println("Loading importers ...");
		Object[] importerObjects = GamtaClassLoader.loadComponents(
				dataPath,
				ImiDocumentImporter.class, 
				new ComponentInitializer() {
					public void initialize(Object component, String componentJarName) throws Exception {
						File dataPath = new File(GoldenGateIMI.this.dataPath, (componentJarName.substring(0, (componentJarName.length() - 4)) + "Data"));
						if (!dataPath.exists())
							dataPath.mkdir();
						ImiDocumentImporter importer = ((ImiDocumentImporter) component);
						System.out.println(" - initializing importer " + importer.getName() + " ...");
						File workingFolder = new File(GoldenGateIMI.this.workingFolder, importer.getName());
						if (!workingFolder.exists())
							workingFolder.mkdir();
						File cacheFolder = new File(GoldenGateIMI.this.cacheFolder, importer.getName());
						if (!cacheFolder.exists())
							cacheFolder.mkdir();
						importer.setDataPath(dataPath);
						importer.setWorkingFolder(workingFolder);
						importer.setCacheFolder(cacheFolder);
						importer.setHost(GoldenGateIMI.this.host);
						importer.init();
						System.out.println(" - importer " + importer.getName() + " initialized");
					}
				});
		System.out.println("Importers loaded");
		
		//	store importers, and sort them by priority
		ImiDocumentImporter[] importers = new ImiDocumentImporter[importerObjects.length];
		for (int i = 0; i < importerObjects.length; i++)
			importers[i] = ((ImiDocumentImporter) importerObjects[i]);
		Arrays.sort(importers, new Comparator() {
			public int compare(Object obj1, Object obj2) {
				ImiDocumentImporter idi1 = ((ImiDocumentImporter) obj1);
				ImiDocumentImporter idi2 = ((ImiDocumentImporter) obj2);
				return (idi2.getPrority() - idi1.getPrority());
			}
		});
		
		//	make importers available
		this.importers = importers;
		System.out.println("Importers registered");
	}
	
	private synchronized void shutdownImporters() {
		
		//	make importers private
		ImiDocumentImporter[] importers = this.importers;
		this.importers = null;
		
		//	shut down importers
		System.out.println("Finalizing importers");
		for (int i = 0; i < importers.length; i++)
			importers[i].exit();
		System.out.println("Importers finalized");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		
		//	shut down import handler thread
		synchronized (this.importQueue) {
			this.importQueue.clear();
			this.importQueue.notify();
		}
		
		//	shut down importers
		this.shutdownImporters();
	}
	
	private static final String IMPORT_DOCUMENT_COMMAND = "import";
	private static final String RELOAD_IMPORTERS_COMMAND = "reload";
	
	/*
	 * (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList();
		ComponentAction ca;
		
		//	schedule URL import (good for testing)
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return IMPORT_DOCUMENT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						IMPORT_DOCUMENT_COMMAND + " <documentUrl> <documentMimeType>",
						"Import a document from a URL:",
						"- <documentUrl>: The URL to import the document from",
						"- <documentMimeType>: The MIME type of the document (optional, defaults to 'application/pdf')"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				try {
					if (arguments.length == 1)
						scheduleImport("application/pdf", new URL(arguments[0]), null);
					else if (arguments.length == 2)
						scheduleImport(arguments[1], new URL(arguments[0]), null);
					else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify the document URL and MIME type as the only arguments.");
				}
				catch (MalformedURLException mue) {
					System.out.println(" '" + arguments[0] + "' is not a valid URL.");
				}
			}
		};
		cal.add(ca);
		
		//	document upload request
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return UPLOAD_DOCUMENT;
			}
			public void performActionNetwork(BufferedLineInputStream input, final BufferedLineOutputStream output) throws IOException {
				
				//	refuse proxied uploads
				if (host.isRequestProxied()) {
					output.write("Proxied requests are not allowed");
					output.newLine();
					return;
				}
				
				//	read upload ID (makes for good cache file name)
				final String uploadId = input.readLine();
				
				//	read and check MIME type
				String mimeType = input.readLine();
				if (mimeType.indexOf('/') == -1) {
					output.write("Invalid MIME type '" + mimeType + "'");
					output.newLine();
					return;
				}
				System.out.println("Got MIME type: " + mimeType);
				
				//	read data name
				final String docDataName = input.readLine();
				if (docDataName.length() == -1) {
					output.write("Invalid document data name '" + docDataName + "'");
					output.newLine();
					return;
				}
				System.out.println("Got document name: " + docDataName);
				
				//	read data name
				final int docDataSize = Integer.parseInt(input.readLine());
				System.out.println("Got document size: " + docDataSize);
				
				//	read user name
				String user = input.readLine();
				if (user.length() == 0)
					user = defaultImportUserName;
				System.out.println("Got user: " + user);
				
				//	read meta data (via string reading methods)
				Attributed docAttributes = new AbstractAttributed();
				for (String dal; (dal = input.readLine()) != null;) {
					
					//	separator between attributes and binary data
					if (dal.length() == 0)
						break;
					
					//	invalid line
					if (dal.indexOf('=') == -1)
						continue;
					
					//	separate attribute name from value
					String an = dal.substring(0, dal.indexOf('='));
					String av = dal.substring(dal.indexOf('=') + "=".length());
					
					//	store attribute
					docAttributes.setAttribute(an, av);
					System.out.println("Attribute '" + an + "' set to '" + av + "'");
				}
				
				//	add user name to attributes
				if (user != defaultImportUserName)
					docAttributes.setAttribute(GoldenGateIMS.CHECKIN_USER_ATTRIBUTE, user);
				
				//	read and cache binary data, computing hash along the way
				File docCacheFile = new File(cacheFolder, ("data." + uploadId + ".cached"));
				OutputStream docCacheFileOut = new BufferedOutputStream(new FileOutputStream(docCacheFile));
				DataHashOutputStream docCacheOut = new DataHashOutputStream(docCacheFileOut);
				byte[] buffer = new byte[1024];
				int docCacheBytes = 0;
				for (int r; (r = input.read(buffer, 0, buffer.length)) != -1;) {
					docCacheOut.write(buffer, 0, r);
					docCacheBytes += r;
					if (docCacheBytes >= docDataSize)
						break;
				}
				docCacheOut.flush();
				docCacheOut.close();
				System.out.println("Got " + docCacheBytes + " bytes of data");
				
				//	compute MD5 hash document ID
				String docId = docCacheOut.getDataHash();
				System.out.println("Document UUID computed: " + docId);
				
				//	check if document already in database, and report back if so
				Attributed exDocAttributes = null;
				try {
					exDocAttributes = ims.getDocumentAttributes(docId);
				} catch (Exception e) {}
				
				//	schedule import if not imported before
				String docStatus;
				if (exDocAttributes == null) {
					scheduleImport(mimeType, docCacheFile, true, docAttributes);
					docStatus = "Scheduled for import.";
				}
				else {
					docAttributes = exDocAttributes;
					docStatus = ("Previously imported by " + exDocAttributes.getAttribute(GoldenGateIMS.CHECKIN_USER_ATTRIBUTE));
				}
				
				//	indicate success
				output.writeLine(UPLOAD_DOCUMENT);
				
				//	report document status, as well as document ID
				output.writeLine(GoldenGateIMS.DOCUMENT_ID_ATTRIBUTE + "=" + docId);
				output.writeLine("status" + "=" + docStatus);
				
				//	send (possibly updated or pre-existing) document attributes
				String[] docAttributeNames = docAttributes.getAttributeNames();
				for (int a = 0; a < docAttributeNames.length; a++) {
					Object attributeValueObj = docAttributes.getAttribute(docAttributeNames[a]);
					if (attributeValueObj instanceof CharSequence)
						output.writeLine(docAttributeNames[a] + "=" + attributeValueObj.toString());
					else if (attributeValueObj instanceof Number)
						output.writeLine(docAttributeNames[a] + "=" + attributeValueObj.toString());
					else if (attributeValueObj instanceof Boolean)
						output.writeLine(docAttributeNames[a] + "=" + attributeValueObj.toString());
					else if (attributeValueObj == null) {}
					else try {
						Method toString = attributeValueObj.getClass().getMethod("toString", ((Class) null));
						if (!toString.getDeclaringClass().equals(Object.class))
							output.writeLine(docAttributeNames[a] + "=" + attributeValueObj.toString());
					} catch (Exception e) {}
				}
				
				//	finally ...
				output.flush();
				output.close();
			}
		};
		cal.add(ca);
		
		//	reload importers
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return RELOAD_IMPORTERS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						RELOAD_IMPORTERS_COMMAND,
						"Reload the importers currently installed in this DIC."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					loadImporters(true);
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	/**
	 * Schedule a document import from a URL.
	 * @param mimeType the MIME type of the document
	 * @param url the URL to import from
	 * @param attributes additional document attributes
	 */
	public void scheduleImport(String mimeType, URL url, Attributed attributes) {
		ImiDocumentImport idi = new ImiDocumentImport(mimeType, url);
		
		String urlString = url.toString();
		idi.setAttribute(ImDocument.DOCUMENT_SOURCE_LINK_ATTRIBUTE, urlString);
		
		urlString = urlString.substring(urlString.lastIndexOf('/') + "/".length());
		if (urlString.indexOf('.') == -1) {}
		else if (urlString.toLowerCase().endsWith(".html")) {}
		else if (urlString.toLowerCase().endsWith(".htm")) {}
		else if (urlString.toLowerCase().endsWith(".shtml")) {}
		else if (urlString.toLowerCase().endsWith(".shtm")) {}
		else if (urlString.toLowerCase().endsWith(".xhtml")) {}
		else if (urlString.toLowerCase().endsWith(".xhtm")) {}
		else if (urlString.toLowerCase().endsWith(".xml")) {}
		else idi.setAttribute(DOCUMENT_NAME_ATTRIBUTE, urlString);
		
		if (attributes != null)
			AttributeUtils.copyAttributes(attributes, idi);
		this.enqueueImport(idi);
	}
	
	/**
	 * Schedule a document import from a file.
	 * @param mimeType the MIME type of the document
	 * @param file the file to import from
	 * @param attributes additional document attributes
	 */
	public void scheduleImport(String mimeType, File file, Attributed attributes) {
		this.scheduleImport(mimeType, file, false, attributes);
	}
	
	/**
	 * Schedule a document import from a file.
	 * @param mimeType the MIME type of the document
	 * @param file the file to import from
	 * @param attributes additional document attributes
	 * @param deleteFile delete the cache file after import?
	 */
	public void scheduleImport(String mimeType, File file, boolean deleteFile, Attributed attributes) {
		ImiDocumentImport idi = new ImiDocumentImport(mimeType, file, deleteFile);
		
		String fileName = file.getName();
		if (fileName.indexOf('.') == -1) {}
		else if (fileName.toLowerCase().endsWith(".html")) {}
		else if (fileName.toLowerCase().endsWith(".htm")) {}
		else if (fileName.toLowerCase().endsWith(".shtml")) {}
		else if (fileName.toLowerCase().endsWith(".shtm")) {}
		else if (fileName.toLowerCase().endsWith(".xhtml")) {}
		else if (fileName.toLowerCase().endsWith(".xhtm")) {}
		else if (fileName.toLowerCase().endsWith(".xml")) {}
		else idi.setAttribute(DOCUMENT_NAME_ATTRIBUTE, fileName);
		
		if (attributes != null)
			AttributeUtils.copyAttributes(attributes, idi);
		this.enqueueImport(idi);
	}
	
	private LinkedList importQueue = new LinkedList() {
		private HashSet deduplicator = new HashSet();
		public Object removeFirst() {
			Object e = super.removeFirst();
			this.deduplicator.remove(e);
			return e;
		}
		public void addLast(Object e) {
			if (this.deduplicator.add(e))
				super.addLast(e);
		}
	};
	private void enqueueImport(ImiDocumentImport idi) {
		synchronized (this.importQueue) {
			this.importQueue.addLast(idi);
			this.importQueue.notify();
		}
	}
	private ImiDocumentImport getImport() {
		synchronized (this.importQueue) {
			if (this.importQueue.isEmpty()) try {
				this.importQueue.wait();
			} catch (InterruptedException ie) {}
			return (this.importQueue.isEmpty() ? null : ((ImiDocumentImport) this.importQueue.removeFirst()));
		}
	}
	
	private class ImporterThread extends Thread {
		public void run() {
			
			//	don't start right away
			try {
				sleep(1000 * 15);
			} catch (InterruptedException ie) {}
			
			//	keep going until shutdown
			while (true) {
				
				//	get next import
				ImiDocumentImport idi = getImport();
				if (idi == null)
					return; // only happens on shutdown
				
				//	find appropriate importer
				ImiDocumentImporter importer = null;
				synchronized (GoldenGateIMI.this) {
					
					//	wait for importers to be reloaded
					while (GoldenGateIMI.this.importers == null) try {
						GoldenGateIMI.this.wait(1000);
					} catch (InterruptedException ie) {}
					
					//	get importer
					for (int i = 0; i < importers.length; i++)
						if (importers[i].canHandleImport(idi)) {
							importer = importers[i];
							break;
						}
				}
				if (importer == null) {
					System.out.println("Could not find importer for " + idi.dataMimeType + " document from " + idi.toString());
					continue;
				}
				
				//	handle import
				importer.handleImport(idi);
				
				//	give the others a little time
				try {
					sleep(1000 * 5);
				} catch (InterruptedException ie) {}
			}
		}
	}
//	
//	private static MessageDigest checksumDigester = null;
//	private static synchronized String computeDocumentID(byte[] pdfBytes) {
//		if (checksumDigester == null) {
//			try {
//				checksumDigester = MessageDigest.getInstance("MD5");
//			}
//			catch (NoSuchAlgorithmException nsae) {
//				System.out.println(nsae.getClass().getName() + " (" + nsae.getMessage() + ") while creating checksum digester.");
//				nsae.printStackTrace(System.out); // should not happen, but Java don't know ...
//				return Gamta.getAnnotationID(); // use random value to avoid collisions
//			}
//		}
//		checksumDigester.reset();
//		checksumDigester.update(pdfBytes);
//		return new String(RandomByteSource.getHexCode(checksumDigester.digest()));
//	}
}