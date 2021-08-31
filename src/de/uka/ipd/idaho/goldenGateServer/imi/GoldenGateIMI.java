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
package de.uka.ipd.idaho.goldenGateServer.imi;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.sql.TableColumnDefinition;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.easyIO.streams.PeekInputStream;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.AttributeUtils;
import de.uka.ipd.idaho.gamta.Attributed;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader.ComponentInitializer;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerActivityLogger;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.dta.DataObjectTransitAuthority;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS.ImsDocumentData;
import de.uka.ipd.idaho.goldenGateServer.util.AsynchronousDataActionHandler;
import de.uka.ipd.idaho.goldenGateServer.util.BufferedLineInputStream;
import de.uka.ipd.idaho.goldenGateServer.util.BufferedLineOutputStream;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.util.ImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.DataHashOutputStream;
import de.uka.ipd.idaho.im.util.ImDocumentData.ImDocumentEntry;
import de.uka.ipd.idaho.im.util.ImDocumentIO;

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
		private String dataId;
		
		private ImiDocumentImportOwner owner;
		
		ImiDocumentImport(String dataId, String dataMimeType, File dataFile, ImiDocumentImportOwner owner) {
			this(dataId, dataMimeType, dataFile, false, owner);
		}
		ImiDocumentImport(String dataId, String dataMimeType, File dataFile, boolean deleteDataFile, ImiDocumentImportOwner owner) {
			this.dataMimeType = dataMimeType;
			this.dataFile = dataFile;
			this.deleteDataFile = deleteDataFile;
			this.dataUrl = null;
			this.dataId = dataId;
			this.owner = owner;
		}
		
		ImiDocumentImport(String dataMimeType, URL dataUrl, ImiDocumentImportOwner owner) {
			this.dataMimeType = dataMimeType;
			this.dataFile = null;
			this.deleteDataFile = true;
			this.dataUrl = dataUrl;
			this.owner = owner;
		}
		void setDataId(String dataId) {
			this.dataId = dataId;
		}
		
		ImiDocumentImport(String dataId, String[] arguments) throws IOException {
			this.dataMimeType = arguments[2];
			if ("F".equals(arguments[0])) {
				this.dataFile = new File(arguments[1]);
				this.deleteDataFile = "D".equals(arguments[3]);
				this.dataUrl = null;
			}
			else if ("U".equals(arguments[0])) {
				this.dataFile = null;
				this.deleteDataFile = true;
				this.dataUrl = new URL(arguments[1]);
			}
			else throw new IllegalArgumentException("Invalid data source type '" + arguments[0] + "'");
			this.dataId = dataId;
			ImDocumentIO.setAttributes(this, arguments[4]);
			if (arguments[5].length() != 0)
				this.owner = getImportOwner(arguments[5]);
		}
		
		String[] getArguments() {
			String[] arguments = {
				((this.dataFile == null) ? "U" : "F"),
				((this.dataFile == null) ? this.dataUrl.toString() : this.dataFile.getAbsolutePath()),
				this.dataMimeType,
				(this.deleteDataFile ? "D" : "R"),
				ImDocumentIO.getAttributesString(this),
				((this.owner == null) ? "" : this.owner.key),
			};
			return arguments;
		}
		
		void notifyStartring() throws RuntimeException {
			if (this.owner != null)
				this.owner.importStarting(this);
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
			File dataFile = new File(dataCacheFolder, (this.dataUrl.toString().replaceAll("[^A-Za-z0-9]+", "_") + ".cache"));
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
			
			//	notify owner
			if (this.owner != null) try {
				this.owner.importFinished(this, doc);
			}
			catch (RuntimeException re) {
				logWarning("Document import cancelled by owner after finishing: " + re.getMessage());
				this.deleteDataFile();
				return;
			}
			
			//	get user name to credit (if any)
			String userName = ((String) this.getAttribute(GoldenGateIMS.CHECKIN_USER_ATTRIBUTE, importUserName));
			
			//	store document in IMS an clean up
			boolean isUpdate = checkDocumentExists(doc.docId);
			try {
				if (isUpdate)
					ims.checkoutDocumentAsData(importUserName, doc.docId);
				ims.updateDocument(userName, importUserName, doc, new EventLogger() {
					public void writeLog(String logEntry) {
						logInfo(logEntry);
					}
				});
				doc.dispose();
			}
			catch (IOException ioe) {
				logError("Error storing document imported from " + this.toString() + ": " + ioe.getMessage());
				logError(ioe);
			}
			finally {
				ims.releaseDocument(importUserName, doc.docId);
			}
			
			//	clean up cached data file
			this.deleteDataFile();
			
			//	notify transit authority
			DataObjectTransitAuthority.notifyDataObjectTransitSuccessful(
					GoldenGateIMI.class.getName(),
					this.dataId,
					((String) this.getAttribute(DOCUMENT_NAME_ATTRIBUTE, this.dataId)),
					"IMI",
					"IMS");
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
			
			//	notify owner
			if (this.owner != null) try {
				this.owner.importFinished(this, docData);
			}
			catch (RuntimeException re) {
				logWarning("Document import cancelled by owner after finishing: " + re.getMessage());
				this.deleteDataFile();
				return;
			}
			
			//	get document ID
			String docId = ((String) docData.getDocumentAttributes().getAttribute(GoldenGateIMS.DOCUMENT_ID_ATTRIBUTE));
			if (docId == null)
				docId = ((String) this.getAttribute(GoldenGateIMS.DOCUMENT_ID_ATTRIBUTE));
			
			//	get user name to credit (if any)
			String userName = ((String) this.getAttribute(GoldenGateIMS.CHECKIN_USER_ATTRIBUTE, importUserName));
			
			//	store document in IMS
			ImsDocumentData iDocData = null;
			boolean isUpdate = checkDocumentExists(docId);
			try {
				
				//	get document data from backing IMS
				if (isUpdate)
					iDocData = ims.checkoutDocumentAsData(importUserName, docId);
				else iDocData = ims.getDocumentData(docId, true);
				
				//	copy entries
				ImDocumentEntry[] docEntries = docData.getEntries();
				byte[] buffer = new byte[1024];
				for (int e = 0; e < docEntries.length; e++) {
					InputStream entryIn = docData.getInputStream(docEntries[e]);
					OutputStream entryOut = iDocData.getOutputStream(docEntries[e]);
					for (int r; (r = entryIn.read(buffer, 0, buffer.length)) != -1;)
						entryOut.write(buffer, 0, r);
					entryOut.flush();
					entryOut.close();
					entryIn.close();
				}
				
				//	finally ...
				ims.updateDocumentFromData(userName, importUserName, iDocData, null);
			}
			catch (IOException ioe) {
				logError("Error storing document imported from " + this.toString() + ": " + ioe.getMessage());
				logError(ioe);
			}
			finally {
				ims.releaseDocument(importUserName, docId);
				if (iDocData != null)
					iDocData.dispose();
			}
			
			//	clean up cached data file
			this.deleteDataFile();
			
			//	notify transit authority
			DataObjectTransitAuthority.notifyDataObjectTransitSuccessful(
					GoldenGateIMI.class.getName(),
					this.dataId,
					((String) this.getAttribute(DOCUMENT_NAME_ATTRIBUTE, this.dataId)),
					"IMI",
					"IMS");
		}
		
		/**
		 * Report an import error. After this method has been called, any
		 * cached data is cleaned up.
		 * @param error some error that prevented the import from succeeding
		 */
		public void setError(Throwable error) {
			
			//	log error
			logError("Error importing document from " + this.toString() + ": " + error.getMessage());
			logError(error);
			
			//	notify owner
			if (this.owner != null)
				this.owner.importFailed(this, error);
			
			//	notify transit authority
			DataObjectTransitAuthority.notifyDataObjectTransitFailed(
					GoldenGateIMI.class.getName(),
					this.dataId,
					((String) this.getAttribute(DOCUMENT_NAME_ATTRIBUTE, this.dataId)),
					"IMI",
					"IMS",
					"importerError",
					"decodingFailure",
					error.getMessage());
			
			//	clean up cached data file
			//	TODOnot really delete on error? ==> need to hold on to files to facilitate investigating decoding errors
//			this.deleteDataFile();
		}
		
		private void deleteDataFile() {
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
	
	/**
	 * Owner of a document import. The owner of an import receives notification
	 * when an the import is started, and when it is finished or has failed. An
	 * import owner is identified by a key. This key is used to map import
	 * owners to their imports as long as the latter are persisted while
	 * awaiting execution. For this reason, keys must both be unique and remain
	 * unchanged once used. The maximum length is 32 characters, which should
	 * not contain spaces, anon-ASCII letters, or punctuation marks other than
	 * underscores or dashes; the minimum length is 8 characters.
	 * 
	 * @author sautter
	 */
	public static abstract class ImiDocumentImportOwner {
		final String key;
		
		/** Constructor
		 * @param the document import owner key
		 */
		protected ImiDocumentImportOwner(String key) {
			if (!key.matches("[a-zA-Z0-9\\-\\_]{8,32}"))
				throw new IllegalArgumentException("Invalid document import owner key '" + key + "'");
			this.key = key;
		}

		/**
		 * Receive notification that a document import is about to be started.
		 * An owner can cancel an import at this point by throwing a runtime
		 * exception; the exception message should explain why the import was
		 * cancelled at the last moment.
		 * @param idi the import about to start
		 * @throws RuntimeException
		 */
		public abstract void importStarting(ImiDocumentImport idi) throws RuntimeException;
		
		/**
		 * Receive notification that a document import has finished successfully
		 * and the document will be stored in IMS. An owner can both make custom
		 * modifications to the imported document or cancel the import at this
		 * point by throwing a runtime exception; the exception message should
		 * explain why the import was cancelled at the last moment.
		 * @param idi the import that has finished
		 * @param doc the imported document
		 * @throws RuntimeException
		 */
		public abstract void importFinished(ImiDocumentImport idi, ImDocument doc) throws RuntimeException;
		
		/**
		 * Receive notification that a document import has finished successfully
		 * and the document will be stored in IMS. An owner can both make custom
		 * modifications to the imported document or cancel the import at this
		 * point by throwing a runtime exception; the exception message should
		 * explain why the import was cancelled at the last moment.
		 * @param idi the import that has finished
		 * @param docData the imported document data
		 * @throws RuntimeException
		 */
		public abstract void importFinished(ImiDocumentImport idi, ImDocumentData docData) throws RuntimeException;
		
		/**
		 * Receive notification that a document import has failed. The argument
		 * exception is the one responsible for the failure.
		 * @param idi the import that has finished
		 * @param error the error that caused the import to fail
		 * @throws RuntimeException
		 */
		public abstract void importFailed(ImiDocumentImport idi, Throwable error);
	}
	
	private static final byte[] PDF_FILE_SIGNATURE = {((byte) '%'), ((byte) 'P'), ((byte) 'D'), ((byte) 'F')}; // OK with letters, they are all basic ASCII
	private static final String PDF_MIME_TYPE = "application/pdf";
	
	private static final byte[] ZIP_FILE_SIGNATURE = {((byte) 'P'), ((byte) 'K'), ((byte) 3), ((byte) 4)}; // OK with letters, they are all basic ASCII
	private static final String IMF_MIME_TYPE = "application/imf"; // this one doesn't really exist, but it's OK for internal use
	
	/* read first 16 or so bytes to determine file MIME type (https://en.wikipedia.org/wiki/List_of_file_signatures):
	 * - '%PDF' --> application/pdf
	 * - 'PK<EXT><EOT>' --> application/zip (IMF)
	 */
	private static String determineMimeType(File file) throws IOException {
		PeekInputStream lookahead = new PeekInputStream(new FileInputStream(file), 16);
		try {
			if (lookahead.startsWith(PDF_FILE_SIGNATURE))
				return PDF_MIME_TYPE;
			else if (lookahead.startsWith(ZIP_FILE_SIGNATURE))
				return IMF_MIME_TYPE;
			else return null;
		}
		finally {
			lookahead.close();
		}
	}
	
	private static Attributed readAttributes(String[] arguments, int fromIndex) {
		Attributed attributes = new AbstractAttributed();
		for (int a = fromIndex; a < arguments.length; a++) {
			if (arguments[a].indexOf('=') == -1)
				continue;
			String[] avPair = arguments[a].trim().split("\\s*\\=\\s*", 2);
			if ((avPair.length == 2) && AnnotationUtils.isValidAnnotationType(avPair[0]))
				attributes.setAttribute(avPair[0], avPair[1]);
		}
		return attributes;
	}
	
	private GoldenGateIMS ims;
	
	private String importUserName;
	
	private File workingFolder;
	private File dataCacheFolder;
	private File importerCacheFolder;
	
//	private DocumentImporterTray[] importerTrays;
	private ImiDocumentImporter[] importers;
	private TreeMap[] importerActions;
	
	private AsynchronousDataActionHandler importHandler;
	private int maxParallelImporterRuns = 1;
	
	/** Constructor passing 'IMI' as the letter code to super constructor
	 */
	public GoldenGateIMI() {
		super("IMI");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
	 */
	protected void initComponent() {
		
		//	get import user name
		this.importUserName = this.configuration.getSetting("importUserName", "GgIMS");
		
		//	get working folder
		String workingFolderName = this.configuration.getSetting("workingFolderName", "Importers");
		while (workingFolderName.startsWith("./"))
			workingFolderName = workingFolderName.substring("./".length());
		this.workingFolder = (((workingFolderName.indexOf(":\\") == -1) && (workingFolderName.indexOf(":/") == -1) && !workingFolderName.startsWith("/")) ? new File(this.dataPath, workingFolderName) : new File(workingFolderName));
		this.workingFolder.mkdirs();
		
		//	get URL import cache folder
		String dataCacheFolderName = this.configuration.getSetting("dataCacheFolderName", "Cache");
		while (dataCacheFolderName.startsWith("./"))
			dataCacheFolderName = dataCacheFolderName.substring("./".length());
		this.dataCacheFolder = (((dataCacheFolderName.indexOf(":\\") == -1) && (dataCacheFolderName.indexOf(":/") == -1) && !dataCacheFolderName.startsWith("/")) ? new File(this.dataPath, dataCacheFolderName) : new File(dataCacheFolderName));
		this.dataCacheFolder.mkdirs();
		
		//	get importer cache folder
		String importerCacheFolderName = this.configuration.getSetting("importerCacheFolderName", "Cache");
		while (importerCacheFolderName.startsWith("./"))
			importerCacheFolderName = importerCacheFolderName.substring("./".length());
		this.importerCacheFolder = (((importerCacheFolderName.indexOf(":\\") == -1) && (importerCacheFolderName.indexOf(":/") == -1) && !importerCacheFolderName.startsWith("/")) ? new File(this.dataPath, importerCacheFolderName) : new File(importerCacheFolderName));
		this.importerCacheFolder.mkdirs();
		
		//	read number of threads to use from config
		String maxParallelImports = this.configuration.getSetting("maxParallelImports", ("" + this.maxParallelImporterRuns));
		try {
			this.maxParallelImporterRuns = Integer.parseInt(maxParallelImports);
		} catch (NumberFormatException nfe) {}
		
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
		
		//	use asynchronous action queue for imports (comes with persistence and can accommodate all the parameters once file is cached)
		IoProvider io = this.host.getIoProvider();
		if (!io.isJdbcAvailable())
			throw new RuntimeException("GoldenGateIMI: Cannot work without database access.");
		TableColumnDefinition[] argumentColumns = {
			new TableColumnDefinition("DataType", TableDefinition.VARCHAR_DATATYPE, 1),
			new TableColumnDefinition("DataFileOrUrl", TableDefinition.VARCHAR_DATATYPE, 256),
			new TableColumnDefinition("MimeType", TableDefinition.VARCHAR_DATATYPE, 32),
			new TableColumnDefinition("DeleteDataFile", TableDefinition.VARCHAR_DATATYPE, 1),
			new TableColumnDefinition("DataAttributes", TableDefinition.VARCHAR_DATATYPE, 1536),
			new TableColumnDefinition("OwnerKey", TableDefinition.VARCHAR_DATATYPE, 32),
		};
		this.importHandler = new AsynchronousDataActionHandler("ImiImporter", this.maxParallelImporterRuns, argumentColumns, this, io) {
			protected void performDataAction(String dataId, String[] arguments) throws Exception {
				handleImport(new ImiDocumentImport(dataId, arguments));
			}
		};
		
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
		this.loadImporters(this);
		
		//	start import handler thread
		this.importHandler.start();
	}
	
	private synchronized void loadImporters(final GoldenGateServerActivityLogger log) {
		
		//	cannot reload importers while imports are running
		if (this.runningImportsByNumber.size() != 0) {
			log.logError("Cannot reload importers, there are running imports.");
			return;
		}
		
		//	if reload, shut down importers
		if (log != this)
			this.shutdownImporters(log);
		
		//	load importers
		log.logResult("Loading importers ...");
		Object[] importerObjects = GamtaClassLoader.loadComponents(
				dataPath,
				ImiDocumentImporter.class, 
				new ComponentInitializer() {
					public void initialize(Object component, String componentJarName) throws Exception {
						File dataPath = new File(GoldenGateIMI.this.dataPath, (componentJarName.substring(0, (componentJarName.length() - 4)) + "Data"));
						if (!dataPath.exists())
							dataPath.mkdir();
						ImiDocumentImporter importer = ((ImiDocumentImporter) component);
						log.logResult(" - initializing importer " + importer.getName() + " ...");
						File workingFolder = new File(GoldenGateIMI.this.workingFolder, importer.getName());
						if (!workingFolder.exists())
							workingFolder.mkdir();
						File cacheFolder = new File(GoldenGateIMI.this.importerCacheFolder, importer.getName());
						if (!cacheFolder.exists())
							cacheFolder.mkdir();
						importer.setDataPath(dataPath);
						importer.setWorkingFolder(workingFolder);
						importer.setCacheFolder(cacheFolder);
						importer.setParent(GoldenGateIMI.this);
						importer.setHost(GoldenGateIMI.this.host);
						importer.init();
						log.logResult(" - importer " + importer.getName() + " initialized");
					}
				});
		log.logResult("Importers loaded");
		
		//	store importers, and sort them by priority
//		DocumentImporterTray[] importerTrays = new DocumentImporterTray[importerObjects.length];
//		for (int i = 0; i < importerObjects.length; i++)
//			importerTrays[i] = new DocumentImporterTray((ImiDocumentImporter) importerObjects[i]);
//		Arrays.sort(importerTrays);
		ImiDocumentImporter[] importers = new ImiDocumentImporter[importerObjects.length];
		for (int i = 0; i < importerObjects.length; i++)
			importers[i] = ((ImiDocumentImporter) importerObjects[i]);
		Arrays.sort(importers, importerPriorityOrder);
		
		//	make importers available
//		this.importerTrays = importerTrays;
		this.importers = importers;
		log.logResult("Importers registered");
		
		//	collect actions per importer
		this.importerActions = new TreeMap[this.importers.length];
		for (int i = 0; i < this.importers.length; i++) {
			this.importerActions[i] = new TreeMap(String.CASE_INSENSITIVE_ORDER);
			ComponentActionConsole[] importerActions = this.importers[i].getActions();
			if (importerActions == null)
				continue;
			for (int a = 0; a < importerActions.length; a++)
				this.importerActions[i].put(importerActions[a].getActionCommand(), importerActions[i]);
		}
		log.logResult("Importer actions integrated");
	}
	private static final Comparator importerPriorityOrder = new Comparator() {
		public int compare(Object obj1, Object obj2) {
			ImiDocumentImporter idi1 = ((ImiDocumentImporter) obj1);
			ImiDocumentImporter idi2 = ((ImiDocumentImporter) obj2);
			return (idi2.getPrority() - idi1.getPrority()); // descending order
		}
	};
	
	private synchronized void shutdownImporters(GoldenGateServerActivityLogger log) {
		
		//	make importers private
//		DocumentImporterTray[] importerTrays = this.importerTrays;
//		this.importerTrays = null;
		ImiDocumentImporter[] importers = this.importers;
		this.importers = null;
		this.importerActions = null;
		
		//	shut down importers
		log.logResult("Finalizing importers");
//		for (int i = 0; i < importerTrays.length; i++)
//			importerTrays[i].importer.exit();
		for (int i = 0; i < importers.length; i++)
			importers[i].exit();
		log.logResult("Importers finalized");
	}
	
	/**
	 * Retrieve the user name IMI uses to check out documents for updates and
	 * for storing new documents.
	 * @return the batch user name
	 */
	public String getImportUserName() {
		return this.importUserName;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		
		//	shut down import handler thread
		this.importHandler.shutdown();
		
		//	shut down importers
		this.shutdownImporters(null);
	}
	
	private static final String LIST_IMPORTERS_COMMAND = "importers";
	private static final String RELOAD_IMPORTERS_COMMAND = "reload";
	private static final String QUEUE_SIZE_COMMAND = "queueSize";
	private static final String ENQUEUE_IMPORT_COMMAND = "enqueueImport";
	private static final String IMPORTER_COMMAND = "importer";
	private static final String IMPORT_STATUS_COMMAND = "impStatus";
	private static final String IMPORT_COMMAND = "import";
	
	/*
	 * (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(this.importHandler.getActions()));
		ComponentAction ca;
		
		//	list importers
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_IMPORTERS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_IMPORTERS_COMMAND,
						"List the importers currently installed in this IMI."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
//				this.reportResult("There are currently " + importerTrays.length + " importers installed in this IMI:");
//				for (int i = 0; i < importerTrays.length; i++) {
//					String[] description = importerTrays[i].importer.getDescription();
//					for (int d = 0; d < description.length; d++)
//						this.reportResult(((d == 0) ? "- " : "  ") + description[d]);
//				}
				this.reportResult("There are currently " + importers.length + " importers installed in this IMI:");
				for (int i = 0; i < importers.length; i++) {
					String[] description = importers[i].getDescription();
					for (int d = 0; d < description.length; d++)
						this.reportResult(((d == 0) ? "- " : "  ") + description[d]);
				}
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
						"Reload the importers currently installed in this IMI."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					loadImporters(this);
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	call action on some importer
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return IMPORTER_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						IMPORTER_COMMAND + " <importerName> <command> <argument> ...",
						"Send a command to an importer installed in this IMI:",
						"- <importerName>: The name of the importer the command is directed to",
						"- <command>: The command to execute",
						"- <argument>: An argument for the command to the importer (there can be no or any number of such arguments)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length < 2) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify at least the importer name and command.");
					return;
				}
				
//				DocumentImporterTray importerTray = null;
//				for (int i = 0; i < importerTrays.length; i++)
//					if (importerTrays[i].importer.getName().equalsIgnoreCase(arguments[0])) {
//						importerTray = importerTrays[i];
//						break;
//					}
//				if (importerTray == null) {
//					this.reportError(" Invalid importer name '" + arguments[0] + "', use the '" + LIST_IMPORTERS_COMMAND + "' command to list available importers.");
//					return;
//				}
				TreeMap actions = null;
				for (int i = 0; i < importers.length; i++)
					if (importers[i].getName().equalsIgnoreCase(arguments[0])) {
						actions = importerActions[i];
						break;
					}
				if (actions == null) {
					this.reportError(" Invalid importer name '" + arguments[0] + "', use the '" + LIST_IMPORTERS_COMMAND + "' command to list available importers.");
					return;
				}
				
				if ("?".equals(arguments[1]) && (arguments.length == 2)) {
//					ComponentActionConsole[] actions = importerTray.getActions();
					if (actions.isEmpty()) {
						this.reportResult("There are no commands for importer '" + arguments[0] + "'");
						return;
					}
					this.reportResult("Commands for importer '" + arguments[0] + "':");
//					for (int a = 0; a < actions.length; a++) {
					for (Iterator ait = actions.keySet().iterator(); ait.hasNext();) {
//						String[] explanation = actions[a].getExplanation();
						String[] explanation = ((ComponentActionConsole) actions.get(ait.next())).getExplanation();
						for (int e = 0; e < explanation.length; e++)
							this.reportResult("  " + ((e == 0) ? "" : "  ") + explanation[e]);
					}
					return;
				}
				
//				ComponentActionConsole cac = importerTray.getAction(arguments[1]);
				ComponentActionConsole cac = ((ComponentActionConsole) actions.get(arguments[1]));
				if (cac == null) {
					this.reportError(" Invalid command '" + arguments[1] + "', use the '?' command to list available commands.");
					return;
				}
				String[] subArguments = new String[arguments.length - 2];
				System.arraycopy(arguments, 2, subArguments, 0, subArguments.length);
				cac.performActionConsole(subArguments, this);
			}
		};
		cal.add(ca);
		
		//	call action on some importer
		if (this.maxParallelImporterRuns != 1) {
			ca = new ComponentActionConsole() {
				public String getActionCommand() {
					return IMPORT_COMMAND;
				}
				public String[] getExplanation() {
					String[] explanation = {
							IMPORT_COMMAND + " <importNumber> <command> <argument> ...",
							"Send a command to an running import installed in this IMI:",
							"- <importerNumber>: The number of the import the command is directed to",
							"- <command>: The command to execute",
							"- <argument>: An argument for the command to the import (there can be no or any number of such arguments)"
						};
					return explanation;
				}
				public void performActionConsole(String[] arguments) {
					if ((maxParallelImporterRuns == 1) ? (arguments.length < 1) : (arguments.length < 2)) {
						this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify at least " + ((maxParallelImporterRuns == 1) ? "" : "the import number and") + " the command.");
						return;
					}
					if (runningImportsByNumber.isEmpty()) {
						this.reportResult("There is no document import running at the moment");
						return;
					}
					ImporterRun ir = ((ImporterRun) ((maxParallelImporterRuns == 1) ? runningImportsByNumber.values().iterator().next() : runningImportsByNumber.get(arguments[0])));
					if (ir == null) {
						this.reportError("Invalid import number '" + arguments[0] + "'");
						return;
					}
					ComponentActionConsole cac = ((ComponentActionConsole) ir.getAction(arguments[1]));
					if (cac == null) {
						this.reportError(" Invalid command '" + arguments[1] + "', use the '?' command to list available commands" + ((maxParallelImporterRuns == 1) ? "" : " (on main importer)") + ".");
						return;
					}
					String[] subArguments = new String[arguments.length - 2];
					System.arraycopy(arguments, 2, subArguments, 0, subArguments.length);
					cac.performActionConsole(subArguments, this);
				}
			};
			cal.add(ca);
			
			//	check processing status of a document
			ca = new ComponentActionConsole() {
				public String getActionCommand() {
					return IMPORT_STATUS_COMMAND;
				}
				public String[] getExplanation() {
					String[] explanation = {
							IMPORT_STATUS_COMMAND,
							"Show the status of a running document import"
							//	TODO update explanation with <batchNumber> argument
						};
					return explanation;
				}
				public void performActionConsole(String[] arguments) {
					if ((maxParallelImporterRuns == 1) ? (arguments.length != 0) : (arguments.length > 1)) {
						this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify " + ((maxParallelImporterRuns == 1) ? "no arguments" : "at most the import number as the only argument") + ".");
						return;
					}
					if (runningImportsByNumber.isEmpty()) {
						this.reportResult("There are no document importing at the moment");
						return;
					}
					if (arguments.length == 1) {
						ImporterRun ir = ((ImporterRun) runningImportsByNumber.get(arguments[1]));
						if (ir == null)
							this.reportError(" Invalid import number '" + arguments[1] + "'.");
						else ir.importer.reportImportStatus(this);
					}
					else {
						ArrayList runningImports = new ArrayList(runningImportsByNumber.values());
						for (int i = 0; i < runningImports.size(); i++)
							((ImporterRun) runningImports.get(i)).importer.reportImportStatus(this);
					}
				}
			};
			cal.add(ca);
		}
		
		//	check import queue
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return QUEUE_SIZE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						QUEUE_SIZE_COMMAND,
						"Show current size of import queue, i.e., number of pending imports."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					this.reportResult(importHandler.getDataActionsPending() + " document imports pending.");
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	schedule import from URL, file, or folder (good for testing, and for trouble shooting)
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return ENQUEUE_IMPORT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						ENQUEUE_IMPORT_COMMAND + " <documentPath> <documentMimeType> <attribute>=<value> ...",
						"Import a document from a file, folder, or URL:",
						"- <documentPath>: The file, folder, or URL to import the document from",
						"- <documentMimeType>: The MIME type of the document (optional, defaults to 'application/pdf' for URLs and is determined automatically for local files)",
						"- <attribute>=<value>: An attribute-value pair to set for the document (there can be no or any number of such pairs)"
					};
				return explanation;
			}
			public void performActionConsole(final String[] arguments) {
				if (arguments.length == 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify at least the document URL and MIME type.");
					return;
				}
				
				//	import from file
				if (arguments[0].indexOf("://") == -1) {
					
					//	check file
					final File docFile = new File(arguments[0]);
					if (!docFile.exists()) {
						this.reportError(" Invalid document file or folder '" + docFile.getAbsolutePath() + "', specify the absolute path.");
						return;
					}
					
					//	IM Directory
					if (docFile.isDirectory()) {
						File docEntryFile = new File(docFile, "entries.txt");
						if (!docEntryFile.exists()) {
							this.reportError(" Invalid document folder '" + docFile.getAbsolutePath() + "', 'entries.txt' entry list file not found.");
							return;
						}
						
						//	import document in separate thread
						Thread importer = new Thread("LocalDocumentImportIMD") {
							public void run() {
								try {
									storeDocument(docFile, readAttributes(arguments, 1));
								}
								catch (IOException ioe) {
									reportError("Error storing document imported from " + docFile.getName() + ": " + ioe.getMessage());
									reportError(ioe);
								}
							}
						};
						this.reportResult(" Starting document import from folder '" + docFile.getAbsolutePath() + "'");
						importer.start();
					}
					
					//	other file
					else try {
						
						//	determine MIME type
						final String mimeType;
						final int attributeStart;
						if ((arguments.length >= 2) && arguments[1].matches("[a-z]+\\/[a-z\\-\\+]+")) {
							mimeType = arguments[1];
							attributeStart = 2;
						}
						else {
							mimeType = determineMimeType(docFile);
							attributeStart = 1;
						}
						
						//	import IMF in separate thread
						if (IMF_MIME_TYPE.equals(mimeType)) {
							Thread importer = new Thread("LocalDocumentImportIMF") {
								public void run() {
									try {
										storeDocument(docFile, readAttributes(arguments, attributeStart));
									}
									catch (IOException ioe) {
										reportError("Error storing document imported from " + docFile.getName() + ": " + ioe.getMessage());
										reportError(ioe);
									}
								}
							};
							this.reportResult(" Starting document import from file '" + docFile.getAbsolutePath() + "'");
							importer.start();
						}
						
						//	schedule other imports in normal queue
						else if (mimeType == null) {
							this.reportError(" Could not determine MIME type of '" + docFile.getAbsolutePath() + "', please specify explicitly in arguments.");
							return;
						}
						
						//	schedule other imports in normal queue
						else {
							
							//	read binary data to compute hash
							InputStream docIn = new BufferedInputStream(new FileInputStream(docFile));
							DataHashOutputStream docHashOut = new DataHashOutputStream(new OutputStream() {
								public void write(int b) throws IOException {}
							});
							byte[] buffer = new byte[1024];
							for (int r; (r = docIn.read(buffer, 0, buffer.length)) != -1;)
								docHashOut.write(buffer, 0, r);
							docHashOut.flush();
							docHashOut.close();
							docIn.close();
							String docId = docHashOut.getDataHash();
							this.reportResult("Document UUID computed: " + docId);
							
							//	schedule import
							scheduleImport(mimeType, docFile, docId, readAttributes(arguments, attributeStart));
						}
					}
					catch (IOException ioe) {
						this.reportError("Error importing document from " + docFile.getName() + ": " + ioe.getMessage());
						this.reportError(ioe);
					}
				}
				
				//	schedule import from URL
				else {
					
					//	determine MIME type
					String mimeType;
					int attributeStart;
					if ((arguments.length >= 2) && arguments[1].matches("[a-z]+\\/[a-z\\-\\+]+")) {
						mimeType = arguments[1];
						attributeStart = 2;
					}
					else {
						mimeType = PDF_MIME_TYPE;
						attributeStart = 1;
					}
					try {
						scheduleImport(mimeType, new URL(arguments[0]), readAttributes(arguments, attributeStart));
					}
					catch (MalformedURLException mue) {
						this.reportError(" '" + arguments[0] + "' is not a valid URL.");
					}
				}
			}
			
			private void storeDocument(File docFile, Attributed attributes) throws IOException {
				
				//	load document TODO when importing IMF, use cache folder to ease resource consumption
				ImDocument doc = ImDocumentIO.loadDocument(docFile);
				
				//	set document attributes
				AttributeUtils.copyAttributes(attributes, doc, AttributeUtils.ADD_ATTRIBUTE_COPY_MODE);
				
				//	get user name to credit (if any)
				String userName = ((String) attributes.getAttribute(GoldenGateIMS.CHECKIN_USER_ATTRIBUTE, importUserName));
				
				//	store document in IMS
				ims.uploadDocument(userName, doc, new EventLogger() {
					public void writeLog(String logEntry) {
						reportResult(logEntry);
					}
				});
				
				//	clean up (no interference with event notification, latter is synchronous)
				doc.dispose();
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
					output.writeLine("Proxied requests are not allowed");
					return;
				}
				
				//	read user name
				String user = input.readLine();
				if (user.length() == 0)
					user = importUserName;
				logInfo("Got user: " + user);
				
				//	read and check MIME type
				String mimeType = input.readLine();
				if (mimeType.indexOf('/') == -1) {
					output.writeLine("Invalid MIME type '" + mimeType + "'");
					return;
				}
				logInfo("Got MIME type: " + mimeType);
				
				//	read data name
				final int docDataSize = Integer.parseInt(input.readLine());
				logInfo("Got document size: " + docDataSize);
				
				//	read data name
				final String docDataNameOrUrl = input.readLine();
				if (docDataNameOrUrl.length() == 0) {
					output.writeLine("Invalid document data " + ((docDataSize < 0) ? "URL" : "name") + " '" + docDataNameOrUrl + "'");
					return;
				}
				logInfo("Got document " + ((docDataSize < 0) ? "URL" : "name") + ": " + docDataNameOrUrl);
				
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
					logInfo("Attribute '" + an + "' set to '" + av + "'");
				}
				
				//	add user name to attributes
				if (user != importUserName)
					docAttributes.setAttribute(GoldenGateIMS.CHECKIN_USER_ATTRIBUTE, user);
				
				//	add file name to attributes
				if (docDataSize > 0)
					docAttributes.setAttribute(GoldenGateIMS.DOCUMENT_NAME_ATTRIBUTE, docDataNameOrUrl);
				
				//	prepare handling upload
				String docId;
				String docStatus;
				
				//	receive and handle URL upload
				if (docDataSize < 0) {
					URL docDataUrl;
					try {
						docDataUrl = new URL(docDataNameOrUrl);
					}
					catch (Exception e) {
						output.writeLine("Invalid document data URL '" + docDataNameOrUrl + "'");
						return;
					}
					scheduleImport(mimeType, docDataUrl, docAttributes);
					docId = null;
					docStatus = "Scheduled for download and import.";
				}
				
				//	receive and handle file upload
				else {
					
					//	read and cache binary data, computing hash along the way
//					File docCacheFile = new File(cacheFolder, ("data." + Gamta.getAnnotationID() + ".cached"));
					File docCacheFile = new File(dataCacheFolder, ("data." + Gamta.getAnnotationID() + ".cached"));
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
					logInfo("Got " + docCacheBytes + " bytes of data");
					
					//	compute MD5 hash document ID
					docId = docCacheOut.getDataHash();
					logInfo("Document UUID computed: " + docId);
					
					//	check if document already in database, and report back if so
					Attributed exDocAttributes = null;
					try {
						exDocAttributes = ims.getDocumentAttributes(docId);
					} catch (Exception e) {}
					
					//	schedule import if not imported before
					if (exDocAttributes == null) {
						scheduleImport(mimeType, docCacheFile, docId, true, docAttributes);
						docStatus = "Scheduled for import.";
					}
					else {
						docAttributes = exDocAttributes;
						docStatus = ("Previously imported by " + exDocAttributes.getAttribute(GoldenGateIMS.CHECKIN_USER_ATTRIBUTE));
					}
				}
				
				//	indicate success
				output.writeLine(UPLOAD_DOCUMENT);
				
				//	report document status, as well as document ID
				if (docId != null)
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
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	private Map importOwners = Collections.synchronizedMap(new HashMap());
	
	/**
	 * Register an owner for document imports. Owners have the chance to both
	 * augment and cancel document imports, or can simply maintain status
	 * information
	 * @param owner the import owner to register
	 */
	public void registerImportOwner(ImiDocumentImportOwner owner) {
		if (owner != null)
			this.importOwners.put(owner.key, owner);
	}
	
	/**
	 * Unregister an owner of document imports.
	 * @param owner the import owner to unregister
	 */
	public void unregisterImportOwner(ImiDocumentImportOwner owner) {
		if (owner != null)
			this.importOwners.remove(owner.key);
	}
	
	ImiDocumentImportOwner getImportOwner(String ownerKey) {
		return ((ImiDocumentImportOwner) this.importOwners.get(ownerKey));
	}
	
	/**
	 * Schedule a document import from a URL.
	 * @param mimeType the MIME type of the document
	 * @param url the URL to import from
	 * @param attributes additional document attributes
	 */
	public void scheduleImport(String mimeType, URL url, Attributed attributes) {
		this.scheduleImport(mimeType, url, attributes, null);
	}
	
	/**
	 * Schedule a document import from a URL.
	 * @param mimeType the MIME type of the document
	 * @param url the URL to import from
	 * @param attributes additional document attributes
	 * @param owner the owner of the import
	 */
	public void scheduleImport(String mimeType, URL url, Attributed attributes, ImiDocumentImportOwner owner) {
		ImiDocumentImport idi = new ImiDocumentImport(mimeType, url, owner);
		
		if (owner != null) // for good measures ... might have been reloaded dynamically or something
			this.registerImportOwner(owner);
		
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
		
		String docId;
		try {
			InputStream docUrlIn = new BufferedInputStream(new ByteArrayInputStream(url.toString().getBytes()));
			DataHashOutputStream docHashOut = new DataHashOutputStream(new OutputStream() {
				public void write(int b) throws IOException {}
			});
			byte[] buffer = new byte[1024];
			for (int r; (r = docUrlIn.read(buffer, 0, buffer.length)) != -1;)
				docHashOut.write(buffer, 0, r);
			docHashOut.flush();
			docHashOut.close();
			docUrlIn.close();
			docId = docHashOut.getDataHash();
		}
		catch (IOException ioe) {
			docId = Gamta.getAnnotationID();
		}
		idi.setDataId(docId);
		this.importHandler.enqueueDataAction(docId, idi.getArguments());
	}
	
	/**
	 * Schedule a document import from a file.
	 * @param mimeType the MIME type of the document
	 * @param file the file to import from
	 * @param id the identifier hashed from the file to import
	 * @param attributes additional document attributes
	 */
	public void scheduleImport(String mimeType, File file, String id, Attributed attributes) {
		this.scheduleImport(mimeType, file, id, false, attributes, null);
	}
	/**
	 * Schedule a document import from a file.
	 * @param mimeType the MIME type of the document
	 * @param file the file to import from
	 * @param id the identifier hashed from the file to import
	 * @param attributes additional document attributes
	 * @param owner the owner of the import
	 */
	public void scheduleImport(String mimeType, File file, String id, Attributed attributes, ImiDocumentImportOwner owner) {
		this.scheduleImport(mimeType, file, id, false, attributes, owner);
	}
	
	/**
	 * Schedule a document import from a file.
	 * @param mimeType the MIME type of the document
	 * @param file the file to import from
	 * @param id the identifier hashed from the file to import
	 * @param attributes additional document attributes
	 * @param deleteFile delete the cache file after import?
	 */
	public void scheduleImport(String mimeType, File file, String id, boolean deleteFile, Attributed attributes) {
		this.scheduleImport(mimeType, file, id, deleteFile, attributes, null);
	}
	
	/**
	 * Schedule a document import from a file.
	 * @param mimeType the MIME type of the document
	 * @param file the file to import from
	 * @param id the identifier hashed from the file to import
	 * @param attributes additional document attributes
	 * @param deleteFile delete the cache file after import?
	 * @param owner the owner of the import
	 */
	public void scheduleImport(String mimeType, File file, String id, boolean deleteFile, Attributed attributes, ImiDocumentImportOwner owner) {
		ImiDocumentImport idi = new ImiDocumentImport(id, mimeType, file, deleteFile, owner);
		
		if (owner != null) // for good measures ... might have been reloaded dynamically or something
			this.registerImportOwner(owner);
		
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
		
		this.importHandler.enqueueDataAction(id, idi.getArguments());
	}
	
	/**
	 * Check if a document with a give ID already exists. This method helps
	 * importers to check whether or not some document has been imported
	 * before.
	 * @param docId the document ID to check
	 * @return true if a document with the argument ID exists
	 */
	public boolean checkDocumentExists(String docId) {
		return (this.ims.getDocumentVersionHistory(docId) != null);
	}
	
	private Map runningImportsByNumber = Collections.synchronizedMap(new TreeMap());
	void handleImport(ImiDocumentImport idi) {
		
		//	find appropriate importer
		ImiDocumentImporter importer = null;
		synchronized (this) {
			
			//	wait for importers to be reloaded
//			while (this.importerTrays == null) try {
			while (this.importers == null) try {
				GoldenGateIMI.this.wait(1000);
			} catch (InterruptedException ie) {}
			
			//	get importer
//			for (int i = 0; i < this.importerTrays.length; i++)
//				if (this.importerTrays[i].importer.canHandleImport(idi)) {
//					importer = this.importerTrays[i].importer;
//					break;
//				}
			for (int i = 0; i < this.importers.length; i++)
				if (this.importers[i].canHandleImport(idi)) {
					importer = this.importers[i];
					break;
				}
		}
		if (importer == null) {
			this.logWarning("Could not find importer for " + idi.dataMimeType + " document from " + idi.toString());
			return;
		}
		
		//	create importer run object at lowest available number
		ImporterRun importerRun = null;
		synchronized (this.runningImportsByNumber) {
			if (this.maxParallelImporterRuns == 1)
				importerRun = new ImporterRun("", importer);
			else {
				String irNumber = null;
				for (int i = 0;; i++) {
					String irn = ("" + (i+1));
					if (this.runningImportsByNumber.containsKey(irn))
						continue;
					irNumber = irn;
					break;
				}
				importerRun = new ImporterRun(irNumber, importer.getRuntimeClone(importer.getName() + irNumber));
			}
			this.runningImportsByNumber.put(importerRun.number, importerRun);
		}
		
		//	notify owner
		try {
			idi.notifyStartring();
		}
		catch (RuntimeException re) {
			this.logError("Exception notifying owner of starting import: " + re.getMessage());
			this.logError(re);
		}
		
		//	handle import
		try {
			importerRun.handleImport(idi);
		}
		finally {
			this.runningImportsByNumber.remove(importerRun.number);
		}
	}
	
	private class ImporterRun {
		final String number;
		final String name;
		final ImiDocumentImporter importer;
		private TreeMap actions = null;
		ImporterRun(String number, ImiDocumentImporter importer) {
			this.number = number;
			this.name = (getLetterCode() + number);
			this.importer = importer;
		}
//		ComponentActionConsole[] getActions() {
//			this.ensureActions();
//			return ((ComponentActionConsole[]) this.actions.values().toArray(new ComponentActionConsole[this.actions.size()]));
//		}
		ComponentActionConsole getAction(String command) {
			this.ensureActions();
			return ((ComponentActionConsole) this.actions.get(command));
		}
		private void ensureActions() {
			if (this.actions != null)
				return;
			this.actions = new TreeMap();
			ComponentActionConsole[] cacs = this.importer.getActions();
			if (cacs == null)
				return;
			for (int a = 0; a < cacs.length; a++)
				this.actions.put(cacs[a].getActionCommand(), cacs[a]);
		}
		void handleImport(ImiDocumentImport idi) {
			this.importer.handleImport(idi);
		}
	}
}