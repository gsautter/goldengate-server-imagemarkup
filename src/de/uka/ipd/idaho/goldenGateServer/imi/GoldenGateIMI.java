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
import java.util.Comparator;

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
		
		ImiDocumentImport(String[] arguments) throws IOException {
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
			else throw new IllegalArgumentException();
			ImDocumentIO.setAttributes(this, arguments[4]);
		}
		
		String[] getArguments() {
			String[] arguments = {
				((this.dataFile == null) ? "U" : "D"),
				((this.dataFile == null) ? this.dataUrl.toString() : this.dataFile.getAbsolutePath()),
				this.dataMimeType,
				(this.deleteDataFile ? "D" : "R"),
				ImDocumentIO.getAttributesString(this),
			};
			return arguments;
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
				logError("Error storing document imported from " + this.toString() + ": " + ioe.getMessage());
				logError(ioe);
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
				logError("Error storing document imported from " + this.toString() + ": " + ioe.getMessage());
				logError(ioe);
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
			logError("Error importing document from " + this.toString() + ": " + error.getMessage());
			logError(error);
			
			//	clean up cached data file
			//	TODO really delete on error?
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
	
	private String defaultImportUserName;
	
	private File workingFolder;
	private File cacheFolder;
	
	private ImiDocumentImporter[] importers;
	
	private AsynchronousDataActionHandler importHandler;
	
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
		};
		this.importHandler = new AsynchronousDataActionHandler("GoldenGateImi", argumentColumns, this, io) {
			protected void performDataAction(String dataId, String[] arguments) throws Exception {
				handleImport(new ImiDocumentImport(arguments));
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
//		
//		//	reload pending imports
//		File importFile = new File(this.dataPath, "imports.txt");
//		if (importFile.exists()) try {
//			BufferedReader importBr = new BufferedReader(new InputStreamReader(new FileInputStream(importFile), "UTF-8"));
//			for (String importData; (importData = importBr.readLine()) != null;) {
//				String[] importAttributes = importData.split("\\t");
//				if (importAttributes.length == 5) {
//					ImiDocumentImport di;
//					if ("".equals(importAttributes[0]))
//						di = new ImiDocumentImport(("".equals(importAttributes[2]) ? null : importAttributes[2]), new File(importAttributes[1]), "D".equals(importAttributes[3]));
//					else di = new ImiDocumentImport(("".equals(importAttributes[2]) ? null : importAttributes[2]), new URL(importAttributes[0]));
//					ImDocumentIO.setAttributes(di, importAttributes[4]);
//					this.enqueueImport(di);
//				}
//			}
//			importBr.close();
//		}
//		catch (Exception e) {
//			System.out.println("ImageMarkupImporter: Error restoring pending update events from file '" + importFile.getAbsolutePath() + "': " + e.getMessage());
//			e.printStackTrace(System.out);
//		}
		
		//	load importers
		this.loadImporters(null);
		
		//	start import handler thread
//		Thread importerThread = new ImporterThread();
//		importerThread.start();
		this.importHandler.start();
	}
	
	private synchronized void loadImporters(final ComponentActionConsole cac) {
		
		//	if reload, shut down importers
		if (cac != null)
			this.shutdownImporters(cac);
		
		//	load importers
		if (cac == null)
			System.out.println("Loading importers ...");
		else cac.reportResult("Loading importers ...");
		Object[] importerObjects = GamtaClassLoader.loadComponents(
				dataPath,
				ImiDocumentImporter.class, 
				new ComponentInitializer() {
					public void initialize(Object component, String componentJarName) throws Exception {
						File dataPath = new File(GoldenGateIMI.this.dataPath, (componentJarName.substring(0, (componentJarName.length() - 4)) + "Data"));
						if (!dataPath.exists())
							dataPath.mkdir();
						ImiDocumentImporter importer = ((ImiDocumentImporter) component);
						if (cac == null)
							System.out.println(" - initializing importer " + importer.getName() + " ...");
						else cac.reportResult(" - initializing importer " + importer.getName() + " ...");
						File workingFolder = new File(GoldenGateIMI.this.workingFolder, importer.getName());
						if (!workingFolder.exists())
							workingFolder.mkdir();
						File cacheFolder = new File(GoldenGateIMI.this.cacheFolder, importer.getName());
						if (!cacheFolder.exists())
							cacheFolder.mkdir();
						importer.setDataPath(dataPath);
						importer.setWorkingFolder(workingFolder);
						importer.setCacheFolder(cacheFolder);
						importer.setParent(GoldenGateIMI.this);
						importer.setHost(GoldenGateIMI.this.host);
						importer.init();
						if (cac == null)
							System.out.println(" - importer " + importer.getName() + " initialized");
						else cac.reportResult(" - importer " + importer.getName() + " initialized");
					}
				});
		if (cac == null)
			System.out.println("Importers loaded");
		else cac.reportResult("Importers loaded");
		
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
		if (cac == null)
			System.out.println("Importers registered");
		else cac.reportResult("Importers registered");
	}
	
	private synchronized void shutdownImporters(ComponentActionConsole cac) {
		
		//	make importers private
		ImiDocumentImporter[] importers = this.importers;
		this.importers = null;
		
		//	shut down importers
		if (cac == null)
			System.out.println("Finalizing importers");
		else cac.reportResult("Finalizing importers");
		for (int i = 0; i < importers.length; i++)
			importers[i].exit();
		if (cac == null)
			System.out.println("Importers finalized");
		else cac.reportResult("Importers finalized");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#exitComponent()
	 */
	protected void exitComponent() {
//		
//		//	get pending imports
//		ArrayList imports = new ArrayList(this.importQueue);
//		
//		//	delete pending import file if empty
//		File importFile = new File(this.dataPath, "imports.txt");
//		if (imports.isEmpty()) {
//			if (importFile.exists()) try {
//				importFile.delete();
//			}
//			catch (Exception e) {
//				System.out.println("ImageMarkupImporter: Error deleting import file '" + importFile.getAbsolutePath() + "': " + e.getMessage());
//				e.printStackTrace(System.out);
//			}
//		}
//		
//		//	store any pending imports on disk
//		else try {
//			BufferedWriter importBw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(importFile), "UTF-8"));
//			for (int i = 0; i < imports.size(); i++) {
//				ImiDocumentImport di = ((ImiDocumentImport) imports.get(i));
//				importBw.write((di.dataUrl == null) ? "" : di.dataUrl.toString());
//				importBw.write("\t" + ((di.dataFile == null) ? "" : di.dataFile.getAbsolutePath()));
//				importBw.write("\t" + ((di.dataMimeType == null) ? "" : di.dataMimeType));
//				importBw.write("\t" + (di.deleteDataFile ? "D" : "R"));
//				importBw.write("\t" + ImDocumentIO.getAttributesString(di));
//				importBw.newLine();
//			}
//			importBw.flush();
//			importBw.close();
//		}
//		catch (Exception e) {
//			System.out.println("ImageMarkupImporter: Error storing pending imports to file '" + importFile.getAbsolutePath() + "': " + e.getMessage());
//			e.printStackTrace(System.out);
//		}
		
		//	shut down import handler thread
//		synchronized (this.importQueue) {
//			this.importQueue.clear();
//			this.importQueue.notify();
//		}
		this.importHandler.shutdown();
		
		//	shut down importers
		this.shutdownImporters(null);
	}
	
	private static final String IMPORT_DOCUMENT_COMMAND = "import";
	private static final String RELOAD_IMPORTERS_COMMAND = "reload";
	private static final String QUEUE_SIZE_COMMAND = "queueSize";
	
	/*
	 * (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
//		ArrayList cal = new ArrayList(this.importHandler.getActions());
		ArrayList cal = new ArrayList(Arrays.asList(this.importHandler.getActions()));
		ComponentAction ca;
		
		//	schedule import from URL, file, or folder (good for testing, and for trouble shooting)
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return IMPORT_DOCUMENT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						IMPORT_DOCUMENT_COMMAND + " <documentPath> <documentMimeType> <attribute>=<value> ...",
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
			
			//	TODO facilitate using IMD
			private void storeDocument(File docFile, Attributed attributes) throws IOException {
				
				//	load document TODO when importing IMF, use cache folder to ease resource consumption
				ImDocument doc = ImDocumentIO.loadDocument(docFile);
				
				//	set document attributes
				AttributeUtils.copyAttributes(attributes, doc, AttributeUtils.ADD_ATTRIBUTE_COPY_MODE);
				
				//	get user name to credit (if any)
				String userName = ((String) attributes.getAttribute(GoldenGateIMS.CHECKIN_USER_ATTRIBUTE, defaultImportUserName));
				
				//	store document in IMS
				ims.uploadDocument(userName, doc, null);
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
					user = defaultImportUserName;
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
				if (user != defaultImportUserName)
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
					File docCacheFile = new File(cacheFolder, ("data." + Gamta.getAnnotationID() + ".cached"));
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
//					this.reportResult(importQueue.size() + " document imports pending.");
					this.reportResult(importHandler.getDataActionsPending() + " document imports pending.");
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
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
		
//		this.enqueueImport(idi);
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
		this.scheduleImport(mimeType, file, id, false, attributes);
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
		
//		this.enqueueImport(idi);
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
//	
//	private LinkedList importQueue = new LinkedList() {
//		private HashSet deduplicator = new HashSet();
//		public Object removeFirst() {
//			Object e = super.removeFirst();
//			this.deduplicator.remove(e);
//			return e;
//		}
//		public void addLast(Object e) {
//			if (this.deduplicator.add(e))
//				super.addLast(e);
//		}
//	};
//	private void enqueueImport(ImiDocumentImport idi) {
//		synchronized (this.importQueue) {
//			this.importQueue.addLast(idi);
//			this.importQueue.notify();
//		}
//	}
//	private ImiDocumentImport getImport() {
//		synchronized (this.importQueue) {
//			if (this.importQueue.isEmpty()) try {
//				this.importQueue.wait();
//			} catch (InterruptedException ie) {}
//			return (this.importQueue.isEmpty() ? null : ((ImiDocumentImport) this.importQueue.removeFirst()));
//		}
//	}
	
	void handleImport(ImiDocumentImport idi) {
		
		//	find appropriate importer
		ImiDocumentImporter importer = null;
		synchronized (this) {
			
			//	wait for importers to be reloaded
			while (this.importers == null) try {
				GoldenGateIMI.this.wait(1000);
			} catch (InterruptedException ie) {}
			
			//	get importer
			for (int i = 0; i < this.importers.length; i++)
				if (this.importers[i].canHandleImport(idi)) {
					importer = this.importers[i];
					break;
				}
		}
		if (importer == null)
			this.logWarning("Could not find importer for " + idi.dataMimeType + " document from " + idi.toString());
		
		//	handle import
		else importer.handleImport(idi);
	}
//	
//	private class ImporterThread extends Thread {
//		public void run() {
//			
//			//	don't start right away
//			try {
//				sleep(1000 * 15);
//			} catch (InterruptedException ie) {}
//			
//			//	keep going until shutdown
//			while (true) {
//				
//				//	get next import
//				ImiDocumentImport idi = getImport();
//				if (idi == null)
//					return; // only happens on shutdown
////				
////				//	find appropriate importer
////				ImiDocumentImporter importer = null;
////				synchronized (GoldenGateIMI.this) {
////					
////					//	wait for importers to be reloaded
////					while (GoldenGateIMI.this.importers == null) try {
////						GoldenGateIMI.this.wait(1000);
////					} catch (InterruptedException ie) {}
////					
////					//	get importer
////					for (int i = 0; i < importers.length; i++)
////						if (importers[i].canHandleImport(idi)) {
////							importer = importers[i];
////							break;
////						}
////				}
////				if (importer == null) {
////					logWarning("Could not find importer for " + idi.dataMimeType + " document from " + idi.toString());
////					continue;
////				}
//				
//				//	handle import
////				importer.handleImport(idi);
//				else handleImport(idi);
//				
//				//	give the others a little time
//				try {
//					sleep(1000 * 5);
//				} catch (InterruptedException ie) {}
//			}
//		}
//	}
}