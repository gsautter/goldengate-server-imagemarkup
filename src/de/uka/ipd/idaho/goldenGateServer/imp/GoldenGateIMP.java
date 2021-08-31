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
package de.uka.ipd.idaho.goldenGateServer.imp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.easyIO.sql.TableColumnDefinition;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.Attributed;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.imi.GoldenGateIMI;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS.ImsDocumentData;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent.ImsDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.uaa.UserAccessAuthority;
import de.uka.ipd.idaho.goldenGateServer.util.AsynchronousDataActionHandler;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveErrorRecorder;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveInstallerUtils;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveJob;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveProcessInterface;
import de.uka.ipd.idaho.im.ImAnnotation;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.util.ImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.FolderImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.ImDocumentEntry;
import de.uka.ipd.idaho.im.util.ImDocumentStyle;
import de.uka.ipd.idaho.im.util.ImDocumentStyleProvider;

/**
 * GoldenGATE Image Markup Processor provides automated processing of Image
 * Markup documents stored in a GoldenGATE IMS. Processing happens in a slave
 * JVM, controlled by document style templates. Documents can only be processed
 * if they match such a template.
 * 
 * @author sautter
 */
public class GoldenGateIMP extends AbstractGoldenGateServerComponent implements GoldenGateImpConstants {
	private GoldenGateIMS ims;
	private UserAccessAuthority uaa;
	
	private String batchUserName;
	private int maxSlaveMemory = 1024;
	private int maxSlaveCores = 1;
	
	private File workingFolder;
	private File logFolder;
	private File cacheFolder;
	private AsynchronousDataActionHandler documentProcessor;
	
	//	TODOne keep these fields _per_slave_ soon as we start using multiple !!!
//	private Process batchRun = null;
//	private ImpSlaveProcessInterface batchInterface = null;
//	private String processingDocId = null;
//	private String processingBatchName = null;
//	private long processingStart = -1;
//	private String processorName = null;
//	private long processorStart = -1;
//	private String processingStep = null;
//	private long processingStepStart = -1;
//	private String processingInfo = null;
//	private long processingInfoStart = -1;
//	private int processingProgress = -1;
	private int maxParallelBatchRuns = 1;
	/*
TODO Do NOT keep IMP batch runs in array ...
... but simply create new object each time (preciously little effort compared to batch run proper ...)
==> saves tons of hassle (owner, etc.)
==> saves cleaning up inner status
==> simply assign number based upon smallest free one ...
==> ... synchronize latter on running batches
==> also synchronize console action access on running batch map
	 */
	
	private String docStyleListUrl;
	private File docStyleFolder;
	private ImDocumentStyleProvider docStyleProvider;
	
	private String ggiConfigHost;
	private String ggiConfigName;
	
	private String[] defaultBatchImTools;
	private ImpBatch defaultBatch;
	private Map batchesByName = Collections.synchronizedMap(new TreeMap(String.CASE_INSENSITIVE_ORDER));
	
	/** Constructor passing 'IMP' as the letter code to super constructor
	 */
	public GoldenGateIMP() {
		super("IMP");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
	 */
	protected void initComponent() {
		
		//	get default import user name
		this.batchUserName = this.configuration.getSetting("batchUserName", "GoldenGateIMP");
		
		//	get maximum memory and CPU core limit for slave process
		try {
			this.maxSlaveMemory = Integer.parseInt(this.configuration.getSetting("maxSlaveMemory", ("" + this.maxSlaveMemory)));
		} catch (RuntimeException re) {}
		try {
			this.maxSlaveCores = Integer.parseInt(this.configuration.getSetting("maxSlaveCores", ("" + this.maxSlaveCores)));
		} catch (RuntimeException re) {}
		
		//	get working folder
		String workingFolderName = this.configuration.getSetting("workingFolderName", "Processor");
		while (workingFolderName.startsWith("./"))
			workingFolderName = workingFolderName.substring("./".length());
		this.workingFolder = (((workingFolderName.indexOf(":\\") == -1) && (workingFolderName.indexOf(":/") == -1) && !workingFolderName.startsWith("/")) ? new File(this.dataPath, workingFolderName) : new File(workingFolderName));
		this.workingFolder.mkdirs();
		
		//	get log folder
		String logFolderName = this.configuration.getSetting("logFolderName", "Logs");
		while (logFolderName.startsWith("./"))
			logFolderName = logFolderName.substring("./".length());
		this.logFolder = (((logFolderName.indexOf(":\\") == -1) && (logFolderName.indexOf(":/") == -1) && !logFolderName.startsWith("/")) ? new File(this.workingFolder, logFolderName) : new File(logFolderName));
		this.logFolder.mkdirs();
		
		//	get URL import cache folder (RAM disc !!!)
		String cacheFolderName = this.configuration.getSetting("cacheFolderName", "Cache");
		while (cacheFolderName.startsWith("./"))
			cacheFolderName = cacheFolderName.substring("./".length());
		this.cacheFolder = (((cacheFolderName.indexOf(":\\") == -1) && (cacheFolderName.indexOf(":/") == -1) && !cacheFolderName.startsWith("/")) ? new File(this.dataPath, cacheFolderName) : new File(cacheFolderName));
		this.cacheFolder.mkdirs();
		
		//	load GGI config host & name
		this.ggiConfigHost = this.configuration.getSetting("ggiConfigHost");
		this.ggiConfigName = this.configuration.getSetting("ggiConfigName");
		
		//	load IM tool sequence to run
		this.defaultBatchImTools = this.configuration.getSetting("batchImTools").trim().split("\\s+");
		
		//	load batch definitions
		this.loadBatches(null);
		
		//	make document style templates available
		this.docStyleListUrl = this.configuration.getSetting("docStyleListUrl");
		if (this.docStyleListUrl == null)
			throw new RuntimeException("Cannot work without document style templates, URL missing");
		this.docStyleFolder = new File(this.workingFolder, "DocStyles");
		this.docStyleFolder.mkdirs();
		
		//	install GGI slave JAR
		SlaveInstallerUtils.installSlaveJar("GgServerImpSlave.jar", this.dataPath, this.workingFolder, true);
		
		//	read number of threads to use from config
		String maxParallelBatches = this.configuration.getSetting("maxParallelBatches", ("" + this.maxParallelBatchRuns));
		try {
			this.maxParallelBatchRuns = Integer.parseInt(maxParallelBatches);
		} catch (NumberFormatException nfe) {}
		
		//	create asynchronous worker
		TableColumnDefinition[] argCols = {
			new TableColumnDefinition("BatchOrImtName", TableDefinition.VARCHAR_DATATYPE, 64),
			new TableColumnDefinition("DocStyleMode", TableDefinition.CHAR_DATATYPE, 1),
			new TableColumnDefinition("LogVerbose", TableDefinition.CHAR_DATATYPE, 1),
			new TableColumnDefinition("UserName", TableDefinition.VARCHAR_DATATYPE, 32),
		};
		this.documentProcessor = new AsynchronousDataActionHandler("Imp", this.maxParallelBatchRuns, argCols, this, this.host.getIoProvider()) {
			protected void performDataAction(String dataId, String[] arguments) throws Exception {
				String batchOrImtName = ((arguments[0].length() == 0) ? null : arguments[0]);
				char docStyleMode = ((arguments[1].length() == 0) ? 'R' : arguments[1].charAt(0));
				if ((docStyleMode != 'U') && (docStyleMode != 'I') && (docStyleMode != 'B'))
					docStyleMode = 'R';
				boolean verbose = "V".equals(arguments[2]);
				String userName = (((arguments.length < 4) || (arguments[3].length() == 0)) ? null : arguments[3]);
				if ((batchOrImtName == null))
					docStyleMode = 'R';
				else if ((docStyleMode == 'B') && (getBatchForName(batchOrImtName) == null))
					docStyleMode = 'R';
				else if ((docStyleMode != 'U') && (docStyleMode != 'I') && (docStyleMode != 'B'))
					docStyleMode = 'R';
				processDocument(dataId, batchOrImtName, docStyleMode, verbose, userName);
			}
		};
		
		//	TODO get list of documents from IMS
		
		//	TODO schedule processing for all documents we still hold the lock for (must have been interrupted by shutdown before we could save them back and release them)
		
		//	set up collecting of errors from our slave processes
		String slaveErrorPath = this.host.getServerProperty("SlaveProcessErrorPath");
		if (slaveErrorPath != null)
			SlaveErrorRecorder.setErrorPath(slaveErrorPath);
	}
	
	private void loadBatches(ComponentActionConsole cac) {
		
		//	clear batch definitions
		this.batchesByName.clear();
		
		//	get batch definitions
		File[] batchFiles = this.dataPath.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return (file.isFile() && file.getName().endsWith(".batch.cnfg"));
			}
		});
		
		//	load batch definitions
		for (int b = 0; b < batchFiles.length; b++) {
			String batchName = batchFiles[b].getName();
			batchName = batchName.substring(0, (batchName.length() - ".batch.cnfg".length()));
			Settings batchSet = Settings.loadSettings(batchFiles[b]);
			String batchImTools = batchSet.getSetting("imTools");
			if (batchImTools == null) {
				if (cac == null)
					System.out.println("GoldenGateIMP: Cannot load batch '" + batchName + "', check setting 'imTools'.");
				else cac.reportError("Cannot load batch '" + batchName + "', check setting 'imTools'.");
				continue;
			}
			String batchGgiConfigName = batchSet.getSetting("ggiConfigName");
			String batchDocStyleMode = batchSet.getSetting("docStyleMode", "R");
			if (!"I".equals(batchDocStyleMode) && !"U".equals(batchDocStyleMode) && !"R".equals(batchDocStyleMode)) {
				if (cac == null)
					System.out.println("GoldenGateIMP: Cannot load batch '" + batchName + "', setting 'docStyleMode' must be 'R', 'U', 'I', or absent.");
				else cac.reportError("Cannot load batch '" + batchName + "', setting 'docStyleMode' must be 'R', 'U', 'I', or absent.");
				continue;
			}
			String batchUserName = batchSet.getSetting("userName");
			ImpBatch batch = new ImpBatch(batchName, batchGgiConfigName, batchImTools.trim().split("\\s+"), batchDocStyleMode.charAt(0), batchUserName);
			batch.label = batchSet.getSetting("label");
			batch.description = batchSet.getSetting("description");
			batch.htmlDescriptor = batchSet.getSetting("htmlDescriptor");
			this.batchesByName.put(batchName, batch);
			if (cac == null)
				System.out.println("GoldenGateIMP: Batch '" + batchName + "' loaded successfully, IM tool sequence is " + batch.getImtNames(false));
			else {
				cac.reportResult("Batch '" + batchName + "' loaded successfully:");
				batch.printDescription(cac);
			}
		}
		
		//	make sure we have a default batch
		if (this.batchesByName.containsKey("Default"))
			this.defaultBatch = ((ImpBatch) this.batchesByName.get("Default"));
		else this.defaultBatch = new ImpBatch("Default", this.ggiConfigName, this.defaultBatchImTools, 'R', null);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
	 */
	public void link() {
		
		//	link up to IMS
		this.ims = ((GoldenGateIMS) this.host.getServerComponent(GoldenGateIMS.class.getName()));
		if (this.ims == null) throw new RuntimeException(GoldenGateIMS.class.getName());
		
		//	link up to UAA
		this.uaa = ((UserAccessAuthority) this.host.getServerComponent(UserAccessAuthority.class.getName()));
		if (this.uaa == null) throw new RuntimeException(UserAccessAuthority.class.getName());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
	 */
	public void linkInit() {
		
		//	get IMI import user name (if IMI present, we can do without)
		String imiImportUserName;
		try {
			GoldenGateServerComponent imi = this.host.getServerComponent(GoldenGateIMI.class.getName());
			imiImportUserName = ((GoldenGateIMI) imi).getImportUserName();
		}
		catch (Throwable t) {
			System.out.println("GoldenGateIMP: Image Markup Importer not installed");
			t.printStackTrace(System.out);
			imiImportUserName = null;
		}
		
		//	establish connection to catch local updates
		final String fImiImportUserName = imiImportUserName;
		this.ims.addDocumentEventListener(new ImsDocumentEventListener() {
			private Map imsUpdatedDocIDs = Collections.synchronizedMap(new HashMap());
			public void documentCheckedOut(ImsDocumentEvent ide) {}
			public void documentUpdated(ImsDocumentEvent ide) {
				if (!ide.sourceClassName.equals(GoldenGateIMS.class.getName()))
					return;
				Attributed docAttributes = ide.documentData.getDocumentAttributes();
				logInfo("GoldenGATE IMP: checking whether or not to process document " + docAttributes.getAttribute(ImDocument.DOCUMENT_NAME_ATTRIBUTE, ide.dataId));
				
				//	let's not loop back on our own updates
				if (batchUserName.equals(ide.authUser)) {
					logInfo(" ==> self-triggered update");
					return;
				}
				
				//	only take on documents from dedicated importer if set up this way
				if ((fImiImportUserName != null) && !fImiImportUserName.equals(ide.authUser)) {
					logInfo(" ==> most likely user upload");
					return;
				}
				
				//	check for assigned document style (coming in from PDF decoder, any existing style is assigned)
				if (docAttributes.hasAttribute(ImDocumentStyle.DOCUMENT_STYLE_ID_ATTRIBUTE)) {}
				else if (docAttributes.hasAttribute(ImDocumentStyle.DOCUMENT_STYLE_NAME_ATTRIBUTE)) {}
				else {
					logInfo(" ==> document style template not assigned");
					this.imsUpdatedDocIDs.remove(ide.dataId);
					return;
				}
				
				//	only process documents that have not been worked on yet
				ImAnnotation[] docAnnots = ide.documentData.getAnnotations();
				if (docAnnots.length != 0) {
					logInfo(" ==> there are already annotations");
					this.imsUpdatedDocIDs.remove(ide.dataId);
					return;
				}
				
				//	make sure we have a style provider
				ensureStyleProvider();
				
				//	use dummy document to get style template (we only want to use the attributes)
				ImDocument doc = new ImDocument(ide.dataId);
				doc.copyAttributes(docAttributes);
				
				//	test if we have a document style template
				ImDocumentStyle docStyle = ImDocumentStyle.getStyleFor(doc);
				if ((docStyle == null) || docStyle.getPropertyNames().length == 0) {
					logInfo(" ==> document style template not found");
					this.imsUpdatedDocIDs.remove(ide.dataId);
					return;
				}
				
				//	get and validate name of batch to use
				String batchName = docStyle.getStringProperty("ggServer.impBatchName", "Default");
				if (getBatchForName(batchName) == null) {
					logInfo(" ==> assigned batch '" + batchName + "does not exist");
					this.imsUpdatedDocIDs.remove(ide.dataId);
					return;
				}
				
				//	schedule processing document
				String batchUser = docStyle.getStringProperty("ggServer.impBatchUser", null);
				this.imsUpdatedDocIDs.put(ide.dataId, new BatchTriggerData(batchName, batchUser));
				logInfo(" ==> processing scheduled for after release");
			}
			public void documentDeleted(ImsDocumentEvent dse) {}
			public void documentReleased(ImsDocumentEvent dse) {
				if (!dse.sourceClassName.equals(GoldenGateIMS.class.getName()))
					return;
				BatchTriggerData btd = ((BatchTriggerData) this.imsUpdatedDocIDs.remove(dse.dataId));
				if (btd != null)
					scheduleProcessing(dse.dataId, btd.batchName, 'B', false, btd.userName, 0);
			}
			class BatchTriggerData {
				final String batchName;
				final String userName;
				BatchTriggerData(String batchName, String userName) {
					this.batchName = batchName;
					this.userName = userName;
				}
			}
		});
		
		//	start processing handler thread
		this.documentProcessor.start();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		
		//	shut down processing handler thread
		this.documentProcessor.shutdown();
	}
	
	/**
	 * Retrieve the user name IMP uses to check out documents for batch
	 * processing to authenticate resulting updates.
	 * @return the batch user name
	 */
	public String getBatchUserName() {
		return this.batchUserName;
	}
	
	/**
	 * Enqueue batch processing of an Image Markup document. If the argument
	 * batch name is <code>null</code>, the configured default is used.
	 * @param docId the ID of the document to process
	 * @param batchName the name of the batch to use
	 */
	public void batchProcessDocument(String docId, String batchName) {
		this.batchProcessDocument(docId, batchName, ((batchName == null) ? 'R' : 'B'), null, 0);
	}
	
	/**
	 * Enqueue batch processing of an Image Markup document. If the argument
	 * batch name is <code>null</code>, the configured default is used.
	 * @param docId the ID of the document to process
	 * @param batchName the name of the batch to use
	 * @param priority the priority of the processing (0 is normal)
	 */
	public void batchProcessDocument(String docId, String batchName, int priority) {
		this.batchProcessDocument(docId, batchName, ((batchName == null) ? 'R' : 'B'), null, priority);
	}
	
	/**
	 * Enqueue batch processing of an Image Markup document. If the argument
	 * batch name is <code>null</code>, the configured default is used.
	 * @param docId the ID of the document to process
	 * @param batchOrImtName the name of the batch to use
	 * @param userName the user name to use for document updates
	 */
	public void batchProcessDocument(String docId, String batchName, String userName) {
		this.batchProcessDocument(docId, batchName, ((batchName == null) ? 'R' : 'B'), userName, 0);
	}
	
	/**
	 * Enqueue batch processing of an Image Markup document. If the argument
	 * batch name is <code>null</code>, the configured default is used.
	 * @param docId the ID of the document to process
	 * @param batchOrImtName the name of the batch to use
	 * @param userName the user name to use for document updates
	 * @param priority the priority of the processing (0 is normal)
	 */
	public void batchProcessDocument(String docId, String batchName, String userName, int priority) {
		this.batchProcessDocument(docId, batchName, ((batchName == null) ? 'R' : 'B'), userName, priority);
	}
	
	/**
	 * Enqueue batch processing of an Image Markup document. If the argument
	 * batch name is <code>null</code>, the configured default is used. Valid
	 * values for the document style mode are 'R' (require), 'U' (use if
	 * available), 'I' (ignore even if available), and 'B' (use configured mode
	 * of batch with argument name). The mode for the configured default batch
	 * must always be 'R'.
	 * @param docId the ID of the document to process
	 * @param batchName the name of the batch to use
	 * @param docStyleMode the way of handling of document style templates
	 */
	public void batchProcessDocument(String docId, String batchName, char docStyleMode) {
		this.batchProcessDocument(docId, batchName, docStyleMode, null, 0);
	}
	
	/**
	 * Enqueue batch processing of an Image Markup document. If the argument
	 * batch name is <code>null</code>, the configured default is used. Valid
	 * values for the document style mode are 'R' (require), 'U' (use if
	 * available), 'I' (ignore even if available), and 'B' (use configured mode
	 * of batch with argument name). The mode for the configured default batch
	 * must always be 'R'.
	 * @param docId the ID of the document to process
	 * @param batchName the name of the batch to use
	 * @param docStyleMode the way of handling of document style templates
	 * @param priority the priority of the processing (0 is normal)
	 */
	public void batchProcessDocument(String docId, String batchName, char docStyleMode, int priority) {
		this.batchProcessDocument(docId, batchName, docStyleMode, null, priority);
	}
	
	/**
	 * Enqueue batch processing of an Image Markup document. If the argument
	 * batch name is <code>null</code>, the configured default is used. Valid
	 * values for the document style mode are 'R' (require), 'U' (use if
	 * available), 'I' (ignore even if available), and 'B' (use configured mode
	 * of batch with argument name). The mode for the configured default batch
	 * must always be 'R'.
	 * @param docId the ID of the document to process
	 * @param batchName the name of the batch to use
	 * @param docStyleMode the way of handling of document style templates
	 * @param userName the user name to use for document updates
	 */
	public void batchProcessDocument(String docId, String batchName, char docStyleMode, String userName) {
		this.batchProcessDocument(docId, batchName, docStyleMode, userName, 0);
	}
	
	/**
	 * Enqueue batch processing of an Image Markup document. If the argument
	 * batch name is <code>null</code>, the configured default is used. Valid
	 * values for the document style mode are 'R' (require), 'U' (use if
	 * available), 'I' (ignore even if available), and 'B' (use configured mode
	 * of batch with argument name). The mode for the configured default batch
	 * must always be 'R'.
	 * @param docId the ID of the document to process
	 * @param batchName the name of the batch to use
	 * @param docStyleMode the way of handling of document style templates
	 * @param userName the user name to use for document updates
	 * @param priority the priority of the processing (0 is normal)
	 */
	public void batchProcessDocument(String docId, String batchName, char docStyleMode, String userName, int priority) {
		if (batchName == null)
			docStyleMode = 'R'; // always require document style for default batch
		else if (this.getBatchForName(batchName) == null)
			throw new IllegalArgumentException("Batch '" + batchName + "' does not exist.");
		else if ((docStyleMode != 'R') && (docStyleMode != 'U') && (docStyleMode != 'I'))
			docStyleMode = 'B';
		this.scheduleProcessing(docId, batchName, docStyleMode, false, userName, priority);
	}
	
	synchronized boolean ensureStyleProvider() {
		if (this.docStyleProvider != null)
			return true;
		try {
			this.docStyleProvider = new ImDocumentStyleProvider(this.docStyleListUrl, this.docStyleFolder, ".docStyle");
			this.docStyleProvider.init();
		}
		catch (IOException ioe) {
			this.logError(ioe);
			this.logError("Cannot work without document style templates, URL invalid or broken");
			return false;
		}
		return true;
	}
	
	private void scheduleProcessing(String docId, String batchOrImtName, char docStyleMode, boolean verbose, String userName, int priority) {
		String[] args = {
			((batchOrImtName == null) ? "" : batchOrImtName),
			("" + docStyleMode),
			(verbose ? "V" : "F"),
			((userName == null) ? "" : userName)
		};
		this.documentProcessor.enqueueDataAction(docId, args, priority);
	}
	
	private static final String PROCESS_DOCUMENT_COMMAND = "process";
	private static final String PROCESS_STATUS_COMMAND = "procStatus";
	private static final String PROCESS_THREADS_COMMAND = "procThreads";
	private static final String PROCESS_THREAD_GROUPS_COMMAND = "procThreadGroups";
	private static final String PROCESS_STACK_COMMAND = "procStack";
	private static final String PROCESS_WAKE_COMMAND = "procWake";
	private static final String PROCESS_KILL_COMMAND = "procKill";
	
	private static final String UPDATE_CONFIG_COMMAND = "updateConfig";
	
	private static final String LIST_TOOLS_COMMAND = "listTools";
	private static final String LIST_BATCHES_COMMAND = "listBatches";
	private static final String RELOAD_BATCHES_COMMAND = "reloadBatches";
	private static final String CHECK_BATCH_COMMAND = "checkBatch";
	private static final String CHECK_BATCHES_COMMAND = "checkBatches";
	private static final String QUEUE_SIZE_COMMAND = "queueSize";
	
	/* TODO create console action factory for slave owners:
	 * - provide slave interface via closure interface ...
	 * - ... taking slave job name as argument
	 * ==> works with multiple slaves per owner
	 * - produce all monitoring and control functions currently in IMP and DPR ...
	 * - ... maybe with custom action name mapping (as envisioned for AEP and IKS)
	 * ==> best but this in master, not in owners
	 */
	
	/*
	 * (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(this.documentProcessor.getActions()));
		ComponentAction ca;
		
		//	sort out generic scheduling actions (we need to validate and adjust parameters)
		for (int a = 0; a < cal.size(); a++) {
			ca = ((ComponentAction) cal.get(a));
			if ("scheduleAction".equals(ca.getActionCommand()))
				cal.remove(a--);
			else if ("enqueueAction".equals(ca.getActionCommand()))
				cal.remove(a--);
		}
		
		//	schedule processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_DOCUMENT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_DOCUMENT_COMMAND + " <documentId> <toolOrBatchName> <documentStyleMode> <verbose> <userName>",
						"Schedule a document for batch processing:",
						"- <documentId>: The ID of the document to process",
						"- <toolOrBatchName>: The name of a single Image Markup tool or configured batch to run (optional, defaults to configured default batch)",
						"- <documentStyleMode>: specify how to handle document style templates (optional, only valid if <toolOrBatchName> also specified)",
						"  - '-dsr': require presence of document style template (default for IM tools, implied for default batch)",
						"  - '-dsu': use document style template if available, but run without",
						"  - '-dsi': ignore/block out document style templates altogether",
						"  - '-dsb': use mode specified in configured batch (default if <toolOrBatchName> is valid batch name)",
						"- <verbose>: Set to '-v' to transmit full output (optional, only valid if <toolOrBatchName> specified as well)",
						"- <userName>: The name of the user to attribute processing to (optional, to override configured default)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify at least the document ID.");
				else if (arguments.length < 6) {
					String batchOrImtName = null;
					char docStyleMode = ' ';
					boolean verbose = false;
					String userName = null;
					for (int a = 1; a < arguments.length; a++) {
						if ("-dsi".equals(arguments[a]))
							docStyleMode = 'I';
						else if ("-dsu".equals(arguments[a]))
							docStyleMode = 'U';
						else if ("-dsr".equals(arguments[a]))
							docStyleMode = 'R';
						else if ("-dsb".equals(arguments[a]))
							docStyleMode = 'B';
						else if ("-v".equals(arguments[a]))
							verbose = true;
						else if (batchOrImtName != null)
							userName = arguments[a];
						else batchOrImtName = arguments[a];
					}
					if (batchOrImtName == null)
						docStyleMode = 'R'; // always require document style for default batch
					else if (docStyleMode == ' ')
						docStyleMode = ((getBatchForName(batchOrImtName) == null) ? 'R' : 'B');
					if ((docStyleMode != 'R') || ensureStyleProvider())
						scheduleProcessing(arguments[0], batchOrImtName, docStyleMode, verbose, userName, 0);
					else {
						this.reportError(" Cannot process documents without style templates, and provider not given.");
						this.reportError(" Use '-dsX' parameters to alter handling of document style templates.");
					}
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID, markup tool name, document style mode, verbosity, and user name as the only arguments.");
			}
		};
		cal.add(ca);
		
		//	schedule a GGI configuration update with next batch run
		if (this.maxParallelBatchRuns > 1) {
			ca = new ComponentActionConsole() {
				public String getActionCommand() {
					return UPDATE_CONFIG_COMMAND;
				}
				public String[] getExplanation() {
					String[] explanation = {
							UPDATE_CONFIG_COMMAND,
							"Schedule a GGI configuration update with next batch run"
						};
					return explanation;
				}
				public void performActionConsole(String[] arguments) {
					if (arguments.length == 0)
						scheduleGgiConfigUpdate();
					else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
				}
			};
			cal.add(ca);
		}
		
		//	check processing status of a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_STATUS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_STATUS_COMMAND,
						"Show the status of a document that is processing"
						//	TODO update explanation with <batchNumber> argument
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if ((maxParallelBatchRuns == 1) ? (arguments.length != 0) : (arguments.length > 1)) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify " + ((maxParallelBatchRuns == 1) ? "no arguments" : "at most the batch number as the only argument") + ".");
					return;
				}
//				if (processingStart == -1) {
//					this.reportResult("There is no document processing at the moment");
//					return;
//				}
//				long time = System.currentTimeMillis();
//				this.reportResult("Processing document " + processingDocId + " (started " + (time - processingStart) + "ms ago)");
//				if (processingBatchName != null)
//					this.reportResult(" - batch is " + processingBatchName);
//				this.reportResult(" - current processor is " + processorName + " (since " + (time - processorStart) + "ms, at " + processingProgress + "%)");
//				this.reportResult(" - current step is " + processingStep + " (since " + (time - processingStepStart) + "ms)");
//				this.reportResult(" - current info is " + processingInfo + " (since " + (time - processingInfoStart) + "ms)");
				if (runningBatchesByNumber.isEmpty()) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				long time = System.currentTimeMillis();
				if (arguments.length == 1) {
					BatchRun br = ((BatchRun) runningBatchesByNumber.get(arguments[1]));
					if (br == null)
						this.reportError(" Invalid batch number '" + arguments[1] + "'.");
					else this.showBatchStatus(br, time);
				}
				else {
					ArrayList runningBatches = new ArrayList(runningBatchesByNumber.values());
					for (int b = 0; b < runningBatches.size(); b++)
						this.showBatchStatus(((BatchRun) runningBatches.get(b)), time);
				}
			}
			private void showBatchStatus(BatchRun br, long time) {
				this.reportResult(((maxParallelBatchRuns == 1) ? "Processing" : (br.name + ": processing")) + " document " + br.processingDocId + " (started " + (time - br.processingStart) + "ms ago)");
				if (br.processingBatchName != null)
					this.reportResult(" - batch is " + br.processingBatchName);
				this.reportResult(" - current processor is " + br.processorName + " (since " + (time - br.processorStart) + "ms, at " + br.processingProgress + "%)");
				this.reportResult(" - current step is " + br.processingStep + " (since " + (time - br.processingStepStart) + "ms)");
				this.reportResult(" - current info is " + br.processingInfo + " (since " + (time - br.processingInfoStart) + "ms)");
			}
		};
		cal.add(ca);
		
		//	list the threads of a batch processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_THREADS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_THREADS_COMMAND,
						"Show the threads of the batch processing a document"
						//	TODO update explanation with <batchNumber> argument
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if ((maxParallelBatchRuns == 1) ? (arguments.length != 0) : (arguments.length != 1)) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify " + ((maxParallelBatchRuns == 1) ? "no arguments" : "the batch number as the only argument") + ".");
					return;
				}
//				if (processingStart == -1) {
//					this.reportResult("There is no document processing at the moment");
//					return;
//				}
//				batchInterface.setReportTo(this);
//				batchInterface.listThreads();
//				batchInterface.setReportTo(null);
				if (runningBatchesByNumber.isEmpty()) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				BatchRun br = ((BatchRun) ((maxParallelBatchRuns == 1) ? runningBatchesByNumber.values().iterator().next() : runningBatchesByNumber.get(arguments[0])));
				if (br == null) {
					this.reportError("Invalid batch number '" + arguments[0] + "'");
					return;
				}
				br.batchInterface.setReportTo(this);
				br.batchInterface.listThreads();
				br.batchInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	list the thread groups of a batch processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_THREAD_GROUPS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_THREAD_GROUPS_COMMAND,
						"Show the thread groups of the batch processing a document"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if ((maxParallelBatchRuns == 1) ? (arguments.length != 0) : (arguments.length != 1)) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify " + ((maxParallelBatchRuns == 1) ? "no arguments" : "the batch number as the only argument") + ".");
					return;
				}
//				if (processingStart == -1) {
//					this.reportResult("There is no document processing at the moment");
//					return;
//				}
//				batchInterface.setReportTo(this);
//				batchInterface.listThreadGroups();
//				batchInterface.setReportTo(null);
				if (runningBatchesByNumber.isEmpty()) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				BatchRun br = ((BatchRun) ((maxParallelBatchRuns == 1) ? runningBatchesByNumber.values().iterator().next() : runningBatchesByNumber.get(arguments[0])));
				if (br == null) {
					this.reportError("Invalid batch number '" + arguments[0] + "'");
					return;
				}
				br.batchInterface.setReportTo(this);
				br.batchInterface.listThreadGroups();
				br.batchInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	check the stack of a batch processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_STACK_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_STACK_COMMAND + " <threadName>",
						"Show the stack trace of the batch processing a document:",
						"- <threadName>: The name of the thread whose stack to show (optional, omitting targets main thread)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if ((maxParallelBatchRuns == 1) ? (arguments.length > 1) : ((arguments.length == 0) || (arguments.length > 2))) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify " + ((maxParallelBatchRuns == 1) ? "at most " : "the batch number and optionally ") + "the name of the target thread.");
					return;
				}
//				if (processingStart == -1) {
//					this.reportResult("There is no document processing at the moment");
//					return;
//				}
//				batchInterface.setReportTo(this);
//				batchInterface.printThreadStack((arguments.length == 0) ? null : arguments[0]);
//				batchInterface.setReportTo(null);
				if (runningBatchesByNumber.isEmpty()) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				BatchRun br;
				String threadName;
				if (maxParallelBatchRuns == 1) {
					br = ((BatchRun) runningBatchesByNumber.values().iterator().next());
					threadName = ((arguments.length == 0) ? null : arguments[0]);
				}
				else {
					br = ((BatchRun) runningBatchesByNumber.get(arguments[0]));
					if (br == null) {
						this.reportError("Invalid batch number '" + arguments[0] + "'");
						return;
					}
					threadName = ((arguments.length == 1) ? null : arguments[1]);
				}
				br.batchInterface.setReportTo(this);
				br.batchInterface.printThreadStack(threadName);
				br.batchInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	wake a batch processing a document, or a thread therein
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_WAKE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_WAKE_COMMAND + " <threadName>",
						"Wake the batch processing a document, or a thread therein:",
						"- <threadName>: The name of the thread to wake (optional, omitting targets main thread)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if ((maxParallelBatchRuns == 1) ? (arguments.length > 1) : ((arguments.length == 0) || (arguments.length > 2))) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify " + ((maxParallelBatchRuns == 1) ? "at most " : "the batch number and optionally ") + "the name of the target thread.");
					return;
				}
//				if (processingStart == -1) {
//					this.reportResult("There is no document processing at the moment");
//					return;
//				}
//				batchInterface.setReportTo(this);
//				batchInterface.wakeThread((arguments.length == 0) ? null : arguments[0]);
//				batchInterface.setReportTo(null);
				if (runningBatchesByNumber.isEmpty()) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				BatchRun br;
				String threadName;
				if (maxParallelBatchRuns == 1) {
					br = ((BatchRun) runningBatchesByNumber.values().iterator().next());
					threadName = ((arguments.length == 0) ? null : arguments[0]);
				}
				else {
					br = ((BatchRun) runningBatchesByNumber.get(arguments[0]));
					if (br == null) {
						this.reportError("Invalid batch number '" + arguments[0] + "'");
						return;
					}
					threadName = ((arguments.length == 1) ? null : arguments[1]);
				}
				br.batchInterface.setReportTo(this);
				br.batchInterface.wakeThread(threadName);
				br.batchInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	kill a batch processing a document, or a thread therein
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_KILL_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_KILL_COMMAND + " <threadName>",
						"Kill the batch processing a document, or a thread therein:",
						"- <threadName>: The name of the thread to kill (optional, omitting targets main thread)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if ((maxParallelBatchRuns == 1) ? (arguments.length > 1) : ((arguments.length == 0) || (arguments.length > 2))) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify " + ((maxParallelBatchRuns == 1) ? "at most " : "the batch number and optionally ") + "the name of the target thread.");
					return;
				}
//				if (processingStart == -1) {
//					this.reportResult("There is no document processing at the moment");
//					return;
//				}
//				batchInterface.setReportTo(this);
//				batchInterface.killThread((arguments.length == 0) ? null : arguments[0]);
//				batchInterface.setReportTo(null);
				if (runningBatchesByNumber.isEmpty()) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				BatchRun br;
				String threadName;
				if (maxParallelBatchRuns == 1) {
					br = ((BatchRun) runningBatchesByNumber.values().iterator().next());
					threadName = ((arguments.length == 0) ? null : arguments[0]);
				}
				else {
					br = ((BatchRun) runningBatchesByNumber.get(arguments[0]));
					if (br == null) {
						this.reportError("Invalid batch number '" + arguments[0] + "'");
						return;
					}
					threadName = ((arguments.length == 1) ? null : arguments[1]);
				}
				br.batchInterface.setReportTo(this);
				br.batchInterface.killThread(threadName);
				br.batchInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	list available markup tools
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_TOOLS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_TOOLS_COMMAND + " <reload> <providers>",
						"List all available markup tools:",
						"- <reload>: Set to '-r' to force reloading the list (optional)",
						"- <providers>: Set to '-p' to only show tool providers (optional)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				boolean forceReload = false;
				boolean providersOnly = false;
				String providerPrefix = null;
				for (int a = 0; a < arguments.length; a++) {
					if ("-r".equals(arguments[a]))
						forceReload = true;
					else if ("-p".equals(arguments[a]))
						providersOnly = true;
					else if (providerPrefix == null)
						providerPrefix = arguments[a];
					else providerPrefix = (providerPrefix + " " + arguments[a]);
				}
				listMarkupTools(forceReload, providersOnly, providerPrefix, this);
			}
		};
		cal.add(ca);
		
		//	list available batches
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_BATCHES_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_BATCHES_COMMAND,
						"List all available batches.",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				ArrayList batchNames = new ArrayList(batchesByName.keySet());
				this.reportResult("There are curently " + batchNames.size() + " batches available:");
				for (int b = 0; b < batchNames.size(); b++) {
					ImpBatch batch = getBatchForName((String) batchNames.get(b));
					if (batch.label == null)
						this.reportResult("Batch '" + batch.name + "':");
					else this.reportResult("Batch '" + batch.label + "' ('" + batch.name + "'):");
					batch.printDescription(this);
				}
			}
		};
		cal.add(ca);
		
		//	reload batches
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return RELOAD_BATCHES_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						RELOAD_BATCHES_COMMAND,
						"Reload batch definitions.",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					loadBatches(this);
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	check a batch for validity
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return CHECK_BATCH_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						CHECK_BATCH_COMMAND + " <batchName> <reload>",
						"Check a batch, namely if all the IM tools are available:",
						"- <batchName>: the name of the batch to check",
						"- <reload>: Set to '-r' to force reloading the list of IM tools (optional)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if ((arguments.length != 1) && (arguments.length != 2)) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the batch name and reload flag only.");
					return;
				}
				ImpBatch batch = getBatchForName(arguments[0]);
				if (batch == null) {
					this.reportError(" Invalid batch name '" + arguments[0] + "', use " + LIST_BATCHES_COMMAND + " to list available batches.");
					return;
				}
				boolean forceReload = false;
				if (arguments.length < 2) {}
				else if ("-r".equals(arguments[1]))
					forceReload = true;
				else {
					this.reportError(" Invalid value '" + arguments[1] + "' for <reload> argument, specify '-r' or nothing.");
					return;
				}
				checkBatch(batch, forceReload, this);
			}
		};
		cal.add(ca);
		
		//	check all batches for validity
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return CHECK_BATCHES_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						CHECK_BATCHES_COMMAND + " <reload>",
						"Check all batches, namely if all the IM tools are available:",
						"- <reload>: Set to '-r' to force reloading the lists of IM tools (optional)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if ((arguments.length != 0) && (arguments.length != 1)) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', at most the reload flag.");
					return;
				}
				HashSet forceReloadConfigNames = null;
				if (arguments.length < 1) {}
				else if ("-r".equals(arguments[0]))
					forceReloadConfigNames = new HashSet();
				else {
					this.reportError(" Invalid value '" + arguments[0] + "' for <reload> argument, specify '-r' or nothing.");
					return;
				}
				ArrayList batchNames = new ArrayList(batchesByName.keySet());
				this.reportResult("Checking " + batchNames.size() + " batches ...");
				for (int b = 0; b < batchNames.size(); b++) {
					ImpBatch batch = getBatchForName((String) batchNames.get(b));
					String batchConfigName = ((batch.ggiConfigName == null) ? ggiConfigName : batch.ggiConfigName);
					checkBatch(batch, ((forceReloadConfigNames != null) && forceReloadConfigNames.add(batchConfigName)), this);
				}
			}
		};
		cal.add(ca);
		
		//	check processing queue
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return QUEUE_SIZE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						QUEUE_SIZE_COMMAND,
						"Show current size of processing queue, i.e., number of documents waiting to be processed."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					this.reportResult(documentProcessor.getDataActionsPending() + " documents waiting to be processed.");
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	send batch descriptors
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_BATCH_DESCRIPTORS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Not logged in");
					output.newLine();
					return;
				}
				
				//	indicate results coming
				output.write(GET_BATCH_DESCRIPTORS);
				output.newLine();
				
				//	write batch descriptors
				ArrayList batchNames = new ArrayList(batchesByName.keySet());
				output.write("[");
				int batchesWritten = 0;
				for (int b = 0; b < batchNames.size(); b++) {
					ImpBatch batch = getBatchForName((String) batchNames.get(b));
					if (batch == null) // TODO filter by user name and assigned permissions as well
						continue;
					ImpBatchDescriptor ibd = batch.getDescriptor();
					if (ibd == null)
						continue;
					if (batchesWritten != 0)
						output.write(",");
					ibd.writeJson(output);
					batchesWritten++;
				}
				output.write("]");
				output.newLine();
			}
		};
		cal.add(ca);
		
		//	send batch description
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_BATCH_DESCRIPTION;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Not logged in");
					output.newLine();
					return;
				}
				
				//	get and check parameters
				String batchName = input.readLine();
				ImpBatch batch = getBatchForName(batchName);
				if (batch == null) /* TODO also check access to particular batch */ {
					output.write("Invalid batch name '" + batchName + "'");
					output.newLine();
					return;
				}
				
				//	get HTML descriptor
				String hd = batch.getHtmlDescriptor();
				if (hd == null) {
					output.write("Descriptor text unavailable for batch '" + batchName + "'");
					output.newLine();
					return;
				}
				
				//	send result
				output.write(GET_BATCH_DESCRIPTION);
				output.newLine();
				output.write(hd);
				output.newLine();
			}
		};
		cal.add(ca);
		
		//	receive batch scheduling
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return SCHEDULE_BATCH_PROCESSING;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Not logged in");
					output.newLine();
					return;
				}
				
				//	TODO check permission to schedule batch processing
				
				//	get and check parameters
				String docId = input.readLine();
				if (!ims.isDocumentAvailable(docId)) /* TODO also check access to particular document (DAA) */  {
					output.write("Invalid document ID '" + docId + "'");
					output.newLine();
					return;
				}
				String batchName = input.readLine();
				if (getBatchForName(batchName) == null) /* TODO also check access to particular batch */ {
					output.write("Invalid batch name '" + batchName + "'");
					output.newLine();
					return;
				}
				
				//	schedule processing
				batchProcessDocument(docId, batchName, uaa.getUserNameForSession(sessionId));
				
				//	indicate success
				output.write(SCHEDULE_BATCH_PROCESSING);
				output.newLine();
			}
		};
		cal.add(ca);
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	private String[][] imToolList = null;
	private void listMarkupTools(boolean forceReload, boolean providersOnly, String providerPrefix, ComponentActionConsole cac) {
		
		//	load markup tools list on demand
		if ((this.imToolList == null) || forceReload) try {
			this.imToolList = this.loadMarkupToolList(cac, this.ggiConfigName);
		}
		catch (IOException ioe) {
			cac.reportError("Could not load markup tool list: " + ioe.getMessage());
			cac.reportError(ioe);
			return;
		}
		
		//	print out (filtered) document processor list
		for (int m = 0; m < this.imToolList.length; m++) {
			if ((providerPrefix != null) && !this.imToolList[m][0].startsWith(providerPrefix))
				continue;
			cac.reportResult(this.imToolList[m][0]);
			if (providersOnly)
				continue;
			for (int p = 1; p < this.imToolList[m].length; p++)
				cac.reportResult(" - " + this.imToolList[m][p]);
		}
	}
	
	private Map imToolListsByConfigName = Collections.synchronizedMap(new TreeMap(String.CASE_INSENSITIVE_ORDER));
	private TreeSet getMarkupTools(String ggiConfigName, boolean forceReload, ComponentActionConsole cac) {
		
		//	check cache
		TreeSet imTools = (forceReload ? null : ((TreeSet) this.imToolListsByConfigName.get(ggiConfigName)));
		
		//	load markup tool list on demand
		if (imTools == null) try {
			String[][] imToolList = this.loadMarkupToolList(cac, ggiConfigName);
			
			//	flatten out list from by-provider arrangement
			imTools = new TreeSet(String.CASE_INSENSITIVE_ORDER);
			for (int m = 0; m < imToolList.length; m++) {
				for (int p = 1; p < imToolList[m].length; p++)
					imTools.add(imToolList[m][p]);
			}
			
			//	cache list
			this.imToolListsByConfigName.put(ggiConfigName, imTools);
		}
		catch (IOException ioe) {
			cac.reportError("Could not load markup tool list: " + ioe.getMessage());
			cac.reportError(ioe);
			return null;
		}
		
		//	finally ...
		return imTools;
		
	}
	
	private String[][] loadMarkupToolList(final ComponentActionConsole cac, String ggiConfigName) throws IOException {
		cac.reportResult("Loading markup tool list for configuration '" + ggiConfigName + "' ...");
		
		//	assemble slave job
		ImpSlaveJob isj = new ImpSlaveJob(Gamta.getAnnotationID(), ggiConfigName, "LISTTOOLS", false); // TODO use persistent UUID
		isj.setMaxMemory(1024);
		isj.setMaxCores(1);
		
		//	start batch processor slave process
		Process imtLister = Runtime.getRuntime().exec(isj.getCommand(null), new String[0], this.workingFolder);
		
		//	collect Image Markup tool listing
		final ArrayList imToolProviderList = new ArrayList();
		SlaveProcessInterface spi = new SlaveProcessInterface(imtLister, ("ImpBatch" + "ListMarkupTools")) {
			private ArrayList imToolList = new ArrayList();
			protected void handleInput(String input) {
				if (input.startsWith("MTP:")) {
					this.finalizeMarkupToolProvider();
					this.imToolList.add(input.substring("MTP:".length()));
				}
				else if (input.startsWith("MT:"))
					this.imToolList.add(input.substring("MT:".length()));
				else cac.reportResult(input);
			}
			protected void handleResult(String result) {
				cac.reportResult(result);
			}
			protected void finalizeSystemOut() {
				this.finalizeMarkupToolProvider();
			}
			protected void handleError(String error, boolean fromSysErr) {
				cac.reportError(error);;
			}
			protected void finalizeSystemErr() {
				this.finalizeMarkupToolProvider();
			}
			private synchronized void finalizeMarkupToolProvider() {
				if (this.imToolList.size() != 0)
					imToolProviderList.add(this.imToolList.toArray(new String[this.imToolList.size()]));
				this.imToolList.clear();
			}
		};
		spi.start();
		
		//	wait for process to finish
		while (true) try {
			imtLister.waitFor();
			break;
		} catch (InterruptedException ie) {}
		
		//	finally ...
		return ((String[][]) imToolProviderList.toArray(new String[imToolProviderList.size()][]));
	}
	
	private ImpBatch getBatchForName(String batchName) {
		if ((batchName == null) || "Default".equals(batchName)) // batch named 'Default' overrides configured default anyway
			return this.defaultBatch;
		else return ((ImpBatch) this.batchesByName.get(batchName));
	}
	
	private void checkBatch(ImpBatch batch, boolean forceReload, ComponentActionConsole cac) {
		cac.reportResult("Checking batch '" + batch.name + "':");
		TreeSet imTools = this.getMarkupTools(((batch.ggiConfigName == null) ? this.ggiConfigName : batch.ggiConfigName), forceReload, cac);
		if (imTools == null) {
			cac.reportError(" ==> could not load IM tools for " + ((batch.ggiConfigName == null) ? "default configuration" : ("configuration '" + batch.ggiConfigName + "'")));
			return;
		}
		cac.reportResult(" - IM tools for " + ((batch.ggiConfigName == null) ? "default configuration" : ("configuration '" + batch.ggiConfigName + "'")) + " loaded");
		int invalidImToolCount = 0;
		for (int t = 0; t < batch.imTools.length; t++) {
			if (imTools.contains(batch.imTools[t]))
				cac.reportResult(" - IM tool '" + batch.imTools[t]+ "' ==> valid");
			else {
				invalidImToolCount++;
				cac.reportError(" - IM tool '" + batch.imTools[t] + "' ==> invalid");
			}
		}
		if (invalidImToolCount == 0)
			cac.reportResult(" ==> batch valid");
		else {
			cac.reportError(" ==> batch invalid, " + invalidImToolCount + " out of " + batch.imTools.length + " IM tools invalid (see above for details)");
			cac.reportError("     IM tool list is " + imTools);
		}
	}
	
	private boolean updateGgiConfig = false;
	private Thread ggiConfigUpdater = null;
	private HashSet ggiConfigUpdateWaiters = new HashSet();
	private Object ggiConfigUpdateLock = new Object();
	void scheduleGgiConfigUpdate() {
		this.updateGgiConfig = true;
	}
	
	void ggiConfigUpdateFinished() {
		
		//	mark update as done
		synchronized (this.ggiConfigUpdateLock) {
			this.updateGgiConfig = false;
			this.ggiConfigUpdater = null;
		}
		
		//	release other batches (need to release monitor between rounds so others can acquire it and actually go out of waiting loop)
		while (this.ggiConfigUpdateWaiters.size() != 0) {
			synchronized (this.ggiConfigUpdateLock) {
				this.ggiConfigUpdateLock.notify();
			}
			Thread.yield();
		}
	}
	
	private ArrayList batchRuns = new ArrayList();
	private ThreadLocal batchRunsByHandler = new ThreadLocal();
	private Map runningBatchesByNumber = Collections.synchronizedMap(new TreeMap());
	void processDocument(String docId, String imtName, char docStyleMode, boolean verbose, String userName) throws IOException {
		
		//	create batch run on first available number
		BatchRun batch = ((BatchRun) this.batchRunsByHandler.get());
		if (batch == null)
			synchronized (this.batchRuns) {
				for (int b = 0; b < this.batchRuns.size(); b++) {
					BatchRun br = ((BatchRun) this.batchRuns.get(b));
					if ((br != null) && br.handler.isAlive())
						continue;
					br = new BatchRun((this.maxParallelBatchRuns == 1) ? "" : ("" + (b+1)));
					this.batchRuns.set(b, br);
					this.batchRunsByHandler.set(br);
					batch = br;
				}
				if (batch == null) {
					batch = new BatchRun((this.maxParallelBatchRuns == 1) ? "" : ("" + (this.batchRuns.size() + 1)));
					this.batchRuns.add(batch);
					this.batchRunsByHandler.set(batch);
				}
			}
		
		//	handle config updates, need to run in solo mode
		boolean doUpdateGgiConfig = this.updateGgiConfig;
		if (doUpdateGgiConfig) {
			synchronized (this.ggiConfigUpdateLock) {
				
				//	wait for all jobs to finish (cannot update configuration with other job using it)
				//	be careful about late arrivers, though (worker might come in from sleeping, etc.)
				//	==> only wait until updating worker selected, right away proceeding to waiting below otherwise
				while (this.updateGgiConfig && (this.ggiConfigUpdater == null) && (this.runningBatchesByNumber.size() != 0)) try {
					this.ggiConfigUpdateLock.wait(1000);
				} catch (InterruptedException ie) {}
				
				//	first thread to emerge from waiting loop gets to do update (if it's still pending might have gone to false while we were waiting on lock)
				if (this.updateGgiConfig) {
					Thread ct = Thread.currentThread();
					if (this.ggiConfigUpdater == null)
						this.ggiConfigUpdater = ct;
					
					//	while other wait until update is done
					else try {
						doUpdateGgiConfig = false; // need to remember not to do update below
						this.ggiConfigUpdateWaiters.add(ct);
						while (this.ggiConfigUpdater != null) try {
							this.ggiConfigUpdateLock.wait();
						} catch (InterruptedException ie) {}
					}
					finally {
						this.ggiConfigUpdateWaiters.remove(ct);
					}
				}
				else doUpdateGgiConfig = false; // need to remember not to do update below (even though the flag was set when we checked initially)
			}
		}
		
		//	release current batch tasked with config update
		try {
			this.runningBatchesByNumber.put(batch.number, batch);
			batch.processDocument(docId, imtName, doUpdateGgiConfig, docStyleMode, verbose, userName);
		}
		finally {
			this.runningBatchesByNumber.remove(batch.number);
		}
	}
	
	private class BatchRun {
		final Thread handler;
		final String number;
		final String name;
		BatchRun(String number) {
			this.handler = Thread.currentThread();
			this.number = number;
			this.name = (getLetterCode() + number);
		}
		
		ImpSlaveProcessInterface batchInterface = null;
		String processingDocId = null;
		String processingBatchName = null;
		long processingStart = -1;
		String processorName = null;
		long processorStart = -1;
		String processingStep = null;
		long processingStepStart = -1;
		String processingInfo = null;
		long processingInfoStart = -1;
		int processingProgress = -1;
		void processDocument(String docId, String imtName, boolean updateGgiConfig, char docStyleMode, boolean verbose, String userName) throws IOException {
			
			//	check out document as data, process it, and clean up
			ImsDocumentData docData = null;
			try {
				this.processingDocId = docId;
				this.processingStart = System.currentTimeMillis();
				docData = ims.checkoutDocumentAsData(batchUserName, docId);
				this.processDocument(docId, docData, imtName, updateGgiConfig, docStyleMode, verbose, userName);
			}
			catch (IOException ioe) {
				ims.releaseDocument(batchUserName, docId); // need to release here in case respective code not reached in processing
				throw ioe;
			}
			finally {
				if (docData != null)
					docData.dispose();
				this.batchInterface = null;
				this.processingDocId = null;
				this.processingBatchName = null;
				this.processingStart = -1;
				this.processorName = null;
				this.processorStart = -1;
				this.processingStep = null;
				this.processingStepStart = -1;
				this.processingInfo = null;
				this.processingInfoStart = -1;
				this.processingProgress = -1;
			}
		}
		
		private void processDocument(String docId, ImsDocumentData docData, String batchOrImtName, final boolean updateGgiConfig, char docStyleMode, boolean verbose, String userName) throws IOException {
			
			//	check document style mode
			if (batchOrImtName == null)
				docStyleMode = 'R';
			else if ((docStyleMode != 'I') && (docStyleMode != 'U') && (docStyleMode != 'B'))
				docStyleMode = 'R';
			
			//	get configured batch and read document style mode and user name
			ImpBatch batch = getBatchForName(batchOrImtName);
			if (docStyleMode == 'B')
				docStyleMode = ((batch == null) ? 'R' : batch.docStyleMode);
			if ((userName == null) && (batch != null))
				userName = batch.userName;
			
			//	make sure we have a document style provider
			if (!ensureStyleProvider() && (docStyleMode == 'R'))
				throw new IOException("Cannot work without document style templates.");
			
			//	create document cache folder
			File cacheFolder = new File(GoldenGateIMP.this.cacheFolder, ("cache-" + docId));
			cacheFolder.mkdirs();
			
			//	create document output folder
			File docFolder = new File(cacheFolder, ("doc-" + docId));
			docFolder.mkdirs();
			
			//	copy document to cache folder (only non-binary entries)
			final CacheImDocumentData cacheDocData = new CacheImDocumentData(docFolder, docData);
			
			//	get maximum number of cores to use by batch run
			int maxSlaveCores = GoldenGateIMP.this.maxSlaveCores;
			if (maxSlaveCores < 1)
				maxSlaveCores = 65536;
			if ((maxSlaveCores * 4) > Runtime.getRuntime().availableProcessors())
				maxSlaveCores = (Runtime.getRuntime().availableProcessors() / 4);
			
			//	assemble slave job
			String isjId = Gamta.getAnnotationID(); // TODO use persistent UUID
			ImpSlaveJob isj = ((batch == null) ? new ImpSlaveJob(isjId, batchOrImtName, updateGgiConfig) : new ImpSlaveJob(isjId, batch, updateGgiConfig));
			isj.setDataPath(docFolder.getAbsolutePath());
			isj.setMaxCores(maxSlaveCores);
			if (verbose && (batchOrImtName != null))
				isj.setProperty(ImpSlaveJob.VERBOSE_PARAMETER);
			if (batchOrImtName == null)
				isj.setProperty("DSMODE", "R");
			else if (docStyleMode == 'I')
				isj.setProperty("DSMODE", "I");
			else if (docStyleMode == 'U')
				isj.setProperty("DSMODE", "U");
			else isj.setProperty("DSMODE", "R");
			
			//	start batch processor slave process
			Process processor = Runtime.getRuntime().exec(isj.getCommand(cacheFolder.getAbsolutePath()), new String[0], workingFolder);
			this.processingBatchName = ((batch == null) ? null : batch.name);
			
			//	get output channel
			this.batchInterface = new ImpSlaveProcessInterface(processor, ("ImpBatch" + docId), cacheDocData, docId);
			
			//	TODOne catch request for further document entries on input stream ...
			//	TODOne ... and move them to cache on demand ...
			//	TODOne ... sending 'ready' message back through process output stream
			//	TODO test command: process 6229FF8AD22B0336DF54FFD7FFD3FF8E
			
			//	TODOne keep slave process responsive to commands like getting stack trace
			this.batchInterface.setProgressMonitor(new ProgressMonitor() {
				private boolean updatingGgiConfig = updateGgiConfig;
				public void setStep(String step) {
					logInfo(name + ": " + step);
					processingStep = step;
					processingStepStart = System.currentTimeMillis();
					if (this.updatingGgiConfig)
						this.notifyConfigUpdateFinished();
				}
				public void setInfo(String info) {
					logDebug(name + ": " + info);
					processingInfo = info;
					processingInfoStart = System.currentTimeMillis();
					if (this.updatingGgiConfig)
						this.notifyConfigUpdateFinished();
				}
				private int baseProgress = 0;
				private int maxProgress = 0;
				public void setBaseProgress(int baseProgress) {
					this.baseProgress = baseProgress;
					if (this.updatingGgiConfig)
						this.notifyConfigUpdateFinished();
				}
				public void setMaxProgress(int maxProgress) {
					this.maxProgress = maxProgress;
					if (this.updatingGgiConfig)
						this.notifyConfigUpdateFinished();
				}
				public void setProgress(int progress) {
					processingProgress = (this.baseProgress + (((this.maxProgress - this.baseProgress) * progress) / 100));
					if (this.updatingGgiConfig)
						this.notifyConfigUpdateFinished();
				}
				private void notifyConfigUpdateFinished() {
					ggiConfigUpdateFinished();
					this.updatingGgiConfig = false;
				}
			});
			this.batchInterface.start();
			
			//	wait for batch process to finish
			while (true) try {
				processor.waitFor();
				break;
			} catch (InterruptedException ie) {}
			
			//	copy back modified entries
			FolderImDocumentData inDocData = new FolderImDocumentData(docFolder, null);
			ImDocumentEntry[] inDocEntries = inDocData.getEntries();
			boolean docModified = false;
			logInfo(name + ": document " + docId + " processed, copying back entries:");
			for (int e = 0; e < inDocEntries.length; e++) {
				ImDocumentEntry docEntry = docData.getEntry(inDocEntries[e].name);
				if ((docEntry != null) && docEntry.dataHash.equals(inDocEntries[e].dataHash)) {
					logInfo(name + ": - " + inDocEntries[e].name + " ==> unmodified");
					continue; // this entry exists and didn't change
				}
				InputStream inDocEntryIn = new BufferedInputStream(inDocData.getInputStream(inDocEntries[e]));
				OutputStream docEntryOut = new BufferedOutputStream(docData.getOutputStream(inDocEntries[e]));
				byte[] buffer = new byte[1024];
				for (int r; (r = inDocEntryIn.read(buffer, 0, buffer.length)) != -1;)
					docEntryOut.write(buffer, 0, r);
				docEntryOut.flush();
				docEntryOut.close();
				inDocEntryIn.close();
				docModified = true;
				logInfo(name + ": - " + inDocEntries[e].name + " ==> modified");
			}
			
			//	update and release document in IMS
			if (docModified)
				ims.updateDocumentFromData(((userName == null) ? batchUserName : userName), batchUserName, docData, new EventLogger() {
					public void writeLog(String logEntry) {
						logInfo(logEntry);
					}
				});
			ims.releaseDocument(batchUserName, docId);
			
			//	clean up cache and document data
			cleanupFile(cacheFolder);
			cleanupFile(docFolder);
		}
		
		private class ImpSlaveProcessInterface extends SlaveProcessInterface {
			private ComponentActionConsole reportTo = null;
			private CacheImDocumentData cacheDocData;
			private String docId;
			ImpSlaveProcessInterface(Process slave, String slaveName, CacheImDocumentData cacheDocData, String docId) {
				super(slave, slaveName);
				this.cacheDocData = cacheDocData;
				this.docId = docId;
			}
			void setReportTo(ComponentActionConsole reportTo) {
				this.reportTo = reportTo;
			}
			protected void handleInput(String input) {
				if (input.startsWith("PR:")) {
					input = input.substring("PR:".length());
					logInfo(name + ": running Image Markup Tool '" + input + "'");
					processorName = input;
					processorStart = System.currentTimeMillis();
				}
				else if (input.startsWith("DER:")) {
					String docEntryName = input.substring("DER:".length());
					ImDocumentEntry docEntry = this.cacheDocData.getEntry(docEntryName);
					if (docEntry == null)
						this.sendOutput("DEN:" + docEntryName);
					else try {
						this.cacheDocData.cacheDocEntry(docEntry);
						this.sendOutput("DEC:" + docEntryName);
					}
					catch (IOException ioe) {
						this.sendOutput("DEE:" + ioe.getMessage());
					}
				}
				else logInfo(name + ": " + input);
			}
			protected void handleResult(String result) {
				ComponentActionConsole cac = this.reportTo;
				if (cac == null)
					logInfo(name + ": " + result);
				else cac.reportResult(result);
			}
			private ArrayList outStackTrace = new ArrayList();
			protected void handleStackTrace(String stackTraceLine) {
				if (stackTraceLine.trim().length() == 0)
					this.reportError(this.outStackTrace);
				else {
					this.outStackTrace.add(stackTraceLine);
					super.handleStackTrace(stackTraceLine);
				}
			}
			protected void finalizeSystemOut() {
				this.reportError(this.outStackTrace);
			}
			private ArrayList errStackTrace = new ArrayList();
			protected void handleError(String error, boolean fromSysErr) {
				ComponentActionConsole cac = this.reportTo;
				if (fromSysErr && (cac == null)) {
					if (error.startsWith("CR\t") || error.startsWith("LA\t") || error.startsWith("Stale "))
						return; // TODO remove this once server fixed
					if (error.matches("(Im|Gamta)Document(Root)?Guard\\:.*"))
						return; // TODO remove this once server fixed
					if (error.startsWith("Font 'Free") && error.endsWith("' loaded successfully."))
						return;
				}
				if (cac == null) {
					if (fromSysErr)
						this.errStackTrace.add(error);
					logError(name + ": " + error);
				}
				else cac.reportError(error);
			}
			protected void finalizeSystemErr() {
				this.reportError(this.errStackTrace);
			}
			private void reportError(ArrayList stackTrace) {
				if (stackTrace.size() == 0)
					return;
				String classAndMessge = ((String) stackTrace.get(0));
				String errorClassName;
				String errorMessage;
				if (classAndMessge.indexOf(":") == -1) {
					errorClassName = classAndMessge;
					errorMessage = "";
				}
				else {
					errorClassName = classAndMessge.substring(0, classAndMessge.indexOf(":")).trim();
					errorMessage = classAndMessge.substring(classAndMessge.indexOf(":") + ":".length()).trim();
				}
				String[] errorStackTrace = ((String[]) stackTrace.toArray(new String[this.outStackTrace.size()]));
				stackTrace.clear();
				SlaveErrorRecorder.recordError(getLetterCode(), this.docId, errorClassName, errorMessage, errorStackTrace);
			}
		}
	}
	
//	private void processDocument(String docId, String imtName, char docStyleMode, boolean verbose, String userName) throws IOException {
//		
//		//	check out document as data, process it, and clean up
//		ImsDocumentData docData = null;
//		try {
//			this.processingDocId = docId;
//			this.processingStart = System.currentTimeMillis();
//			docData = this.ims.checkoutDocumentAsData(this.batchUserName, docId);
////			this.processDocument(docId, docData, imtName, waiveStyle, verbose);
//			this.processDocument(docId, docData, imtName, docStyleMode, verbose, userName);
//		}
//		catch (IOException ioe) {
//			this.ims.releaseDocument(this.batchUserName, docId); // need to release here in case respective code not reached in processing
//			throw ioe;
//		}
//		finally {
//			if (docData != null)
//				docData.dispose();
//			this.batchRun = null;
//			this.batchInterface = null;
//			this.processingDocId = null;
//			this.processingBatchName = null;
//			this.processingStart = -1;
//			this.processorName = null;
//			this.processorStart = -1;
//			this.processingStep = null;
//			this.processingStepStart = -1;
//			this.processingInfo = null;
//			this.processingInfoStart = -1;
//			this.processingProgress = -1;
//		}
//	}
//	
//	private void processDocument(String docId, ImsDocumentData docData, String batchOrImtName, char docStyleMode, boolean verbose, String userName) throws IOException {
//		
//		//	check document style mode
//		if (batchOrImtName == null)
//			docStyleMode = 'R';
//		else if ((docStyleMode != 'I') && (docStyleMode != 'U') && (docStyleMode != 'B'))
//			docStyleMode = 'R';
//		
//		//	get configured batch and read document style mode and user name
//		ImpBatch batch = this.getBatchForName(batchOrImtName);
//		if (docStyleMode == 'B')
//			docStyleMode = ((batch == null) ? 'R' : batch.docStyleMode);
//		if ((userName == null) && (batch != null))
//			userName = batch.userName;
//		
//		//	make sure we have a document style provider
////		if (!this.ensureStyleProvider() && !waiveStyle)
//		if (!this.ensureStyleProvider() && (docStyleMode == 'R'))
//			throw new IOException("Cannot work without document style templates.");
//		
//		//	create document cache folder
//		File cacheFolder = new File(this.cacheFolder, ("cache-" + docId));
//		cacheFolder.mkdirs();
//		
//		//	create document output folder
//		File docFolder = new File(this.cacheFolder, ("doc-" + docId));
//		docFolder.mkdirs();
//		
//		//	copy document to cache folder (only non-binary entries)
//		final CacheImDocumentData cacheDocData = new CacheImDocumentData(docFolder, docData);
//		
//		//	get maximum number of cores to use by batch run
//		int maxSlaveCores = this.maxSlaveCores;
//		if (maxSlaveCores < 1)
//			maxSlaveCores = 65536;
//		if ((maxSlaveCores * 4) > Runtime.getRuntime().availableProcessors())
//			maxSlaveCores = (Runtime.getRuntime().availableProcessors() / 4);
//		
//		//	assemble slave job
////		ImpSlaveJob isj = new ImpSlaveJob(Gamta.getAnnotationID(), batchOrImtName); // TODO use persistent UUID
//		String isjId = Gamta.getAnnotationID(); // TODO use persistent UUID
//		ImpSlaveJob isj = ((batch == null) ? new ImpSlaveJob(isjId, batchOrImtName) : new ImpSlaveJob(isjId, batch));
//		isj.setDataPath(docFolder.getAbsolutePath());
//		isj.setMaxCores(maxSlaveCores);
//		if (verbose && (batchOrImtName != null))
//			isj.setProperty(ImpSlaveJob.VERBOSE_PARAMETER);
////		if (waiveStyle && (imtName != null))
////			isj.setProperty("WAIVEDS"); // waive requiring document style only for single IM tool
//		if (batchOrImtName == null)
//			isj.setProperty("DSMODE", "R");
//		else if (docStyleMode == 'I')
//			isj.setProperty("DSMODE", "I");
//		else if (docStyleMode == 'U')
//			isj.setProperty("DSMODE", "U");
//		else isj.setProperty("DSMODE", "R");
//		
//		//	start batch processor slave process
//		this.batchRun = Runtime.getRuntime().exec(isj.getCommand(cacheFolder.getAbsolutePath()), new String[0], this.workingFolder);
//		this.processingBatchName = ((batch == null) ? null : batch.name);
//		
//		//	get output channel
//		this.batchInterface = new ImpSlaveProcessInterface(this.batchRun, ("ImpBatch" + docId), cacheDocData, docId);
//		
//		//	TODOne catch request for further document entries on input stream ...
//		//	TODOne ... and move them to cache on demand ...
//		//	TODOne ... sending 'ready' message back through process output stream
//		//	TODO test command: process 6229FF8AD22B0336DF54FFD7FFD3FF8E
//		
//		//	TODOne keep slave process responsive to commands like getting stack trace
//		this.batchInterface.setProgressMonitor(new ProgressMonitor() {
//			public void setStep(String step) {
//				logInfo("IMP: " + step);
//				processingStep = step;
//				processingStepStart = System.currentTimeMillis();
//			}
//			public void setInfo(String info) {
//				logDebug("IMP: " + info);
//				processingInfo = info;
//				processingInfoStart = System.currentTimeMillis();
//			}
//			private int baseProgress = 0;
//			private int maxProgress = 0;
//			public void setBaseProgress(int baseProgress) {
//				this.baseProgress = baseProgress;
//			}
//			public void setMaxProgress(int maxProgress) {
//				this.maxProgress = maxProgress;
//			}
//			public void setProgress(int progress) {
//				processingProgress = (this.baseProgress + (((this.maxProgress - this.baseProgress) * progress) / 100));
//			}
//		});
//		this.batchInterface.start();
//		
//		//	wait for batch process to finish
//		while (true) try {
//			this.batchRun.waitFor();
//			break;
//		} catch (InterruptedException ie) {}
//		
//		//	copy back modified entries
//		FolderImDocumentData inDocData = new FolderImDocumentData(docFolder, null);
//		ImDocumentEntry[] inDocEntries = inDocData.getEntries();
//		boolean docModified = false;
//		logInfo("Document " + docId + " processed, copying back entries:");
//		for (int e = 0; e < inDocEntries.length; e++) {
//			ImDocumentEntry docEntry = docData.getEntry(inDocEntries[e].name);
//			if ((docEntry != null) && docEntry.dataHash.equals(inDocEntries[e].dataHash)) {
//				logInfo(" - " + inDocEntries[e].name + " ==> unmodified");
//				continue; // this entry exists and didn't change
//			}
//			InputStream inDocEntryIn = new BufferedInputStream(inDocData.getInputStream(inDocEntries[e]));
//			OutputStream docEntryOut = new BufferedOutputStream(docData.getOutputStream(inDocEntries[e]));
//			byte[] buffer = new byte[1024];
//			for (int r; (r = inDocEntryIn.read(buffer, 0, buffer.length)) != -1;)
//				docEntryOut.write(buffer, 0, r);
//			docEntryOut.flush();
//			docEntryOut.close();
//			inDocEntryIn.close();
//			docModified = true;
//			logInfo(" - " + inDocEntries[e].name + " ==> modified");
//		}
//		
//		//	update and release document in IMS
//		if (docModified)
//			this.ims.updateDocumentFromData(((userName == null) ? this.batchUserName : userName), this.batchUserName, docData, new EventLogger() {
//				public void writeLog(String logEntry) {
//					logInfo(logEntry);
//				}
//			});
//		this.ims.releaseDocument(this.batchUserName, docId);
//		
//		//	clean up cache and document data
//		cleanupFile(cacheFolder);
//		cleanupFile(docFolder);
//	}
	
	private static void cleanupFile(File file) {
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (int f = 0; f < files.length; f++)
				cleanupFile(files[f]);
		}
		file.delete();
	}
	
	private class ImpBatch {
		final String name;
		final String ggiConfigName;
		final String[] imTools;
		final char docStyleMode;
		final String userName;
		ImpBatch(String name, String ggiConfigName, String[] imTools, char docStyleMode, String userName) {
			this.name = name;
			this.ggiConfigName = ggiConfigName;
			this.imTools = imTools;
			this.docStyleMode = docStyleMode;
			this.userName = userName;
		}
		
		void printDescription(ComponentActionConsole cac) {
			cac.reportResult(" - IM tool sequence: " + this.getImtNames(false));
			cac.reportResult(" - GGI configuration name: " + ((this.ggiConfigName == null) ? "default" : this.ggiConfigName));
			cac.reportResult(" - Document style mode: " + ((this.docStyleMode == 'I') ? "ignore" : ((this.docStyleMode == 'U') ? "use (if given)" : "require")));
		}
		
		String getImtNames(boolean forSlaveParam) {
			if (this.imTools.length == 1)
				return this.imTools[0];
			StringBuffer imtNames = new StringBuffer(this.imTools[0]);
			for (int t = 1; t < this.imTools.length; t++) {
				imtNames.append(forSlaveParam ? "+" : " ");
				imtNames.append(this.imTools[t]);
			}
			return imtNames.toString();
		}
		
		String label;
		String description;
		ImpBatchDescriptor getDescriptor() {
			return ((this.label == null) ? null : new ImpBatchDescriptor(this.name, this.label, this.description));
		}
		
		String htmlDescriptor;
		String getHtmlDescriptor() {
			if (this.htmlDescriptor == null)
				return null;
			if (!this.htmlDescriptor.startsWith("file:"))
				return this.htmlDescriptor;
			String hdFileName = this.htmlDescriptor.substring("file:".length()).trim();
			try {
				File hdFile;
				if (hdFileName.startsWith("/") || hdFileName.matches("[A-Z]\\:.*"))
					hdFile = new File(hdFileName);
				else hdFile = new File(dataPath, hdFileName);
				BufferedReader hdBr = new BufferedReader(new InputStreamReader(new FileInputStream(hdFile), "UTF-8"));
				char[] hdBuffer = new char[1024];
				StringBuffer hdSb = new StringBuffer();
				for (int r; (r = hdBr.read(hdBuffer, 0, hdBuffer.length)) != -1;)
					hdSb.append(hdBuffer, 0, r);
				hdBr.close();
				this.htmlDescriptor = hdSb.toString();
			}
			catch (IOException ioe) {
				logError("Could not load HTML descriptor for batch '" + this.name + "' from '" + hdFileName + "': " + ioe.getMessage());
				logError(ioe);
				this.htmlDescriptor = null; // we only try to read this once
			}
			return this.htmlDescriptor;
		}
		
		/* TODO for use in UI:
- add label and description (for selector in UI, etc.) ...
- ... and maybe details in HTML files (denoted as "file:<path>")
		 * 
		 */
	}
	
	private class ImpSlaveJob extends SlaveJob {
		ImpSlaveJob(String slaveJobId, boolean updateGgiConfig) {
			super(slaveJobId, "GgServerImpSlave.jar");
			if (maxSlaveMemory > 512)
				this.setMaxMemory(maxSlaveMemory);
			this.setLogPath(logFolder.getAbsolutePath());
			if ((updateGgiConfig || (maxParallelBatchRuns == 1)) && (ggiConfigHost != null))
				this.setProperty("CONFHOST", ggiConfigHost);
		}
		ImpSlaveJob(String slaveJobId, String imtName, boolean updateGgiConfig) {
			this(slaveJobId, ggiConfigName, imtName, updateGgiConfig);
		}
		ImpSlaveJob(String slaveJobId, String ggiConfigName, String imtName, boolean updateGgiConfig) {
			this(slaveJobId, updateGgiConfig);
			this.setProperty("CONFNAME", ggiConfigName);
			this.setProperty("TOOLS", ((imtName == null) ? defaultBatch.getImtNames(true) : imtName));
		}
		ImpSlaveJob(String slaveJobId, ImpBatch batch, boolean updateGgiConfig) {
			this(slaveJobId, updateGgiConfig);
			this.setProperty("CONFNAME", ((batch.ggiConfigName == null) ? ggiConfigName : batch.ggiConfigName));
			this.setProperty("TOOLS", batch.getImtNames(true));
		}
	}
	
	private static class CacheImDocumentData extends FolderImDocumentData {
		ImDocumentData sourceDocData;
		CacheImDocumentData(File cacheFolder, ImDocumentData sourceDocData) throws IOException {
			super(cacheFolder);
			this.sourceDocData = sourceDocData;
			ImDocumentEntry[] outDocEntries = this.sourceDocData.getEntries();
			for (int e = 0; e < outDocEntries.length; e++) {
				//	TODOnot put graphics supplements in cache, too (for table detection !!!)
				//	==> supplements proper are there, and underlying data is loaded on demand
				if (outDocEntries[e].name.endsWith(".csv"))
					this.cacheDocEntry(outDocEntries[e]);
				else this.putEntry(outDocEntries[e]); // add entry as virtual for now
			}
			this.storeEntryList();
		}
		void cacheDocEntry(ImDocumentEntry docEntry) throws IOException {
			InputStream docEntryIn = new BufferedInputStream(this.sourceDocData.getInputStream(docEntry));
			if (this.hasEntryData(docEntry))
				return;
			OutputStream cacheDocEntryOut = new BufferedOutputStream(this.getOutputStream(docEntry));
			byte[] buffer = new byte[1024];
			for (int r; (r = docEntryIn.read(buffer, 0, buffer.length)) != -1;)
				cacheDocEntryOut.write(buffer, 0, r);
			cacheDocEntryOut.flush();
			cacheDocEntryOut.close();
			docEntryIn.close();
		}
	}
//	
//	private class ImpSlaveProcessInterface extends SlaveProcessInterface {
//		private ComponentActionConsole reportTo = null;
//		private CacheImDocumentData cacheDocData;
//		private String docId;
//		ImpSlaveProcessInterface(Process slave, String slaveName, CacheImDocumentData cacheDocData, String docId) {
//			super(slave, slaveName);
//			this.cacheDocData = cacheDocData;
//			this.docId = docId;
//		}
//		void setReportTo(ComponentActionConsole reportTo) {
//			this.reportTo = reportTo;
//		}
//		protected void handleInput(String input) {
//			if (input.startsWith("PR:")) {
//				input = input.substring("PR:".length());
//				logInfo("Running Image Markup Tool '" + input + "'");
//				processorName = input;
//				processorStart = System.currentTimeMillis();
//			}
//			else if (input.startsWith("DER:")) {
//				String docEntryName = input.substring("DER:".length());
//				ImDocumentEntry docEntry = this.cacheDocData.getEntry(docEntryName);
//				if (docEntry == null)
//					this.sendOutput("DEN:" + docEntryName);
//				else try {
//					this.cacheDocData.cacheDocEntry(docEntry);
//					this.sendOutput("DEC:" + docEntryName);
//				}
//				catch (IOException ioe) {
//					this.sendOutput("DEE:" + ioe.getMessage());
//				}
//			}
//			else logInfo("IMP: " + input);
//		}
//		protected void handleResult(String result) {
//			ComponentActionConsole cac = this.reportTo;
//			if (cac == null)
//				logInfo("IMP: " + result);
//			else cac.reportResult(result);
//		}
//		private ArrayList outStackTrace = new ArrayList();
//		protected void handleStackTrace(String stackTraceLine) {
//			if (stackTraceLine.trim().length() == 0)
//				this.reportError(this.outStackTrace);
//			else {
//				this.outStackTrace.add(stackTraceLine);
//				super.handleStackTrace(stackTraceLine);
//			}
//		}
//		protected void finalizeSystemOut() {
//			this.reportError(this.outStackTrace);
//		}
//		private ArrayList errStackTrace = new ArrayList();
//		protected void handleError(String error, boolean fromSysErr) {
//			ComponentActionConsole cac = this.reportTo;
//			if (fromSysErr && (cac == null)) {
//				if (error.startsWith("CR\t") || error.startsWith("LA\t") || error.startsWith("Stale "))
//					return; // TODO remove this once server fixed
//				if (error.matches("(Im|Gamta)Document(Root)?Guard\\:.*"))
//					return; // TODO remove this once server fixed
//				if (error.startsWith("Font 'Free") && error.endsWith("' loaded successfully."))
//					return;
//			}
//			if (cac == null) {
//				if (fromSysErr)
//					this.errStackTrace.add(error);
//				logError("IMP: " + error);
//			}
//			else cac.reportError(error);
//		}
//		protected void finalizeSystemErr() {
//			this.reportError(this.errStackTrace);
//		}
//		private void reportError(ArrayList stackTrace) {
//			if (stackTrace.size() == 0)
//				return;
//			String classAndMessge = ((String) stackTrace.get(0));
//			String errorClassName;
//			String errorMessage;
//			if (classAndMessge.indexOf(":") == -1) {
//				errorClassName = classAndMessge;
//				errorMessage = "";
//			}
//			else {
//				errorClassName = classAndMessge.substring(0, classAndMessge.indexOf(":")).trim();
//				errorMessage = classAndMessge.substring(classAndMessge.indexOf(":") + ":".length()).trim();
//			}
//			String[] errorStackTrace = ((String[]) stackTrace.toArray(new String[this.outStackTrace.size()]));
//			stackTrace.clear();
//			SlaveErrorRecorder.recordError(getLetterCode(), this.docId, errorClassName, errorMessage, errorStackTrace);
//		}
//	}
//	
//	private class BatchRun extends Thread {
//		private ImsDocumentData sourceDocData;
//		private File docCacheFolder;
//		
//		private String imtName;
//		private boolean waiveStyle;
//		private boolean verbose;
//		
//		private CacheImDocumentData cacheDocData;
//		private File slaveCacheFolder;
//		private Process slave;
//		private PrintStream toSlave;
//		
//		private String processingDocId = null;
//		private long processingStart = -1;
//		private String processorName = null;
//		private long processorStart = -1;
//		private String processingStep = null;
//		private long processingStepStart = -1;
//		private String processingInfo = null;
//		private long processingInfoStart = -1;
//		private ComponentActionConsole dumpSlaveStackCac;
//		private ComponentActionConsole killSlaveCac;
//		
//		//	TODO figure out how to make sure only limited number running in parallel !!!
//		
//		BatchRun(String id, String processingDocId, String imtName, boolean waiveStyle, boolean verbose) {
//			super(id);
//			this.processingDocId = processingDocId;
//			this.imtName = imtName;
//			this.waiveStyle = waiveStyle;
//			this.verbose = verbose;
//		}
//		
//		public void run() {
//			
//			//	check out document as data, process it, and clean up
//			try {
//				//	TODO register with parent
//				
//				this.processingDocId = this.sourceDocData.getDocumentId();
//				this.processingStart = System.currentTimeMillis();
//				this.sourceDocData = GoldenGateIMP.this.ims.checkoutDocumentAsData(GoldenGateIMP.this.updateUserName, this.processingDocId);
//				this.processDocument();
//			}
//			catch (Exception e) {
//				logError("Error processing document '" + this.processingDocId + "': " + e.getMessage());
//				logError(e);
//				//	TODO maybe hold on to exception and throw in master ???
//			}
//			finally {
//				if (this.sourceDocData != null) {
//					GoldenGateIMP.this.ims.releaseDocument(GoldenGateIMP.this.updateUserName, this.processingDocId);
//					this.sourceDocData.dispose();
//				}
//				
//				//	clean up cache and document data
//				cleanupFile(this.docCacheFolder);
//				cleanupFile(this.slaveCacheFolder);
//				
//				//	TODO unregister from parent
//			}
//		}
//		private void processDocument() throws IOException {
//			
//			//	create document cache folder
//			this.slaveCacheFolder = new File(GoldenGateIMP.this.cacheFolder, ("cache-" + this.processingDocId));
//			this.slaveCacheFolder.mkdirs();
//			
//			//	create document output folder
//			this.docCacheFolder = new File(GoldenGateIMP.this.cacheFolder, ("doc-" + this.processingDocId));
//			this.docCacheFolder.mkdirs();
//			
//			//	copy document to cache folder (only non-binary entries)
//			this.cacheDocData = new CacheImDocumentData(this.docCacheFolder, this.sourceDocData);
//			
//			//	assemble command
//			StringVector command = new StringVector();
//			command.addElement("java");
//			command.addElement("-jar");
//			if (GoldenGateIMP.this.maxSlaveMemory > 512)
//				command.addElement("-Xmx" + GoldenGateIMP.this.maxSlaveMemory + "m");
//			command.addElement("-Djava.io.tmpdir=" + this.slaveCacheFolder.getAbsolutePath() + "/Temp");
//			command.addElement("GgServerImpSlave.jar");
//			
//			//	add parameters
//			command.addElement("DATA=" + this.docCacheFolder.getAbsolutePath()); // document folder
//			command.addElement("CACHE=" + this.slaveCacheFolder.getAbsolutePath()); // cache folder
//			if (GoldenGateIMP.this.ggiConfigHost != null)
//				command.addElement("CONFHOST=" + GoldenGateIMP.this.ggiConfigHost); // config host (if any)
//			command.addElement("CONFNAME=" + GoldenGateIMP.this.ggiConfigName); // config name
//			command.addElement("TOOLS=" + ((this.imtName == null) ? GoldenGateIMP.this.batchImTools : this.imtName)); // IM tool(s) to run
//			if (this.waiveStyle && (this.imtName != null))
//				command.addElement("WAIVEDS"); // waive requiring document style only for single IM tool
//			if (this.verbose && (this.imtName != null))
//				command.addElement("VERBOSE"); // loop through all output (good for debugging)
////			command.addElement("SINGLECORE"); // run on single CPU core only (we don't want to knock out the whole server, do we?)
//			int maxSlaveCores = GoldenGateIMP.this.maxSlaveCores;
//			if (maxSlaveCores < 1)
//				maxSlaveCores = 65536;
//			if ((maxSlaveCores * 4) > Runtime.getRuntime().availableProcessors())
//				maxSlaveCores = (Runtime.getRuntime().availableProcessors() / 4);
//			if (maxSlaveCores == 1)
//				command.addElement("SINGLECORE");
//			else command.addElement("MAXCORES=" + maxSlaveCores);
//			
//			//	start batch processor slave process
//			this.slave = Runtime.getRuntime().exec(command.toStringArray(), new String[0], GoldenGateIMP.this.workingFolder);
//			
//			//	get output channel
//			this.toSlave = new PrintStream(this.slave.getOutputStream(), true);
//			
//			//	loop through error messages
//			final BufferedReader slaveError = new BufferedReader(new InputStreamReader(this.slave.getErrorStream()));
//			new Thread() {
//				public void run() {
//					try {
//						for (String errorLine; (errorLine = slaveError.readLine()) != null;)
//							logError(errorLine);
//					}
//					catch (Exception e) {
//						logError(e);
//					}
//				}
//			}.start();
//			
//			//	loop through step information only
//			BufferedReader fromSlave = new BufferedReader(new InputStreamReader(this.slave.getInputStream()));
//			for (String inLine; (inLine = fromSlave.readLine()) != null;) {
//				if (inLine.startsWith("S:")) {
//					inLine = inLine.substring("S:".length());
//					logInfo(inLine);
//					this.processingStep = inLine;
//					this.processingStepStart = System.currentTimeMillis();
//				}
//				else if (inLine.startsWith("I:")) {
//					inLine = inLine.substring("I:".length());
//					logDebug(inLine);
//					this.processingInfo = inLine;
//					this.processingInfoStart = System.currentTimeMillis();
//				}
//				else if (inLine.startsWith("PR:")) {
//					inLine = inLine.substring("PR:".length());
//					logInfo("Running Image Markup Tool '" + inLine + "'");
//					this.processorName = inLine;
//					this.processorStart = System.currentTimeMillis();
//				}
//				else if (inLine.startsWith("P:")) {}
//				else if (inLine.startsWith("BP:")) {}
//				else if (inLine.startsWith("MP:")) {}
//				else if (inLine.startsWith("DER:")) {
//					String docEntryName = inLine.substring("DER:".length());
//					ImDocumentEntry docEntry = this.cacheDocData.getEntry(docEntryName);
//					if (docEntry == null)
//						this.toSlave.println("DEN:" + docEntryName);
//					else try {
//						this.cacheDocData.cacheDocEntry(docEntry);
//						this.toSlave.println("DEC:" + docEntryName);
//					}
//					catch (IOException ioe) {
//						this.toSlave.println("DEE:" + docEntryName + "\t" + ioe.getMessage());
//						logError("Error caching document entry '" + docEntryName + "': " + ioe.getMessage());
//						logError(ioe);
//					}
//				}
//				else if (inLine.startsWith("SST:")) {
//					inLine = inLine.substring("SST:".length());
//					ComponentActionConsole cac = this.dumpSlaveStackCac; // play this one absolutely safe
//					if (cac != null)
//						cac.reportResult(inLine);
//				}
//				else if (inLine.equals("SSC:")) {
//					this.dumpSlaveStackCac = null;
//				}
//				else logInfo(inLine);
//			}
//			
//			//	wait for batch process to finish
//			while (true) try {
//				this.slave.waitFor();
//				ComponentActionConsole cac = this.killSlaveCac; // play this one absolutely safe
//				if (cac != null)
//					cac.reportResult("Slave process terminated successfully");
//				break;
//			} catch (InterruptedException ie) {}
//			
//			//	clean up
//			synchronized (this) {
//				this.slave = null;
//				this.toSlave = null;
//			}
//			
//			//	copy back modified entries
//			FolderImDocumentData inDocData = new FolderImDocumentData(this.docCacheFolder, null);
//			ImDocumentEntry[] inDocEntries = inDocData.getEntries();
//			boolean docModified = false;
//			logInfo("Document " + this.processingDocId + " processed, copying back entries:");
//			for (int e = 0; e < inDocEntries.length; e++) {
//				if (!inDocData.hasEntryData(inDocEntries[e])) {
//					logInfo(" - " + inDocEntries[e].name + " ==> never cached");
//					continue; // skip over virtual entry, no way this was modified
//				}
//				ImDocumentEntry sourceDocEntry = this.sourceDocData.getEntry(inDocEntries[e].name);
//				if ((sourceDocEntry != null) && sourceDocEntry.dataHash.equals(inDocEntries[e].dataHash)) {
//					logInfo(" - " + inDocEntries[e].name + " ==> unmodified");
//					continue; // this entry exists and didn't change
//				}
//				InputStream inDocEntryIn = new BufferedInputStream(inDocData.getInputStream(inDocEntries[e]));
//				OutputStream docEntryOut = new BufferedOutputStream(this.sourceDocData.getOutputStream(inDocEntries[e]));
//				byte[] buffer = new byte[1024];
//				for (int r; (r = inDocEntryIn.read(buffer, 0, buffer.length)) != -1;)
//					docEntryOut.write(buffer, 0, r);
//				docEntryOut.flush();
//				docEntryOut.close();
//				inDocEntryIn.close();
//				docModified = true;
//				logInfo(" - " + inDocEntries[e].name + " ==> modified");
//			}
//			
//			//	update document in IMS
//			if (docModified)
//				GoldenGateIMP.this.ims.updateDocumentFromData(GoldenGateIMP.this.updateUserName, GoldenGateIMP.this.updateUserName, this.sourceDocData, new EventLogger() {
//					public void writeLog(String logEntry) {
//						logInfo(logEntry);
//					}
//				});
//		}
//		synchronized void dumpSlaveStack(ComponentActionConsole cac) {
//			if (this.toSlave == null)
//				return;
//			this.dumpSlaveStackCac = cac;
//			this.toSlave.println("DSS:");
//		}
//		synchronized void kill(ComponentActionConsole cac) {
//			if (this.slave == null)
//				return;
//			this.killSlaveCac = cac;
//			this.slave.destroy();
//		}
//		void reportStatus(ComponentActionConsole cac) {
//			long time = System.currentTimeMillis();
//			cac.reportResult("Processing document " + processingDocId + " (started " + (time - processingStart) + "ms ago)");
//			cac.reportResult(" - current processor is " + processorName + " (since " + (time - processorStart) + "ms)");
//			cac.reportResult(" - current step is " + processingStep + " (since " + (time - processingStepStart) + "ms)");
//			cac.reportResult(" - current info is " + processingInfo + " (since " + (time - processingInfoStart) + "ms)");
//		}
//	}
}
