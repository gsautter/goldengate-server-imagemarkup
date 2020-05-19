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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import de.uka.ipd.idaho.easyIO.sql.TableColumnDefinition;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.Attributed;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.gamta.util.imaging.DocumentStyle;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS.ImsDocumentData;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent.ImsDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.ims.util.StandaloneDocumentStyleProvider;
import de.uka.ipd.idaho.goldenGateServer.util.AsynchronousDataActionHandler;
import de.uka.ipd.idaho.goldenGateServer.util.SlaveInstallerUtils;
import de.uka.ipd.idaho.im.ImAnnotation;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.util.ImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.FolderImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.ImDocumentEntry;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * GoldenGATE Image Markup Processor provides automated processing of Image
 * Markup documents stored in a GoldenGATE IMS. Processing happens in a slave
 * JVM, controlled by document style templates. Documents can only be processed
 * if they match such a template.
 * 
 * @author sautter
 */
public class GoldenGateIMP extends AbstractGoldenGateServerComponent {
	
	private GoldenGateIMS ims;
	
	private String updateUserName;
	private int maxSlaveMemory = 1024;
	private int maxSlaveCores = 1;
	
	private File workingFolder;
	private File cacheFolder;
	private AsynchronousDataActionHandler documentProcessor;
	
	//	TODO keep these fields _per_slave_ soon as we start using multiple !!!
	private Process batchRun = null;
	private PrintStream batchOut = null;
	private ComponentActionConsole dumpSlaveStackCac = null;
	private String processingDocId = null;
	private long processingStart = -1;
	private String processorName = null;
	private long processorStart = -1;
	private String processingStep = null;
	private long processingStepStart = -1;
	private String processingInfo = null;
	private long processingInfoStart = -1;
	
	private String docStyleListUrl;
	private String docStyleNamePattern;
	private File docStyleFolder;
	private DocumentStyle.Provider docStyleProvider;
	
	private String ggiConfigHost;
	private String ggiConfigName;
	private String batchImTools;
	
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
		this.updateUserName = this.configuration.getSetting("updateUserName", "GgIMP");
		
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
		this.batchImTools = this.configuration.getSetting("batchImTools");
		this.batchImTools = this.batchImTools.replaceAll("\\s+", "+");
		
		//	make document style templates available
		this.docStyleListUrl = this.configuration.getSetting("docStyleListUrl");
		if (this.docStyleListUrl == null)
			throw new RuntimeException("Cannot work without document style templates, URL missing");
		this.docStyleNamePattern = this.configuration.getSetting("docStyleNamePattern");
		this.docStyleFolder = new File(this.workingFolder, "DocStyles");
		this.docStyleFolder.mkdirs();
		
		//	install GGI slave JAR
		SlaveInstallerUtils.installSlaveJar("GgServerImpSlave.jar", this.dataPath, this.workingFolder, true);
		
		//	create asynchronous worker
		TableColumnDefinition[] argCols = {
			new TableColumnDefinition("ImtName", TableDefinition.VARCHAR_DATATYPE, 64),
			new TableColumnDefinition("WaiveStyle", TableDefinition.CHAR_DATATYPE, 1),
			new TableColumnDefinition("LogVerbose", TableDefinition.CHAR_DATATYPE, 1)
		};
		this.documentProcessor = new AsynchronousDataActionHandler("Imp", argCols, this, this.host.getIoProvider()) {
			protected void performDataAction(String dataId, String[] arguments) throws Exception {
				String imtName = null;
				boolean waiveStyle = false;
				boolean verbose = false;
				for (int a = 0; a < arguments.length; a++) {
					if ("W".equals(arguments[a]))
						waiveStyle = true;
					else if ("V".equals(arguments[a]))
						verbose = true;
					else if ("F".equals(arguments[a])) {}
					else if (arguments[a].length() != 0)
						imtName = arguments[a];
				}
				processDocument(dataId, imtName, waiveStyle, verbose);
			}
		};
		
		//	TODO get list of documents from IMS
		
		//	TODO schedule processing for all documents we still hold the lock for (must have been interrupted by shutdown before we could save them back and release them)
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
		
		//	establish connection to catch local updates
		this.ims.addDocumentEventListener(new ImsDocumentEventListener() {
			public void documentCheckedOut(ImsDocumentEvent ide) {}
			public void documentUpdated(ImsDocumentEvent ide) {
				if (!ide.sourceClassName.equals(GoldenGateIMS.class.getName()))
					return;
				Attributed docAttributes = ide.documentData.getDocumentAttributes();
				logInfo("GoldenGATE IMP: checking whether or not to process document " + docAttributes.getAttribute(ImDocument.DOCUMENT_NAME_ATTRIBUTE, ide.dataId));
				
				//	let's not loop back on our own updates
				if (updateUserName.equals(ide.user)) {
					logInfo(" ==> self-triggered update");
					return;
				}
				
				//	only process documents that have not been worked on yet
				ImAnnotation[] docAnnots = ide.documentData.getAnnotations();
				if (docAnnots.length != 0) {
					logInfo(" ==> there are already annotations");
					imsUpdatedDocIDs.remove(ide.dataId);
					return;
				}
				
				//	load document proper only now
				ImDocument doc;
				try {
					doc = ide.documentData.getDocument(new ProgressMonitor() {
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
					logError("Could not investigate document: " + ioe.getMessage());
					logError(ioe);
					return;
				}
				
				//	test if we have a document style template
				DocumentStyle docStyle = DocumentStyle.getStyleFor(doc);
				if (docStyle.isEmpty()) {
					logInfo(" ==> document style template not found");
					imsUpdatedDocIDs.remove(ide.dataId);
					return;
				}
				
				//	schedule processing document
				imsUpdatedDocIDs.add(ide.dataId);
				logInfo(" ==> processing scheduled for after release");
			}
			public void documentDeleted(ImsDocumentEvent dse) {}
			public void documentReleased(ImsDocumentEvent dse) {
				if (!dse.sourceClassName.equals(GoldenGateIMS.class.getName()))
					return;
				if (imsUpdatedDocIDs.remove(dse.dataId)) {
					String[] args = {"", "F", "F"};
					documentProcessor.enqueueDataAction(dse.dataId, args);
				}
			}
		});
		
		//	start processing handler thread
		this.documentProcessor.start();
	}
	
	private Set imsUpdatedDocIDs = Collections.synchronizedSet(new HashSet());
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		
		//	shut down processing handler thread
		this.documentProcessor.shutdown();
	}
	
	private static final String PROCESS_DOCUMENT_COMMAND = "process";
	private static final String PROCESS_STATUS_COMMAND = "status";
	private static final String PROCESS_STACK_COMMAND = "stack";
	private static final String PROCESS_KILL_COMMAND = "kill";
	private static final String LIST_TOOLS_COMMAND = "tools";
	private static final String QUEUE_SIZE_COMMAND = "queueSize";
	
	/*
	 * (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(this.documentProcessor.getActions()));
		ComponentAction ca;
		
		//	schedule processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_DOCUMENT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_DOCUMENT_COMMAND + " <documentId> <toolName> <waiveStyle> <verbose>",
						"Schedule a document for batch processing:",
						"- <documentId>: The ID of the document to process",
						"- <toolName>: The name of a single Image Markup Tool to run (optional, defaults to whole configured batch)",
						"- <waiveStyle>: Set to '-wds' to run without a document style template available (optional, only valid if <toolName> specified as well)",
						"- <verbose>: Set to '-v' to transmit full output (optional, only valid if <toolName> specified as well)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify at least the document ID.");
				else if (arguments.length < 4) {
					String imtName = null;
					boolean waiveStyle = false;
					boolean verbose = false;
					for (int a = 1; a < arguments.length; a++) {
						if ("-wds".equals(arguments[a]))
							waiveStyle = true;
						else if ("-v".equals(arguments[a]))
							verbose = true;
						else imtName = arguments[a];
					}
					String[] args = {
						((imtName == null) ? "" : imtName),
						(waiveStyle ? "W" : "F"),
						(verbose ? "V" : "F")
					};
					documentProcessor.enqueueDataAction(arguments[0], args);
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID, markup tool name, document style waiver, and verbosity as the only arguments.");
			}
		};
		cal.add(ca);
		
		//	check processing status of a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_STATUS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_STATUS_COMMAND,
						"Show the status of a document that is processing"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				if (processingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				//	TODO list status of all running slaves
				//	TODO include slave IDs
				//	TODO list more detailed status of individual slave if ID specified
				long time = System.currentTimeMillis();
				this.reportResult("Processing document " + processingDocId + " (started " + (time - processingStart) + "ms ago)");
				this.reportResult(" - current processor is " + processorName + " (since " + (time - processorStart) + "ms)");
				this.reportResult(" - current step is " + processingStep + " (since " + (time - processingStepStart) + "ms)");
				this.reportResult(" - current info is " + processingInfo + " (since " + (time - processingInfoStart) + "ms)");
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
						PROCESS_STACK_COMMAND,
						"Show the stack trace of the batch processing a document"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				if (processingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				dumpSlaveStackCac = this;
				batchOut.println("DSS:");
			}
		};
		cal.add(ca);
		
		//	kill a batch processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_KILL_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_KILL_COMMAND,
						"Kill the batch processing a document"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				if (processingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				batchRun.destroy();
			}
		};
		cal.add(ca);
		
		//	list available document processors
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
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	private String[][] markupToolList = null;
	private void listMarkupTools(boolean forceReload, boolean providersOnly, String providerPrefix, ComponentActionConsole cac) {
		
		//	load document processor list on demand
		if ((this.markupToolList == null) || forceReload) try {
			this.markupToolList = this.loadMarkupToolList(cac);
		}
		catch (IOException ioe) {
			cac.reportError("Could not load markup tool list: " + ioe.getMessage());
			cac.reportError(ioe);
			return;
		}
		
		//	print out (filtered) document processor list
		for (int m = 0; m < this.markupToolList.length; m++) {
			if ((providerPrefix != null) && !this.markupToolList[m][0].startsWith(providerPrefix))
				continue;
			cac.reportResult(this.markupToolList[m][0]);
			if (providersOnly)
				continue;
			for (int p = 1; p < this.markupToolList[m].length; p++)
				cac.reportResult(this.markupToolList[m][p]);
		}
	}
	
	private String[][] loadMarkupToolList(final ComponentActionConsole cac) throws IOException {
		cac.reportResult("Loading markup tool list ...");
		
		//	assemble command
		StringVector command = new StringVector();
		command.addElement("java");
		command.addElement("-jar");
		command.addElement("-Xmx1024m");
		command.addElement("GgServerImpSlave.jar");
		
		//	add parameters
		if (this.ggiConfigHost != null)
			command.addElement("CONFHOST=" + this.ggiConfigHost); // config host (if any)
		command.addElement("CONFNAME=" + this.ggiConfigName); // config name
		command.addElement("TOOLS=" + "LISTTOOLS"); // constant returning list
		command.addElement("SINGLECORE"); // run on single CPU core only (we don't want to knock out the whole server, do we?)
		
		//	start batch processor slave process
		Process processing = Runtime.getRuntime().exec(command.toStringArray(), new String[0], this.workingFolder);
		
		//	loop through error messages
		final BufferedReader processorError = new BufferedReader(new InputStreamReader(processing.getErrorStream()));
		new Thread() {
			public void run() {
				try {
					for (String errorLine; (errorLine = processorError.readLine()) != null;)
						cac.reportError(errorLine);
				}
				catch (Exception e) {
					cac.reportError(e);
				}
			}
		}.start();
		
		//	collect Image Markup tool listing
		ArrayList markupToolProviderList = new ArrayList();
		ArrayList markupToolList = new ArrayList();
		BufferedReader processorIn = new BufferedReader(new InputStreamReader(processing.getInputStream()));
		for (String inLine; (inLine = processorIn.readLine()) != null;) {
			if (inLine.startsWith("MTP:")) {
				if (markupToolList.size() != 0)
					markupToolProviderList.add(markupToolList.toArray(new String[markupToolList.size()]));
				markupToolList.clear();
				markupToolList.add(inLine.substring("MTP:".length()));
			}
			else if (inLine.startsWith("MT:"))
				markupToolList.add(inLine.substring("MT:".length()));
			else cac.reportResult(inLine);
		}
		if (markupToolList.size() != 0)
			markupToolProviderList.add(markupToolList.toArray(new String[markupToolList.size()]));
		
		//	wait for process to finish
		while (true) try {
			processing.waitFor();
			break;
		} catch (InterruptedException ie) {}
		
		//	finally ...
		return ((String[][]) markupToolProviderList.toArray(new String[markupToolProviderList.size()][]));
	}
	
	private void processDocument(String docId, String imtName, boolean waiveStyle, boolean verbose) throws IOException {
		
		//	check out document as data, process it, and clean up
		ImsDocumentData docData = null;
		try {
			this.processingDocId = docId;
			this.processingStart = System.currentTimeMillis();
			docData = this.ims.checkoutDocumentAsData(this.updateUserName, docId);
			this.processDocument(docId, docData, imtName, waiveStyle, verbose);
		}
		finally {
			if (docData != null)
				docData.dispose();
			this.dumpSlaveStackCac = null;
			this.batchRun = null;
			this.batchOut = null;
			this.processingDocId = null;
			this.processingStart = -1;
			this.processorName = null;
			this.processorStart = -1;
			this.processingStep = null;
			this.processingStepStart = -1;
			this.processingInfo = null;
			this.processingInfoStart = -1;
		}
	}
	
	private void processDocument(String docId, ImsDocumentData docData, String imtName, boolean waiveStyle, boolean verbose) throws IOException {
		
		//	create document style provider on demand (now that we're sure ECS is up and available)
		if (this.docStyleProvider == null) try {
			this.docStyleProvider = new StandaloneDocumentStyleProvider(this.docStyleListUrl, this.docStyleNamePattern, this.docStyleFolder);
		}
		catch (IOException ioe) {
			this.logError(ioe);
			this.logError("Cannot work without document style templates, URL invalid or broken");
			if (!waiveStyle)
				throw ioe;
		}
		
		//	create document cache folder
		File cacheFolder = new File(this.cacheFolder, ("cache-" + docId));
		cacheFolder.mkdirs();
		
		//	create document output folder
		File docFolder = new File(this.cacheFolder, ("doc-" + docId));
		docFolder.mkdirs();
		
		//	copy document to cache folder (only non-binary entries)
//		FolderImDocumentData outDocData = new FolderImDocumentData(docFolder);
//		ImDocumentEntry[] outDocEntries = docData.getEntries();
//		for (int e = 0; e < outDocEntries.length; e++) {
//			if (!outDocEntries[e].name.endsWith(".csv"))
//				continue;
//			InputStream docEntryIn = new BufferedInputStream(docData.getInputStream(outDocEntries[e]));
//			OutputStream outDocEntryOut = new BufferedOutputStream(outDocData.getOutputStream(outDocEntries[e]));
//			byte[] buffer = new byte[1024];
//			for (int r; (r = docEntryIn.read(buffer, 0, buffer.length)) != -1;)
//				outDocEntryOut.write(buffer, 0, r);
//			outDocEntryOut.flush();
//			outDocEntryOut.close();
//			docEntryIn.close();
//		}
//		outDocData.storeEntryList();
		final CacheImDocumentData cacheDocData = new CacheImDocumentData(docFolder, docData);
		
		//	assemble command
		StringVector command = new StringVector();
		command.addElement("java");
		command.addElement("-jar");
		if (this.maxSlaveMemory > 512)
			command.addElement("-Xmx" + this.maxSlaveMemory + "m");
		command.addElement("GgServerImpSlave.jar");
		
		//	add parameters
		command.addElement("DATA=" + docFolder.getAbsolutePath()); // document folder
		command.addElement("CACHE=" + cacheFolder.getAbsolutePath()); // cache folder
		if (this.ggiConfigHost != null)
			command.addElement("CONFHOST=" + this.ggiConfigHost); // config host (if any)
		command.addElement("CONFNAME=" + this.ggiConfigName); // config name
		command.addElement("TOOLS=" + ((imtName == null) ? this.batchImTools : imtName)); // IM tool(s) to run
		if (waiveStyle && (imtName != null))
			command.addElement("WAIVEDS"); // waive requiring document style only for single IM tool
		if (verbose && (imtName != null))
			command.addElement("VERBOSE"); // loop through all output (good for debugging)
//		command.addElement("SINGLECORE"); // run on single CPU core only (we don't want to knock out the whole server, do we?)
		int maxSlaveCores = this.maxSlaveCores;
		if (maxSlaveCores < 1)
			maxSlaveCores = 65536;
		if ((maxSlaveCores * 4) > Runtime.getRuntime().availableProcessors())
			maxSlaveCores = (Runtime.getRuntime().availableProcessors() / 4);
		if (maxSlaveCores == 1)
			command.addElement("SINGLECORE");
		else command.addElement("MAXCORES=" + maxSlaveCores);
		//	TODO use JVM argument -XX:ActiveProcessorCount=nn instead or in addition (limits also JVM owned threads)
		//	==> see https://stackoverflow.com/questions/33723373/can-i-set-the-number-of-threads-cpus-available-to-the-java-vm
		
		//	start batch processor slave process
		this.batchRun = Runtime.getRuntime().exec(command.toStringArray(), new String[0], this.workingFolder);
		
		//	get output channel
		this.batchOut = new PrintStream(this.batchRun.getOutputStream(), true);
		
		//	loop through error messages
		final BufferedReader slaveError = new BufferedReader(new InputStreamReader(this.batchRun.getErrorStream()));
		new Thread() {
			public void run() {
				try {
					for (String errorLine; (errorLine = slaveError.readLine()) != null;)
						logError(errorLine);
				}
				catch (Exception e) {
					logError(e);
				}
			}
		}.start();
		
		//	TODO catch request for further document entries on input stream ...
		//	TODO ... and move them to cache on demand ...
		//	TODO ... sending 'ready' message back through process output stream
		//	TODO test command: process 6229FF8AD22B0336DF54FFD7FFD3FF8E
		
		//	TODO keep slave process responsive to commands like getting stack trace
		//	TODO put graphics supplements in cache
		
		//	loop through step information only
		BufferedReader slaveIn = new BufferedReader(new InputStreamReader(this.batchRun.getInputStream()));
		for (String inLine; (inLine = slaveIn.readLine()) != null;) {
			if (inLine.startsWith("S:")) {
				inLine = inLine.substring("S:".length());
				logInfo(inLine);
				processingStep = inLine;
				processingStepStart = System.currentTimeMillis();
			}
			else if (inLine.startsWith("I:")) {
				inLine = inLine.substring("I:".length());
				logDebug(inLine);
				processingInfo = inLine;
				processingInfoStart = System.currentTimeMillis();
			}
			else if (inLine.startsWith("PR:")) {
				inLine = inLine.substring("PR:".length());
				logInfo("Running Image Markup Tool '" + inLine + "'");
				processorName = inLine;
				processorStart = System.currentTimeMillis();
			}
			else if (inLine.startsWith("P:")) {}
			else if (inLine.startsWith("BP:")) {}
			else if (inLine.startsWith("MP:")) {}
			else if (inLine.startsWith("DER:")) {
				String docEntryName = inLine.substring("DER:".length());
				ImDocumentEntry docEntry = cacheDocData.getEntry(docEntryName);
				if (docEntry == null)
					this.batchOut.println("DEN:" + docEntryName);
				else try {
					cacheDocData.cacheDocEntry(docEntry);
					this.batchOut.println("DEC:" + docEntryName);
				}
				catch (IOException ioe) {
					this.batchOut.println("DEE:" + ioe.getMessage());
				}
			}
			else if (inLine.startsWith("SST:")) {
				inLine = inLine.substring("SST:".length());
				ComponentActionConsole cac = this.dumpSlaveStackCac; // play this one absolutely safe
				if (cac != null)
					cac.reportResult(inLine);
			}
			else if (inLine.equals("SSC:")) {
				this.dumpSlaveStackCac = null;
			}
			else logInfo(inLine);
		}
		
		//	wait for batch process to finish
		while (true) try {
			this.batchRun.waitFor();
			break;
		} catch (InterruptedException ie) {}
		
		//	copy back modified entries
		FolderImDocumentData inDocData = new FolderImDocumentData(docFolder, null);
		ImDocumentEntry[] inDocEntries = inDocData.getEntries();
		boolean docModified = false;
		logInfo("Document " + docId + " processed, copying back entries:");
		for (int e = 0; e < inDocEntries.length; e++) {
			ImDocumentEntry docEntry = docData.getEntry(inDocEntries[e].name);
			if ((docEntry != null) && docEntry.dataHash.equals(inDocEntries[e].dataHash)) {
				logInfo(" - " + inDocEntries[e].name + " ==> unmodified");
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
			logInfo(" - " + inDocEntries[e].name + " ==> modified");
		}
		
		//	update and release document in IMS
		if (docModified)
			this.ims.updateDocumentFromData(this.updateUserName, this.updateUserName, docData, new EventLogger() {
				public void writeLog(String logEntry) {
					logInfo(logEntry);
				}
			});
		this.ims.releaseDocument(this.updateUserName, docId);
		
		//	clean up cache and document data
		cleanupFile(cacheFolder);
		cleanupFile(docFolder);
	}
	
	private static void cleanupFile(File file) {
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (int f = 0; f < files.length; f++)
				cleanupFile(files[f]);
		}
		file.delete();
	}
	
	private static class CacheImDocumentData extends FolderImDocumentData {
		ImDocumentData sourceDocData;
		CacheImDocumentData(File cacheFolder, ImDocumentData sourceDocData) throws IOException {
			super(cacheFolder);
			this.sourceDocData = sourceDocData;
			ImDocumentEntry[] outDocEntries = this.sourceDocData.getEntries();
			for (int e = 0; e < outDocEntries.length; e++) {
				if (outDocEntries[e].name.endsWith(".csv"))
					this.cacheDocEntry(outDocEntries[e]);
				else this.putEntry(outDocEntries[e]); // add entry as vitual for now
			}
			this.storeEntryList();
		}
		void cacheDocEntry(ImDocumentEntry docEntry) throws IOException {
			InputStream docEntryIn = new BufferedInputStream(this.sourceDocData.getInputStream(docEntry));
			OutputStream outDocEntryOut = new BufferedOutputStream(this.getOutputStream(docEntry));
			byte[] buffer = new byte[1024];
			for (int r; (r = docEntryIn.read(buffer, 0, buffer.length)) != -1;)
				outDocEntryOut.write(buffer, 0, r);
			outDocEntryOut.flush();
			outDocEntryOut.close();
			docEntryIn.close();
		}
	}
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
