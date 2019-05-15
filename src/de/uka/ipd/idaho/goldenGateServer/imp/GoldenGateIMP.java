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
package de.uka.ipd.idaho.goldenGateServer.imp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import de.uka.ipd.idaho.easyIO.sql.TableColumnDefinition;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.Attributed;
import de.uka.ipd.idaho.gamta.util.imaging.DocumentStyle;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS.ImsDocumentData;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent.ImsDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.ims.util.StandaloneDocumentStyleProvider;
import de.uka.ipd.idaho.goldenGateServer.util.AsynchronousDataActionHandler;
import de.uka.ipd.idaho.im.ImAnnotation;
import de.uka.ipd.idaho.im.ImDocument;
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
	
	private String docStyleListUrl;
	private String docStyleNamePattern;
	
	private File workingFolder;
	private File cacheFolder;
	private AsynchronousDataActionHandler documentProcessor;
	
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
		//	TODO load this on first use (won't work before ECS is up and available)
		this.docStyleListUrl = this.configuration.getSetting("docStyleListUrl");
		if (this.docStyleListUrl == null)
			throw new RuntimeException("Cannot work without document style templates, URL missing");
		this.docStyleNamePattern = this.configuration.getSetting("docStyleNamePattern");
		File docStyleFolder = new File(this.workingFolder, "DocStyles");
		docStyleFolder.mkdirs();
		try {
			new StandaloneDocumentStyleProvider(this.docStyleListUrl, this.docStyleNamePattern, docStyleFolder);
		}
		catch (IOException ioe) {
			ioe.printStackTrace(System.out);
			throw new RuntimeException("Cannot work without document style templates, URL invalid or broken");
		}
		
		//	install base JARs
		this.installJar("StringUtils.jar");
		this.installJar("HtmlXmlUtil.jar");
		this.installJar("Gamta.jar");
		this.installJar("mail.jar");
		this.installJar("EasyIO.jar");
		this.installJar("GamtaImagingAPI.jar");
		this.installJar("GamtaFeedbackAPI.jar");
		
		//	install image markup and OCR JARs
		this.installJar("ImageMarkup.jar");
		this.installJar("ImageMarkup.bin.jar");
		this.installJar("ImageMarkupOCR.jar");
		
		//	install PDF decoder JARs
		this.installJar("icepdf-core.jar");
		this.installJar("ImageMarkupPDF.jar");
		
		//	install GG Imagine JARs
		this.installJar("GoldenGATE.jar");
		this.installJar("GgImagine.jar");
		
		//	install GGI slave JAR
		this.installJar("GgServerImpSlave.jar");
		
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
	
	private void installJar(String name) {
		System.out.println("Installing JAR '" + name + "'");
		File source = new File(this.dataPath, name);
		if (!source.exists())
			throw new RuntimeException("Missing JAR: " + name);
		
		File target = new File(this.workingFolder, name);
		if ((target.lastModified() + 1000) > source.lastModified()) {
			System.out.println(" ==> up to date");
			return;
		}
		
		try {
			InputStream sourceIn = new BufferedInputStream(new FileInputStream(source));
			OutputStream targetOut = new BufferedOutputStream(new FileOutputStream(target));
			byte[] buffer = new byte[1024];
			for (int r; (r = sourceIn.read(buffer, 0, buffer.length)) != -1;)
				targetOut.write(buffer, 0, r);
			targetOut.flush();
			targetOut.close();
			sourceIn.close();
			System.out.println(" ==> installed");
		}
		catch (IOException ioe) {
			throw new RuntimeException("Could not install JAR '" + name + "': " + ioe.getMessage());
		}
		catch (Exception e) {
			e.printStackTrace(System.out);
			throw new RuntimeException("Could not install JAR '" + name + "': " + e.getMessage());
		}
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
				logInfo("GoldenGATE IMP: checking whether or not to process document " + docAttributes.getAttribute(ImDocument.DOCUMENT_NAME_ATTRIBUTE, ide.documentId));
				
				//	let's not loop back on our own updates
				if (updateUserName.equals(ide.user)) {
					logInfo(" ==> self-triggered update");
					return;
				}
				
				//	only process documents that have not been worked on yet
				ImAnnotation[] docAnnots = ide.documentData.getAnnotations();
				if (docAnnots.length != 0) {
					logInfo(" ==> there are already annotations");
					imsUpdatedDocIDs.remove(ide.documentId);
					return;
				}
				
				//	load document proper only now
				ImDocument doc;
				try {
					doc = ide.documentData.getDocument();
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
					imsUpdatedDocIDs.remove(ide.documentId);
					return;
				}
				
				//	schedule processing document
				imsUpdatedDocIDs.add(ide.documentId);
				logInfo(" ==> processing scheduled for after release");
			}
			public void documentDeleted(ImsDocumentEvent dse) {}
			public void documentReleased(ImsDocumentEvent dse) {
				if (!dse.sourceClassName.equals(GoldenGateIMS.class.getName()))
					return;
				if (imsUpdatedDocIDs.remove(dse.documentId)) {
//					scheduleBatchRun(new BatchRunRequest(dse.documentId));
					String[] args = {"", "F", "F"};
					documentProcessor.enqueueDataAction(dse.documentId, args);
				}
			}
		});
		
		//	start processing handler thread
//		Thread batchRunner = new BatchRunnerThread();
//		batchRunner.start();
		this.documentProcessor.start();
	}
	
	private Set imsUpdatedDocIDs = Collections.synchronizedSet(new HashSet());
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		
		//	shut down processing handler thread
//		synchronized (this.batchRunQueue) {
//			this.batchRunQueue.clear();
//			this.batchRunQueue.notify();
//		}
		this.documentProcessor.shutdown();
	}
	
	private static final String PROCESS_DOCUMENT_COMMAND = "process";
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
//				if (arguments.length == 1)
//					scheduleBatchRun(new BatchRunRequest(arguments[0]));
//				else if (arguments.length == 2)
//					scheduleBatchRun(new BatchRunRequest(arguments[0], arguments[1], false, false));
//				else if (arguments.length == 3)
//					scheduleBatchRun(new BatchRunRequest(arguments[0], arguments[1], "-wds".equals(arguments[2]), "-v".equals(arguments[2])));
//				else if (arguments.length == 4)
//					scheduleBatchRun(new BatchRunRequest(arguments[0], arguments[1], ("-wds".equals(arguments[2]) || "-wds".equals(arguments[3])), ("-v".equals(arguments[2]) || "-v".equals(arguments[3]))));
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
//	
//	private static class BatchRunRequest {
//		final String documentId;
//		final String imtName;
//		final boolean waiveStyle;
//		final boolean verbose;
//		BatchRunRequest(String documentId) {
//			this(documentId, null, false, false);
//		}
//		BatchRunRequest(String documentId, String imtName, boolean waiveStyle, boolean verbose) {
//			this.documentId = documentId;
//			this.imtName = imtName;
//			this.waiveStyle = waiveStyle;
//			this.verbose = verbose;
//		}
//	}
//	
//	private LinkedList batchRunQueue = new LinkedList() {
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
//	private void scheduleBatchRun(BatchRunRequest brr) {
//		synchronized (this.batchRunQueue) {
//			this.batchRunQueue.addLast(brr);
//			this.batchRunQueue.notify();
//		}
//	}
//	private BatchRunRequest getBatchRun() {
//		synchronized (this.batchRunQueue) {
//			if (this.batchRunQueue.isEmpty()) try {
//				this.batchRunQueue.wait();
//			} catch (InterruptedException ie) {}
//			return (this.batchRunQueue.isEmpty() ? null : ((BatchRunRequest) this.batchRunQueue.removeFirst()));
//		}
//	}
//	
//	private class BatchRunnerThread extends Thread {
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
//				//	get next document processing request to process
//				BatchRunRequest brr = getBatchRun();
//				if (brr == null)
//					return; // only happens on shutdown
//				
//				//	process document
//				long brrStart = System.currentTimeMillis();
//				try {
//					handleBatchRun(brr);
//				}
//				catch (Exception e) {
//					e.printStackTrace(System.out);
//				}
//				
//				//	give the others a little time
//				try {
//					sleep(Math.max((1000 * 5), (System.currentTimeMillis() - brrStart)));
//				} catch (InterruptedException ie) {}
//			}
//		}
//	}
//	
//	private void handleBatchRun(BatchRunRequest brr) throws IOException {
//		
//		//	check out document as data
//		ImsDocumentData docData = this.ims.checkoutDocumentAsData(this.updateUserName, brr.documentId);
//		
//		//	create document cache folder
//		File cacheFolder = new File(this.cacheFolder, ("cache-" + brr.documentId));
//		cacheFolder.mkdirs();
//		
//		//	create document output folder
//		File docFolder = new File(this.cacheFolder, ("doc-" + brr.documentId));
//		docFolder.mkdirs();
//		
//		//	copy document to cache folder (only non-binary entries)
//		FolderImDocumentData cDocData = new FolderImDocumentData(docFolder);
//		ImDocumentEntry[] docEntries = docData.getEntries();
//		for (int e = 0; e < docEntries.length; e++) {
//			if (!docEntries[e].name.endsWith(".csv"))
//				continue;
//			InputStream docEntryIn = new BufferedInputStream(docData.getInputStream(docEntries[e]));
//			OutputStream cDocEntryOut = new BufferedOutputStream(cDocData.getOutputStream(docEntries[e]));
//			byte[] buffer = new byte[1024];
//			for (int r; (r = docEntryIn.read(buffer, 0, buffer.length)) != -1;)
//				cDocEntryOut.write(buffer, 0, r);
//			cDocEntryOut.flush();
//			cDocEntryOut.close();
//			docEntryIn.close();
//		}
//		cDocData.storeEntryList();
//		
//		//	assemble command
//		StringVector command = new StringVector();
//		command.addElement("java");
//		command.addElement("-jar");
//		command.addElement("-Xmx1024m");
//		command.addElement("GgServerImpSlave.jar");
//		
//		//	add parameters
//		command.addElement("DATA=" + docFolder.getAbsolutePath()); // document folder
//		command.addElement("CACHE=" + cacheFolder.getAbsolutePath()); // cache folder
//		if (this.ggiConfigHost != null)
//			command.addElement("CONFHOST=" + this.ggiConfigHost); // config host (if any)
//		command.addElement("CONFNAME=" + this.ggiConfigName); // config name
//		command.addElement("TOOLS=" + ((brr.imtName == null) ? this.batchImTools : brr.imtName)); // IM tool(s) to run
//		if (brr.waiveStyle && (brr.imtName != null))
//			command.addElement("WAIVEDS"); // waive requiring document style only for single IM tool
//		if (brr.verbose && (brr.imtName != null))
//			command.addElement("VERBOSE"); // loop through all output (good for debugging)
//		command.addElement("SINGLECORE"); // run on single CPU core only (we don't want to knock out the whole server, do we?)
//		
//		//	start batch processor slave process
//		Process batchRun = Runtime.getRuntime().exec(command.toStringArray(), new String[0], this.workingFolder);
//		
//		//	loop through error messages
//		final BufferedReader importerError = new BufferedReader(new InputStreamReader(batchRun.getErrorStream()));
//		new Thread() {
//			public void run() {
//				try {
//					for (String errorLine; (errorLine = importerError.readLine()) != null;)
//						System.out.println(errorLine);
//				}
//				catch (Exception e) {
//					e.printStackTrace(System.out);
//				}
//			}
//		}.start();
//		
//		//	TODO catch request for further document entries on input stream ...
//		//	TODO ... and move them to cache on demand ...
//		//	TODO ... sending 'ready' message back through process output stream
//		//	TODO test command: process 6229FF8AD22B0336DF54FFD7FFD3FF8E
//		
//		//	loop through step information only
//		BufferedReader importerIn = new BufferedReader(new InputStreamReader(batchRun.getInputStream()));
//		for (String inLine; (inLine = importerIn.readLine()) != null;) {
//			if (inLine.startsWith("S:"))
//				System.out.println(inLine.substring("S:".length()));
//			else if (inLine.startsWith("I:")) {}
//			else if (inLine.startsWith("P:")) {}
//			else if (inLine.startsWith("BP:")) {}
//			else if (inLine.startsWith("MP:")) {}
//			else System.out.println(inLine);
//		}
//		
//		//	wait for batch process to finish
//		while (true) try {
//			batchRun.waitFor();
//			break;
//		} catch (InterruptedException ie) {}
//		
//		//	copy back modified entries
//		cDocData = new FolderImDocumentData(docFolder, null);
//		ImDocumentEntry[] cDocEntries = cDocData.getEntries();
//		boolean docModified = false;
//		System.out.println("Document " + brr.documentId + " processed, copying back entries:");
//		for (int e = 0; e < cDocEntries.length; e++) {
//			ImDocumentEntry docEntry = docData.getEntry(cDocEntries[e].name);
//			if ((docEntry != null) && docEntry.dataHash.equals(cDocEntries[e].dataHash)) {
//				System.out.println(" - " + cDocEntries[e].name + " ==> unmodified");
//				continue; // this entry exists and didn't change
//			}
//			InputStream cDocEntryIn = new BufferedInputStream(cDocData.getInputStream(cDocEntries[e]));
//			OutputStream docEntryOut = new BufferedOutputStream(docData.getOutputStream(cDocEntries[e]));
//			byte[] buffer = new byte[1024];
//			for (int r; (r = cDocEntryIn.read(buffer, 0, buffer.length)) != -1;)
//				docEntryOut.write(buffer, 0, r);
//			docEntryOut.flush();
//			docEntryOut.close();
//			cDocEntryIn.close();
//			docModified = true;
//			System.out.println(" - " + cDocEntries[e].name + " ==> modified");
//		}
//		
//		//	update and release document in IMS
//		if (docModified)
//			this.ims.updateDocumentFromData(this.updateUserName, this.updateUserName, docData, new EventLogger() {
//				public void writeLog(String logEntry) {
//					System.out.println(logEntry);
//				}
//			});
//		this.ims.releaseDocument(this.updateUserName, brr.documentId);
//		
//		//	clean up cache and document data
//		this.cleanupFile(cacheFolder);
//		this.cleanupFile(docFolder);
//	}
	
	private void processDocument(String docId, String imtName, boolean waiveStyle, boolean verbose) throws IOException {
		
		//	check out document as data
		ImsDocumentData docData = this.ims.checkoutDocumentAsData(this.updateUserName, docId);
		
		//	create document cache folder
		File cacheFolder = new File(this.cacheFolder, ("cache-" + docId));
		cacheFolder.mkdirs();
		
		//	create document output folder
		File docFolder = new File(this.cacheFolder, ("doc-" + docId));
		docFolder.mkdirs();
		
		//	copy document to cache folder (only non-binary entries)
		FolderImDocumentData cDocData = new FolderImDocumentData(docFolder);
		ImDocumentEntry[] docEntries = docData.getEntries();
		for (int e = 0; e < docEntries.length; e++) {
			if (!docEntries[e].name.endsWith(".csv"))
				continue;
			InputStream docEntryIn = new BufferedInputStream(docData.getInputStream(docEntries[e]));
			OutputStream cDocEntryOut = new BufferedOutputStream(cDocData.getOutputStream(docEntries[e]));
			byte[] buffer = new byte[1024];
			for (int r; (r = docEntryIn.read(buffer, 0, buffer.length)) != -1;)
				cDocEntryOut.write(buffer, 0, r);
			cDocEntryOut.flush();
			cDocEntryOut.close();
			docEntryIn.close();
		}
		cDocData.storeEntryList();
		
		//	assemble command
		StringVector command = new StringVector();
		command.addElement("java");
		command.addElement("-jar");
		command.addElement("-Xmx1024m");
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
		command.addElement("SINGLECORE"); // run on single CPU core only (we don't want to knock out the whole server, do we?)
		
		//	start batch processor slave process
		Process batchRun = Runtime.getRuntime().exec(command.toStringArray(), new String[0], this.workingFolder);
		
		//	loop through error messages
		final BufferedReader importerError = new BufferedReader(new InputStreamReader(batchRun.getErrorStream()));
		new Thread() {
			public void run() {
				try {
					for (String errorLine; (errorLine = importerError.readLine()) != null;)
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
		
		//	loop through step information only
		BufferedReader importerIn = new BufferedReader(new InputStreamReader(batchRun.getInputStream()));
		for (String inLine; (inLine = importerIn.readLine()) != null;) {
			if (inLine.startsWith("S:"))
				logInfo(inLine.substring("S:".length()));
			else if (inLine.startsWith("I:")) {}
			else if (inLine.startsWith("P:")) {}
			else if (inLine.startsWith("BP:")) {}
			else if (inLine.startsWith("MP:")) {}
			else logInfo(inLine);
		}
		
		//	wait for batch process to finish
		while (true) try {
			batchRun.waitFor();
			break;
		} catch (InterruptedException ie) {}
		
		//	copy back modified entries
		cDocData = new FolderImDocumentData(docFolder, null);
		ImDocumentEntry[] cDocEntries = cDocData.getEntries();
		boolean docModified = false;
		logInfo("Document " + docId + " processed, copying back entries:");
		for (int e = 0; e < cDocEntries.length; e++) {
			ImDocumentEntry docEntry = docData.getEntry(cDocEntries[e].name);
			if ((docEntry != null) && docEntry.dataHash.equals(cDocEntries[e].dataHash)) {
				logInfo(" - " + cDocEntries[e].name + " ==> unmodified");
				continue; // this entry exists and didn't change
			}
			InputStream cDocEntryIn = new BufferedInputStream(cDocData.getInputStream(cDocEntries[e]));
			OutputStream docEntryOut = new BufferedOutputStream(docData.getOutputStream(cDocEntries[e]));
			byte[] buffer = new byte[1024];
			for (int r; (r = cDocEntryIn.read(buffer, 0, buffer.length)) != -1;)
				docEntryOut.write(buffer, 0, r);
			docEntryOut.flush();
			docEntryOut.close();
			cDocEntryIn.close();
			docModified = true;
			logInfo(" - " + cDocEntries[e].name + " ==> modified");
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
		this.cleanupFile(cacheFolder);
		this.cleanupFile(docFolder);
	}
	
	private void cleanupFile(File file) {
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (int f = 0; f < files.length; f++)
				this.cleanupFile(files[f]);
		}
		file.delete();
	}
}
