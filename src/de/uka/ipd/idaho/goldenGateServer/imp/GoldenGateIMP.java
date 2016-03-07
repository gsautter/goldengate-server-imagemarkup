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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import de.uka.ipd.idaho.gamta.util.imaging.DocumentStyle;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS.ImsDocumentData;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent.ImsDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.ims.util.StandaloneDocumentStyleProvider;
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
		this.installJar("ImageMarkupOCR.jar");
//		this.installJar("ImageMarkupOCR.bin.jar");
		
		//	install PDF decoder JARs
		this.installJar("icepdf-core.jar");
		this.installJar("ImageMarkupPDF.jar");
//		this.installJar("ImageMarkupPDF.bin.jar");
		
		//	install GG Imagine JARs
		this.installJar("GoldenGATE.jar");
		this.installJar("GgImagine.jar");
		
		//	install GGI slave JAR
		this.installJar("GgServerImpSlave.jar");
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
			public void documentCheckedOut(ImsDocumentEvent dse) {}
			public void documentUpdated(ImsDocumentEvent dse) {
				if (!dse.sourceClassName.equals(GoldenGateIMS.class.getName()))
					return;
				System.out.println("GoldenGATE IMP: checking whether or not to process document " + dse.document.getAttribute(ImDocument.DOCUMENT_NAME_ATTRIBUTE, dse.documentId));
				
				//	let's not loop back on our own updates
				if (updateUserName.equals(dse.user)) {
					System.out.println(" ==> self-triggered update");
					return;
				}
				
				//	only process documents that have not been worked on yet
				ImAnnotation[] docAnnots = dse.document.getAnnotations();
				if (docAnnots.length != 0) {
					System.out.println(" ==> there are already annotations");
					imsUpdatedDocIDs.remove(dse.documentId);
					return;
				}
				
				//	test if we have a document style template
				DocumentStyle docStyle = DocumentStyle.getStyleFor(dse.document);
				if (docStyle.isEmpty()) {
					System.out.println(" ==> document style template not found");
					imsUpdatedDocIDs.remove(dse.documentId);
					return;
				}
				
				//	schedule processing document
				imsUpdatedDocIDs.add(dse.documentId);
				System.out.println(" ==> processing scheduled");
			}
			public void documentDeleted(ImsDocumentEvent dse) {}
			public void documentReleased(ImsDocumentEvent dse) {
				if (!dse.sourceClassName.equals(GoldenGateIMS.class.getName()))
					return;
				if (imsUpdatedDocIDs.remove(dse.documentId))
					scheduleBatchRun(dse.documentId);
			}
		});
		
		//	start processing handler thread
		Thread importerThread = new BatchRunnerThread();
		importerThread.start();
	}
	
	private Set imsUpdatedDocIDs = Collections.synchronizedSet(new HashSet());
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		
		//	shut down processing handler thread
		synchronized (this.batchRunQueue) {
			this.batchRunQueue.clear();
			this.batchRunQueue.notify();
		}
	}
	
	private static final String PROCESS_DOCUMENT_COMMAND = "process";
	
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
				return PROCESS_DOCUMENT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_DOCUMENT_COMMAND + " <documentId>",
						"Schedule a document for batch processing:",
						"- <documentId>: The ID of the document to process"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1)
					scheduleBatchRun(arguments[0]);
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID as the only argument.");
			}
		};
		cal.add(ca);
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	private void scheduleBatchRun(String docId) {
		this.enqueueBatchRun(docId);
	}
	
	private LinkedList batchRunQueue = new LinkedList() {
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
	private void enqueueBatchRun(String docId) {
		synchronized (this.batchRunQueue) {
			this.batchRunQueue.addLast(docId);
			this.batchRunQueue.notify();
		}
	}
	private String getBatchRun() {
		synchronized (this.batchRunQueue) {
			if (this.batchRunQueue.isEmpty()) try {
				this.batchRunQueue.wait();
			} catch (InterruptedException ie) {}
			return (this.batchRunQueue.isEmpty() ? null : ((String) this.batchRunQueue.removeFirst()));
		}
	}
	
	private class BatchRunnerThread extends Thread {
		public void run() {
			
			//	don't start right away
			try {
				sleep(1000 * 15);
			} catch (InterruptedException ie) {}
			
			//	keep going until shutdown
			while (true) {
				
				//	get next document ID to process
				String docId = getBatchRun();
				if (docId == null)
					return; // only happens on shutdown
				
				//	process document
				try {
					handleBatchRun(docId);
				}
				catch (Exception e) {
					e.printStackTrace(System.out);
				}
				
				//	give the others a little time
				try {
					sleep(1000 * 5);
				} catch (InterruptedException ie) {}
			}
		}
	}
	
	private void handleBatchRun(String docId) throws IOException {
		
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
		command.addElement("TOOLS=" + this.batchImTools); // IM tools to run
		command.addElement("SINGLECORE"); // run on single CPU core only (we don't want to knock out the whole server, do we?)
		
		//	start batch processor slave process
		Process batchRun = Runtime.getRuntime().exec(command.toStringArray(), new String[0], this.workingFolder);
		
		//	loop through error messages
		final BufferedReader importerError = new BufferedReader(new InputStreamReader(batchRun.getErrorStream()));
		new Thread() {
			public void run() {
				try {
					for (String errorLine; (errorLine = importerError.readLine()) != null;)
						System.out.println(errorLine);
				}
				catch (Exception e) {
					e.printStackTrace(System.out);
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
				System.out.println(inLine.substring("S:".length()));
			else if (inLine.startsWith("I:")) {}
			else if (inLine.startsWith("P:")) {}
			else if (inLine.startsWith("BP:")) {}
			else if (inLine.startsWith("MP:")) {}
			else System.out.println(inLine);
		}
		
		//	wait for batch process to finish
		while (true) try {
			batchRun.waitFor();
			break;
		} catch (InterruptedException ie) {}
		
		//	copy back modified entries
		cDocData = new FolderImDocumentData(docFolder, null);
		ImDocumentEntry[] cDocEntries = cDocData.getEntries();
		for (int e = 0; e < cDocEntries.length; e++) {
			InputStream cDocEntryIn = new BufferedInputStream(cDocData.getInputStream(cDocEntries[e]));
			OutputStream docEntryOut = new BufferedOutputStream(docData.getOutputStream(cDocEntries[e]));
			byte[] buffer = new byte[1024];
			for (int r; (r = cDocEntryIn.read(buffer, 0, buffer.length)) != -1;)
				docEntryOut.write(buffer, 0, r);
			docEntryOut.flush();
			docEntryOut.close();
			cDocEntryIn.close();
		}
		
		//	update and release document in IMS
		this.ims.updateDocumentFromData(this.updateUserName, this.updateUserName, docData, new EventLogger() {
			public void writeLog(String logEntry) {
				System.out.println(logEntry);
			}
		});
		this.ims.releaseDocument(this.updateUserName, docId);
		
		//	clean up cache and document data
		this.cleanupFile(cacheFolder);
//		this.cleanupFile(docFolder); // TODO reactivate this once that sucker works
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
