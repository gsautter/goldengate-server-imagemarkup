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
package de.uka.ipd.idaho.goldenGateServer.imp.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.util.ParallelJobRunner;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.gamta.util.imaging.DocumentStyle;
import de.uka.ipd.idaho.goldenGate.GoldenGateConfiguration;
import de.uka.ipd.idaho.goldenGate.configuration.ConfigurationUtils;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.imagine.GoldenGateImagine;
import de.uka.ipd.idaho.im.imagine.GoldenGateImagineConstants;
import de.uka.ipd.idaho.im.imagine.plugins.ImageMarkupToolProvider;
import de.uka.ipd.idaho.im.util.ImDocumentData.FolderImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentIO;
import de.uka.ipd.idaho.im.util.ImDocumentMarkupPanel.ImageMarkupTool;

/**
 * Stripped-down copy of GoldenGATE Imagine's batch runner utility, loading
 * documents from a folder rather than directly from PDFs, with options
 * restricted to specific application needs.
 * 
 * @author sautter
 */
public class GoldenGateImpSlave implements GoldenGateImagineConstants {
	private static final String CACHE_PATH_PARAMETER = "CACHE";
	private static final String DATA_PATH_PARAMETER = "DATA";
	private static final String CONFIG_HOST_PARAMETER = "CONFHOST";
	private static final String CONFIG_NAME_PARAMETER = "CONFNAME";
	private static final String TOOL_SEQUENCE_PARAMETER = "TOOLS";
	private static final String LIST_TOOLS_SEQUENCE_NAME = "LISTTOOLS";
	private static final String WAIVE_DOCUMENT_SYTLE_PARAMETER = "WAIVEDS";
	private static final String VERBOSE_PARAMETER = "VERBOSE";
	private static final String MAX_CORES_PARAMETER = "MAXCORES";
	private static final String USE_SINGLE_CORE_PARAMETER = "SINGLECORE";
	
	/**	the main method to run GoldenGATE Imagine as a batch application
	 */
	public static void main(String[] args) throws Exception {
		
		//	adjust basic parameters
		String basePathStr = ".";
		String cacheRootPath = null;
		String docRootPath = null;
		String ggiConfigHost = null;
		String ggiConfigName = null;
		String imtNameString = null;
		boolean requireDocStyle = true;
		boolean silenceSystemOut = true;
		int maxCpuCores = -1;
		boolean useSingleCpuCore = false;
		
		//	parse remaining args
		for (int a = 0; a < args.length; a++) {
			if (args[a] == null)
				continue;
			if (args[a].startsWith(CACHE_PATH_PARAMETER + "="))
				cacheRootPath = args[a].substring((CACHE_PATH_PARAMETER + "=").length());
			else if (args[a].startsWith(DATA_PATH_PARAMETER + "="))
				docRootPath = args[a].substring((DATA_PATH_PARAMETER + "=").length());
			else if (args[a].startsWith(CONFIG_HOST_PARAMETER + "="))
				ggiConfigHost = args[a].substring((CONFIG_HOST_PARAMETER + "=").length());
			else if (args[a].startsWith(CONFIG_NAME_PARAMETER + "="))
				ggiConfigName = args[a].substring((CONFIG_NAME_PARAMETER + "=").length());
			else if (args[a].startsWith(TOOL_SEQUENCE_PARAMETER + "="))
				imtNameString = args[a].substring((TOOL_SEQUENCE_PARAMETER + "=").length());
			else if (WAIVE_DOCUMENT_SYTLE_PARAMETER.equals(args[a]))
				requireDocStyle = false;
			else if (VERBOSE_PARAMETER.equals(args[a]))
				silenceSystemOut = false;
			else if (USE_SINGLE_CORE_PARAMETER.equals(args[a]))
				useSingleCpuCore = true;
			else if (args[a].startsWith(MAX_CORES_PARAMETER + "=")) try {
				maxCpuCores = Integer.parseInt(args[a].substring((MAX_CORES_PARAMETER + "=").length()).trim());
			} catch (RuntimeException re) {}
		}
		
		//	remember program base path
		File basePath = new File(basePathStr);
		
		//	preserve System.out and System.err
		final PrintStream sysOut = System.out;
		final PrintStream sysErr = System.err;
		
		//	set up logging
		ProgressMonitor pm = new ProgressMonitor() {
			public void setStep(String step) {
				sysOut.println("S:" + step);
			}
			public void setInfo(String info) {
				sysOut.println("I:" + info);
			}
			public void setBaseProgress(int baseProgress) {
				sysOut.println("BP:" + baseProgress);
			}
			public void setMaxProgress(int maxProgress) {
				sysOut.println("MP:" + maxProgress);
			}
			public void setProgress(int progress) {
				sysOut.println("P:" + progress);
			}
		};
		
		//	silence System.out
		if (silenceSystemOut)
			System.setOut(new PrintStream(new OutputStream() {
				public void write(int b) throws IOException {}
			}));
		
		//	get list of image markup tools to run
		if (imtNameString == null) {
			sysOut.println("No Image Markup Tools configured to run, check parameter " + TOOL_SEQUENCE_PARAMETER);
			System.exit(0);
		}
		String[] imtNames = imtNameString.split("\\+");
		
		//	get GoldenGATE Imagine configuration
		GoldenGateConfiguration ggiConfig = ConfigurationUtils.getConfiguration(ggiConfigName, null, ggiConfigHost, basePath);
		
		//	check if configuration found
		if (ggiConfig == null) {
			sysOut.println("Configuration '" + ggiConfigName + "' not found, check parameter " + CONFIG_NAME_PARAMETER);
			System.exit(0);
			return;
		}
		
		//	if cache path set, add settings for page image and supplement cache
		if (cacheRootPath != null) {
			if (!cacheRootPath.endsWith("/"))
				cacheRootPath += "/";
			Settings set = ggiConfig.getSettings();
			set.setSetting("cacheRootFolder", cacheRootPath);
			set.setSetting("pageImageFolder", (cacheRootPath + "PageImages"));
			set.setSetting("supplementFolder", (cacheRootPath + "Supplements"));
		}
		
		//	instantiate GoldenGATE Imagine
		GoldenGateImagine goldenGateImagine = GoldenGateImagine.openGoldenGATE(ggiConfig, basePath, false);
		sysOut.println("GoldenGATE Imagine core created, configuration is " + ggiConfigName);
		
		//	list markup tools
		if (LIST_TOOLS_SEQUENCE_NAME.equals(imtNameString)) {
			ImageMarkupToolProvider[] imtps = goldenGateImagine.getImageMarkupToolProviders();
			for (int p = 0; p < imtps.length; p++) {
				String imtpName = imtps[p].getMainMenuTitle();
				if (imtpName == null)
					imtpName = imtps[p].getPluginName();
				if (imtpName == null)
					continue;
				String[] pImtNames = imtps[p].getToolsMenuItemNames();
				if(pImtNames == null)
					continue;
				if(pImtNames.length == 0)
					continue;
				sysOut.println("MTP:" + imtpName + " (" + pImtNames.length + "):");
				for (int t = 0; t < pImtNames.length; t++)
					sysOut.println("MT: - " + pImtNames[t]);
			}
			System.exit(0);
		}
		
		//	get individual image markup tools
		ImageMarkupTool[] imts = new ImageMarkupTool[imtNames.length];
		for (int t = 0; t < imtNames.length; t++) {
			imts[t] = goldenGateImagine.getImageMarkupToolForName(imtNames[t]);
			if (imts[t] == null) {
				sysOut.println("Image Markup Tool '" + imtNames[t] + "' not found, check parameter " + TOOL_SEQUENCE_PARAMETER);
				System.exit(0);
			}
			else sysOut.println("Image Markup Tool '" + imtNames[t] + "' loaded");
		}
		
		//	switch parallel jobs to linear or limited parallel execution if requested to
		if (maxCpuCores == 1)
			useSingleCpuCore = true;
		if (useSingleCpuCore)
			ParallelJobRunner.setLinear(true);
		else if (1 < maxCpuCores)
			ParallelJobRunner.setMaxCores(maxCpuCores);
		
		//	load document from folder
		File docFolder = new File(docRootPath);
		final SlaveImDocumentData docData = new SlaveImDocumentData(docFolder, sysOut);
		
		//	listen on system in for commands in dedicated thread
		final Thread mainThread = Thread.currentThread();
		final BufferedReader fromMaster = new BufferedReader(new InputStreamReader(System.in));
		new Thread() {
			public void run() {
				try {
					for (String inLine; (inLine = fromMaster.readLine()) != null;) {
						if (inLine.startsWith("DEC:")) {
							String docEntryName = inLine.substring("DEC:".length());
							docData.notifyEntryRequestComplete(docEntryName);
						}
						else if (inLine.startsWith("DEN:")) {
							String docEntryName = inLine.substring("DEN:".length());
							docData.notifyEntryRequestError(docEntryName, docEntryName);
						}
						else if (inLine.startsWith("DEE:")) {
							inLine = inLine.substring("DEE:".length());
							String docEntryName;
							String docEntryError;
							if (inLine.indexOf('\t') == -1) {
								docEntryName = inLine;
								docEntryError = inLine;
							}
							else {
								docEntryName = inLine.substring(0, inLine.indexOf('\t'));
								docEntryError = inLine.substring(inLine.indexOf('\t') + "\t".length());
							}
							docData.notifyEntryRequestError(docEntryName, docEntryError);
						}
						else if (inLine.equals("DSS:")) {
							StackTraceElement[] stes = mainThread.getStackTrace();
							for (int e = 0; e < stes.length; e++)
								sysOut.println("SST:  at " + stes[e].toString());
							sysOut.println("SSC:");
						}
						else sysOut.println(inLine);
					}
				}
				catch (Exception e) {
					sysOut.println(e.getMessage());
				}
			}
		}.start();
		
		//	process document
		try {
			ImDocument doc = ImDocumentIO.loadDocument(docData, pm);
			goldenGateImagine.notifyDocumentOpened(doc, docFolder, pm);
			
			//	test if document style detected
			if (requireDocStyle) {
				if (DocumentStyle.getStyleFor(doc) == null) {
					sysOut.println(" - unable to assign document style");
					return;
				}
				else sysOut.println(" - assigned document style '" + ((String) doc.getAttribute(DocumentStyle.DOCUMENT_STYLE_NAME_ATTRIBUTE)) + "'");
			}
			
			//	process document
			for (int t = 0; t < imts.length; t++) {
				sysOut.println("PR:" + imts[t].getLabel());
				imts[t].process(doc, null, null, pm);
			}
			
			//	store updates
			goldenGateImagine.notifyDocumentSaving(doc, docFolder, pm);
			ImDocumentIO.storeDocument(doc, docData, pm);
			goldenGateImagine.notifyDocumentSaved(doc, docFolder, pm);
			goldenGateImagine.notifyDocumentClosed(doc.docId);
			doc.dispose();
		}
		
		//	catch and log whatever might go wrong
		catch (Throwable t) {
			sysOut.println("Error processing document: " + t.getMessage());
			t.printStackTrace(sysOut);
		}
		
		//	shut down whatever threads are left
		System.exit(0);
	}
	
	private static class SlaveImDocumentData extends FolderImDocumentData {
		private PrintStream sysOut;
		SlaveImDocumentData(File docFolder, PrintStream sysOut) throws IOException {
			super(docFolder, null);
			this.sysOut = sysOut;
		}
		public InputStream getInputStream(String entryName) throws IOException {
			ImDocumentEntry entry = this.getEntry(entryName);
			if (entry == null)
				throw new FileNotFoundException(entryName);
			if (this.hasEntryData(entry))
				return super.getInputStream(entryName);
			return this.sendEntryRequest(entryName).getInputStreamWhenComplete();
		}
		private Map entryRequestsByName = Collections.synchronizedMap(new HashMap());
		synchronized ImDocumentEntryRequest sendEntryRequest(String entryName) {
			ImDocumentEntryRequest der = ((ImDocumentEntryRequest) this.entryRequestsByName.get(entryName));
			if (der == null) {
				der = new ImDocumentEntryRequest(entryName);
				this.entryRequestsByName.put(entryName, der);
			}
			return der;
		}
		void notifyEntryRequestComplete(String entryName) {
			ImDocumentEntryRequest der = ((ImDocumentEntryRequest) this.entryRequestsByName.remove(entryName));
			if (der != null)
				der.notifyComplete();
		}
		void notifyEntryRequestError(String entryName, String error) {
			ImDocumentEntryRequest der = ((ImDocumentEntryRequest) this.entryRequestsByName.remove(entryName));
			if (der != null)
				der.notifyError(error);
		}
		private class ImDocumentEntryRequest {
			private String entryName;
			private String error;
			ImDocumentEntryRequest(String entryName) {
				this.entryName = entryName;
			}
			synchronized InputStream getInputStreamWhenComplete() throws IOException {
				sysOut.println("DER:" + this.entryName);
				while (true) try {
					this.wait();
					break;
				} catch (InterruptedException ie) {}
				if (this.error == null)
					return getInputStream(this.entryName);
				else if (this.error.equals(this.entryName))
					throw new FileNotFoundException(this.entryName);
				else throw new IOException(this.error);
			}
			synchronized void notifyComplete() {
				this.notifyAll();
			}
			synchronized void notifyError(String error) {
				this.error = error;
				this.notifyAll();
			}
		}
	}
}