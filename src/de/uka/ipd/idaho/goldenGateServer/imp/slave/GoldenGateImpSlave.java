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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.util.DocumentStyle;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.goldenGate.GoldenGateConfiguration;
import de.uka.ipd.idaho.goldenGate.configuration.ConfigurationUtils;
import de.uka.ipd.idaho.goldenGate.plugins.GoldenGatePlugin;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.MasterProcessInterface;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveConstants;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveRuntimeUtils;
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
public class GoldenGateImpSlave implements GoldenGateImagineConstants, SlaveConstants {
	private static final String CONFIG_HOST_PARAMETER = "CONFHOST";
	private static final String CONFIG_NAME_PARAMETER = "CONFNAME";
	private static final String TOOL_SEQUENCE_PARAMETER = "TOOLS";
	private static final String LIST_TOOLS_SEQUENCE_NAME = "LISTTOOLS";
//	private static final String WAIVE_DOCUMENT_SYTLE_PARAMETER = "WAIVEDS";
	private static final String DOCUMENT_SYTLE_MODE_PARAMETER = "DSMODE";
	
	/**	the main method to run GoldenGATE Imagine as a batch application
	 */
	public static void main(String[] args) throws Exception {
//		
//		//	adjust basic parameters
//		String basePathStr = ".";
//		String cacheRootPath = null;
//		String docRootPath = null;
//		String logRootPath = null;
//		String ggiConfigHost = null;
//		String ggiConfigName = null;
//		String imtNameString = null;
//		boolean requireDocStyle = true;
//		boolean forkSystemOut = false;
//		int maxCpuCores = -1;
//		boolean useSingleCpuCore = false;
//		
//		//	parse remaining args
//		for (int a = 0; a < args.length; a++) {
//			if (args[a] == null)
//				continue;
//			if (args[a].startsWith(CACHE_PATH_PARAMETER + "="))
//				cacheRootPath = args[a].substring((CACHE_PATH_PARAMETER + "=").length());
//			else if (args[a].startsWith(DATA_PATH_PARAMETER + "="))
//				docRootPath = args[a].substring((DATA_PATH_PARAMETER + "=").length());
//			else if (args[a].startsWith(LOG_PATH_PARAMETER + "="))
//				logRootPath = args[a].substring((LOG_PATH_PARAMETER + "=").length());
//			else if (args[a].startsWith(CONFIG_HOST_PARAMETER + "="))
//				ggiConfigHost = args[a].substring((CONFIG_HOST_PARAMETER + "=").length());
//			else if (args[a].startsWith(CONFIG_NAME_PARAMETER + "="))
//				ggiConfigName = args[a].substring((CONFIG_NAME_PARAMETER + "=").length());
//			else if (args[a].startsWith(TOOL_SEQUENCE_PARAMETER + "="))
//				imtNameString = args[a].substring((TOOL_SEQUENCE_PARAMETER + "=").length());
//			else if (WAIVE_DOCUMENT_SYTLE_PARAMETER.equals(args[a]))
//				requireDocStyle = false;
//			else if (VERBOSE_PARAMETER.equals(args[a]))
//				forkSystemOut = true;
//			else if (USE_SINGLE_CORE_PARAMETER.equals(args[a]))
//				useSingleCpuCore = true;
//			else if (args[a].startsWith(MAX_CORES_PARAMETER + "=")) try {
//				maxCpuCores = Integer.parseInt(args[a].substring((MAX_CORES_PARAMETER + "=").length()).trim());
//			} catch (RuntimeException re) {}
//		}
		
		//	adjust basic parameters
		Properties argsMap = SlaveRuntimeUtils.parseArguments(args);
		
		//	set up communication with master (before logging tampers with output streams)
		ImpMasterProcessInterface mpi = new ImpMasterProcessInterface();
		
		//	get list of image markup tools to run
		String imtNameString = argsMap.getProperty(TOOL_SEQUENCE_PARAMETER);
		if (imtNameString == null) {
			mpi.sendError("No Image Markup Tools configured to run, check parameter " + TOOL_SEQUENCE_PARAMETER);
			System.exit(0);
		}
		String[] imtNames = imtNameString.split("\\+");
		
		//	set up logging (if we have a folder)
		SlaveRuntimeUtils.setUpLogFiles(argsMap, "ImpSlaveBatch");
		
		//	start receiving control commands from master process
		mpi.start();
		
		//	remember program base path
		File basePath = new File(".");
		
		//	get GoldenGATE Imagine configuration
		String ggiConfigHost = argsMap.getProperty(CONFIG_HOST_PARAMETER);
		String ggiConfigName = argsMap.getProperty(CONFIG_NAME_PARAMETER);
		GoldenGateConfiguration ggiConfig = ConfigurationUtils.getConfiguration(ggiConfigName, null, ggiConfigHost, basePath);
		
		//	check if configuration found
		if (ggiConfig == null) {
			mpi.sendError("Configuration '" + ggiConfigName + "' not found, check parameter " + CONFIG_NAME_PARAMETER);
//			System.exit(0);
			return;
		}
		
		//	if cache path set, add settings for page image and supplement cache
		String cacheRootPath = argsMap.getProperty(CACHE_PATH_PARAMETER);
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
		mpi.sendResult("GoldenGATE Imagine core created, configuration is " + ggiConfigName);
		
		//	list markup tools
		if (LIST_TOOLS_SEQUENCE_NAME.equals(imtNameString)) {
			ImageMarkupToolProvider[] imtps = goldenGateImagine.getImageMarkupToolProviders();
			for (int p = 0; p < imtps.length; p++) {
				String imtpName = imtps[p].getMainMenuTitle();
				if (imtpName == null)
					imtpName = imtps[p].getPluginName();
				if (imtpName == null)
					continue;
				TreeSet pImtNames = new TreeSet();
				String[] pemImtNames = imtps[p].getEditMenuItemNames();
				if(pemImtNames != null)
					pImtNames.addAll(Arrays.asList(pemImtNames));
				String[] ptmImtNames = imtps[p].getToolsMenuItemNames();
				if(ptmImtNames != null)
					pImtNames.addAll(Arrays.asList(ptmImtNames));
				if(pImtNames.isEmpty())
					continue;
				mpi.sendOutput("MTP:" + imtpName + " (" + pImtNames.size() + "):");
				for (Iterator tnit = pImtNames.iterator(); tnit.hasNext();)
					mpi.sendOutput("MT:" + tnit.next());
			}
//			System.exit(0);
			return;
		}
		
		//	get individual image markup tools
		ImageMarkupTool[] imts = new ImageMarkupTool[imtNames.length];
		for (int t = 0; t < imtNames.length; t++) {
			imts[t] = goldenGateImagine.getImageMarkupToolForName(imtNames[t]);
			if (imts[t] == null) {
				mpi.sendError("Image Markup Tool '" + imtNames[t] + "' not found, check parameter " + TOOL_SEQUENCE_PARAMETER);
//				System.exit(0);
				return;
			}
			else mpi.sendResult("Image Markup Tool '" + imtNames[t] + "' loaded");
		}
		
		//	impose parallel processing limitations
		SlaveRuntimeUtils.setUpMaxCores(argsMap);
		
		//	load document from folder
		String docRootPath = argsMap.getProperty(DATA_PATH_PARAMETER);
		File docFolder = new File(docRootPath);
		SlaveImDocumentData docData = new SlaveImDocumentData(docFolder, mpi);
		
		//	create progress monitor reporting back to master
		ProgressMonitor pm = mpi.createProgressMonitor();
		
		//	process document
		try {
			ImDocument doc = ImDocumentIO.loadDocument(docData, pm);
			goldenGateImagine.notifyDocumentOpened(doc, docFolder, pm);
			
			//	test if document style detected
			String docStyleMode = argsMap.getProperty(DOCUMENT_SYTLE_MODE_PARAMETER, "R");
			
			//	remove at least any plug-in based providers if ignoring document style templates
			if ("I".equals(docStyleMode)) {
				GoldenGatePlugin[] dsps = goldenGateImagine.getImplementingPlugins(DocumentStyle.Provider.class);
				for (int p = 0; p < dsps.length; p++)
					DocumentStyle.removeProvider((DocumentStyle.Provider) dsps[p]);
			}
			
			//	get and check document style template otherwise
			else {
				DocumentStyle docStyle = DocumentStyle.getStyleFor(doc);
				if (docStyle != null)
					mpi.sendOutput(" - assigned document style '" + ((String) doc.getAttribute(DocumentStyle.DOCUMENT_STYLE_NAME_ATTRIBUTE)) + "'");
				else if ("U".equals(docStyleMode))
					mpi.sendOutput(" - could not assigned document style");
				else {
					mpi.sendError("Unable to assign document style");
					return;
				}
			}
			
			//	process document
			for (int t = 0; t < imts.length; t++) {
				mpi.sendOutput("PR:" + imts[t].getLabel());
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
			mpi.sendError("Error processing document: " + t.getMessage());
			mpi.sendError(t);
		}
//		
//		//	shut down whatever threads are left
//		System.exit(0);
	}
	
	private static class ImpMasterProcessInterface extends MasterProcessInterface {
		private SlaveImDocumentData docData;
		void setDocData(SlaveImDocumentData docData) {
			this.docData = docData;
		}
		protected void handleInput(String input) {
			if (input.startsWith("DEC:")) {
				String docEntryName = input.substring("DEC:".length());
				docData.notifyEntryRequestComplete(docEntryName);
			}
			else if (input.startsWith("DEN:")) {
				String docEntryName = input.substring("DEN:".length());
				docData.notifyEntryRequestError(docEntryName, docEntryName);
			}
			else if (input.startsWith("DEE:")) {
				input = input.substring("DEE:".length());
				String docEntryName;
				String docEntryError;
				if (input.indexOf('\t') == -1) {
					docEntryName = input;
					docEntryError = input;
				}
				else {
					docEntryName = input.substring(0, input.indexOf('\t'));
					docEntryError = input.substring(input.indexOf('\t') + "\t".length());
				}
				docData.notifyEntryRequestError(docEntryName, docEntryError);
			}
		}
	}
	
	private static class SlaveImDocumentData extends FolderImDocumentData {
		ImpMasterProcessInterface mpi;
		SlaveImDocumentData(File docFolder, ImpMasterProcessInterface mpi) throws IOException {
			super(docFolder, null);
			this.mpi = mpi;
			this.mpi.setDocData(this);
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
				mpi.sendOutput("DER:" + this.entryName);
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