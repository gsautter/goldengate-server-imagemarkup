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
package de.uka.ipd.idaho.goldenGateServer.imp.slave;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.util.ParallelJobRunner;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.gamta.util.imaging.DocumentStyle;
import de.uka.ipd.idaho.goldenGate.GoldenGateConfiguration;
import de.uka.ipd.idaho.goldenGate.GoldenGateConfiguration.ConfigurationDescriptor;
import de.uka.ipd.idaho.goldenGate.configuration.ConfigurationUtils;
import de.uka.ipd.idaho.goldenGate.configuration.FileConfiguration;
import de.uka.ipd.idaho.goldenGate.configuration.UrlConfiguration;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.imagine.GoldenGateImagine;
import de.uka.ipd.idaho.im.imagine.GoldenGateImagineConstants;
import de.uka.ipd.idaho.im.util.ImDocumentData;
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
			else if (USE_SINGLE_CORE_PARAMETER.equals(args[a]))
				useSingleCpuCore = true;
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
		System.setOut(new PrintStream(new OutputStream() {
			public void write(int b) throws IOException {}
		}));
		
		//	get list of image markup tools to run
		if (imtNameString == null) {
			sysOut.println("No Image Markup Tools configured to run, check parameter " + TOOL_SEQUENCE_PARAMETER);
			System.exit(0);
		}
		String[] imtNames = imtNameString.split("\\+");
		
		//	select new configuration
		ConfigurationDescriptor configuration = getConfiguration(basePath, ggiConfigHost, ggiConfigName);
		
		//	check if cancelled
		if (configuration == null) {
			sysOut.println("Configuration '" + ggiConfigName + "' not found, check parameter " + CONFIG_NAME_PARAMETER);
			System.exit(0);
			return;
		}
		
		//	open GoldenGATE Imagine window
		GoldenGateConfiguration ggiConfig = null;
		
		//	local configuration selected
		if (configuration.host == null)
			ggiConfig = new FileConfiguration(configuration.name, new File(new File(basePath, CONFIG_FOLDER_NAME), configuration.name), false, true, null);
		
		//	remote configuration selected
		else ggiConfig = new UrlConfiguration((configuration.host + (configuration.host.endsWith("/") ? "" : "/") + configuration.name), configuration.name);
		
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
		
		//	switch parallel jobs to linear execution if requested to
		if (useSingleCpuCore)
			ParallelJobRunner.setLinear(true);
		
		//	process document
		try {
			
			//	load document from folder
			//	TODO use specialized document data object fetching entries on demand from master process
			ImDocumentData docData = new FolderImDocumentData(new File(docRootPath), null);
			ImDocument doc = ImDocumentIO.loadDocument(docData, pm);
			
			//	test if document style detected
			if (DocumentStyle.getStyleFor(doc) == null) {
				sysOut.println(" - unable to assign document style");
				return;
			}
			else sysOut.println(" - assigned document style '" + ((String) doc.getAttribute(DocumentStyle.DOCUMENT_STYLE_NAME_ATTRIBUTE)) + "'");
			
			//	process document
			for (int t = 0; t < imts.length; t++) {
				sysOut.println("Running Image Markup Tool '" + imts[t].getLabel() + "'");
				imts[t].process(doc, null, null, pm);
			}
			
			//	store updates
			ImDocumentIO.storeDocument(doc, docData, pm);
		}
		
		//	catch and log whatever might go wrong
		catch (Throwable t) {
			sysOut.println("Error processing document: " + t.getMessage());
			t.printStackTrace(sysOut);
		}
		
		//	shut down whatever threads are left
		System.exit(0);
	}
	
	private static ConfigurationDescriptor getConfiguration(File dataBasePath, String configHost, String configName) {
		
		//	get available configurations
		ConfigurationDescriptor[] configurations = getConfigurations(dataBasePath, configHost);
		
		//	get selected configuration, doing update if required
		return ConfigurationUtils.getConfiguration(configurations, configName, dataBasePath, false, false);
	}
	
	private static ConfigurationDescriptor[] getConfigurations(File dataBasePath, String configHost) {
		
		//	collect configurations
		final ArrayList configList = new ArrayList();
		
		//	load local non-default configurations
		ConfigurationDescriptor[] configs = ConfigurationUtils.getLocalConfigurations(dataBasePath);
		for (int c = 0; c < configs.length; c++) {
			if (configs[c].name.endsWith(".imagine"))
				configList.add(configs[c]);
		}
		
		//	get downloaded zip files
		configs = ConfigurationUtils.getZipConfigurations(dataBasePath);
		for (int c = 0; c < configs.length; c++) {
			if (configs[c].name.endsWith(".imagine"))
				configList.add(configs[c]);
		}
		
		//	get remote configurations
		if (configHost != null) {
			configs = ConfigurationUtils.getRemoteConfigurations(configHost);
			for (int c = 0; c < configs.length; c++) {
				if (configs[c].name.endsWith(".imagine"))
					configList.add(configs[c]);
			}
		}
		
		//	finally ...
		return ((ConfigurationDescriptor[]) configList.toArray(new ConfigurationDescriptor[configList.size()]));
	}
}