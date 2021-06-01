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
package de.uka.ipd.idaho.goldenGateServer.imp.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Properties;

import de.uka.ipd.idaho.gamta.util.DocumentStyle;
import de.uka.ipd.idaho.gamta.util.DocumentStyle.ParameterDescription;
import de.uka.ipd.idaho.gamta.util.DocumentStyle.ParameterGroupDescription;
import de.uka.ipd.idaho.gamta.util.DocumentStyle.PropertiesData;
import de.uka.ipd.idaho.goldenGateServer.imp.GoldenGateImpConstants.ImpBatchDescriptor;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticatedClient;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticationManager;
import de.uka.ipd.idaho.im.imagine.plugins.AbstractGoldenGateImaginePlugin;

/**
 * Batch selector integration for document style template editor
 * 
 * @author sautter
 */
public class GoldenGateImpDocSytleBatchSelector extends AbstractGoldenGateImaginePlugin {
	private String[] batchNames;
	private Properties batchNamesToLabels = new Properties();
	private boolean batchNamesFromServer = false;
	
	/** usual zero-argument constructor for class loading */
	public GoldenGateImpDocSytleBatchSelector() {}
	
	public String getPluginName() {
		return "IM Server Batch Selector";
	}
	
	public void init() {
		
		//	load cached batch descriptors
		this.loadBatchNames();
		
		//	add parameter group description for server in general (unless done elsewhere before)
		ParameterGroupDescription pgd = DocumentStyle.getParameterGroupDescription("ggServer");
		if (pgd == null) {
			pgd = new ParameterGroupDescription("ggServer");
			pgd.setLabel("Server Side Document Handling");
			pgd.setDescription("Parameters controling the handling of documents inside GoldenGATE Server.");
			DocumentStyle.addParameterGroupDescription(pgd);
		}
		
		//	add parameter group description for IMP
		pgd = new ParameterGroupDescription("ggServer.imp");
		pgd.setLabel("Server Side Batch Processing");
		pgd.setDescription("Parameters controling batch processing of documents inside GoldenGATE Server right after import and decoding.");
		pgd.setParamLabel("batchUserName", "User Name to Use for Batch Processing");
		pgd.setParamDescription("batchUserName", "A custom user name to credit for batch processing of documents in provenance data");
		ParameterDescription pd = new ValueAdHocLoadingParameterDescription("ggServer.imp.batchName");
		pd.setLabel("Batch to Use for Processing");
		pd.setDescription("The batch to use in GoldenGATE IMP for processing documents matching this style template right after decoding");
		pd.setRequired();
		pd.addExcludedParameter("NoBatchProcessing", "batchUserName");
		pgd.setParameterDescription(pd.localName, pd);
		DocumentStyle.addParameterGroupDescription(pgd);
		
		//	put parameters on the map (only ever requested server side)
		DocumentStyle docStyle = new DocumentStyle(new PropertiesData(new Properties()));
		docStyle.getStringProperty("ggServer.imp.batchUserName", null);
		docStyle.getStringProperty("ggServer.imp.batchName", null);
	}
	
	private void loadBatchNames() {
		if (this.batchNamesFromServer)
			return;
		AuthenticatedClient authClient = AuthenticationManager.getAuthenticatedClient(true);
		ImpBatchDescriptor[] ibds = null;
		
		//	no connection to server yet, get any cached descriptors (unless done before)
		if ((authClient == null) && this.dataProvider.isDataAvailable("batchDescriptors.cached.json") && (this.batchNames == null)) try {
			BufferedReader bdBr = new BufferedReader(new InputStreamReader(this.dataProvider.getInputStream("batchDescriptors.cached.json"), "UTF-8"));
			ibds = ImpBatchDescriptor.readJson(bdBr);
			bdBr.close();
		}
		catch (IOException ioe) {
			System.out.println("Could not load cached batch descriptors: " + ioe.getMessage());
			ioe.printStackTrace(System.out);
		}
		
		//	load batch descriptors from server
		if (authClient != null) try {
			GoldenGateImpClient impClient = new GoldenGateImpClient(authClient);
			ibds = impClient.getBatchDescriptors();
			this.batchNamesFromServer = true;
		}
		catch (IOException ioe) {
			System.out.println("Could not load batch descriptors from server: " + ioe.getMessage());
			ioe.printStackTrace(System.out);
		}
		
		//	anything to work with?
		if (ibds == null)
			return;
		
		//	tray up data
		this.batchNames = new String[ibds.length + 1];
		this.batchNames[0] = "NoBatchProcessing";
		this.batchNamesToLabels.put("NoBatchProcessing", "No Batch Processing");
		for (int b = 0; b < ibds.length; b++) {
			this.batchNames[b + 1] = ibds[b].name;
			this.batchNamesToLabels.put(ibds[b].name, ibds[b].label);
		}
		
		//	cache any batch descriptors from server
		if (this.batchNamesFromServer && this.dataProvider.isDataEditable("batchDescriptors.cached.json")) try {
			BufferedWriter bdBw = new BufferedWriter(new OutputStreamWriter(this.dataProvider.getOutputStream("batchDescriptors.cached.json"), "UTF-8"));
			bdBw.write("[");
			for (int b = 0; b < ibds.length; b++) {
				if (b != 0)
					bdBw.write(", ");
				ibds[b].writeJson(bdBw);
			}
			bdBw.write("]");
			bdBw.flush();
			bdBw.close();
		}
		catch (IOException ioe) {
			System.out.println("Could not cache batch descriptors: " + ioe.getMessage());
			ioe.printStackTrace(System.out);
		}

	}
	
	String[] getBatchNames() {
		this.loadBatchNames();
		return this.batchNames;
	}
	
	String getBatchLabel(String batchName) {
		this.loadBatchNames();
		return this.batchNamesToLabels.getProperty(batchName, batchName);
	}
	
	private class ValueAdHocLoadingParameterDescription extends ParameterDescription {
		ValueAdHocLoadingParameterDescription(String fpn) {
			super(fpn);
		}
		public String[] getValues() {
			return getBatchNames();
		}
		public String getValueLabel(String pv) {
			return getBatchLabel(pv);
		}
	}
}
