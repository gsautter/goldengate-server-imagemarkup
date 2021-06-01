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

import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection.Connection;
import de.uka.ipd.idaho.goldenGateServer.imp.GoldenGateImpConstants;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticatedClient;

/**
 * Client object for GoldenGATE Image Markup Processing service.
 * 
 * @author sautter
 */
public class GoldenGateImpClient implements GoldenGateImpConstants {
	private AuthenticatedClient authClient;
	
	/** Constructor
	 * @param authClient the authenticated client to communicate through
	 */
	public GoldenGateImpClient(AuthenticatedClient authClient) {
		this.authClient = authClient;
	}
	
	/**
	 * Retrieve descriptors of the batches that are available on the backing
	 * IMP to the user logged in on the wrapped authenticated client.
	 * @return an array holding the batch descriptors
	 */
	public ImpBatchDescriptor[] getBatchDescriptors() throws IOException {
		
		//	make sure we're logged in and connect to backend
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		Connection con = this.authClient.getConnection();
		
		//	send request
		BufferedWriter bw = con.getWriter();
		bw.write(GET_BATCH_DESCRIPTORS);
		bw.newLine();
		bw.write(this.authClient.getSessionID());
		bw.newLine();
		bw.flush();
		
		//	read batch descriptors
		try {
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (GET_BATCH_DESCRIPTORS.equals(error))
				return ImpBatchDescriptor.readJson(br);
			else throw new IOException(error);
		}
		finally {
			con.close();
		}
	}
	
	/**
	 * Retrieve the detailed description of a batch with a given name. The
	 * returned string will usually represent an HTML snippet.
	 * @param batchName the name of the batch to get the description for
	 * @return the description of the batch with the argument name
	 */
	public String getBatchDescription(String batchName) throws IOException {
		
		//	make sure we're logged in and connect to backend
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		Connection con = this.authClient.getConnection();
		
		//	send request
		BufferedWriter bw = con.getWriter();
		bw.write(GET_BATCH_DESCRIPTION);
		bw.newLine();
		bw.write(this.authClient.getSessionID());
		bw.newLine();
		bw.write(batchName);
		bw.newLine();
		bw.flush();
		
		//	read batch description
		try {
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (GET_BATCH_DESCRIPTION.equals(error)) {
				StringBuffer ibd = new StringBuffer();
				char[] ibdBuffer = new char[1024];
				for (int r; (r = br.read(ibdBuffer, 0, ibdBuffer.length)) != -1;)
					ibd.append(ibdBuffer, 0, r);
				return ibd.toString();
			}
			else throw new IOException(error);
		}
		finally {
			con.close();
		}
	}
	
	/**
	 * Schedule processing of a document with a given ID through a batch with a
	 * given name.
	 * @param docId the ID of the document to process
	 * @param batchName the name of the batch to use for processing
	 */
	public void scheduleBatchProcessing(String docId, String batchName) throws IOException {
		
		//	make sure we're logged in and connect to backend
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		Connection con = this.authClient.getConnection();
		
		//	send request
		BufferedWriter bw = con.getWriter();
		bw.write(SCHEDULE_BATCH_PROCESSING);
		bw.newLine();
		bw.write(this.authClient.getSessionID());
		bw.newLine();
		bw.write(docId);
		bw.newLine();
		bw.write(batchName);
		bw.newLine();
		bw.flush();
		
		//	get response and check for error message
		try {
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (!SCHEDULE_BATCH_PROCESSING.equals(error))
				throw new IOException(error);
		}
		finally {
			con.close();
		}
	}
//	
//	//	TODOne use this for testing once update deployed
//	public static void main(String[] args) throws Exception {
//		ApplicationHttpsEnabler.enableHttps();
//		ServerConnection sc = ServerConnection.getServerConnection("https://tb.plazi.org/GgServer/proxy");
//		AuthenticatedClient authClient = AuthenticatedClient.getAuthenticatedClient(sc);
//		authClient.login("guido", ""); // TODO always clear PWD for builds, this is client side code !!!
//		GoldenGateImpClient impc = new GoldenGateImpClient(authClient);
//		ImpBatchDescriptor[] ibds = impc.getBatchDescriptors();
//		for (int b = 0; b < ibds.length; b++) {
//			StringWriter sw = new StringWriter();
//			ibds[b].writeJson(sw);
//			System.out.println(sw.toString());
//		}
//	}
}
