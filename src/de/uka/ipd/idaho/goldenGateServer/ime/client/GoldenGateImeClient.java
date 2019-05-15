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
package de.uka.ipd.idaho.goldenGateServer.ime.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Properties;

import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection.Connection;
import de.uka.ipd.idaho.goldenGateServer.ime.GoldenGateImeConstants;
import de.uka.ipd.idaho.goldenGateServer.ime.data.ImeDocumentErrorSummary;
import de.uka.ipd.idaho.goldenGateServer.ims.data.ImsDocumentList;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticatedClient;
import de.uka.ipd.idaho.im.util.ImDocumentErrorProtocol;
//import java.security.cert.CertificateEncodingException;
//import java.security.cert.X509Certificate;

/**
 * Client object for GoldenGATE IME.
 * 
 * @author sautter
 */
public class GoldenGateImeClient implements GoldenGateImeConstants {
	private AuthenticatedClient authClient;
	
	/** Constructor
	 * @param authClient the authenticated client to use for communication
	 *            with the backing GoldenGATE IMS
	 */
	public GoldenGateImeClient(AuthenticatedClient authClient) {
		this.authClient = authClient;
	}
	
	/**
	 * Obtain the list of documents available from the IMS. This method will
	 * return only documents that are not checked out by any user (including the
	 * user connected through this client). This is to prevent a document from
	 * being opened more than once simultaneously. If the user this client is
	 * authenticated with has administrative privileges, however, the list will
	 * contain all document, plus the information which user has currently
	 * checked them out. This is to enable administrators to release documents
	 * blocked by other users. If the size of the document list would exceed the
	 * limit configured in the backing IMS and the user this client is
	 * authenticated with does not have administrative privileges, the returned
	 * list will not contain any elements. However, the getListFieldValues()
	 * method of the document list will provide summary sets that can be used as
	 * filter suggestions for the user.
	 * @param pm a progress monitor to observe the loading process
	 * @return the list of documents available from the backing IMS
	 */
	public ImsDocumentList getDocumentList(ProgressMonitor pm) throws IOException {
		return this.getDocumentList(null, pm);
	}
	
	/**
	 * Obtain the list of documents available from the IMS. This method will
	 * return only documents that are not checked out by any user (including the
	 * user connected through this client). This is to prevent a document from
	 * being opened more than once simultaneously. If the user this client is
	 * authenticated with has administrative privileges, however, the list will
	 * contain all document, plus the information which user has currently
	 * checked them out. This is to enable administrators to release documents
	 * blocked by other users. If the size of the document list would exceed the
	 * limit configured in the backing IMS and the user this client is
	 * authenticated with does not have administrative privileges, the returned
	 * list will not contain any elements. However, the getListFieldValues()
	 * method of the document list will provide summary sets that can be used as
	 * filter suggestions for the user.<br>
	 * The filter values specified for for string valued attributes will be
	 * treated as infix filters, e.g. for some part of the name of a document
	 * author. Including the value 'ish' as a filter for document author, for
	 * instance, will return a list that includes documents by all of 'Fisher',
	 * 'Bishop', 'Singuish', and 'Ishgil'. All filter predicates on time-related
	 * attributes will be interpreted as 'later than' or '&gt;' by default. To
	 * specify a custom comparison operator (one of '=', '&lt;', '&lt;=' and
	 * '&gt;='), set an additional property named &lt;attributeName&gt;Operator
	 * to the respective comparison symbol. For specifying several alternatives
	 * for the value of a filter predicate (which makes sense only for
	 * predicates on string valued attributes), separate the individual
	 * alternatives with a line break. All specified filter predicates will be
	 * conjuncted.
	 * @param filter a properties object containing filter predicates for the
	 *            document list
	 * @param pm a progress monitor to observe the loading process
	 * @return the list of documents available from the backing IMS
	 */
	public ImsDocumentList getDocumentList(Properties filter, ProgressMonitor pm) throws IOException {
		
		//	make sure we're logged in
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		//	connect to backend
		final Connection con = this.authClient.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(GET_DOCUMENT_LIST);
		bw.newLine();
		bw.write(this.authClient.getSessionID());
		bw.newLine();
		
		//	send filter (if any)
		if ((filter == null) || filter.isEmpty())
			bw.write("");
		else {
			StringBuffer filterString = new StringBuffer();
			for (Iterator fit = filter.keySet().iterator(); fit.hasNext();) {
				String filterName = ((String) fit.next());
				String filterValue = filter.getProperty(filterName, "");
				String[] filterValues = filterValue.split("[\\r\\n]++");
				for (int v = 0; v < filterValues.length; v++) {
					filterValue = filterValues[v].trim();
					if (filterValue.length() == 0)
						continue;
					if (filterString.length() != 0)
						filterString.append("&");
					filterString.append(filterName + "=" + URLEncoder.encode(filterValues[v], ENCODING));
				}
			}
			bw.write(filterString.toString());
		}
		bw.newLine();
		bw.flush();
		
		//	get document list
		final BufferedReader br = con.getReader();
		String error = br.readLine();
		if (GET_DOCUMENT_LIST.equals(error))
			return ImsDocumentList.readDocumentList(new Reader() {
				public void close() throws IOException {
					br.close();
					con.close();
				}
				public int read(char[] cbuf, int off, int len) throws IOException {
					return br.read(cbuf, off, len);
				}
			});
		
		else {
			con.close();
			throw new IOException(error);
		}
	}
	
	/**
	 * Retrieve an error protocol summary for a specific document. The summary
	 * contains all the error categories and types, and counts for each, but no
	 * actual errors.
	 * @param docId the ID of the document to get the summary for
	 * @return the error summary
	 * @throws IOException
	 */
	public ImeDocumentErrorSummary getErrorSummary(String docId) throws IOException {
		
		//	make sure we're logged in
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		Connection con = null;
		try {
			con = this.authClient.getConnection();
			
			//	connect to backend
			BufferedWriter bw = con.getWriter();
			
			bw.write(GET_ERROR_SUMMARY);
			bw.newLine();
			bw.write(this.authClient.getSessionID());
			bw.newLine();
			bw.write(docId);
			bw.newLine();
			bw.flush();
			
			//	check result
			//	TODO_really??? ZIP these guys, and use streams instead
			final BufferedReader br = con.getReader();
			String error = br.readLine();
			if (!GET_ERROR_SUMMARY.equals(error))
				throw new IOException(error);
			
			//	get error summary
			ImeDocumentErrorSummary ides = new ImeDocumentErrorSummary(docId);
			ImeDocumentErrorSummary.fillErrorSummary(ides, br);
			return ides;
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * Retrieve an error protocol summary for a specific document. The summary
	 * contains all the error categories and types, and counts for each, but no
	 * actual errors.
	 * @param docId the ID of the document to get the summary for
	 * @return the error summary
	 * @throws IOException
	 */
	public ImDocumentErrorProtocol getErrorProtocol(String docId) throws IOException {
		
		//	make sure we're logged in
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		Connection con = null;
		try {
			con = this.authClient.getConnection();
			
			//	connect to backend
			BufferedWriter bw = con.getWriter();
			
			bw.write(GET_ERROR_PROTOCOL);
			bw.newLine();
			bw.write(this.authClient.getSessionID());
			bw.newLine();
			bw.write(docId);
			bw.newLine();
			bw.flush();
			
			//	check result
			//	TODO_really??? ZIP these guys, and use streams instead
			final BufferedReader br = con.getReader();
			String error = br.readLine();
			if (!GET_ERROR_PROTOCOL.equals(error))
				throw new IOException(error);
			
			//	get error summary
			ImDocumentErrorProtocol idep = new ImDocumentErrorProtocol(null);
			ImDocumentErrorProtocol.fillErrorProtocol(idep, br);
			return idep;
		}
		finally {
			if (con != null)
				con.close();
		}
	}
//	
//	/**
//	 * @param args
//	 */
//	public static void main(String[] args) throws Exception {
//		AbstractHttpsEnabler https = new AbstractHttpsEnabler(false) {
//			protected OutputStream getKeyStoreOutputStream() throws IOException {
//				return null;
//			}
//			protected InputStream getKeyStoreInputStream() throws IOException {
//				return null;
//			}
//			protected boolean askPermissionToAccept(X509Certificate[] chain) throws CertificateEncodingException {
//				return true;
//			}
//		};
//		https.init();
//		AuthenticatedClient ac = AuthenticatedClient.getAuthenticatedClient(ServerConnection.getServerConnection("https://tb.plazi.org/GgServer/proxy"));
//		ac.login("plazi", "plabor");
//		GoldenGateImeClient imec = new GoldenGateImeClient(ac);
//		ImsDocumentList docList = imec.getDocumentList(ProgressMonitor.dummy);
//		System.out.println("Got document list: " + Arrays.toString(docList.listFieldNames));
//		while (docList.hasNextDocument()) {
//			ImsDocumentListElement doc = docList.getNextDocument();
//			System.out.println(doc.toTabString(docList.listFieldNames));
//		}
//		imec.getErrorProtocol("1F040F71EF4EFFA9FFC12F02FFED2626");
//		System.out.println("Got error protocol");
//		imec.getErrorSummary("1F040F71EF4EFFA9FFC12F02FFED2626");
//		System.out.println("Got error summary");
//	}
}
