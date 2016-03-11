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
package de.uka.ipd.idaho.goldenGateServer.imi.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;

import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection.Connection;
import de.uka.ipd.idaho.goldenGateServer.imi.GoldenGateImiConstants;
import de.uka.ipd.idaho.goldenGateServer.util.BufferedLineOutputStream;

/**
 * @author sautter
 *
 */
public class GoldenGateImiClient implements GoldenGateImiConstants {
	
	private ServerConnection serverConnection;
	
	/**
	 * Constructor
	 * @param serverConnection the ServerConnection to use for communication
	 *            with the backing BDP
	 */
	public GoldenGateImiClient(ServerConnection serverConnection) {
		this.serverConnection = serverConnection;
	}
	
	/**
	 * Upload a document to the backing IMI.
	 * @param docDataSource an input stream providing the actual document data
	 * @param docDataName an name of the document source, e.g. a file name
	 * @param docDataSize an size of the document source in bytes
	 * @param mimeType the MIME type of the document
	 * @param docAttributes additional attributes to the document
	 * @param user the user to credit for the upload
	 * @return a properties object holding the upload attributes
	 * @throws IOException
	 */
	public Properties uploadDocument(InputStream docDataSource, String uploadId, String docDataName, int docDataSize, String mimeType, Properties docAttributes, String user) throws IOException {
		Connection con = this.serverConnection.getConnection();
		try {
			BufferedLineOutputStream out = con.getOutputStream();
			
			//	write command
			out.writeLine(UPLOAD_DOCUMENT);
			
			//	send upload ID (makes for good cache file name)
			out.writeLine(uploadId);
			
			//	send MIME type
			out.writeLine(mimeType);
			
			//	send file name
			out.writeLine(docDataName);
			
			//	send file name
			out.writeLine("" + docDataSize);
			
			//	send user name
			out.writeLine((user == null) ? "" : user);
			
			//	send attributes (watch line breaks)
			for (Enumeration ane = docAttributes.propertyNames(); ane.hasMoreElements();) {
				String an = ((String) ane.nextElement());
				String av = docAttributes.getProperty(an);
				if (av != null)
					out.writeLine(an + "=" + av);
			}
			
			//	send separator line
			out.newLine();
			
			//	send document data
			byte[] buffer = new byte[1024];
			for (int r; (r = docDataSource.read(buffer, 0, buffer.length)) != -1;)
				out.write(buffer, 0, r);
			out.flush();
			
			//	read response / error message
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (!UPLOAD_DOCUMENT.equals(error))
				throw new IOException(error);
			
			//	read document attributes, and keep them in order
			Properties uDocAttributes = new Properties() {
				private Vector keys = new Vector();
				public synchronized Object setProperty(String key, String value) {
					Object oldValue = super.setProperty(key, value);
					if (oldValue == null)
						this.keys.add(key);
					return oldValue;
				}
				public Enumeration propertyNames() {
					return this.keys.elements();
				}
			};
			uDocAttributes.putAll(docAttributes);
			for (String dal; (dal = br.readLine()) != null;) {
				
				//	invalid line
				if (dal.indexOf('=') == -1)
					continue;
				
				//	separate attribute name from value
				String an = dal.substring(0, dal.indexOf('='));
				String av = dal.substring(dal.indexOf('=') + "=".length());
				
				//	store attribute
				uDocAttributes.setProperty(an, av);
			}
			
			//	finally ...
			return uDocAttributes;
		}
		
		//	make sure to disconnect
		finally {
			con.close();
		}
	}
}