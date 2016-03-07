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
package de.uka.ipd.idaho.goldenGateServer.ims.client;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import de.uka.ipd.idaho.gamta.util.ControllingProgressMonitor;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection.Connection;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants;
import de.uka.ipd.idaho.goldenGateServer.ims.data.ImsDocumentList;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticatedClient;
import de.uka.ipd.idaho.goldenGateServer.util.BufferedLineInputStream;
import de.uka.ipd.idaho.goldenGateServer.util.BufferedLineOutputStream;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.util.ImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.FolderImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.ImDocumentEntry;
import de.uka.ipd.idaho.im.util.ImDocumentIO;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * Network client for GoldenGATE Image Markup Storage facility.
 * 
 * @author sautter
 */
public class GoldenGateImsClient implements GoldenGateImsConstants {
	
	/**
	 * A cache for the entry data of Image Markup documents. 
	 * 
	 * @author sautter
	 */
	public static interface DocumentDataCache {
		
		/**
		 * Create a document data object for a document with a given ID.
		 * @param docId the document ID
		 * @return a document data object
		 * @throws IOException
		 */
		public abstract ImDocumentData getDocumentData(String docId) throws IOException;
		
		/**
		 * Store the entry list of a document data object. The runtime type of
		 * the argument document data object is that of the document data
		 * objects returned by the <code>getDocumentData()</code> method.
		 * @param docData the document data object whose entry list to store
		 * @throws IOException
		 */
		public abstract void storeEntryList(ImDocumentData docData) throws IOException;
	}
	
	/**
	 * Image Markup document data object loading entries on demand from a backing
	 * GoldenGATE Image Markup Store. Once an entry is loaded, it is cached in the
	 * document data object handed to the constructor. Entries that are updated are
	 * first stored in that document data object as well, and only calling the
	 * <code>pushUpdates()</code> will send the updates to the backing IMS.
	 * 
	 * @author sautter
	 */
	public static class ImsClientDocumentData extends ImDocumentData {
		private ImDocumentData localDocData;
		private String docId;
		private GoldenGateImsClient imsClient;
		
		/** Constructor
		 * @param docId the ID of the document represented by this data object
		 * @param entries the document entries available remotely
		 * @param imsClient the IMS client to use for server interaction
		 * @param localDocData the document data object to use for local storage
		 * @throws IOException
		 */
		public ImsClientDocumentData(String docId, ImDocumentEntry[] entries, GoldenGateImsClient imsClient, ImDocumentData localDocData) throws IOException {
			this.imsClient = imsClient;
			this.docId = docId;
			this.localDocData = localDocData;
			
			//	put existing entries from local data
			ImDocumentEntry[] localEntries = this.localDocData.getEntries();
			for (int e = 0; e < localEntries.length; e++)
				this.putEntry(localEntries[e]);
			
			//	put entries only second, to give priority to remote versions
			for (int e = 0; e < entries.length; e++)
				this.putEntry(entries[e]);
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.im.util.ImDocumentData#hasEntryData(de.uka.ipd.idaho.im.util.ImDocumentData.ImDocumentEntry)
		 */
		public boolean hasEntryData(ImDocumentEntry entry) {
			if (this.localDocData.hasEntryData(entry))
				return true;
			ImDocumentEntry imsEntry = this.getEntry(entry.name);
			return entry.getFileName().equals(imsEntry.getFileName());
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.im.util.ImDocumentData#getInputStream(java.lang.String)
		 */
		public InputStream getInputStream(String entryName) throws IOException {
			ImDocumentEntry entry = this.getEntry(entryName);
			
			//	we don't know this entry at all, not even remotely
			if (entry == null)
				throw new FileNotFoundException(entryName);
			
			//	entry data already local
			if (this.localDocData.hasEntryData(entry))
				return this.localDocData.getInputStream(entryName);
			
			//	fetch entry from backing IMS
			return this.imsClient.getDocumentEntry(this.docId, entry, this);
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.im.util.ImDocumentData#getOutputStream(java.lang.String, boolean)
		 */
		public OutputStream getOutputStream(final String entryName, boolean writeDirectly) throws IOException {
			return new FilterOutputStream(this.localDocData.getOutputStream(entryName, writeDirectly)) {
				public void write(int b) throws IOException {
					this.out.write(b);
				}
				public void write(byte[] b) throws IOException {
					this.out.write(b);
				}
				public void write(byte[] b, int off, int len) throws IOException {
					this.out.write(b, off, len);
				}
				public void flush() throws IOException {
					this.out.flush();
				}
				public void close() throws IOException {
					this.out.close();
					putEntry(localDocData.getEntry(entryName));
				}
			};
		}
		
		//	TODO consider adding fetchAll() method
		
		//	TODO consider adding pushUpdates() method
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.im.util.ImDocumentData#getDocumentDataId()
		 */
		public String getDocumentDataId() {
			return this.localDocData.getDocumentDataId();
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.im.util.ImDocumentData#canLoadDocument()
		 */
		public boolean canLoadDocument() {
			return this.hasEntry("document.csv");
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.im.util.ImDocumentData#canStoreDocument()
		 */
		public boolean canStoreDocument() {
			return this.localDocData.canLoadDocument();
		}
	}
	
	private AuthenticatedClient authClient;
	private DocumentDataCache dataCache;
	
	/** Constructor
	 * @param authClient the authenticated client to use for communication
	 *            with the backing GoldenGATE IMS
	 * @param dataCache the cache to use
	 */
	public GoldenGateImsClient(AuthenticatedClient authClient, DocumentDataCache dataCache) {
		this.authClient = authClient;
		this.dataCache = dataCache;
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
	 * Obtain a document from the IMS. The valid document IDs can be read from
	 * the document list returned by getDocumentList(). The document is not
	 * locked at the backing IMS, so any attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @return the document with the specified ID
	 */
	public ImDocument getDocument(String documentId, ProgressMonitor pm) throws IOException {
		return this.getDocument(documentId, 0, pm);
	}
	
	/**
	 * Obtain a document from the IMS. The valid document IDs and respective
	 * current version numbers can be read from the document list returned by
	 * getDocumentList(). The document is not locked at the backing IMS, so any
	 * attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @param version the number of the document version to load
	 * @return the specified version of the document with the specified ID
	 */
	public ImDocument getDocument(String documentId, int version, ProgressMonitor pm) throws IOException {
		if (pm == null)
			pm = ProgressMonitor.dummy;
		ImDocumentData docData = this.getDocumentAsData(documentId, version, pm);
		return ImDocumentIO.loadDocument(docData, pm);
	}
	
	/**
	 * Check out a document from the backing IMS. The valid document IDs can be
	 * read from the document list returned by getDocumentList(). The document
	 * will be marked as checked out and will not be able to be worked on until
	 * released by the user checking it out, or an administrator.
	 * @param documentId the ID of the document to load
	 * @param pm a progress monitor to observe the checkout process
	 * @return the document with the specified ID
	 */
	public ImDocument checkoutDocument(String documentId, ProgressMonitor pm) throws IOException {
		return this.checkoutDocument(documentId, 0, pm);
	}
	
	/**
	 * Check out a document from the backing IMS. The valid document IDs and
	 * respective current version numbers can be read from the document list
	 * returned by getDocumentList(). The document will be marked as checked out
	 * and will not be able to be worked on until released by the user checking
	 * it out, or an administrator.
	 * @param documentId the ID of the document to load
	 * @param version the number of the document version to load (version number
	 *            0 always marks the most recent version, positive integers
	 *            indicate absolute versions numbers, while negative integers
	 *            indicate a version number backward from the most recent one)
	 * @param pm a progress monitor to observe the checkout process
	 * @return the specified version of the document with the specified ID
	 */
	public ImDocument checkoutDocument(String documentId, int version, ProgressMonitor pm) throws IOException {
		if (pm == null)
			pm = ProgressMonitor.dummy;
		ImDocumentData docData = this.checkoutDocumentAsData(documentId, version, pm);
		return ImDocumentIO.loadDocument(docData, pm);
	}
	
	/**
	 * Obtain a document from the IMS. The valid document IDs can be read from
	 * the document list returned by getDocumentList(). The document is not
	 * locked at the backing IMS, so any attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @return the document with the specified ID
	 */
	public ImDocumentData getDocumentAsData(String documentId, ProgressMonitor pm) throws IOException {
		return this.getDocumentAsData(documentId, 0, pm);
	}
	
	/**
	 * Obtain a document from the IMS. The valid document IDs and respective
	 * current version numbers can be read from the document list returned by
	 * getDocumentList(). The document is not locked at the backing IMS, so any
	 * attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @param version the number of the document version to load
	 * @return the specified version of the document with the specified ID
	 */
	public ImDocumentData getDocumentAsData(String documentId, int version, ProgressMonitor pm) throws IOException {
		return this.fetchDocumentData(GET_DOCUMENT, documentId, version, ((pm == null) ? ProgressMonitor.dummy : pm));
	}
	
	/**
	 * Check out a document from the backing IMS. The valid document IDs can be
	 * read from the document list returned by getDocumentList(). The document
	 * will be marked as checked out and will not be able to be worked on until
	 * released by the user checking it out, or an administrator.
	 * @param documentId the ID of the document to load
	 * @param pm a progress monitor to observe the checkout process
	 * @return the document with the specified ID
	 */
	public ImDocumentData checkoutDocumentAsData(String documentId, ProgressMonitor pm) throws IOException {
		return this.checkoutDocumentAsData(documentId, 0, pm);
	}
	
	/**
	 * Check out a document from the backing IMS. The valid document IDs and
	 * respective current version numbers can be read from the document list
	 * returned by getDocumentList(). The document will be marked as checked out
	 * and will not be able to be worked on until released by the user checking
	 * it out, or an administrator.
	 * @param documentId the ID of the document to load
	 * @param version the number of the document version to load (version number
	 *            0 always marks the most recent version, positive integers
	 *            indicate absolute versions numbers, while negative integers
	 *            indicate a version number backward from the most recent one)
	 * @param pm a progress monitor to observe the checkout process
	 * @return the specified version of the document with the specified ID
	 */
	public ImDocumentData checkoutDocumentAsData(String documentId, int version, ProgressMonitor pm) throws IOException {
		return this.fetchDocumentData(CHECKOUT_DOCUMENT, documentId, version, ((pm == null) ? ProgressMonitor.dummy : pm));
	}
	
	private ImDocumentData fetchDocumentData(String command, String docId, int version, ProgressMonitor pm) throws IOException {
		
		//	TODO add fetchFast flag to indicate fetching entries only on demand (supplements !!!)
		
		//	TODO also facilitate specifying range of pages to load images for (opening two pages out of 150 !!!)
		
		//	TODO use ImsClientDocumentData wrapper for that
		
		//	make sure we're logged in
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		Connection con = null;
		
		//	update locally cached entries
		pm.setStep("Updating local cache");
		
		//	get remote entry list
		pm.setBaseProgress(0);
		pm.setMaxProgress(10);
		ArrayList docEntries = null;
		try {
			pm.setInfo("Connecting to server ...");
			con = this.authClient.getConnection();
			BufferedWriter bw = con.getWriter();
			bw.write(command);
			bw.newLine();
			bw.write(this.authClient.getSessionID());
			bw.newLine();
			bw.write(docId);
			bw.newLine();
			bw.write("" + version);
			bw.newLine();
			bw.flush();
			
			pm.setInfo("Receiving entry list ...");
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (command.equals(error)) {
				docEntries = new ArrayList();
				for (String entryString; (entryString = br.readLine()) != null;) {
					if (entryString.length() == 0)
						break;
					ImDocumentEntry entry = ImDocumentEntry.fromTabString(entryString);
					if (entry != null)
						docEntries.add(entry);
				}
				pm.setInfo("Received list of " + docEntries.size() + " entries");
			}
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
			con = null;
		}
		
		//	get local document data
		ImDocumentData docData = this.dataCache.getDocumentData(docId);
		
		//	create mapping from logical entry names to (hash-extended) cache file names, and collect to-fetch entries
		ArrayList toFetchDocEntries = new ArrayList();
		for (int e = 0; e < docEntries.size(); e++) {
			ImDocumentEntry docEntry = ((ImDocumentEntry) docEntries.get(e));
			if (docData.hasEntryData(docEntry))
				docData.putEntry(docEntry);
			else toFetchDocEntries.add(docEntry);
		}
		
		//	TODO if in fastFetch mode, only get entries required for instantiating document (everything but supplements, basically)
		
		//	fetch missing local entries (if any)
		pm.setBaseProgress(10);
		pm.setMaxProgress(50);
		if (toFetchDocEntries.isEmpty())
			pm.setProgress(100);
		else this.getDocumentEntries(docId, ((ImDocumentEntry[]) toFetchDocEntries.toArray(new ImDocumentEntry[toFetchDocEntries.size()])), docData, pm);
		
		//	TODO in fastFetch mode, let's specify ourselves as the data provider to below method
		
		//	store updated entry list
		this.dataCache.storeEntryList(docData);
		
		//	finally
		return docData;
	}
	
//	/**
//	 * Obtain an input stream fetching an individual document entry from the
//	 * backing IMS. If there are multiple entries to fetch, it is far more
//	 * efficient to use the <code>getDocumentEntries()</code> method.
//	 * @param docId the ID of the document the entry belongs to
//	 * @param entry the entry to fetch the data for
//	 * @return an input stream to read the entry data from
//	 * @throws IOException
//	 */
//	private InputStream getDocumentEntry(String docId, ImDocumentEntry entry) throws IOException {
//		return this.getDocumentEntry(docId, entry, null);
//	}
//	
//	/**
//	 * Obtain an input stream fetching an individual document entry from the
//	 * backing IMS. If there are multiple entries to fetch, it is far more
//	 * efficient to use the <code>getDocumentEntries()</code> method.
//	 * @param docId the ID of the document the entry belongs to
//	 * @param entry the entry to fetch the data for
//	 * @param docData the document data object to store the entry data in
//	 * @return an input stream to read the entry data from
//	 * @throws IOException
//	 */
	private InputStream getDocumentEntry(String docId, ImDocumentEntry entry, ImDocumentData docData) throws IOException {
		ImDocumentEntry[] entries = {entry};
		this.getDocumentEntries(docId, entries, docData, null);
		return docData.getInputStream(entry);
	}
	private void getDocumentEntries(String docId, ImDocumentEntry[] entries, ImDocumentData docData, ProgressMonitor pm) throws IOException {
		Connection con = null;
		try {
			con = this.authClient.getConnection();
			BufferedWriter bw = con.getWriter();
			bw.write(GET_DOCUMENT_ENTRIES);
			bw.newLine();
			bw.write(this.authClient.getSessionID());
			bw.newLine();
			bw.write(docId);
			bw.newLine();
			for (int e = 0; e < entries.length; e++) {
				bw.write(entries[e].toTabString());
				bw.newLine();
			}
			bw.newLine();
			bw.flush();
			
			BufferedLineInputStream blis = con.getInputStream();
			String error = blis.readLine();
			if (!GET_DOCUMENT_ENTRIES.equals(error))
				throw new IOException(error);
			
			ZipInputStream zin = new ZipInputStream(blis);
			int zeCount = 0;
			byte[] buffer = new byte[1024];
			for (ZipEntry ze; (ze = zin.getNextEntry()) != null;) {
				zeCount++;
				ImDocumentEntry docEntry = new ImDocumentEntry(ze);
				if (pm != null) {
					pm.setInfo(" - " + docEntry.name);
					pm.setProgress((zeCount * 100) / entries.length);
				}
				OutputStream cacheOut = docData.getOutputStream(docEntry, true);
				for (int r; (r = zin.read(buffer, 0, buffer.length)) != -1;)
					cacheOut.write(buffer, 0, r);
				cacheOut.flush();
				cacheOut.close();
			}
			if (pm != null)
				pm.setProgress(100);
		}
		finally {
			if (con != null)
				con.close();
			con = null;
		}
	}
	
	/**
	 * Upload a new document to the backing IMS. If the specified document has
	 * actually been loaded from the backing IMS, the upload will fail. If the
	 * document was loaded from some other source (e.g. a local file on the client
	 * machine), IMS will store the document, but will not mark it as checked
	 * out by the user this client is authenticated with. To acquire a lock for
	 * a newly uploaded document, use the updateDocument() method. If the
	 * argument progress monitor is a controlling progress monitor, this method
	 * disables pausing and aborting after the document is sent. If the
	 * controlling functionality of the progress monitor is to be used again
	 * later, client code has to re-enable it.
	 * @param document the document to store
	 * @param pm a progress monitor to observe the upload process
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @throws IOException
	 */
	public String[] uploadDocument(ImDocument document, ProgressMonitor pm) throws IOException {
		return this.uploadDocument(document, this.authClient.getUserName(), pm);
	}
	
	/**
	 * Upload a new document to the backing IMS. If the specified document has
	 * actually been loaded from the backing IMS, the upload will fail. If the
	 * document was loaded from some other source (e.g. a local file on the client
	 * machine), IMS will store the document, but will not mark it as checked
	 * out by the user this client is authenticated with. To acquire a lock for
	 * a newly uploaded document, use the updateDocument() method. If the
	 * argument progress monitor is a controlling progress monitor, this method
	 * disables pausing and aborting after the document is sent. If the
	 * controlling functionality of the progress monitor is to be used again
	 * later, client code has to re-enable it.
	 * @param document the document to store
	 * @param userName the name of the user to credit for the upload, if
	 *            different from the user this IMS client is authenticated with
	 * @param pm a progress monitor to observe the upload process
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @throws IOException
	 */
	public String[] uploadDocument(ImDocument document, String userName, final ProgressMonitor pm) throws IOException {
		return this.sendDocument(UPLOAD_DOCUMENT, document, userName, ((pm == null) ? ProgressMonitor.dummy : pm));
	}
	
	/**
	 * Store/update a document in the backing IMS. If the specified document is
	 * not marked as checked out by the user this client is authenticated with,
	 * the upload will fail, with one exception: If the document was not loaded
	 * from IMS but from some other source (e.g. a local file on the client
	 * machine), IMS will store the document and subsequently mark it as checked
	 * out by the user this client is authenticated with. This implies that
	 * newly uploaded documents have to be released before any other users can
	 * work on them. For uploading a new document without acquiring a lock, use
	 * the uploadDocument() method. If the argument progress monitor is a
	 * controlling progress monitor, this method disabled pausing and aborting
	 * after the document is sent. If the controlling functionality of the
	 * progress monitor is to be used again later, client code has to re-enable
	 * it.
	 * @param document the document to store
	 * @param pm a progress monitor to observe the update process
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @throws IOException
	 */
	public String[] updateDocument(ImDocument document, final ProgressMonitor pm) throws IOException {
		return this.updateDocument(document, this.authClient.getUserName(), pm);
	}
	
	/**
	 * Store/update a document in the backing IMS. If the specified document is
	 * not marked as checked out by the user this client is authenticated with,
	 * the upload will fail, with one exception: If the document was not loaded
	 * from IMS but from some other source (e.g. a local file on the client
	 * machine), IMS will store the document and subsequently mark it as checked
	 * out by the user this client is authenticated with. This implies that
	 * newly uploaded documents have to be released before any other users can
	 * work on them. For uploading a new document without acquiring a lock, use
	 * the uploadDocument() method. If the argument progress monitor is a
	 * controlling progress monitor, this method disabled pausing and aborting
	 * after the document is sent. If the controlling functionality of the
	 * progress monitor is to be used again later, client code has to re-enable
	 * it.
	 * @param document the document to store
	 * @param userName the name of the user to credit for the upload, if
	 *            different from the user this IMS client is authenticated with
	 * @param pm a progress monitor to observe the update process
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @throws IOException
	 */
	public String[] updateDocument(ImDocument document, String userName, ProgressMonitor pm) throws IOException {
		return this.sendDocument(UPDATE_DOCUMENT, document, userName, ((pm == null) ? ProgressMonitor.dummy : pm));
	}
	
	private String[] sendDocument(String command, ImDocument doc, String userName, ProgressMonitor pm) throws IOException {
		
		//	make sure we're logged in
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		//	TODO adjust progress indication to leave space for server interaction
		
		//	generate and cache local entries (we still have to send them all to the server so it has a complete list)
		ImDocumentData docData = this.dataCache.getDocumentData(doc.docId);
		ImDocumentIO.storeDocument(doc, docData, pm);
		this.dataCache.storeEntryList(docData);
		
		//	send update
		return this.sendDocumentData(command, doc.docId, docData, userName, pm);
	}
	
	/**
	 * Upload a new document to the backing IMS. If the specified document has
	 * actually been loaded from the backing IMS, the upload will fail. If the
	 * document was loaded from some other source (e.g. a local file on the client
	 * machine), IMS will store the document, but will not mark it as checked
	 * out by the user this client is authenticated with. To acquire a lock for
	 * a newly uploaded document, use the updateDocument() method. If the
	 * argument progress monitor is a controlling progress monitor, this method
	 * disables pausing and aborting after the document is sent. If the
	 * controlling functionality of the progress monitor is to be used again
	 * later, client code has to re-enable it.
	 * @param document the document to store
	 * @param pm a progress monitor to observe the upload process
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @throws IOException
	 */
	public String[] uploadDocumentFromData(String docId, ImDocumentData docData, ProgressMonitor pm) throws IOException {
		return this.uploadDocumentFromData(docId, docData, this.authClient.getUserName(), pm);
	}
	
	/**
	 * Upload a new document to the backing IMS. If the specified document has
	 * actually been loaded from the backing IMS, the upload will fail. If the
	 * document was loaded from some other source (e.g. a local file on the client
	 * machine), IMS will store the document, but will not mark it as checked
	 * out by the user this client is authenticated with. To acquire a lock for
	 * a newly uploaded document, use the updateDocument() method. If the
	 * argument progress monitor is a controlling progress monitor, this method
	 * disables pausing and aborting after the document is sent. If the
	 * controlling functionality of the progress monitor is to be used again
	 * later, client code has to re-enable it.
	 * @param document the document to store
	 * @param userName the name of the user to credit for the upload, if
	 *            different from the user this IMS client is authenticated with
	 * @param pm a progress monitor to observe the upload process
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @throws IOException
	 */
	public String[] uploadDocumentFromData(String docId, ImDocumentData docData, String userName, final ProgressMonitor pm) throws IOException {
		return this.sendDocumentData(UPLOAD_DOCUMENT, docId, docData, userName, ((pm == null) ? ProgressMonitor.dummy : pm));
	}
	
	/**
	 * Store/update a document in the backing IMS. If the specified document is
	 * not marked as checked out by the user this client is authenticated with,
	 * the upload will fail, with one exception: If the document was not loaded
	 * from IMS but from some other source (e.g. a local file on the client
	 * machine), IMS will store the document and subsequently mark it as checked
	 * out by the user this client is authenticated with. This implies that
	 * newly uploaded documents have to be released before any other users can
	 * work on them. For uploading a new document without acquiring a lock, use
	 * the uploadDocument() method. If the argument progress monitor is a
	 * controlling progress monitor, this method disabled pausing and aborting
	 * after the document is sent. If the controlling functionality of the
	 * progress monitor is to be used again later, client code has to re-enable
	 * it.
	 * @param document the document to store
	 * @param pm a progress monitor to observe the update process
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @throws IOException
	 */
	public String[] updateDocumentFromData(String docId, ImDocumentData docData, final ProgressMonitor pm) throws IOException {
		return this.updateDocumentFromData(docId, docData, this.authClient.getUserName(), pm);
	}
	
	/**
	 * Store/update a document in the backing IMS. If the specified document is
	 * not marked as checked out by the user this client is authenticated with,
	 * the upload will fail, with one exception: If the document was not loaded
	 * from IMS but from some other source (e.g. a local file on the client
	 * machine), IMS will store the document and subsequently mark it as checked
	 * out by the user this client is authenticated with. This implies that
	 * newly uploaded documents have to be released before any other users can
	 * work on them. For uploading a new document without acquiring a lock, use
	 * the uploadDocument() method. If the argument progress monitor is a
	 * controlling progress monitor, this method disabled pausing and aborting
	 * after the document is sent. If the controlling functionality of the
	 * progress monitor is to be used again later, client code has to re-enable
	 * it.
	 * @param document the document to store
	 * @param userName the name of the user to credit for the upload, if
	 *            different from the user this IMS client is authenticated with
	 * @param pm a progress monitor to observe the update process
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @throws IOException
	 */
	public String[] updateDocumentFromData(String docId, ImDocumentData docData, String userName, ProgressMonitor pm) throws IOException {
		return this.sendDocumentData(UPDATE_DOCUMENT, docId, docData, userName, ((pm == null) ? ProgressMonitor.dummy : pm));
	}
	
	private String[] sendDocumentData(String command, String docId, ImDocumentData docData, String userName, ProgressMonitor pm) throws IOException {
		
		/* TODO add option for disabling local caching:
		 * - saves all the disc writing for document uploads in server side PDF conversion and batch runs
		 * - BUT THEN it's sufficient to put client document folder on RAM disc ...
		 * - ... OR directly hand it the storage folder to write to the final destination (breach of encapsulation, but saves tons of copying effort)
		 *  */
		
		// 	TODO if we have an on-demand fetching document data object, upload only entries whose data is available
		//	TODO and then, the rest will be unmodified, so server won't ever ask for entries that were never fetched
		
		//	make sure we're logged in
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		//	TODO adjust progress indication to leave space for server interaction
		
		//	generate and cache local entries (we still have to send them all to the server so it has a complete list)
		ImDocumentEntry[] docEntries = docData.getEntries();
		
		//	obtain list of to-update entries
		Connection con = null;
		ArrayList toUpdateDocEntries = null;
		String updateKey = null;
		try {
			pm.setInfo("Connecting to server ...");
			con = this.authClient.getConnection();
			BufferedWriter bw = con.getWriter();
			bw.write(command);
			bw.newLine();
			bw.write(this.authClient.getSessionID());
			bw.newLine();
			bw.write(docId);
			bw.newLine();
			bw.write((userName == null) ? "" : userName);
			bw.newLine();
			pm.setInfo("Sending list of " + docEntries.length + " entries ...");
			for (int e = 0; e < docEntries.length; e++) {
				bw.write(docEntries[e].toTabString());
				bw.newLine();
			}
			bw.newLine();
			bw.flush();
			
			pm.setInfo("Receiving list of to-update entries ...");
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (command.equals(error)) {
				updateKey = br.readLine();
				
				//	empty update key indicates nothing to update
				if (updateKey.length() == 0) {
					pm.setInfo("No entries to update, receiving update result ...");
					if (pm instanceof ControllingProgressMonitor) {
						((ControllingProgressMonitor) pm).setPauseResumeEnabled(false);
						((ControllingProgressMonitor) pm).setAbortEnabled(false);
					}
					StringVector log = new StringVector();
					for (String logEntry; (logEntry = br.readLine()) != null;)
						log.addElement(logEntry);
					pm.setInfo("Update complete.");
					pm.setProgress(100);
					return log.toStringArray();
				}
				
				//	receive list of to-update entries
				else {
					toUpdateDocEntries = new ArrayList();
					for (String entryString; (entryString = br.readLine()) != null;) {
						if (entryString.length() == 0)
							break;
						ImDocumentEntry entry = ImDocumentEntry.fromTabString(entryString);
						if (entry != null)
							toUpdateDocEntries.add(entry);
					}
					pm.setInfo("Received list of " + toUpdateDocEntries.size() + " to-update entries");
				}
			}
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
		
		//	nothing changed at all, we're done
		if (toUpdateDocEntries.size() == 0) {
			String[] log = {"Document up to date on server"};
			return log;
		}
		
		//	send to-update entries (make sure to include name and timestamp)
		try {
			pm.setInfo("Sending " + toUpdateDocEntries.size() + " entries to server ...");
			con = this.authClient.getConnection();
			BufferedLineOutputStream out = con.getOutputStream();
			out.writeLine(UPDATE_DOCUMENT_ENTRIES);
			out.writeLine(this.authClient.getSessionID());
			out.writeLine(updateKey);
			ZipOutputStream zout = new ZipOutputStream(out);
			byte[] buffer = new byte[1024];
			for (int e = 0; e < toUpdateDocEntries.size(); e++) {
				ImDocumentEntry docEntry = ((ImDocumentEntry) toUpdateDocEntries.get(e));
				pm.setInfo(" - " + docEntry.name);
				pm.setProgress((0 * 100) / toUpdateDocEntries.size());
				ZipEntry ze = new ZipEntry(docEntry.getFileName());
				ze.setTime(docEntry.updateTime);
				zout.putNextEntry(ze);
				InputStream cacheIn = docData.getInputStream(docEntry);
				for (int r; (r = cacheIn.read(buffer, 0, buffer.length)) != -1;)
					zout.write(buffer, 0, r);
				cacheIn.close();
				zout.closeEntry();
				
			}
			ZipEntry ze = new ZipEntry(updateKey);
			zout.putNextEntry(ze);
			zout.closeEntry();
			zout.flush();
			
			pm.setInfo("Receiving update result ...");
			if (pm instanceof ControllingProgressMonitor) {
				((ControllingProgressMonitor) pm).setPauseResumeEnabled(false);
				((ControllingProgressMonitor) pm).setAbortEnabled(false);
			}
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (UPDATE_DOCUMENT_ENTRIES.equals(error)) {
				StringVector log = new StringVector();
				for (String logEntry; (logEntry = br.readLine()) != null;)
					log.addElement(logEntry);
				pm.setInfo("Update complete.");
				pm.setProgress(100);
				return log.toStringArray();
			}
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
			con = null;
		}
	}
	
	/**
	 * Retrieve the update protocol of document, i.e. an array of messages that
	 * describe which other modifications the update to the latest version
	 * incurred throughout the server. This includes only modifications that
	 * happen synchronously on update notification, though.
	 * @param docId the ID of the document to get the update protocol for
	 * @return the update protocol
	 * @throws IOException
	 */
	public String[] getUpdateProtocol(String docId) throws IOException {
		Connection con = null;
		try {
			con = this.authClient.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(GET_UPDATE_PROTOCOL);
			bw.newLine();
			bw.write(docId);
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (GET_UPDATE_PROTOCOL.equals(error)) {
				StringVector log = new StringVector();
				for (String logEntry; (logEntry = br.readLine()) != null;)
					log.addElement(logEntry);
				return log.toStringArray();
			}
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * Delete a documents from the IMS. If a user other than the one this client
	 * is authenticated with holds the lock for the document with the specified
	 * ID, the deletion fails and an IOException will be thrown.
	 * @param documentId the ID of the document to delete
	 * @return an array holding the logging messages collected during the
	 *         deletion process.
	 */
	public String[] deleteDocument(String documentId) throws IOException {
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		Connection con = null;
		try {
			con = this.authClient.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(DELETE_DOCUMENT);
			bw.newLine();
			bw.write(this.authClient.getSessionID());
			bw.newLine();
			bw.write(documentId);
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (DELETE_DOCUMENT.equals(error)) {
				StringVector log = new StringVector();
				for (String logEntry; (logEntry = br.readLine()) != null;)
					log.addElement(logEntry);
				return log.toStringArray();
			}
			else throw new IOException(error);
		}
		catch (Exception e) {
			throw new IOException(e.getMessage());
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * Release a document so other users can work on it again. This is possible
	 * only under two conditions: (1) the document was checked out by the user
	 * this client is authenticated with, which is the normal use, or (2) with
	 * administrative privileges, which should be done only in rare cases
	 * because it possibly annihilates all the work the checkout user has done
	 * on the document.
	 * @param documentId the ID of the document to release
	 * @throws IOException
	 */
	public void releaseDocument(String documentId) throws IOException {
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		Connection con = null;
		try {
			con = this.authClient.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(RELEASE_DOCUMENT);
			bw.newLine();
			bw.write(this.authClient.getSessionID());
			bw.newLine();
			bw.write(documentId);
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (!RELEASE_DOCUMENT.equals(error))
				throw new IOException("Document release failed: " + error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * @param args 
	 */
	public static void main(String[] args) throws Exception {
		ServerConnection sc = ServerConnection.getServerConnection("localhost", 8015);
		AuthenticatedClient ac = AuthenticatedClient.getAuthenticatedClient(sc);
		ac.login("Admin", "GG");
		GoldenGateImsClient ggic = new GoldenGateImsClient(ac, new DocumentDataCache() {
			private File cacheRootFolder = new File("E:/Testdaten/ImsTest");
			public ImDocumentData getDocumentData(String docId) throws IOException {
				File docCacheFolder = new File(this.cacheRootFolder, docId);
				docCacheFolder.mkdirs();
				if ((new File(docCacheFolder, "entries.txt")).exists())
					return new FolderImDocumentData(docCacheFolder, null);
				else return new FolderImDocumentData(docCacheFolder);
			}
			public void storeEntryList(ImDocumentData docData) throws IOException {
				((FolderImDocumentData) docData).storeEntryList();
			}
		});
		
		InputStream docIn = new BufferedInputStream(new FileInputStream(new File("E:/Testdaten/PdfExtract/zt00872.pdf.imf")));
		ImDocument doc = ImDocumentIO.loadDocument(docIn);
		docIn.close();
//		ImDocument doc = ggic.checkoutDocument("FFF1CA60FFCDF655E279E450FFFD2C09", null);
//		doc.setAttribute("test", ("test at " + System.currentTimeMillis()));
		doc.setAttribute("test", ("" + System.currentTimeMillis()));
		
		String[] up = ggic.updateDocument(doc, "TEST", null);
		for (int e = 0; e < up.length; e++)
			System.out.println(up[e]);
//		
//		ggic.releaseDocument("FFF1CA60FFCDF655E279E450FFFD2C09");
//		
//		ImDocument doc = ggic.getDocument("FFF1CA60FFCDF655E279E450FFFD2C09", null);
////		OutputStream docOut = new BufferedOutputStream(new FileOutputStream(new File("E:/Testdaten/PdfExtract/zt00872.pdf.resaved.imf")));
////		ImfIO.storeDocument(doc, docOut);
////		docOut.flush();
////		docOut.close();
//		doc.getPage(0).removeAttribute("test");
//		doc.getPage(0).setAttribute("test2");
//		ggic.uploadDocument(doc, "TEST", null);
	}
}