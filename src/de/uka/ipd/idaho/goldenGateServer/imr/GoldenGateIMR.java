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
package de.uka.ipd.idaho.goldenGateServer.imr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.Attributed;
import de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed;
import de.uka.ipd.idaho.gamta.util.transfer.DocumentListElement;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentRegistry;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerEventService;
import de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection.Connection;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS.ImsDocumentData;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants;
import de.uka.ipd.idaho.goldenGateServer.ims.data.ImsDocumentList;
import de.uka.ipd.idaho.goldenGateServer.res.GoldenGateRES;
import de.uka.ipd.idaho.goldenGateServer.res.GoldenGateRES.RemoteEventList;
import de.uka.ipd.idaho.goldenGateServer.res.GoldenGateRES.ResEventFilter;
import de.uka.ipd.idaho.goldenGateServer.res.GoldenGateRES.ResRemoteEvent;
import de.uka.ipd.idaho.goldenGateServer.res.GoldenGateRES.ResRemoteEvent.ResRemoteEventListener;
import de.uka.ipd.idaho.goldenGateServer.util.AsynchronousConsoleAction;
import de.uka.ipd.idaho.goldenGateServer.util.BufferedLineInputStream;
import de.uka.ipd.idaho.goldenGateServer.util.BufferedLineOutputStream;
import de.uka.ipd.idaho.im.util.ImDocumentData.ImDocumentEntry;
import de.uka.ipd.idaho.im.util.ImDocumentIO;

/**
 * GoldenGATE Image Markup document Replicator replicates document updates and
 * deletions between GoldenGATE IMS instances installed in different GoldenGATE
 * Servers. It relies on GoldenGATE Remote Event Service for event forwarding.
 * 
 * @author sautter
 */
public class GoldenGateIMR extends GoldenGateAEP implements GoldenGateImsConstants {
	
	private static final String ORIGINAL_UPDATE_USER_ATTRIBUTE = "originalUpdateUser";
	private static final String ORIGINAL_UPDATE_TIME_ATTRIBUTE = "originalUpdateTime";
	private static final String ORIGINAL_UPDATE_DOMAIN_ATTRIBUTE = "originalUpdateDomain";
	
	private static final String GET_DOCUMENT = "IMR_GET_DOCUMENT";
	private static final String GET_DOCUMENT_ENTRIES = "IMR_GET_DOCUMENT_ENTRIES";
	private static final String GET_DOCUMENT_LIST = "IMR_GET_DOCUMENT_LIST";
	private static final String GET_DOCUMENT_LIST_SHARED = "IMR_GET_DOCUMENT_LIST_SHARED";
	
	private static final String defaultPassPhrase = "IMR provides remote access!";
	
	/** The GoldenGATE DIO to work with */
	protected GoldenGateIMS ims;
	private GoldenGateRES res;
	
	private String localPassPhrase = null;
	private Properties remotePassPhrases = new Properties();
	
	/** Constructor passing 'IMR' as the letter code to super constructor
	 */
	public GoldenGateIMR() {
		super("IMR", "ImsReplicator");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#persistProcessingEvents()
	 */
	protected boolean persistProcessingEvents() {
		return true; // fetching a whole IMF can take a good while, so let's be safe here
	}
	
	/**
	 * This implementation reads the pass phrases. Sub classes overwriting this
	 * method have to make the super call.
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#initComponent()
	 */
	protected void initComponent() {
		
		//	initialize super class
		super.initComponent();
		
		//	read our own access data
		this.readPassPhrases();
	}
	
	private void readPassPhrases() {
		Settings passPhrases = Settings.loadSettings(new File(this.dataPath, "passPhrases.cnfg"));
		
		//	load pass phrases for incoming connections
		this.localPassPhrase = passPhrases.getSetting("localPassPhrase", defaultPassPhrase);
		
		//	load pass phrases for accessing remote IMR's
		Settings remotePassPhrases = passPhrases.getSubset("remotePassPhrase");
		String[] remoteDomainNames = remotePassPhrases.getKeys();
		for (int d = 0; d < remoteDomainNames.length; d++)
			this.remotePassPhrases.setProperty(remoteDomainNames[d], remotePassPhrases.getSetting(remoteDomainNames[d]));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
	 */
	public void link() {
		
		//	get DIO
		this.ims = ((GoldenGateIMS) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateIMS.class.getName()));
		
		//	check success
		if (this.ims == null) throw new RuntimeException(GoldenGateIMS.class.getName());
		
		//	hook up to local RES
		this.res = ((GoldenGateRES) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateRES.class.getName()));
		
		//	check success
		if (this.res == null) throw new RuntimeException(GoldenGateRES.class.getName());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
	 */
	public void linkInit() {
		
		//	listen for events
		GoldenGateServerEventService.addServerEventListener(new ResRemoteEventListener() {
			public void notify(ResRemoteEvent rre) {
				if (!ImsDocumentEvent.class.getName().equals(rre.eventClassName))
					return;
				
				//	reconstruct and handle document event
				ImsDocumentEvent ide = ImsDocumentEvent.parseEvent(rre.paramString);
				handleDocumentEvent(rre, ide);
			}
		});
		
		//	prevent remote document updates from being re-published
		this.res.addEventFilter(new ResEventFilter() {
			public boolean allowPublishEvent(GoldenGateServerEvent gse) {
				if ((gse instanceof ImsDocumentEvent) && ((ImsDocumentEvent) gse).user.startsWith("IMR."))
					return false;
				return true;
			}
		});
		
		//	link initialize super class
		super.linkInit();
		
		//	diff events with remote IMR
		this.diffAction = new AsynchronousConsoleAction(DIFF_FROM_IMR_COMMAND, "Run a full diff with a specific remote GoldenGATE IMR, i.e., compare document update events and handle unhandled ones", "update event", null, null) {
			protected String[] getArgumentNames() {
				String[] args = {"remoteDomain"};
				return args;
			}
			protected String[] getArgumentExplanation(String argument) {
				if ("remoteDomain".equals(argument)) {
					String[] explanation = {"The alias of the remote GoldenGATE IMR to compare the document list with"};
					return explanation;
				}
				else return super.getArgumentExplanation(argument);
			}
			protected void checkRunnable() {
				if (syncAction.isRunning())
					throw new RuntimeException("Document list sync running, diff cannot run in parallel");
			}
			protected String checkArguments(String[] arguments) {
				if (arguments.length != 1)
					return ("Invalid arguments for '" + this.getActionCommand() + "', specify the alias of the IMR to diff with as the only argument.");
				String address = res.getRemoteDomainAddress(arguments[0]);
				if (address == null)
					return ("No remote IMR found for name " + arguments[0]);
				else return null;
			}
			protected void performAction(String[] arguments) throws Exception {
				String remoteDomain = arguments[0];
				
				//	get remote events
				RemoteEventList rel = res.getRemoteEventList(remoteDomain, 0, GoldenGateIMS.class.getName());
				if (rel == null)
					return;
				this.enteringMainLoop("Got event list from " + remoteDomain);
				int handleCount = 0;
				int skipCount = 0;
				
				//	do diff
				while (this.continueAction() && rel.hasNextEvent()) {
					ResRemoteEvent rre = rel.getNextEvent();
					if (!ImsDocumentEvent.class.getName().equals(rre.eventClassName))
						continue;
					
					//	reconstruct document event
					ImsDocumentEvent ide = ImsDocumentEvent.parseEvent(rre.paramString);
					
					//	check against local update time
					Attributed docAttributes = ims.getDocumentAttributes(ide.dataId);
					if (docAttributes != null) {
						long updateTime = Long.parseLong((String) docAttributes.getAttribute(ORIGINAL_UPDATE_TIME_ATTRIBUTE, docAttributes.getAttribute(UPDATE_TIME_ATTRIBUTE, "0")));
						if (rre.eventTime < updateTime) {
							skipCount++;
							continue;
						}
					}
					
					//	handle document event
					dataUpdated(ide.dataId, false, remoteDomain, PRIORITY_LOW);
					handleCount++;
					
					//	update status
					this.loopRoundComplete("Handled " + handleCount + " update events, skipped " + skipCount + " ones.");
				}
			}
		};
		
		//	sync documents with a remote IMR
		this.syncAction = new AsynchronousConsoleAction(SYNC_WITH_IMR_COMMAND, "Run a full sync with a specific remote GoldenGATE IMR, i.e., compare the document lists and fetch missing updates", "document list", null, null) {
			protected String[] getArgumentNames() {
				String[] args = {"remoteDomain", "mode"};
				return args;
			}
			protected String[] getArgumentExplanation(String argument) {
				if ("remoteDomain".equals(argument)) {
					String[] explanation = {"The alias of the remote GoldenGATE IMR to compare the document list with"};
					return explanation;
				}
				else if ("mode".equals(argument)) {
					String[] explanation = {"The sync mode: '-u' for 'update' (the default), '-d' for 'delete', or '-ud' for both"};
					return explanation;
				}
				else return super.getArgumentExplanation(argument);
			}
			protected void checkRunnable() {
				if (diffAction.isRunning())
					throw new RuntimeException("Update event diff running, sync cannot run in parallel");
			}
			protected String checkArguments(String[] arguments) {
				if ((arguments.length < 1) || (arguments.length > 2)) 
					return ("Invalid arguments for '" + this.getActionCommand() + "', specify the alias of the IMR to sync with, and optionally the sync mode, as the only arguments.");
				String address = res.getRemoteDomainAddress(arguments[0]);
				if (address == null)
					return ("No remote IMR found for name " + arguments[0]);
				else if ((arguments.length == 2) && ("-u -d -ud".indexOf(arguments[1]) == -1))
					return ("Invalid sync mode " + arguments[1]);
				else return null;
			}
			protected void performAction(String[] arguments) throws Exception {
				String remoteDomain = arguments[0];
				boolean update = ((arguments.length == 1) || (arguments[1].indexOf("u") != -1));
				boolean delete = ((arguments.length == 2) && (arguments[1].indexOf("d") != -1));
				
				//	get remote domain access data
				String remoteAddress = res.getRemoteDomainAddress(remoteDomain);
				int remotePort = res.getRemoteDomainPort(remoteDomain);
				
				//	get document list from remote domain, and index document records by ID
				ImsDocumentList remoteDl = getDocumentList(remoteAddress, remotePort);
				HashMap remoteDlesById = new HashMap();
				while (remoteDl.hasNextDocument()) {
					DocumentListElement dle = remoteDl.getNextDocument();
					remoteDlesById.put(((String) dle.getAttribute(DOCUMENT_ID_ATTRIBUTE)), dle);
				}
				
				//	iterate over local document list, collecting IDs of documents to update or delete
				HashSet updateDocIDs = new HashSet();
				HashSet deleteDocIDs = new HashSet();
				ImsDocumentList localDl = ims.getDocumentListFull();
				while (localDl.hasNextDocument()) {
					DocumentListElement localDle = localDl.getNextDocument();
					String docId = ((String) localDle.getAttribute(DOCUMENT_ID_ATTRIBUTE));
					DocumentListElement remoteDle = ((DocumentListElement) remoteDlesById.remove(docId));
					
					//	this one doesn't even exist in the remote domain
					if (remoteDle == null) {
						if (delete)
							deleteDocIDs.add(docId);
						continue;
					}
					
					//	extract update timestamps for comparison
					long localUpdateTime;
					long remoteUpdateTime;
					try {
						localUpdateTime = Long.parseLong((String) localDle.getAttribute(UPDATE_TIME_ATTRIBUTE));
						remoteUpdateTime = Long.parseLong((String) remoteDle.getAttribute(UPDATE_TIME_ATTRIBUTE));
					}
					catch (Exception e) {
						this.log(("Could not parse update timestamps for document '" + docId + "'"), e);
						continue;
					}
					
					//	remote version is newer than local one (even with a one second tolerance), mark for update
					if (update && ((localUpdateTime + 1000) < remoteUpdateTime))
						updateDocIDs.add(docId);
				}
				
				//	add updates for new document not yet available locally
				if (update)
					updateDocIDs.addAll(remoteDlesById.keySet());
				
				//	do updates and deletions
				int updateCount = updateDocIDs.size();
				int deleteCount = deleteDocIDs.size();
				this.enteringMainLoop("Got event list from " + remoteDomain + ", " + updateCount + " updates, " + deleteCount + " deletions");
				while (this.continueAction() && ((updateDocIDs.size() + deleteDocIDs.size()) != 0)) {
					
					//	do deletions first ...
					if (deleteDocIDs.size() != 0) {
						String docId = ((String) deleteDocIDs.iterator().next());
						deleteDocIDs.remove(docId);
						this.reportResult("GoldenGateIMR: forwarding deletion from " + remoteDomain + " (" + remoteAddress + ":" + remotePort + ") ...");
						try {
							
							//	get update user, and reuse if starting with 'IMR.'
							Attributed docAttributes = ims.getDocumentAttributes(docId);
							String updateUser = ((String) docAttributes.getAttribute(UPDATE_USER_ATTRIBUTE));
							if ((updateUser == null) || !updateUser.startsWith("IMR."))
								updateUser = ("IMR." + remoteDomain);
							
							//	delete document
							ims.deleteDocument(updateUser, docId, null);
						}
						catch (IOException ioe) {
							this.reportError("GoldenGateIMR: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document " + docId + ".");
							this.reportError(ioe);
						}
					}
					
					//	... and updates second
					else if (updateDocIDs.size() != 0) {
						String docId = ((String) updateDocIDs.iterator().next());
						updateDocIDs.remove(docId);
						this.reportResult("GoldenGateIMR: getting update from " + remoteDomain + " (" + remoteAddress + ":" + remotePort + ") ...");
						dataUpdated(docId, false, remoteDomain, PRIORITY_LOW);
					}
					
					//	update status
					this.loopRoundComplete("Handled " + (updateCount - updateDocIDs.size()) + " of " + updateCount + " updates, " + (deleteCount - deleteDocIDs.size()) + " of " + deleteCount + " deletions.");
				}
			}
		};
	}
	
	void handleDocumentEvent(ResRemoteEvent rre, ImsDocumentEvent ide) {
		
		//	handle update event (simply enqueue for processing and handle asynchronously)
		if (ide.type == ImsDocumentEvent.UPDATE_TYPE) {
			this.logInfo("GoldenGateIMR: scheduling update from " + rre.sourceDomainAlias + " (" + rre.sourceDomainAddress + ":" + rre.sourceDomainPort + ") ...");
			this.dataUpdated(ide.dataId, (ide.version == 1), rre.sourceDomainAlias, PRIORITY_LOW);
		}
		
		//	handle delete event
		else if (ide.type == ImsDocumentEvent.DELETE_TYPE) {
			try {
				this.ims.deleteDocument(("IMR." + rre.originDomainName), ide.dataId, null);
			}
			catch (IOException ioe) {
				this.logError("GoldenGateIMR: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document.");
				this.logError(ioe);
			}
		}
		
		//	we are not interested in checkouts and releases (for now)
	}
	
	private static final String READ_PASS_PHRASES_COMMAND = "readPassPhrases";
	private static final String DIFF_FROM_IMR_COMMAND = "diff";
	private static final String SYNC_WITH_IMR_COMMAND = "sync";
	
	private AsynchronousConsoleAction diffAction;
	private AsynchronousConsoleAction syncAction;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(super.getActions()));
		ComponentAction ca;
		
		//	add console action for issuing updates events for all existing documents
		ca = new AsynchronousConsoleAction("publishEvents", "Publish update events for the documents in the local IMS.", "update event", null, null) {
			protected String[] getArgumentNames() {
				return new String[0];
			}
			protected String checkArguments(String[] arguments) {
				return ((arguments.length == 0) ? null : " Specify no arguments.");
			}
			protected void performAction(String[] arguments) throws Exception {
				ImsDocumentList dl = ims.getDocumentListFull();
				if (dl.hasNextDocument()) {
					DocumentListElement de = dl.getNextDocument();
					RemoteEventList rel = ((GoldenGateRES) res).getEventList(0, GoldenGateIMS.class.getName());
					HashSet deDuplicator = new HashSet();
					while (rel.hasNextEvent()) {
						ResRemoteEvent re = rel.getNextEvent();
						if (re.type == ImsDocumentEvent.UPDATE_TYPE)
							deDuplicator.add(re.eventId);
					}
					int existingEventCount = deDuplicator.size();
					this.enteringMainLoop("Got " + existingEventCount + " existing document update events, enqueued 0 new ones");
					
					int newEventCount = 0;
					do {
						String docId = ((String) de.getAttribute(DOCUMENT_ID_ATTRIBUTE));
						String updateUser = ((String) de.getAttribute(UPDATE_USER_ATTRIBUTE));
						long updateTime = Long.parseLong((String) de.getAttribute(UPDATE_TIME_ATTRIBUTE));
						int version = Integer.parseInt((String) de.getAttribute(DOCUMENT_VERSION_ATTRIBUTE));
						
						//	import local updates only, as importing remote updates might create new update IDs and thus cause unnecessary traffic
						if (!updateUser.startsWith("IMR.")) {
							ImsDocumentEvent dde = new ImsDocumentEvent(updateUser, docId, null, version, ImsDocumentEvent.UPDATE_TYPE, GoldenGateIMS.class.getName(), updateTime, null);
							if (deDuplicator.add(dde.eventId)) {
								((GoldenGateRES) res).publishEvent(dde);
								this.loopRoundComplete("Got " + existingEventCount + " existing update events, enqueued " + (++newEventCount) + " new ones");
							}
						}
					}
					while (this.continueAction() && ((de = dl.getNextDocument()) != null));
				}
				else this.log(" There are no documents available in IMS.");
			}
		};
		cal.add(ca);
		
		//	request for document
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_DOCUMENT;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	get pass phrase and document ID
				String passPhraseHash = input.readLine();
				String docId = input.readLine();
				
				//	check authentication
				if (!passPhraseHash.equals("" + (docId + localPassPhrase).hashCode())) {
					output.write("Invalid pass phrase for loading document with ID " + docId);
					output.newLine();
					return;
				}
				
				//	get document proxy from IMS
				ImsDocumentData docData = ims.getDocumentData(docId, false);
				if (docData == null) {
					output.write("Invalid document ID " + docId);
					output.newLine();
					return;
				}
				
				//	send back document entry list
				try {
					ImDocumentEntry[] docEntries = docData.getEntries();
					output.write(GET_DOCUMENT);
					output.newLine();
					output.write(ImDocumentIO.getAttributesString(docData));
					output.newLine();
					for (int e = 0; e < docEntries.length; e++) {
						output.write(docEntries[e].toTabString());
						output.newLine();
					}
					output.newLine();
					output.flush();
				}
				finally {
					docData.dispose();
				}
			}
		};
		cal.add(ca);
		
		// deliver document entries through network
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_DOCUMENT_ENTRIES;
			}
			public void performActionNetwork(BufferedLineInputStream input, BufferedLineOutputStream output) throws IOException {
				
				//	get pass phrase and document ID
				String passPhraseHash = input.readLine();
				String docId = input.readLine();
				
				//	check authentication
				if (!passPhraseHash.equals("" + (docId + localPassPhrase).hashCode())) {
					output.writeLine("Invalid pass phrase for loading document with ID " + docId);
					return;
				}
				
				//	get entry list
				ArrayList docEntries = new ArrayList();
				for (String entryString; (entryString = input.readLine()) != null;) {
					if (entryString.length() == 0)
						break;
					ImDocumentEntry entry = ImDocumentEntry.fromTabString(entryString);
					if (entry != null)
						docEntries.add(entry);
				}
				
				//	get document data from IMS
				ImsDocumentData docData = ims.getDocumentData(docId, false);
				
				//	indicate data coming
				output.writeLine(GET_DOCUMENT_ENTRIES);
				
				//	send requested entries
				try {
					ZipOutputStream zout = new ZipOutputStream(output);
					byte[] buffer = new byte[1024];
					for (int e = 0; e < docEntries.size(); e++) {
						ImDocumentEntry entry = ((ImDocumentEntry) docEntries.get(e));
						ZipEntry ze = new ZipEntry(entry.getFileName());
						ze.setTime(entry.updateTime);
						zout.putNextEntry(ze);
						InputStream entryIn = docData.getInputStream(entry);
						for (int r; (r = entryIn.read(buffer, 0, buffer.length)) != -1;)
							zout.write(buffer, 0, r);
						entryIn.close();
						zout.closeEntry();
					}
					zout.flush();
				}
				finally {
					docData.dispose();
				}
			}
		};
		cal.add(ca);
		
		//	request for document list
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_DOCUMENT_LIST;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				ImsDocumentList dl = ims.getDocumentListFull();
				
				output.write(GET_DOCUMENT_LIST);
				output.newLine();
				
				dl.writeData(output);
			}
		};
		cal.add(ca);
		
		//	request for document list
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_DOCUMENT_LIST_SHARED;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				ImsDocumentList dl = ims.getDocumentListFull();
				
				output.write(GET_DOCUMENT_LIST_SHARED);
				output.newLine();
				
				dl.writeData(output);
			}
		};
		cal.add(ca);
		
		//	re-read pass phrases
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return READ_PASS_PHRASES_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						READ_PASS_PHRASES_COMMAND,
						"Re-read the local and remote pass phrases from the config file.",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					readPassPhrases();
					this.reportResult(" Pass phrases re-read.");
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	diff events with remote IMR
		cal.add(this.diffAction);
		
		//	sync documents with a remote IMR
		cal.add(this.syncAction);
		
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#doUpdate(java.lang.String, java.lang.String, java.util.Properties, long)
	 */
	protected void doUpdate(String dataId, String remoteDomainAlias, Properties dataAttributes, long params) throws IOException {
		
		//	get remote domain access data
		String remoteAddress = this.res.getRemoteDomainAddress(remoteDomainAlias);
		int remotePort = this.res.getRemoteDomainPort(remoteDomainAlias);
		
		//	do update
		this.logInfo("GoldenGateIMR: updating from " + remoteDomainAlias + " (" + remoteAddress + ":" + remotePort + ") ...");
		this.updateDocument(dataId, remoteAddress, remotePort, remoteDomainAlias);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#doDelete(java.lang.String, java.lang.String, java.util.Properties, long)
	 */
	protected void doDelete(String dataId, String user, Properties dataAttributes, long params) throws IOException {
		//	deletions are handled synchronously, as they are pretty fast
	}
	
	private void updateDocument(String docId, String sourceDomainAddress, int sourceDomainPort, String sourceDomainAlias) throws IOException {
		
		//	get document data (null if no changes at all)
		ImsDocumentData docData = this.getDocumentData(docId, sourceDomainAddress, sourceDomainPort, sourceDomainAlias);
		if (docData == null)
			return;
		
		//	get update user
		String updateUser = ((String) docData.getAttribute(ORIGINAL_UPDATE_USER_ATTRIBUTE));
		if (updateUser == null)
			updateUser = ("IMR." + sourceDomainAlias);
		
		//	get original update domain
		String updateDomain = ((String) docData.getAttribute(ORIGINAL_UPDATE_DOMAIN_ATTRIBUTE));
		if (updateDomain == null)
			updateDomain = sourceDomainAlias;
		
		//	store document
		try {
			this.ims.updateDocumentFromData(updateUser, ("IMR." + updateDomain), docData, null);
		}
		finally {
			docData.dispose();
		}
	}
	
	private ImsDocumentData getDocumentData(String docId, String remoteAddress, int remotePort, String remoteDomain) throws IOException {
		ServerConnection sc = ((remotePort == -1) ? ServerConnection.getServerConnection(remoteAddress) : ServerConnection.getServerConnection(remoteAddress, remotePort));
		String passPhrase = this.remotePassPhrases.getProperty(remoteDomain, defaultPassPhrase);
		Connection con = null;
		this.logInfo("Getting document data " + docId);
		
		//	get current document entry list
		Attributed docAttributes = new AbstractAttributed();
		ArrayList docEntries = new ArrayList();
		try {
			con = sc.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(GET_DOCUMENT);
			bw.newLine();
			bw.write("" + (docId + passPhrase).hashCode());
			bw.newLine();
			bw.write(docId);
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (GET_DOCUMENT.equals(error)) {
				ImDocumentIO.setAttributes(docAttributes, br.readLine());
				for (String entryString; (entryString = br.readLine()) != null;) {
					if (entryString.length() == 0)
						break;
					ImDocumentEntry entry = ImDocumentEntry.fromTabString(entryString);
					if (entry != null)
						docEntries.add(entry);
				}
			}
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
		this.logInfo(" - got remote entry list with " + docEntries.size() + " entries");
		
		//	get document data from backing IMS
		ImsDocumentData docData = this.ims.getDocumentData(docId, true);
		this.logInfo(" - got local document data");
		
		//	sort out entries whose data is local, and count how many change at all
		int updatedDocEntries = 0;
		for (int e = 0; e < docEntries.size(); e++) {
			ImDocumentEntry entry = ((ImDocumentEntry) docEntries.get(e));
			if (docData.hasEntryData(entry)) {
				ImDocumentEntry oldEntry = docData.putEntry(entry);
				if ((oldEntry == null) || !oldEntry.dataHash.equals(entry.dataHash))
					updatedDocEntries++;
				else docEntries.remove(e--);
			}
		}
		this.logInfo(" - got " + docEntries.size() + " missing entries and " + updatedDocEntries + " updated ones to fetch");
		
		//	these two are absolutely in sync, we're done here
		if ((docEntries.size() + updatedDocEntries) == 0) {
			this.logInfo(" ==> document data already in sync");
			return null;
		}
		
		/* get remote provenance attributes (if not set, remote domain is
		 * immediate source of update, and we default to respective attributes
		 * or values) */
		String remoteUpdateUser = ((String) docAttributes.getAttribute(ORIGINAL_UPDATE_USER_ATTRIBUTE, docAttributes.getAttribute(UPDATE_USER_ATTRIBUTE)));
		String remoteUpdateTime = ((String) docAttributes.getAttribute(ORIGINAL_UPDATE_TIME_ATTRIBUTE, docAttributes.getAttribute(UPDATE_TIME_ATTRIBUTE)));
		String remoteUpdateDomain = ((String) docAttributes.getAttribute(ORIGINAL_UPDATE_DOMAIN_ATTRIBUTE, remoteDomain));
		
		/* set provenance attributes (IMS will overwrite update user and time,
		 * but we need the user name for storing the document) */
		docData.setAttribute(UPDATE_USER_ATTRIBUTE, remoteUpdateUser);
		docData.setAttribute(UPDATE_TIME_ATTRIBUTE, remoteUpdateTime);
		docData.setAttribute(ORIGINAL_UPDATE_USER_ATTRIBUTE, remoteUpdateUser);
		docData.setAttribute(ORIGINAL_UPDATE_TIME_ATTRIBUTE, remoteUpdateTime);
		docData.setAttribute(ORIGINAL_UPDATE_DOMAIN_ATTRIBUTE, remoteUpdateDomain);
		
		//	fetch any missing entries
		HashSet fetchedDocEntryNames = new HashSet();
		int fetchedNoDocEntriesErrorCount = 0;
		boolean isPreDocEntryError = true;
		while (docEntries.size() != 0) {
			this.logInfo(" - getting " + docEntries.size() + " entries");
			
			//	try and get missing document entries
			try {
				con = sc.getConnection();
				BufferedWriter bw = con.getWriter();
				
				bw.write(GET_DOCUMENT_ENTRIES);
				bw.newLine();
				bw.write("" + (docId + passPhrase).hashCode());
				bw.newLine();
				bw.write(docId);
				bw.newLine();
				for (int e = 0; e < docEntries.size(); e++) {
					bw.write(((ImDocumentEntry) docEntries.get(e)).toTabString());
					bw.newLine();
				}
				bw.newLine();
				bw.flush();
				
				BufferedLineInputStream blis = con.getInputStream();
				String error = blis.readLine();
				if (!GET_DOCUMENT_ENTRIES.equals(error))
					throw new IOException(error);
				
				isPreDocEntryError = false;
				ZipInputStream zin = new ZipInputStream(blis);
				byte[] buffer = new byte[1024];
				for (ZipEntry ze; (ze = zin.getNextEntry()) != null;) {
					ImDocumentEntry entry = new ImDocumentEntry(ze);
					OutputStream entryOut = docData.getOutputStream(entry);
					for (int r; (r = zin.read(buffer, 0, buffer.length)) != -1;)
						entryOut.write(buffer, 0, r);
					entryOut.flush();
					entryOut.close();
					fetchedDocEntryNames.add(entry.getFileName());
					fetchedNoDocEntriesErrorCount = 0;
					this.logInfo("   - got " + entry.name);
				}
			}
			
			//	throw exception to fail if we didn't get any new entries for a few rounds
			catch (IOException ioe) {
				if (isPreDocEntryError)
					throw ioe;
				else if (fetchedDocEntryNames.size() != 0)
					this.logInfo(" - caught " + ioe.getMessage() + ", trying again");
				else if (fetchedNoDocEntriesErrorCount < 10) {
					fetchedNoDocEntriesErrorCount++;
					this.logInfo(" - failed to get any entries, re-trying after " + fetchedNoDocEntriesErrorCount + " seconds");
					try {
						Thread.sleep(fetchedNoDocEntriesErrorCount * 1000);
					} catch (InterruptedException ie) {}
				}
				else {
					this.logInfo(" ==> failed to get any entries on 10 attempts, giving up");
					throw ioe;
				}
			}
			
			//	close connection no matter what
			finally {
				if (con != null)
					con.close();
				con = null;
			}
			
			//	remove entries we got this round
			for (int e = 0; e < docEntries.size(); e++) {
				ImDocumentEntry entry = ((ImDocumentEntry) docEntries.get(e));
				if (fetchedDocEntryNames.contains(entry.getFileName()))
					docEntries.remove(e--);
			}
			
			//	prepare for next round
			fetchedDocEntryNames.clear();
			isPreDocEntryError = true;
			
			//	wait a little so we don't incur the same error again right away
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ie) {}
		}
		
		//	finally ...
		this.logInfo(" ==> document data synchronized");
		return docData;
	}
	
	private ImsDocumentList getDocumentList(String remoteAddress, int remotePort) throws IOException {
		ServerConnection sc = ((remotePort == -1) ? ServerConnection.getServerConnection(remoteAddress) : ServerConnection.getServerConnection(remoteAddress, remotePort));
		Connection con = sc.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(GET_DOCUMENT_LIST);
		bw.newLine();
		bw.flush();
		
		BufferedReader br = con.getReader();
		String error = br.readLine();
		if (GET_DOCUMENT_LIST.equals(error))
			return ImsDocumentList.readDocumentList(br);
		else {
			con.close();
			throw new IOException(error);
		}
	}
}
//public class GoldenGateIMR extends AbstractGoldenGateServerComponent implements GoldenGateImsConstants {
//	
//	private static final String ORIGINAL_UPDATE_USER_ATTRIBUTE = "originalUpdateUser";
//	private static final String ORIGINAL_UPDATE_TIME_ATTRIBUTE = "originalUpdateTime";
//	private static final String ORIGINAL_UPDATE_DOMAIN_ATTRIBUTE = "originalUpdateDomain";
//	
//	private static final String GET_DOCUMENT = "IMR_GET_DOCUMENT";
//	private static final String GET_DOCUMENT_ENTRIES = "IMR_GET_DOCUMENT_ENTRIES";
//	private static final String GET_DOCUMENT_LIST = "IMR_GET_DOCUMENT_LIST";
//	
//	private static final String defaultPassPhrase = "IMR provides remote access!";
//	
//	/** The GoldenGATE DIO to work with */
//	protected GoldenGateIMS ims;
//	private GoldenGateRES res;
//	
//	private String localPassPhrase = null;
//	private Properties remotePassPhrases = new Properties();
//	
//	/** Constructor passing 'IMR' as the letter code to super constructor
//	 */
//	public GoldenGateIMR() {
//		super("IMR");
//	}
//	
//	/**
//	 * This implementation reads the pass phrases. Sub classes overwriting this
//	 * method have to make the super call.
//	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
//	 */
//	protected void initComponent() {
//		this.readPassPhrases();
//	}
//	
//	private void readPassPhrases() {
//		Settings passPhrases = Settings.loadSettings(new File(this.dataPath, "passPhrases.cnfg"));
//		
//		//	load pass phrases for incoming connections
//		this.localPassPhrase = passPhrases.getSetting("localPassPhrase", defaultPassPhrase);
//		
//		//	load pass phrases for accessing remote IMR's
//		Settings remotePassPhrases = passPhrases.getSubset("remotePassPhrase");
//		String[] remoteDomainNames = remotePassPhrases.getKeys();
//		for (int d = 0; d < remoteDomainNames.length; d++)
//			this.remotePassPhrases.setProperty(remoteDomainNames[d], remotePassPhrases.getSetting(remoteDomainNames[d]));
//	}
//	
//	/* (non-Javadoc)
//	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
//	 */
//	public void link() {
//		
//		//	get DIO
//		this.ims = ((GoldenGateIMS) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateIMS.class.getName()));
//		
//		//	check success
//		if (this.ims == null) throw new RuntimeException(GoldenGateIMS.class.getName());
//		
//		//	hook up to local RES
//		this.res = ((GoldenGateRES) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateRES.class.getName()));
//		
//		//	check success
//		if (this.res == null) throw new RuntimeException(GoldenGateRES.class.getName());
//	}
//	
//	/* (non-Javadoc)
//	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
//	 */
//	public void linkInit() {
//		
//		//	listen for events
//		GoldenGateServerEventService.addServerEventListener(new ResRemoteEventListener() {
//			public void notify(ResRemoteEvent rre) {
//				if (!ImsDocumentEvent.class.getName().equals(rre.eventClassName))
//					return;
//				
//				//	reconstruct and handle document event
//				ImsDocumentEvent ide = ImsDocumentEvent.parseEvent(rre.paramString);
//				handleDocumentEvent(rre, ide);
//			}
//		});
//		
//		//	prevent remote document updates from being re-published
//		this.res.addEventFilter(new ResEventFilter() {
//			public boolean allowPublishEvent(GoldenGateServerEvent gse) {
//				if ((gse instanceof ImsDocumentEvent) && ((ImsDocumentEvent) gse).user.startsWith("IMR."))
//					return false;
//				return true;
//			}
//		});
//	}
//	
//	void handleDocumentEvent(ResRemoteEvent rre, ImsDocumentEvent ide) {
//		
//		//	handle update event
//		if (ide.type == ImsDocumentEvent.UPDATE_TYPE) {
//			try {
//				System.out.println("GoldenGateIMR: updating from " + rre.sourceDomainAlias + " (" + rre.sourceDomainAddress + ":" + rre.sourceDomainPort + ") ...");
//				updateDocument(ide.documentId, rre.sourceDomainAddress, rre.sourceDomainPort, rre.sourceDomainAlias);
//			}
//			catch (IOException ioe) {
//				System.out.println("GoldenGateIMR: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while updating document " + ide.documentId + ".");
//				ioe.printStackTrace(System.out);
//			}
//		}
//		
//		//	handle delete event
//		else if (ide.type == ImsDocumentEvent.DELETE_TYPE) {
//			try {
//				ims.deleteDocument(("IMR." + rre.originDomainName), ide.documentId, null);
//			}
//			catch (IOException ioe) {
//				System.out.println("GoldenGateIMR: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document.");
//				ioe.printStackTrace(System.out);
//			}
//		}
//		
//		//	we are not interested in checkouts and releases (for now)
//	}
//	
//	private static final String READ_PASS_PHRASES_COMMAND = "readPassPhrases";
//	private static final String DIFF_FROM_IMR_COMMAND = "diff";
//	private static final String SYNC_WITH_IMR_COMMAND = "sync";
//	
//	private AsynchronousConsoleAction diffAction;
//	private AsynchronousConsoleAction syncAction;
//	
//	/* (non-Javadoc)
//	 * @see de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent#getActions()
//	 */
//	public ComponentAction[] getActions() {
//		ArrayList cal = new ArrayList();
//		ComponentAction ca;
//		
//		//	if local RES given, add console action for issuing updates events for all existing documents
//		if (this.res != null) {
//			ca = new AsynchronousConsoleAction("publishEvents", "Publish update events for the documents in the local DIO.", "update event", null, null) {
//				protected String[] getArgumentNames() {
//					return new String[0];
//				}
//				protected String checkArguments(String[] arguments) {
//					return ((arguments.length == 0) ? null : " Specify no arguments.");
//				}
//				protected void performAction(String[] arguments) throws Exception {
//					ImsDocumentList dl = ims.getDocumentListFull();
//					if (dl.hasNextDocument()) {
//						ImsDocumentListElement de = dl.getNextDocument();
//						RemoteEventList rel = ((GoldenGateRES) res).getEventList(0, GoldenGateIMS.class.getName());
//						HashSet deDuplicator = new HashSet();
//						while (rel.hasNextEvent()) {
//							ResRemoteEvent re = rel.getNextEvent();
//							if (re.type == ImsDocumentEvent.UPDATE_TYPE)
//								deDuplicator.add(re.eventId);
//						}
//						int existingEventCount = deDuplicator.size();
//						this.enteringMainLoop("Got " + existingEventCount + " existing document update events, enqueued 0 new ones");
//						
//						int newEventCount = 0;
//						do {
//							String docId = ((String) de.getAttribute(DOCUMENT_ID_ATTRIBUTE));
//							String updateUser = ((String) de.getAttribute(UPDATE_USER_ATTRIBUTE));
//							long updateTime = Long.parseLong((String) de.getAttribute(UPDATE_TIME_ATTRIBUTE));
//							int version = Integer.parseInt((String) de.getAttribute(DOCUMENT_VERSION_ATTRIBUTE));
//							
//							//	import local updates only, as importing remote updates might create new update IDs and thus cause unnecessary traffic
//							if (!updateUser.startsWith("IMR.")) {
//								ImsDocumentEvent dde = new ImsDocumentEvent(updateUser, docId, null, version, ImsDocumentEvent.UPDATE_TYPE, GoldenGateIMS.class.getName(), updateTime, null);
//								if (deDuplicator.add(dde.eventId)) {
//									((GoldenGateRES) res).publishEvent(dde);
//									this.loopRoundComplete("Got " + existingEventCount + " existing update events, enqueued " + (++newEventCount) + " new ones");
//								}
//							}
//						}
//						while (this.continueAction() && ((de = dl.getNextDocument()) != null));
//					}
//					else this.log(" There are no documents available in DIO.");
//				}
//			};
//			cal.add(ca);
//		}
//		
//		//	request for document
//		ca = new ComponentActionNetwork() {
//			public String getActionCommand() {
//				return GET_DOCUMENT;
//			}
//			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
//				
//				//	get pass phrase and document ID
//				String passPhraseHash = input.readLine();
//				String docId = input.readLine();
//				
//				//	check authentication
//				if (!passPhraseHash.equals("" + (docId + localPassPhrase).hashCode())) {
//					output.write("Invalid pass phrase for loading document with ID " + docId);
//					output.newLine();
//					return;
//				}
//				
//				//	get document proxy from IMS
//				ImsDocumentData docData = ims.getDocumentData(docId, false);
//				if (docData == null) {
//					output.write("Invalid document ID " + docId);
//					output.newLine();
//					return;
//				}
//				
//				//	send back document entry list
//				ImDocumentEntry[] docEntries = docData.getEntries();
//				output.write(GET_DOCUMENT);
//				output.newLine();
//				output.write(ImDocumentIO.getAttributesString(docData));
//				output.newLine();
//				for (int e = 0; e < docEntries.length; e++) {
//					output.write(docEntries[e].toTabString());
//					output.newLine();
//				}
//				output.newLine();
//				output.flush();
//			}
//		};
//		cal.add(ca);
//		
//		// deliver document entries through network
//		ca = new ComponentActionNetwork() {
//			public String getActionCommand() {
//				return GET_DOCUMENT_ENTRIES;
//			}
//			public void performActionNetwork(BufferedLineInputStream input, BufferedLineOutputStream output) throws IOException {
//				
//				//	get pass phrase and document ID
//				String passPhraseHash = input.readLine();
//				String docId = input.readLine();
//				
//				//	check authentication
//				if (!passPhraseHash.equals("" + (docId + localPassPhrase).hashCode())) {
//					output.write("Invalid pass phrase for loading document with ID " + docId);
//					output.newLine();
//					return;
//				}
//				
//				//	get entry list
//				ArrayList docEntries = new ArrayList();
//				for (String entryString; (entryString = input.readLine()) != null;) {
//					if (entryString.length() == 0)
//						break;
//					ImDocumentEntry entry = ImDocumentEntry.fromTabString(entryString);
//					if (entry != null)
//						docEntries.add(entry);
//				}
//				
//				//	get document proxy from IMS
//				ImDocumentData docData = ims.getDocumentData(docId, false);
//				
//				//	send requested entries
//				ZipOutputStream zout = new ZipOutputStream(output);
//				byte[] buffer = new byte[1024];
//				for (int e = 0; e < docEntries.size(); e++) {
//					ImDocumentEntry entry = ((ImDocumentEntry) docEntries.get(e));
//					ZipEntry ze = new ZipEntry(entry.getFileName());
//					ze.setTime(entry.updateTime);
//					zout.putNextEntry(ze);
//					InputStream entryIn = docData.getInputStream(entry);
//					for (int r; (r = entryIn.read(buffer, 0, buffer.length)) != -1;)
//						zout.write(buffer, 0, r);
//					entryIn.close();
//					zout.closeEntry();
//				}
//				zout.flush();
//			}
//		};
//		cal.add(ca);
//		
//		//	request for document list
//		ca = new ComponentActionNetwork() {
//			public String getActionCommand() {
//				return GET_DOCUMENT_LIST;
//			}
//			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
//				ImsDocumentList dl = ims.getDocumentListFull();
//				
//				output.write(GET_DOCUMENT_LIST);
//				output.newLine();
//				
//				dl.writeData(output);
//			}
//		};
//		cal.add(ca);
//		
//		//	re-read pass phrases
//		ca = new ComponentActionConsole() {
//			public String getActionCommand() {
//				return READ_PASS_PHRASES_COMMAND;
//			}
//			public String[] getExplanation() {
//				String[] explanation = {
//						READ_PASS_PHRASES_COMMAND,
//						"Re-read the local and remote pass phrases from the config file.",
//					};
//				return explanation;
//			}
//			public void performActionConsole(String[] arguments) {
//				if (arguments.length == 0) {
//					readPassPhrases();
//					System.out.println(" Pass phrases re-read.");
//				}
//				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
//			}
//		};
//		cal.add(ca);
//		
//		//	diff events with remote IMR
//		this.diffAction = new AsynchronousConsoleAction(DIFF_FROM_IMR_COMMAND, "Run a full diff with a specific remote GoldenGATE IMR, i.e., compare document update events and handle unhandled ones", "update event", null, null) {
//			protected String[] getArgumentNames() {
//				String[] args = {"remoteDomain"};
//				return args;
//			}
//			protected String[] getArgumentExplanation(String argument) {
//				if ("remoteDomain".equals(argument)) {
//					String[] explanation = {"The alias of the remote GoldenGATE IMR to compare the document list with"};
//					return explanation;
//				}
//				else return super.getArgumentExplanation(argument);
//			}
//			protected void checkRunnable() {
//				if (syncAction.isRunning())
//					throw new RuntimeException("Document list sync running, diff cannot run in parallel");
//			}
//			protected String checkArguments(String[] arguments) {
//				if (arguments.length != 1)
//					return ("Invalid arguments for '" + this.getActionCommand() + "', specify the alias of the IMR to diff with as the only argument.");
//				String address = res.getRemoteDomainAddress(arguments[0]);
//				if (address == null)
//					return ("No remote IMR found for name " + arguments[0]);
//				else return null;
//			}
//			protected void performAction(String[] arguments) throws Exception {
//				String remoteDomain = arguments[0];
//				
//				//	get remote events
//				RemoteEventList rel = res.getRemoteEventList(remoteDomain, 0, GoldenGateIMS.class.getName());
//				if (rel == null)
//					return;
//				this.enteringMainLoop("Got event list from " + remoteDomain);
//				int handleCount = 0;
//				int skipCount = 0;
//				
//				//	do diff
//				while (this.continueAction() && rel.hasNextEvent()) {
//					ResRemoteEvent rre = rel.getNextEvent();
//					if (!ImsDocumentEvent.class.getName().equals(rre.eventClassName))
//						continue;
//					
//					//	reconstruct document event
//					ImsDocumentEvent dde = ImsDocumentEvent.parseEvent(rre.paramString);
//					
//					//	check against local update time
//					Attributed docAttributes = ims.getDocumentAttributes(dde.documentId);
//					if (docAttributes != null) {
//						long updateTime = Long.parseLong((String) docAttributes.getAttribute(ORIGINAL_UPDATE_TIME_ATTRIBUTE, docAttributes.getAttribute(UPDATE_TIME_ATTRIBUTE, "0")));
//						if (rre.eventTime < updateTime) {
//							skipCount++;
//							continue;
//						}
//					}
//					
//					//	handle document event
//					handleDocumentEvent(new ResRemoteEvent(rre, remoteDomain, res.getRemoteDomainAddress(remoteDomain), res.getRemoteDomainPort(remoteDomain)), dde);
//					handleCount++;
//					
//					//	update status
//					this.loopRoundComplete("Handled " + handleCount + " update events, skipped " + skipCount + " ones.");
//				}
//			}
//		};
//		cal.add(this.diffAction);
//		
//		//	sync documents with a remote IMR
//		this.syncAction = new AsynchronousConsoleAction(SYNC_WITH_IMR_COMMAND, "Run a full sync with a specific remote GoldenGATE IMR, i.e., compare the document lists and fetch missing updates", "document list", null, null) {
//			protected String[] getArgumentNames() {
//				String[] args = {"remoteDomain", "mode"};
//				return args;
//			}
//			protected String[] getArgumentExplanation(String argument) {
//				if ("remoteDomain".equals(argument)) {
//					String[] explanation = {"The alias of the remote GoldenGATE IMR to compare the document list with"};
//					return explanation;
//				}
//				else if ("mode".equals(argument)) {
//					String[] explanation = {"The sync mode: '-u' for 'update' (the default), '-d' for 'delete', or '-ud' for both"};
//					return explanation;
//				}
//				else return super.getArgumentExplanation(argument);
//			}
//			protected void checkRunnable() {
//				if (diffAction.isRunning())
//					throw new RuntimeException("Update event diff running, sync cannot run in parallel");
//			}
//			protected String checkArguments(String[] arguments) {
//				if ((arguments.length < 1) || (arguments.length > 2)) 
//					return ("Invalid arguments for '" + this.getActionCommand() + "', specify the alias of the IMR to sync with, and optionally the sync mode, as the only arguments.");
//				String address = res.getRemoteDomainAddress(arguments[0]);
//				if (address == null)
//					return ("No remote IMR found for name " + arguments[0]);
//				else if ((arguments.length == 2) && ("-u -d -ud".indexOf(arguments[1]) == -1))
//					return ("Invalid sync mode " + arguments[1]);
//				else return null;
//			}
//			protected void performAction(String[] arguments) throws Exception {
//				String remoteDomain = arguments[0];
//				boolean update = ((arguments.length == 1) || (arguments[1].indexOf("u") != -1));
//				boolean delete = ((arguments.length == 2) && (arguments[1].indexOf("d") != -1));
//				
//				//	get remote domain access data
//				String remoteAddress = res.getRemoteDomainAddress(remoteDomain);
//				int remotePort = res.getRemoteDomainPort(remoteDomain);
//				
//				//	get document list from remote domain, and index document records by ID
//				ImsDocumentList remoteDl = getDocumentList(remoteAddress, remotePort);
//				HashMap remoteDlesById = new HashMap();
//				while (remoteDl.hasNextDocument()) {
//					ImsDocumentListElement dle = remoteDl.getNextDocument();
//					remoteDlesById.put(((String) dle.getAttribute(DOCUMENT_ID_ATTRIBUTE)), dle);
//				}
//				
//				//	iterate over local document list, collecting IDs of documents to update or delete
//				HashSet updateDocIDs = new HashSet();
//				HashSet deleteDocIDs = new HashSet();
//				ImsDocumentList localDl = ims.getDocumentListFull();
//				while (localDl.hasNextDocument()) {
//					ImsDocumentListElement localDle = localDl.getNextDocument();
//					String docId = ((String) localDle.getAttribute(DOCUMENT_ID_ATTRIBUTE));
//					ImsDocumentListElement remoteDle = ((ImsDocumentListElement) remoteDlesById.remove(docId));
//					
//					//	this one doesn't even exist in the remote domain
//					if (remoteDle == null) {
//						if (delete)
//							deleteDocIDs.add(docId);
//						continue;
//					}
//					
//					//	extract update timestamps for comparison
//					long localUpdateTime;
//					long remoteUpdateTime;
//					try {
//						localUpdateTime = Long.parseLong((String) localDle.getAttribute(UPDATE_TIME_ATTRIBUTE));
//						remoteUpdateTime = Long.parseLong((String) remoteDle.getAttribute(UPDATE_TIME_ATTRIBUTE));
//					}
//					catch (Exception e) {
//						this.log(("Could not parse update timestamps for document '" + docId + "'"), e);
//						continue;
//					}
//					
//					//	remote version is newer than local one, even with a one second tolerance), mark for update
//					if (update && ((localUpdateTime + 1000) < remoteUpdateTime))
//						updateDocIDs.add(docId);
//				}
//				
//				//	add updates for new document not yet available locally
//				if (update)
//					updateDocIDs.addAll(remoteDlesById.keySet());
//				
//				//	do updates and deletions
//				int updateCount = updateDocIDs.size();
//				int deleteCount = deleteDocIDs.size();
//				this.enteringMainLoop("Got event list from " + remoteDomain + ", " + updateCount + " updates, " + deleteCount + " deletions");
//				while (this.continueAction() && ((updateDocIDs.size() + deleteDocIDs.size()) != 0)) {
//					
//					//	do deletions first ...
//					if (deleteDocIDs.size() != 0) {
//						String docId = ((String) deleteDocIDs.iterator().next());
//						deleteDocIDs.remove(docId);
//						System.out.println("GoldenGateIMR: forwarding deletion from " + remoteDomain + " (" + remoteAddress + ":" + remotePort + ") ...");
//						try {
//							
//							//	get update user, and reuse if starting with 'IMR.'
//							Attributed docAttributes = ims.getDocumentAttributes(docId);
//							String updateUser = ((String) docAttributes.getAttribute(UPDATE_USER_ATTRIBUTE));
//							if ((updateUser == null) || !updateUser.startsWith("IMR."))
//								updateUser = ("IMR." + remoteDomain);
//							
//							//	delete document
//							ims.deleteDocument(updateUser, docId, null);
//						}
//						catch (IOException ioe) {
//							System.out.println("GoldenGateIMR: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document " + docId + ".");
//							ioe.printStackTrace(System.out);
//						}
//					}
//					
//					//	... and updates second
//					else if (updateDocIDs.size() != 0) {
//						String docId = ((String) updateDocIDs.iterator().next());
//						updateDocIDs.remove(docId);
//						System.out.println("GoldenGateIMR: getting update from " + remoteDomain + " (" + remoteAddress + ":" + remotePort + ") ...");
//						try {
//							updateDocument(docId, remoteAddress, remotePort, remoteDomain);
//						}
//						catch (IOException ioe) {
//							System.out.println("GoldenGateIMR: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while updating document " + docId + ".");
//							ioe.printStackTrace(System.out);
//						}
//					}
//					
//					//	update status
//					this.loopRoundComplete("Handled " + (updateCount - updateDocIDs.size()) + " of " + updateCount + " updates, " + (deleteCount - deleteDocIDs.size()) + " of " + deleteCount + " deletions.");
//				}
//			}
//		};
//		cal.add(this.syncAction);
//		
//		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
//	}
//	
//	void updateDocument(String docId, String sourceDomainAddress, int sourceDomainPort, String sourceDomainAlias) throws IOException {
//		
//		//	get document data (null if no changes at all)
//		ImsDocumentData docData = getDocumentData(docId, sourceDomainAddress, sourceDomainPort, sourceDomainAlias);
//		if (docData == null)
//			return;
//		
//		//	get update user
//		String updateUser = ((String) docData.getAttribute(ORIGINAL_UPDATE_USER_ATTRIBUTE));
//		if (updateUser == null)
//			updateUser = ("IMR." + sourceDomainAlias);
//		
//		//	get original update domain
//		String updateDomain = ((String) docData.getAttribute(ORIGINAL_UPDATE_DOMAIN_ATTRIBUTE));
//		if (updateDomain == null)
//			updateDomain = sourceDomainAlias;
//		
//		//	store document
//		ims.updateDocumentFromData(updateUser, ("IMR." + updateDomain), docData, null);
//	}
//	
//	ImsDocumentData getDocumentData(String docId, String remoteAddress, int remotePort, String remoteDomain) throws IOException {
//		ServerConnection sc = ((remotePort == -1) ? ServerConnection.getServerConnection(remoteAddress) : ServerConnection.getServerConnection(remoteAddress, remotePort));
//		String passPhrase = this.remotePassPhrases.getProperty(remoteDomain, defaultPassPhrase);
//		Connection con = null;
//		
//		//	get current document entry list
//		Attributed docAttributes = new AbstractAttributed();
//		ArrayList docEntries = new ArrayList();
//		try {
//			con = sc.getConnection();
//			BufferedWriter bw = con.getWriter();
//			
//			bw.write(GET_DOCUMENT);
//			bw.newLine();
//			bw.write("" + (docId + passPhrase).hashCode());
//			bw.newLine();
//			bw.write(docId);
//			bw.newLine();
//			bw.flush();
//			
//			BufferedReader br = con.getReader();
//			String error = br.readLine();
//			if (GET_DOCUMENT.equals(error)) {
//				ImDocumentIO.setAttributes(docAttributes, br.readLine());
//				for (String entryString; (entryString = br.readLine()) != null;) {
//					if (entryString.length() == 0)
//						break;
//					ImDocumentEntry entry = ImDocumentEntry.fromTabString(entryString);
//					if (entry != null)
//						docEntries.add(entry);
//				}
//			}
//			else throw new IOException(error);
//		}
//		finally {
//			if (con != null)
//				con.close();
//		}
//		
//		//	get document data from backing IMS
//		ImsDocumentData docData = this.ims.getDocumentData(docId, true);
//		
//		//	sort out entries whose data is local, and count how many change at all
//		int updatedDocEntries = 0;
//		for (int e = 0; e < docEntries.size(); e++) {
//			ImDocumentEntry entry = ((ImDocumentEntry) docEntries.get(e));
//			if (docData.hasEntryData(entry)) {
//				ImDocumentEntry oldEntry = docData.putEntry(entry);
//				if ((oldEntry == null) || !oldEntry.dataHash.equals(entry.dataHash))
//					updatedDocEntries++;
//				docEntries.remove(e--);
//			}
//		}
//		
//		//	these two are absolutely in sync, we're done here
//		if ((docEntries.size() + updatedDocEntries) == 0)
//			return null;
//		
//		/* get remote provenance attributes (if not set, remote domain is
//		 * immediate source of update, and we default to respective attributes
//		 * or values) */
//		String remoteUpdateUser = ((String) docAttributes.getAttribute(ORIGINAL_UPDATE_USER_ATTRIBUTE, docAttributes.getAttribute(UPDATE_USER_ATTRIBUTE)));
//		String remoteUpdateTime = ((String) docAttributes.getAttribute(ORIGINAL_UPDATE_TIME_ATTRIBUTE, docAttributes.getAttribute(UPDATE_TIME_ATTRIBUTE)));
//		String remoteUpdateDomain = ((String) docAttributes.getAttribute(ORIGINAL_UPDATE_DOMAIN_ATTRIBUTE, remoteDomain));
//		
//		/* set provenance attributes (IMS will overwrite update user and time,
//		 * but we need the user name for storing the document) */
//		docData.setAttribute(CHECKIN_USER_ATTRIBUTE, docData.getAttribute(CHECKIN_USER_ATTRIBUTE));
//		docData.setAttribute(CHECKIN_TIME_ATTRIBUTE, docData.getAttribute(CHECKIN_TIME_ATTRIBUTE));
//		docData.setAttribute(UPDATE_USER_ATTRIBUTE, remoteUpdateUser);
//		docData.setAttribute(UPDATE_TIME_ATTRIBUTE, remoteUpdateTime);
//		docData.setAttribute(ORIGINAL_UPDATE_USER_ATTRIBUTE, remoteUpdateUser);
//		docData.setAttribute(ORIGINAL_UPDATE_TIME_ATTRIBUTE, remoteUpdateTime);
//		docData.setAttribute(ORIGINAL_UPDATE_DOMAIN_ATTRIBUTE, remoteUpdateDomain);
//		
//		//	fetch any missing entries
//		if (docEntries.size() != 0) try {
//			con = sc.getConnection();
//			BufferedWriter bw = con.getWriter();
//			
//			bw.write(GET_DOCUMENT_ENTRIES);
//			bw.newLine();
//			bw.write("" + (docId + passPhrase).hashCode());
//			bw.newLine();
//			bw.write(docId);
//			bw.newLine();
//			for (int e = 0; e < docEntries.size(); e++) {
//				bw.write(((ImDocumentEntry) docEntries.get(e)).toTabString());
//				bw.newLine();
//			}
//			bw.newLine();
//			bw.flush();
//			
//			BufferedLineInputStream blis = con.getInputStream();
//			String error = blis.readLine();
//			if (!GET_DOCUMENT_ENTRIES.equals(error))
//				throw new IOException(error);
//			
//			ZipInputStream zin = new ZipInputStream(blis);
//			byte[] buffer = new byte[1024];
//			for (ZipEntry ze; (ze = zin.getNextEntry()) != null;) {
//				ImDocumentEntry entry = new ImDocumentEntry(ze);
//				OutputStream entryOut = docData.getOutputStream(entry);
//				for (int r; (r = zin.read(buffer, 0, buffer.length)) != -1;)
//					entryOut.write(buffer, 0, r);
//				entryOut.flush();
//				entryOut.close();
//			}
//		}
//		finally {
//			if (con != null)
//				con.close();
//			con = null;
//		}
//		
//		//	finally ...
//		return docData;
//	}
//	
//	private ImsDocumentList getDocumentList(String remoteAddress, int remotePort) throws IOException {
//		ServerConnection sc = ((remotePort == -1) ? ServerConnection.getServerConnection(remoteAddress) : ServerConnection.getServerConnection(remoteAddress, remotePort));
//		Connection con = sc.getConnection();
//		BufferedWriter bw = con.getWriter();
//		
//		bw.write(GET_DOCUMENT_LIST);
//		bw.newLine();
//		bw.flush();
//		
//		BufferedReader br = con.getReader();
//		String error = br.readLine();
//		if (GET_DOCUMENT_LIST.equals(error))
//			return ImsDocumentList.readDocumentList(br);
//		else {
//			con.close();
//			throw new IOException(error);
//		}
//	}
//}
