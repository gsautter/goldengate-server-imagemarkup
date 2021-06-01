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
package de.uka.ipd.idaho.goldenGateServer.ime.client;

import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.easyIO.utilities.AbstractHttpsEnabler;
import de.uka.ipd.idaho.gamta.util.DocumentErrorSummary;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.gamta.util.swing.DialogFactory;
import de.uka.ipd.idaho.gamta.util.transfer.DocumentList;
import de.uka.ipd.idaho.goldenGate.qc.imagine.GgImagineQcToolDataBaseProvider;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants;
import de.uka.ipd.idaho.goldenGateServer.ims.client.GoldenGateImsClient;
import de.uka.ipd.idaho.goldenGateServer.ims.client.GoldenGateImsClient.DocumentDataCache;
import de.uka.ipd.idaho.goldenGateServer.ims.client.GoldenGateImsClient.FastFetchFilter;
import de.uka.ipd.idaho.goldenGateServer.ims.client.GoldenGateImsClient.ImsClientDocumentData;
import de.uka.ipd.idaho.goldenGateServer.ims.client.GoldenGateImsDocumentIO;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticatedClient;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticationManagerPlugin;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.ImSupplement;
import de.uka.ipd.idaho.im.util.ImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.FolderImDocumentData;
import de.uka.ipd.idaho.im.util.ImDocumentData.ImDocumentEntry;
import de.uka.ipd.idaho.im.util.ImDocumentErrorProtocol;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * QC Tool data base provider connecting to backing GoldenGATE Server, namely
 * to IMS and IME.
 * 
 * @author sautter
 */
public class ImeQcToolDataBaseProvider extends GgImagineQcToolDataBaseProvider implements GoldenGateImsConstants {
	private AuthenticationManagerPlugin authManager;
	private GoldenGateImsDocumentIO imsDocIo; // just to make sure configuration export brings along IMS stuff
	private Settings configuration;
	
	/** zero-argument constructor for class loading */
	public ImeQcToolDataBaseProvider() {}
	
	public void init() {
		
		//	load configuration
		try {
			this.configuration = Settings.loadSettings(this.dataProvider.getInputStream("config.cnfg"));
		}
		catch (IOException ioe) {
			System.out.println("Could not load configuration: " + ioe.getMessage());
			ioe.printStackTrace(System.out);
		}
	}
	
	public String getPluginName() {
		return "IM QcTool Server Connector";
	}
	
	public String getDataSourceName() {
		return "GoldenGATE Server";
	}
	
	public String[] getParameterDescriptions() {
		String[] pds = {
			"HOST:\tthe host name of the GoldenGATE Server providing the data to process:",
			"\t- set to a plain host name to indicate host+port access",
			"\t- set to a URL (starting with 'http://') to indicate HTTP access",
			"PORT:\tthe port of the GoldenGATE Server to upload to (with host+port access)",
			"AUTH:\tthe login data for the GoldenGATE Server to upload to (as 'user:pwd')"
		};
		return pds;
	}
	
	public GgImagineQcToolDataBase getDataBase(File cacheFolder, Properties parameters) throws IOException {
		
		//	this is what we ultimately need
		AuthenticatedClient authClt;
		
		//	we're in interactive mode
		if (parameters == null) {
			
			//	we need a cache !!!
			if (cacheFolder == null)
				throw new IOException("Cannot work without cache folder in GoldenGATE Server mode");
			
			//	get authentication manager
			if (this.authManager == null)
				this.authManager = ((AuthenticationManagerPlugin) this.parent.getPlugin(AuthenticationManagerPlugin.class.getName()));
			if (this.authManager == null)
				return null;
			
			//	use authentication plug-in
			authClt = this.authManager.getAuthenticatedClient();
			if (authClt == null)
				return null;
		}
		
		//	we're on command line arguments
		else {
			
			//	get parameters
			String hostOrUrl = parameters.getProperty("HOST");
			int port = -1;
			try {
				port = Integer.parseInt(parameters.getProperty("PORT", "-1"));
			} catch (NumberFormatException nfe) {}
			String auth = parameters.getProperty("AUTH");
			
			//	check required parameters (we might not be the intended provider, after all)
			if ((hostOrUrl == null) || (auth == null))
				return null;
			
			//	we need a cache (but only if we're actually the one the parameters are meant for) !!!
			if (cacheFolder == null)
				throw new IOException("Cannot work without cache folder in GoldenGATE Server mode");
			
			//	check host and port
			if (hostOrUrl.toLowerCase().startsWith("http://") || hostOrUrl.toLowerCase().startsWith("https://"))
				port = -1;
			else if (port == -1) {
				System.out.println("Port of target GoldenGATE Server missing.");
				System.out.println("Use 'PORT=<port>' to specify the port number.");
				System.out.println("The port number may only be omitted if 'HOST' is set to a URL.");
				throw new IOException("Cannot directly connect to GoldenGATE server without port number");
			}
			
			//	enable HTTPS if required
			if (hostOrUrl.toLowerCase().startsWith("https://"))
				AbstractHttpsEnabler.enableHttps(); // we generally trust what user enters in command line
			
			//	connect to target GoldenGATE Server
			ServerConnection srvCon;
			if (port == -1)
				srvCon = ServerConnection.getServerConnection(hostOrUrl);
			else srvCon = ServerConnection.getServerConnection(hostOrUrl, port);
			
			//	check authentication
			if (auth.indexOf(':') < 1) {
				System.out.println("Invalid login data for target GoldenGATE Server.");
				System.out.println("Specify the login data as 'AUTH=<user>:<pwd>'.");
				throw new IOException("Cannot connect to GoldenGATE server without proper authentication");
			}
			String userName = auth.substring(0, auth.indexOf(':'));
			String userPwd = auth.substring(auth.indexOf(':') + ":".length());
			
			//	log in to target GoldenGATE Server
			authClt = AuthenticatedClient.getAuthenticatedClient(srvCon);
			try {
				if (authClt.login(userName, userPwd))
					System.out.println("Logged in to GoldenGATE Server at '" + hostOrUrl + ((port == -1) ? "" : (":" + port)) + "'");
				else {
					System.out.println("Login to GoldenGATE Server at '" + hostOrUrl + ((port == -1) ? "" : (":" + port)) + "' failed.");
					System.out.println("Please check parameter AUTH.");
					throw new IOException("Cannot connect to GoldenGATE server without valid authentication");
				}
			}
			catch (IOException ioe) {
				System.out.println("Failed to connect to GoldenGATE Server at '" + hostOrUrl + ((port == -1) ? "" : (":" + port)) + "'");
				System.out.println("Please check parameter" + ((port == -1) ? (" HOST") : "s HOST and PORT") + ".");
				System.out.println();
				System.out.println("Detailed error message:");
				ioe.printStackTrace(System.out);
				throw new IOException("Cannot connect ro GoldenGATE server without valid authentication");
			}
		}
		
		//	we're all set
		return new ImeQcToolDataBase(cacheFolder, authClt, this.configuration);
	}
	
	private static class ImeQcToolDataBase extends GgImagineQcToolDataBase {
		private int pageImageFetchMode = FastFetchFilter.FETCH_DEFERRED;
		private int supplementFetchMode = FastFetchFilter.FETCH_ON_DEMAND;
		private FastFetchFilter fastFetchFilter = new FastFetchFilter() {
			public int getFetchMode(ImDocumentEntry entry) {
				if (entry.name.startsWith(ImSupplement.SOURCE_TYPE + "."))
					return supplementFetchMode;
				else if (entry.name.startsWith(ImSupplement.FIGURE_TYPE + "@"))
					return supplementFetchMode;
				else if (entry.name.matches(ImSupplement.FIGURE_TYPE + "\\-[0-9]+" + "\\@.*"))
					return supplementFetchMode;
				else if (entry.name.startsWith(ImSupplement.SCAN_TYPE + "@"))
					return supplementFetchMode;
				else if (entry.name.matches(ImSupplement.SCAN_TYPE + "\\-[0-9]+" + "\\@.*"))
					return supplementFetchMode;
				else if (entry.name.startsWith("page") && entry.name.endsWith(".png")) {
					String pidStr = entry.name;
					pidStr = pidStr.substring("page".length());
					pidStr = pidStr.substring(0, (pidStr.length() - ".png".length()));
					while (pidStr.startsWith("0"))
						pidStr = pidStr.substring("0".length());
					try {
						int pid = Integer.parseInt(pidStr);
						return ((pid < 5) ? FETCH_IMMEDIATELY : pageImageFetchMode);
					}
					catch (NumberFormatException nfe) {
						return FETCH_IMMEDIATELY;
					}
					
				}
				else return FETCH_IMMEDIATELY;
			}
		};
		private String fastFetchFilterLabel = "Page Images: fetch deferred - Supplements: fetch on demand";
		private JButton fetchModeButton = new JButton("Fetch Mode");
		
		private AuthenticatedClient authClient;
		private GoldenGateImsClient imsClient;
		private GoldenGateImeClient imeClient;
		
		ImeQcToolDataBase(File cacheFolder, AuthenticatedClient authClt, Settings config) {
			super(cacheFolder);
			
			//	store user name and password (required for re-authentication after session timeout)
			this.authClient = authClt;
			
			//	create IMS and IME clients
			this.imsClient = new GoldenGateImsClient(authClt, new DocumentDataCache() {
				public ImsClientDocumentData getDocumentData(String docId) throws IOException {
					File docCacheFolder = getDocumentCacheFolder(docId, true);
					ImDocumentData localDocData;
					if ((new File(docCacheFolder, "entries.txt")).exists())
						localDocData = new FolderImDocumentData(docCacheFolder, null);
					else localDocData = new FolderImDocumentData(docCacheFolder);
					return new ImsClientDocumentData(docId, localDocData);
				}
				public void storeEntryList(ImsClientDocumentData docData) throws IOException {
					((FolderImDocumentData) docData.getLocalDocData()).storeEntryList();
				}
			});
			this.imeClient = new GoldenGateImeClient(authClt);
			
			//	initialize fast fetch mode
			if (config != null) try {
				this.pageImageFetchMode = Integer.parseInt(config.getSetting("pageImageFetchMode", ("" + this.pageImageFetchMode)));
			} catch (NumberFormatException nfe) {}
			if (config != null) try {
				this.supplementFetchMode = Integer.parseInt(config.getSetting("supplementFetchMode", ("" + this.supplementFetchMode)));
			} catch (NumberFormatException nfe) {}
			this.updateFastFetchFilterLabel();
		}
		protected String getDataSourceName() {
			return "GoldenGATE Server";
		}
		protected JButton[] getButtons() {
			this.fetchModeButton = new JButton("Fetch Mode");
			this.fetchModeButton.setToolTipText(this.fastFetchFilterLabel);
			this.fetchModeButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					if (editFastFetchStrategy())
						fetchModeButton.setToolTipText(fastFetchFilterLabel);
				}
			});
			JButton[] bs = {
				this.fetchModeButton	
			};
			return bs;
		}
		private static class FetchModeOption {
			final int mode;
			final String label;
			FetchModeOption(int mode) {
				this.mode = mode;
				this.label = getFetchModeLabel(this.mode);
			}
			public String toString() {
				return this.label;
			}
		}
		private static FetchModeOption[] fetchModeOptions = {
			new FetchModeOption(FastFetchFilter.FETCH_IMMEDIATELY),
			new FetchModeOption(FastFetchFilter.FETCH_DEFERRED),
			new FetchModeOption(FastFetchFilter.FETCH_ON_DEMAND)
		};
		private static String getFetchModeLabel(int fm) {
			if (fm == FastFetchFilter.FETCH_ON_DEMAND)
				return "on demand";
			else if (fm == FastFetchFilter.FETCH_DEFERRED)
				return "deferred";
			else return "immediately";
		}
		boolean editFastFetchStrategy() {
			JComboBox piFetchMode = new JComboBox(fetchModeOptions);
			piFetchMode.setSelectedIndex(this.pageImageFetchMode);
			piFetchMode.setEditable(false);
			JPanel piFetchModePanel = new JPanel(new BorderLayout());
			piFetchModePanel.add(new JLabel("Page Images: fetch "), BorderLayout.CENTER);
			piFetchModePanel.add(piFetchMode, BorderLayout.EAST);
			
			JComboBox sFetchMode = new JComboBox(fetchModeOptions);
			sFetchMode.setSelectedIndex(this.supplementFetchMode);
			sFetchMode.setEditable(false);
			JPanel sFetchModePanel = new JPanel(new BorderLayout());
			sFetchModePanel.add(new JLabel("Supplements: fetch "), BorderLayout.CENTER);
			sFetchModePanel.add(sFetchMode, BorderLayout.EAST);
			
			JPanel fetchModePanel = new JPanel(new GridLayout(0, 1));
			fetchModePanel.add(piFetchModePanel);
			fetchModePanel.add(sFetchModePanel);
			
			int choice = DialogFactory.confirm(fetchModePanel, "Select Fast Fetch Options", JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE);
			if (choice != JOptionPane.OK_OPTION)
				return false;
			
			this.pageImageFetchMode = ((FetchModeOption) piFetchMode.getSelectedItem()).mode;
			this.supplementFetchMode = ((FetchModeOption) sFetchMode.getSelectedItem()).mode;
			this.updateFastFetchFilterLabel();
			return true;
		}
		private void updateFastFetchFilterLabel() {
			this.fastFetchFilterLabel = ("Page Images: fetch " + getFetchModeLabel(this.pageImageFetchMode) + " - Supplements: fetch " + getFetchModeLabel(this.supplementFetchMode));
		}
		protected DocumentList loadDocumentList(ProgressMonitor pm) throws IOException {
			if (this.authClient.ensureLoggedIn())
				return this.imeClient.getDocumentList(pm);
			else throw new IOException("The connected GoldenGATE Server cannot be reached at this time, please try again later.");
		}
		protected boolean isUtcTimeField(String fieldName) {
			return (false
					|| CHECKIN_TIME_ATTRIBUTE.equals(fieldName)
					|| UPDATE_TIME_ATTRIBUTE.equals(fieldName)
					|| CHECKOUT_TIME_ATTRIBUTE.equals(fieldName)
				);
		}
		protected DocumentErrorSummary getErrorSummary(String docId) throws IOException {
			if (this.authClient.ensureLoggedIn())
				return this.imeClient.getErrorSummary(docId);
			else throw new IOException("The connected GoldenGATE Server cannot be reached at this time, please try again later.");
		}
		protected ImDocumentErrorProtocol getErrorProtocol(String docId, ImDocument doc) throws IOException {
			if (this.authClient.ensureLoggedIn())
				return this.imeClient.getErrorProtocol(docId);
			else throw new IOException("The connected GoldenGATE Server cannot be reached at this time, please try again later.");
		}
		protected boolean isDocumentEditable(StringTupel docData) {
			String checkoutUser = docData.getValue(CHECKOUT_USER_ATTRIBUTE);
			if ((checkoutUser != null) && (checkoutUser.trim().length() == 0))
				checkoutUser = null;
			return ((checkoutUser == null) || checkoutUser.equals(this.authClient.getUserName()));
		}
		protected ImDocument loadDocument(String docId, ProgressMonitor pm) throws IOException {
			if (this.authClient.ensureLoggedIn())
				return this.imsClient.checkoutDocument(docId, this.fastFetchFilter, pm);
			else throw new IOException("The connected GoldenGATE Server cannot be reached at this time, please try again later.");
		}
		protected void saveDocument(String docId, ImDocument doc, ProgressMonitor pm) throws IOException {
			if (this.authClient.ensureLoggedIn())
				this.imsClient.updateDocument(doc, pm);
			else throw new IOException("The connected GoldenGATE Server cannot be reached at this time, please try again later.");
		}
		protected void closeDocument(String docId, ImDocument doc) throws IOException {
			if (!this.authClient.ensureLoggedIn())
				throw new IOException("The connected GoldenGATE Server cannot be reached at this time, please try again later.");
			
			//	check if any errors left
			ImDocumentErrorProtocol idep = ImDocumentErrorProtocol.loadErrorProtocol(doc);
			boolean docDone = ((idep == null) || (idep.getErrorCount() == 0));
			
			//	release document
			this.imsClient.releaseDocument(docId);
			
			//	clean up cache if document done
			if (docDone) {
				File docCacheFolder = this.getDocumentCacheFolder(docId, false);
				if ((docCacheFolder != null) && docCacheFolder.exists())
					cleanCacheFolder(docCacheFolder);
			}
		}
	}
}
