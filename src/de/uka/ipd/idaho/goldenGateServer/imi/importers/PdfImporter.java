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
package de.uka.ipd.idaho.goldenGateServer.imi.importers;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.easyIO.util.RandomByteSource;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.gamta.util.imaging.DocumentStyle;
import de.uka.ipd.idaho.goldenGateServer.imi.GoldenGateIMI.ImiDocumentImport;
import de.uka.ipd.idaho.goldenGateServer.imi.ImiDocumentImporter;
import de.uka.ipd.idaho.goldenGateServer.ims.util.StandaloneDocumentStyleProvider;
import de.uka.ipd.idaho.goldenGateServer.util.SlaveInstallerUtils;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.util.ImDocumentIO;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * Importer for PDF documents, decoding them synchronously, but in a slave JVM.
 * 
 * @author sautter
 */
public class PdfImporter extends ImiDocumentImporter {
	private int maxSlaveMemory = 4096;
	private int maxSlaveCores = 1;
	private String docStyleListUrl;
	private String docStyleNamePattern;
	private File docStyleFolder;
	private DocumentStyle.Provider docStyleProvider;
	private String fontCharsetPath;
	
	/** the usual zero-argument constructor for class loading */
	public PdfImporter() {}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.imi.ImiDocumentImporter#getName()
	 */
	public String getName() {
		return "SyncPDF";
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.imi.ImiDocumentImporter#init()
	 */
	public void init() {
		
		//	load settings from data path
		Settings config = Settings.loadSettings(new File(this.dataPath, "config.cnfg"));
		
		//	get maximum memory and CPU core limit for slave process
		try {
			this.maxSlaveMemory = Integer.parseInt(config.getSetting("maxSlaveMemory", ("" + this.maxSlaveMemory)));
		} catch (RuntimeException re) {}
		try {
			this.maxSlaveCores = Integer.parseInt(config.getSetting("maxSlaveCores", ("" + this.maxSlaveCores)));
		} catch (RuntimeException re) {}
		
		//	make document style templates available (we can do without, however)
		this.docStyleListUrl = config.getSetting("docStyleListUrl");
		this.docStyleNamePattern = config.getSetting("docStyleNamePattern");
		this.docStyleFolder = new File(this.workingFolder, "DocStyles");
		this.docStyleFolder.mkdirs();
		
		//	get font decoding charset path (file or URL)
		this.fontCharsetPath = config.getSetting("fontCharsetPath", "fontDecoderCharset.cnfg");
		if (this.fontCharsetPath.length() > 1) {
			if (this.fontCharsetPath.startsWith("http://") || this.fontCharsetPath.startsWith("https://")) {}
			else if (this.fontCharsetPath.startsWith("/")) {
				File fontCharsetFile = new File(this.fontCharsetPath);
				if (fontCharsetFile.exists())
					this.fontCharsetPath = fontCharsetFile.getAbsolutePath();
				else this.fontCharsetPath = null;
			}
			else {
				while (this.fontCharsetPath.startsWith("./"))
					this.fontCharsetPath = this.fontCharsetPath.substring("./".length());
				File fontCharsetFile = new File(this.dataPath, this.fontCharsetPath.substring("./".length()));
				if (fontCharsetFile.exists())
					this.fontCharsetPath = fontCharsetFile.getAbsolutePath();
				else this.fontCharsetPath = null;
			}
		}
		
		//	install slave JAR to run it all from
		SlaveInstallerUtils.installSlaveJar("PdfImporterSlave.jar", this.dataPath, this.workingFolder, true);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.imi.ImiDocumentImporter#canHandleImport(de.uka.ipd.idaho.goldenGateServer.imi.GoldenGateIMI.ImiDocumentImport)
	 */
	public boolean canHandleImport(ImiDocumentImport idi) {
		return idi.dataMimeType.toLowerCase().endsWith("/pdf");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.imi.ImiDocumentImporter#handleImport(de.uka.ipd.idaho.goldenGateServer.imi.GoldenGateIMI.ImiDocumentImport)
	 */
	public void handleImport(ImiDocumentImport idi) {
		try {
			this.doHandleImport(idi);
		}
		catch (Exception e) {
			idi.setError(e);
		}
		catch (Error e) {
			idi.setError(e);
		}
	}
	
	private void doHandleImport(ImiDocumentImport idi) throws IOException {
		
		//	create document style provider on demand (now that we're sure ECS is up and available)
		if (this.docStyleProvider == null) try {
			this.docStyleProvider = new StandaloneDocumentStyleProvider(this.docStyleListUrl, this.docStyleNamePattern, this.docStyleFolder);
		}
		catch (IOException ioe) {
			this.parent.logError("Could not load document style templates, URL invalid or broken");
			this.parent.logError(ioe);
		}
		
		//	create document cache folder
		File docCacheFolder = new File(this.cacheFolder, ("cache-" + idi.hashCode()));
		docCacheFolder.mkdirs();
		
		//	create document output folder
		File docOutFolder = new File(this.cacheFolder, ("doc-" + idi.hashCode()));
		docOutFolder.mkdirs();
		
		//	get document source file
		File docInFile = idi.getDataFile();
		
		//	hash input file first to check whether or not all the decoding hassle makes sense
		String docInFileHash = null;
		boolean docExists = false;
		try {
			docInFileHash = this.getDocInFileChecksum(docInFile);
			if (this.parent.checkDocumentExists(docInFileHash))
				docExists = true;
		}
		catch (IOException ioe) {
			System.out.println("Error computing hash of import file '" + docInFile.getAbsolutePath() + "': " + ioe.getMessage());
			ioe.printStackTrace(System.out);
		}
		if (docExists)
			throw new IOException("Document '" + docInFileHash + "' imported before.");
		
		//	check if we know born-digital or scanned (assume meta pages in the latter case for good measures)
		String bornDigital = ((String) idi.removeAttribute("isBornDigital"));
		String pdfType = "G";
		if ("true".equalsIgnoreCase(bornDigital) || "yes".equalsIgnoreCase(bornDigital))
			pdfType = "D";
		else if ("false".equalsIgnoreCase(bornDigital) || "no".equalsIgnoreCase(bornDigital))
			pdfType = "M";
		
		//	assemble command
		StringVector command = new StringVector();
		command.addElement("java");
		command.addElement("-jar");
		if (this.maxSlaveMemory > 512)
			command.addElement("-Xmx" + this.maxSlaveMemory + "m");
		command.addElement("PdfImporterSlave.jar");
		
		//	add parameters
		command.addElement("-s"); // source: file from importer job
		command.addElement(docInFile.getAbsolutePath());
		command.addElement("-c"); // cache: cache folder
		command.addElement(docCacheFolder.getAbsolutePath());
		command.addElement("-p"); // CPU usage: single (slower, but we don't want to knock out the whole server)
//		command.addElement("S");
		int maxSlaveCores = this.maxSlaveCores;
		if (maxSlaveCores < 1)
			maxSlaveCores = 65536;
		if ((maxSlaveCores * 4) > Runtime.getRuntime().availableProcessors())
			maxSlaveCores = (Runtime.getRuntime().availableProcessors() / 4);
		if (maxSlaveCores == 1)
			command.addElement("S");
		else command.addElement("" + maxSlaveCores);
		if (this.docStyleFolder != null) {
			command.addElement("-y"); // style path: the folder holding document styles
			command.addElement(this.docStyleFolder.getAbsolutePath());
		}
		command.addElement("-t"); // PDF type: generic
		command.addElement(pdfType);
		if ("D".equals(pdfType)) {
			command.addElement("-f"); // font decoding mode: fully decode
			command.addElement("D");
			command.addElement("-cs"); // font decoding charset
			if (this.fontCharsetPath == null)
				command.addElement("U"); // no font decoding charset specified, fall back to Unicode
			else {
				command.addElement("C"); // custom font decoding charset: load from file or URL
				command.addElement("-cp"); // path (file or URL) to load font decoding charset from
				command.addElement(this.fontCharsetPath);
			}
		}
		command.addElement("-o"); // output destination: document output folder
		command.addElement(docOutFolder.getAbsolutePath());
		
		//	start document decoder slave process
		Process importer = Runtime.getRuntime().exec(command.toStringArray(), new String[0], this.workingFolder);
		
		//	loop through error messages
		final BufferedReader importerError = new BufferedReader(new InputStreamReader(importer.getErrorStream()));
		new Thread() {
			public void run() {
				try {
					for (String errorLine; (errorLine = importerError.readLine()) != null;)
						System.out.println(errorLine);
				}
				catch (Exception e) {
					e.printStackTrace(System.out);
				}
			}
		}.start();
		
		//	loop through step information only
		BufferedReader importerIn = new BufferedReader(new InputStreamReader(importer.getInputStream()));
		for (String inLine; (inLine = importerIn.readLine()) != null;) {
			if (inLine.startsWith("S:"))
				host.logInfo(inLine.substring("S:".length()));
			else if (inLine.startsWith("I:"))
				host.logDebug(inLine.substring("I:".length()));
			else if (inLine.startsWith("P:")) {}
			else if (inLine.startsWith("BP:")) {}
			else if (inLine.startsWith("MP:")) {}
			else host.logInfo(inLine);
		}
		
		//	wait for decoder to finish
		while (true) try {
			importer.waitFor();
			break;
		} catch (InterruptedException ie) {}
		
		//	create document on top of import result
		ImDocument doc = ImDocumentIO.loadDocument(docOutFolder, new ProgressMonitor() {
			public void setStep(String step) {
				host.logInfo(step);
			}
			public void setInfo(String info) {
				host.logDebug(info);
			}
			public void setBaseProgress(int baseProgress) {}
			public void setMaxProgress(int maxProgress) {}
			public void setProgress(int progress) {}
		});
		
		//	hand document back to caller via idi.setDocument()
		idi.setDocument(doc);
		
		//	clean up cache and document data
		this.cleanupFile(docCacheFolder);
		this.cleanupFile(docOutFolder);
	}
	
	private MessageDigest checksumDigester = null;
	private synchronized String getDocInFileChecksum(File docInFile) throws IOException {
		if (this.checksumDigester == null) {
			try {
				this.checksumDigester = MessageDigest.getInstance("MD5");
			}
			catch (NoSuchAlgorithmException nsae) {
				System.out.println(nsae.getClass().getName() + " (" + nsae.getMessage() + ") while creating checksum digester.");
				nsae.printStackTrace(System.out); // should not happen, but Java don't know ...
				return Gamta.getAnnotationID(); // use random value to avoid collisions
			}
		}
		this.checksumDigester.reset();
		InputStream docInFileIn = new BufferedInputStream(new FileInputStream(docInFile));
		byte[] buffer = new byte[1024];
		for (int r; (r = docInFileIn.read(buffer, 0, buffer.length)) != -1;)
			this.checksumDigester.update(buffer, 0, r);
		docInFileIn.close();
		byte[] checksumBytes = checksumDigester.digest();
		String checksum = new String(RandomByteSource.getHexCode(checksumBytes));
		return checksum;
	}
	
	private void cleanupFile(File file) {
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (int f = 0; f < files.length; f++)
				this.cleanupFile(files[f]);
		}
		file.delete();
	}
}