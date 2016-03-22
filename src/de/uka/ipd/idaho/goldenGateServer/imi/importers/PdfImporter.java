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
package de.uka.ipd.idaho.goldenGateServer.imi.importers;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.goldenGateServer.imi.GoldenGateIMI.ImiDocumentImport;
import de.uka.ipd.idaho.goldenGateServer.imi.ImiDocumentImporter;
import de.uka.ipd.idaho.goldenGateServer.ims.util.StandaloneDocumentStyleProvider;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.util.ImDocumentIO;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * Importer for PDF documents, decoding them synchronously, but in a slave JVM.
 * 
 * @author sautter
 */
public class PdfImporter extends ImiDocumentImporter {
	private String docStyleListUrl;
	private String docStyleNamePattern;
	private File docStyleFolder;
	
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
		
		//	make document style templates available (we can do without, however)
		this.docStyleListUrl = config.getSetting("docStyleListUrl");
		if (this.docStyleListUrl != null) try {
			this.docStyleNamePattern = config.getSetting("docStyleNamePattern");
			this.docStyleFolder = new File(this.workingFolder, "DocStyles");
			this.docStyleFolder.mkdirs();
			new StandaloneDocumentStyleProvider(this.docStyleListUrl, this.docStyleNamePattern, this.docStyleFolder);
		}
		catch (IOException ioe) {
			ioe.printStackTrace(System.out);
		}
		
		//	install base JARs
		this.installJar("StringUtils.jar");
		this.installJar("HtmlXmlUtil.jar");
		this.installJar("Gamta.jar");
		this.installJar("mail.jar");
		this.installJar("EasyIO.jar");
		this.installJar("GamtaImagingAPI.jar");
		this.installJar("BibRefUtils.jar");
		
		//	install image markup and OCR JARs
		this.installJar("ImageMarkup.jar");
		this.installJar("ImageMarkup.bin.jar");
		this.installJar("ImageMarkupOCR.jar");
		this.installJar("ImageMarkupOCR.bin.jar");
		
		//	install PDF decoder JARs
		this.installJar("icepdf-core.jar");
		this.installJar("ImageMarkupPDF.jar");
		this.installJar("ImageMarkupPDF.bin.jar");
		
		//	install own source JAR to run slave from
		this.installJar(null);
	}
	
	private void installJar(String name) {
		File source;
		if (name == null) {
			name = this.dataPath.getName();
			name = name.substring(0, (name.length() - "Data".length()));
			name = (name + ".jar");
			System.out.println("Installing JAR '" + name + "'");
			source = new File(this.dataPath.getAbsoluteFile().getParentFile(), name);
		}
		else {
			System.out.println("Installing JAR '" + name + "'");
			source = new File(this.dataPath, name);
		}
		if (!source.exists())
			throw new RuntimeException("Missing JAR: " + name);
		
		File target = new File(this.workingFolder, name);
		if ((target.lastModified() + 1000) > source.lastModified()) {
			System.out.println(" ==> up to date");
			return;
		}
		
		try {
			InputStream sourceIn = new BufferedInputStream(new FileInputStream(source));
			OutputStream targetOut = new BufferedOutputStream(new FileOutputStream(target));
			byte[] buffer = new byte[1024];
			for (int r; (r = sourceIn.read(buffer, 0, buffer.length)) != -1;)
				targetOut.write(buffer, 0, r);
			targetOut.flush();
			targetOut.close();
			sourceIn.close();
			System.out.println(" ==> installed");
		}
		catch (IOException ioe) {
			throw new RuntimeException("Could not install JAR '" + name + "': " + ioe.getMessage());
		}
		catch (Exception e) {
			e.printStackTrace(System.out);
			throw new RuntimeException("Could not install JAR '" + name + "': " + e.getMessage());
		}
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
		
		//	create document cache folder
		File docCacheFolder = new File(this.cacheFolder, ("cache-" + idi.hashCode()));
		docCacheFolder.mkdirs();
		
		//	create document output folder
		File docOutFolder = new File(this.cacheFolder, ("doc-" + idi.hashCode()));
		docOutFolder.mkdirs();
		
		//	get document source file
		File docInFile = idi.getDataFile();
		
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
		command.addElement("-Xmx1024m");
		command.addElement("PdfImporter.jar");
		
		//	add parameters
		command.addElement("-s"); // source: file from importer job
		command.addElement(docInFile.getAbsolutePath());
		command.addElement("-c"); // cache: cache folder
		command.addElement(docCacheFolder.getAbsolutePath());
		command.addElement("-p"); // CPU usage: single (slower, but we don't want to knock out the whole server)
		command.addElement("S");
		if (this.docStyleFolder != null) {
			command.addElement("-y"); // style path: the folder holding document styles
			command.addElement(this.docStyleFolder.getAbsolutePath());
		}
		command.addElement("-t"); // PDF type: generic
		command.addElement(pdfType);
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
				System.out.println(inLine.substring("S:".length()));
			else if (inLine.startsWith("I:")) {}
			else if (inLine.startsWith("P:")) {}
			else if (inLine.startsWith("BP:")) {}
			else if (inLine.startsWith("MP:")) {}
			else System.out.println(inLine);
		}
		
		//	wait for decoder to finish
		while (true) try {
			importer.waitFor();
			break;
		} catch (InterruptedException ie) {}
		
		//	create document on top of import result
		ImDocument doc = ImDocumentIO.loadDocument(docOutFolder, ProgressMonitor.dummy);
		
		//	hand document back to caller via idi.setDocument()
		idi.setDocument(doc);
		
		//	clean up cache and document data
		this.cleanupFile(docCacheFolder);
		this.cleanupFile(docOutFolder);
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