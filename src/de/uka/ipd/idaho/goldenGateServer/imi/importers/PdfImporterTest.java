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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

/**
 * @author sautter
 *
 */
public class PdfImporterTest {
	private static File workingFolder = new File("./PdfImportWorking");
	private static File cacheFolder = new File("./PdfImportCache");
	
	private static void cleanupFile(File file) {
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (int f = 0; f < files.length; f++)
				cleanupFile(files[f]);
		}
		file.delete();
	}
	
	//	having this extra method is basically to enable testing the import independent of having a host IMI around
	private static void doHandleImport(File docInFile, File docCacheFolder, File docOutFolder) throws IOException {
		
		//	assemble command
		Vector command = new Vector();
		command.addElement("java");
		command.addElement("-jar");
		command.addElement("-Xmx1024m");
		command.addElement("ImageMarkupPDF.jar");
		
		//	add parameters
		command.addElement("-s"); // source: file from importer job
		command.addElement(docInFile.getAbsolutePath());
		command.addElement("-c"); // cache: cache folder
		command.addElement(docCacheFolder.getAbsolutePath());
		command.addElement("-p"); // CPU usage: single (slower, but we don't want to knock out the whole server)
		command.addElement("S");
		command.addElement("-t"); // PDF type: generic
		command.addElement("G");
		command.addElement("-l"); // log mode: remote progress monitor over streams
		command.addElement("M");
		command.addElement("-m"); // output mode: Image Markup directory (saves all the zipping and un-zipping effort)
		command.addElement("D");
		command.addElement("-o"); // output destination: document output folder
		command.addElement(docOutFolder.getAbsolutePath());
		
		//	start document decoder slave process
		Process importer = Runtime.getRuntime().exec(((String[]) command.toArray(new String[command.size()])), new String[0], workingFolder);
		
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
			else break;
		}
		
		//	wait for decoder to finish
		while (true) try {
			importer.waitFor();
			break;
		} catch (InterruptedException ie) {}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String pdfPath = args[0];
		String pdfName = pdfPath.substring(pdfPath.lastIndexOf('/') + 1);
		
		//	create document cache folder
		File docCacheFolder = new File(cacheFolder, ("cache-" + pdfName));
		docCacheFolder.mkdirs();
		System.out.println("Cache folder created");
		
		//	create document output folder
		File docOutFolder = new File(cacheFolder, ("doc-" + pdfName));
		docOutFolder.mkdirs();
		System.out.println("Output folder created");
		
		//	get document source file
		while (pdfPath.startsWith("./"))
			pdfPath = pdfPath.substring("./".length());
		File docInFile;
		if (pdfPath.startsWith("/") || (pdfPath.indexOf(":/") != -1) || (pdfPath.indexOf(":\\") != -1))
			docInFile = new File(pdfPath);
		else docInFile = new File("./" + pdfPath);
		
		//	import document (it's enough to see it's there, no need for loading it)
		doHandleImport(docInFile, docCacheFolder, docOutFolder);
		System.out.println("Import done");
//		
//		//	hand document back to caller via idi.setDocument()
//		idi.setDocument(doc);
		
		//	clean up cache and document data
		cleanupFile(docCacheFolder);
		System.out.println("Cache folder cleaned up");
//		pdfi.cleanupFile(docOutFolder);
	}
}