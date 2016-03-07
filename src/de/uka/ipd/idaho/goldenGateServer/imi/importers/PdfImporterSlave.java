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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;

import javax.imageio.ImageIO;

import de.uka.ipd.idaho.gamta.util.AnalyzerDataProvider;
import de.uka.ipd.idaho.gamta.util.AnalyzerDataProviderFileBased;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.gamta.util.imaging.PageImage;
import de.uka.ipd.idaho.gamta.util.imaging.PageImageInputStream;
import de.uka.ipd.idaho.gamta.util.imaging.PageImageStore;
import de.uka.ipd.idaho.gamta.util.imaging.PageImageStore.AbstractPageImageStore;
import de.uka.ipd.idaho.goldenGateServer.ims.util.StandaloneDocumentStyleProvider;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.ImSupplement;
import de.uka.ipd.idaho.im.pdf.PdfExtractor;
import de.uka.ipd.idaho.im.util.ImDocumentIO;

/**
 * Stripped-down copy of Image Markup's generic PDF importer command line tool,
 * with options restricted to specific application needs, and amended with URL
 * based document style provider for increased accuracy.
 * 
 * @author sautter
 */
public class PdfImporterSlave {
	private static final int maxInMemoryImageSupplementBytes = (50 * 1024 * 1024); // 50 MB
	
	public static void main(String[] args) throws Exception {
		
		//	read parameters
		String sourcePath = null;
		String sourceType = "G";
		String outPath = null;
		String cacheBasePath = ".";
		String cpuMode = "M";
		String docStylePath = null;
		for (int a = 0; a < args.length;) {
			/* source parameter -s
			 * - missing: System.in
			 * - set: file path */
			if ("-s".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
				sourcePath = args[a+1];
				a += 2;
			}
			/* path parameter -c
			 * - missing or set to .: execution folder
			 * - set to folder path: cache folder */
			else if ("-c".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
				cacheBasePath = args[a+1];
				a += 2;
			}
			/* style path parameter -y
			 * - missing: document style templates unavailable
			 * - set to folder path: document style templates loaded from there */
			else if ("-y".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
				docStylePath = args[a+1];
				a += 2;
			}
			/* path parameter -p
			 * - missing or set to M: multiple processors
			 * - set to S: single processor */
			else if ("-p".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
				cpuMode = args[a+1];
				a += 2;
			}
			/* source type parameter -t
			 * - missing or set to G: generic PDF
			 * - set to D: born-digital
			 * - set to S: scanned
			 * - set to M: scanned with meta pages */
			else if ("-t".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
				sourceType = args[a+1];
				a += 2;
			}
			/* output parameter -o
			 * - missing or S: write IMF to file (named after source file, or doc ID on System.in source)
			 * - set to O: write IMF to System.out
			 * - set to file path: write to that file
			 * - set to folder path: write data to that folder */
			else if ("-o".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
				outPath = args[a+1];
				a += 2;
			}
		}
		
		//	check parameters before investing effort in loading data
		if (("GDSM".indexOf(sourceType) == -1) || (sourceType.length() != 1)) {
			printError("Invalid source type '" + sourceType + "'");
			return;
		}
		if (("MS".indexOf(cpuMode) == -1) || (cpuMode.length() != 1)) {
			printError("Invalid CPU usage mode '" + cpuMode + "'");
			return;
		}
		
		//	create input source
		BufferedInputStream pdfIn;
		if (sourcePath == null)
			pdfIn = new BufferedInputStream(System.in);
		else {
			File sourceFile = new File(sourcePath);
			if (!sourceFile.exists()) {
				printError("Invalid input file '" + sourcePath + "'");
				return;
			}
			pdfIn = new BufferedInputStream(new FileInputStream(sourceFile));
		}
		
		//	create output destination
		File outFile = new File(outPath);
		if (!outFile.exists() || !outFile.isDirectory()) {
			printError("Invalid output destination '" + outPath + "'");
			return;
		}
		
		//	read input PDF
		byte[] pdfByteBuffer = new byte[1024];
		ByteArrayOutputStream pdfByteCollector = new ByteArrayOutputStream();
		for (int r; (r = pdfIn.read(pdfByteBuffer, 0, pdfByteBuffer.length)) != -1;)
			pdfByteCollector.write(pdfByteBuffer, 0, r);
		pdfIn.close();
		byte[] pdfBytes = pdfByteCollector.toByteArray();
		
		//	create document style provider
		if (docStylePath != null)
			new StandaloneDocumentStyleProvider(new File(docStylePath));
		
		//	preserve System.out and System.err
		final PrintStream sysOut = System.out;
		final PrintStream sysErr = System.err;
		
		//	set up logging
		ProgressMonitor pm = new ProgressMonitor() {
			public void setStep(String step) {
				sysOut.println("S:" + step);
			}
			public void setInfo(String info) {
				sysOut.println("I:" + info);
			}
			public void setBaseProgress(int baseProgress) {
				sysOut.println("BP:" + baseProgress);
			}
			public void setMaxProgress(int maxProgress) {
				sysOut.println("MP:" + maxProgress);
			}
			public void setProgress(int progress) {
				sysOut.println("P:" + progress);
			}
		};
		
		//	silence System.out
		System.setOut(new PrintStream(new OutputStream() {
			public void write(int b) throws IOException {}
		}));
		
		//	create page image store
		ImageIO.setUseCache(false);
		PageImageStore pis = new PisPageImageStore(new File(cacheBasePath + "/PageImages/"));
		PageImage.addPageImageSource(pis);
		
		//	create PDF extractor
		final File supplementFolder = new File(cacheBasePath + "/Supplements/");
		if (!supplementFolder.exists())
			supplementFolder.mkdirs();
		PdfExtractor pdfExtractor = new PisPdfExtractor(new File("."), new File(cacheBasePath), pis, "M".equals(cpuMode), supplementFolder);
		
		//	decode input PDF
		ImDocument imDoc;
		if ("G".equals(sourceType))
			imDoc = pdfExtractor.loadGenericPdf(pdfBytes, pm);
		else if ("D".equals(sourceType))
			imDoc = pdfExtractor.loadTextPdf(pdfBytes, pm);
		else if ("S".equals(sourceType))
			imDoc = pdfExtractor.loadImagePdf(pdfBytes, false, pm);
		else if ("M".equals(sourceType))
			imDoc = pdfExtractor.loadImagePdf(pdfBytes, true, pm);
		else imDoc = null;
		
		//	shut down PDF extractor
		pdfExtractor.shutdown();
		
		//	write output to folder
		ImDocumentIO.storeDocument(imDoc, outFile, pm);
	}
	
	private static class PisPageImageStore extends AbstractPageImageStore {
		private AnalyzerDataProvider pisDataProvider;
		private HashMap byteCache = new HashMap();
		PisPageImageStore(File pisDataPath) {
			this.pisDataProvider = new AnalyzerDataProviderFileBased(pisDataPath);
		}
		public boolean isPageImageAvailable(String name) {
			if (!name.endsWith(IMAGE_FORMAT))
				name += ("." + IMAGE_FORMAT);
			if (pisDataProvider.isDataAvailable(name))
				return true;
			else return this.byteCache.containsKey(name);
		}
		public PageImageInputStream getPageImageAsStream(String name) throws IOException {
			if (!name.endsWith(IMAGE_FORMAT))
				name += ("." + IMAGE_FORMAT);
			if (pisDataProvider.isDataAvailable(name))
				return new PageImageInputStream(pisDataProvider.getInputStream(name), this);
			else if (this.byteCache.containsKey(name))
				return new PageImageInputStream(new ByteArrayInputStream((byte[]) this.byteCache.get(name)), this);
			else return null;
		}
		public boolean storePageImage(String name, PageImage pageImage) throws IOException {
			if (!name.endsWith(IMAGE_FORMAT))
				name += ("." + IMAGE_FORMAT);
			try {
				OutputStream imageOut = pisDataProvider.getOutputStream(name);
				pageImage.write(imageOut);
				imageOut.close();
				return true;
			}
			catch (IOException ioe) {
				ioe.printStackTrace(System.out);
				return false;
			}
		}
		public int getPriority() {
			return 0; // we're a general page image store, yield to specific ones
		}
	};
	
	private static class PisPdfExtractor extends PdfExtractor {
		private File supplementFolder;
		PisPdfExtractor(File basePath, File cachePath, PageImageStore imageStore, boolean useMultipleCores, File supplementFolder) {
			super(basePath, cachePath, imageStore, useMultipleCores);
			this.supplementFolder = supplementFolder;
		}
		protected ImDocument createDocument(String docId) {
			return new PisImDocument(docId, this.supplementFolder);
		}
	}
	
	private static class PisImDocument extends ImDocument {
		private File supplementFolder;
		PisImDocument(String docId, File supplementFolder) {
			super(docId);
			this.supplementFolder = supplementFolder;
		}
		
		private long inMemorySupplementBytes = 0;
		public ImSupplement addSupplement(ImSupplement ims) {
			
			//	store known type supplements on disc if there are too many or too large
			if ((ims instanceof ImSupplement.Figure) || (ims instanceof ImSupplement.Scan) || (ims instanceof ImSupplement.Source)) try {
				
				//	threshold already exceeded, disc cache right away
				if (this.inMemorySupplementBytes > maxInMemoryImageSupplementBytes)
					ims = this.createDiscSupplement(ims, null);
				
				//	still below threshold, check source
				else {
					InputStream sis = ims.getInputStream();
					
					//	this one resides in memory, count it
					if (sis instanceof ByteArrayInputStream)
						this.inMemorySupplementBytes += sis.available();
					
					//	threshold just exceeded
					if (this.inMemorySupplementBytes > maxInMemoryImageSupplementBytes) {
						
						//	disc cache all existing image supplements
						ImSupplement[] imss = this.getSupplements();
						for (int s = 0; s < imss.length; s++) {
							if ((imss[s] instanceof ImSupplement.Figure) || (imss[s] instanceof ImSupplement.Scan))
								super.addSupplement(this.createDiscSupplement(imss[s], null));
						}
						
						//	disc cache argument supplement
						ims = this.createDiscSupplement(ims, sis);
					}
				}
			}
			catch (IOException ioe) {
				System.out.println("Error caching supplement '" + ims.getId() + "': " + ioe.getMessage());
				ioe.printStackTrace(System.out);
			}
			
			//	store (possibly modified) supplement
			return super.addSupplement(ims);
		}
		
		private ImSupplement createDiscSupplement(ImSupplement ims, InputStream sis) throws IOException {
			
			//	get input stream if not already done
			if (sis == null)
				sis = ims.getInputStream();
			
			//	this one's not in memory, close input stream and we're done
			if (!(sis instanceof ByteArrayInputStream)) {
				sis.close();
				return ims;
			}
			
			//	get file name and extension
			String sDataName = ims.getId().replaceAll("[^a-zA-Z0-9]", "_");
			String sDataType = ims.getMimeType();
			if (sDataType.indexOf('/') != -1)
				sDataType = sDataType.substring(sDataType.indexOf('/') + "/".length());
			
			//	create file
			final File sFile = new File(supplementFolder, (this.docId + "." + sDataName + "." + sDataType));
			
			//	store supplement in file (if not done in previous run)
			if (!sFile.exists()) {
				sFile.createNewFile();
				OutputStream sos = new BufferedOutputStream(new FileOutputStream(sFile));
				byte[] sBuffer = new byte[1024];
				for (int r; (r = sis.read(sBuffer, 0, sBuffer.length)) != -1;)
					sos.write(sBuffer, 0, r);
				sos.flush();
				sos.close();
			}
			
			//	replace supplement with disc based one
			if (ims instanceof ImSupplement.Figure)
				return new ImSupplement.Figure(this, ims.getMimeType(), ((ImSupplement.Figure) ims).getPageId(), ((ImSupplement.Figure) ims).getDpi(), ((ImSupplement.Figure) ims).getBounds()) {
					public InputStream getInputStream() throws IOException {
						return new BufferedInputStream(new FileInputStream(sFile));
					}
				};
			else if (ims instanceof ImSupplement.Scan)
				return new ImSupplement.Scan(this, ims.getMimeType(), ((ImSupplement.Scan) ims).getPageId(), ((ImSupplement.Scan) ims).getDpi()) {
					public InputStream getInputStream() throws IOException {
						return new BufferedInputStream(new FileInputStream(sFile));
					}
				};
			else if (ims instanceof ImSupplement.Source)
				return new ImSupplement.Source(this, ims.getMimeType()) {
					public InputStream getInputStream() throws IOException {
						return new BufferedInputStream(new FileInputStream(sFile));
					}
				};
			else return ims; // never gonna happen, but Java don't know
		}
	}
	
	private static void printError(String error) {
		System.out.println("=== PDF to IMF Converter / Data Extractor ===");
		System.out.println(error);
	}
}