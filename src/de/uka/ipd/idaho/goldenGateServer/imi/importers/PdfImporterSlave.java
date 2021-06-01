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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Properties;

import javax.imageio.ImageIO;

import de.uka.ipd.idaho.gamta.util.AnalyzerDataProvider;
import de.uka.ipd.idaho.gamta.util.AnalyzerDataProviderFileBased;
import de.uka.ipd.idaho.gamta.util.DocumentStyle;
import de.uka.ipd.idaho.gamta.util.DocumentStyle.Data;
import de.uka.ipd.idaho.gamta.util.DocumentStyleProvider;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.gamta.util.imaging.PageImage;
import de.uka.ipd.idaho.gamta.util.imaging.PageImageInputStream;
import de.uka.ipd.idaho.gamta.util.imaging.PageImageStore;
import de.uka.ipd.idaho.gamta.util.imaging.PageImageStore.AbstractPageImageStore;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.MasterProcessInterface;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveConstants;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveRuntimeUtils;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.ImSupplement;
import de.uka.ipd.idaho.im.pdf.PdfExtractor;
import de.uka.ipd.idaho.im.pdf.PdfFontDecoder;
import de.uka.ipd.idaho.im.pdf.PdfFontDecoder.CustomFontDecoderCharset;
import de.uka.ipd.idaho.im.pdf.PdfFontDecoder.FontDecoderCharset;
import de.uka.ipd.idaho.im.util.ImDocumentIO;
import de.uka.ipd.idaho.im.util.ImDocumentStyle;
import de.uka.ipd.idaho.im.util.ImSupplementCache;

/**
 * Stripped-down copy of Image Markup's generic PDF importer command line tool,
 * with options restricted to specific application needs, and amended with URL
 * based document style provider for increased accuracy.
 * 
 * @author sautter
 */
public class PdfImporterSlave implements SlaveConstants {
	private static final int maxInMemorySupplementBytes = (50 * 1024 * 1024); // 50 MB
	
	public static void main(String[] args) throws Exception {
//		
//		//	read parameters
//		String cacheBasePath = ".";
//		String logPath = "./Logs";
//		String cpuMode = "M";
//		String sourcePath = null;
//		String sourceType = "G";
//		String fontMode = "V";
//		String fontCharSet = "U";
//		String fontCharSetPath = null;
//		String scanDecodeFlagsHex = null;
//		int scanDecodeFlags = -1;
//		String docStylePath = null;
//		String docStyleListUrl = null;
//		String outPath = null;
//		for (int a = 0; a < args.length;) {
//			/* source parameter -s
//			 * - missing: System.in
//			 * - set: file path */
//			if ("-s".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
//				sourcePath = args[a+1];
//				a += 2;
//			}
//			/* cache base path parameter -c
//			 * - missing or set to .: execution folder
//			 * - set to folder path: cache folder */
//			else if ("-c".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
//				cacheBasePath = args[a+1];
//				a += 2;
//			}
//			/* log path parameter -l
//			 * - missing or set to .: execution folder
//			 * - set to folder path: log folder */
//			else if ("-l".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
//				logPath = args[a+1];
//				a += 2;
//			}
//			/* style path parameter -y
//			 * - missing: document style templates unavailable
//			 * - set to folder path: document style templates loaded from there */
//			else if ("-y".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
//				docStylePath = args[a+1];
//				a += 2;
//			}
//			/* style path parameter -u
//			 * - missing: document style templates unavailable
//			 * - set to folder path: document style templates loaded from there */
//			else if ("-u".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
//				docStyleListUrl = args[a+1];
//				a += 2;
//			}
//			/* number of processors parameter -p
//			 * - missing or set to M: multiple processors
//			 * - set to positive integer: number of cores
//			 * - set to S: single processor */
//			else if ("-p".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
//				cpuMode = args[a+1];
//				a += 2;
//			}
//			/* font mode parameter -f
//			 * - set to D: decode all
//			 * - missing or set to V: verify mapped
//			 * - set to U: decode unmapped
//			 * - set to R: render glyphs only
//			 * - set to Q: no decoding (quick mode) */
//			else if ("-f".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
//				fontMode = args[a+1];
//				a += 2;
//			}
//			/* font charset parameter -cs
//			 * - missing or set to U: Unicode
//			 * - set to F: full Latin
//			 * - set to E: extended Latin
//			 * - set to B: basic Latin
//			 * - set to C: custom (path to come in -cp) */
//			else if ("-cs".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
//				fontCharSet = args[a+1];
//				a += 2;
//			}
//			/* font charset path parameter -cp
//			 * - path to load custom charset from */
//			else if ("-cp".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
//				fontCharSetPath = args[a+1];
//				a += 2;
//			}
//			/* source type parameter -t
//			 * - missing or set to G: generic PDF
//			 * - set to D: born-digital
//			 * - set to S: scanned
//			 * - set to M: scanned with meta pages */
//			else if ("-t".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
//				sourceType = args[a+1];
//				a += 2;
//			}
//			/* scan decoding flags parameter -sf
//			 * - missing or S: write IMF to file (named after source file, or doc ID on System.in source)
//			 * - set to O: write IMF to System.out
//			 * - set to file path: write to that file
//			 * - set to folder path: write data to that folder */
//			else if ("-sf".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
//				scanDecodeFlagsHex = args[a+1];
//				a += 2;
//			}
//			/* output parameter -o
//			 * - missing or S: write IMF to file (named after source file, or doc ID on System.in source)
//			 * - set to O: write IMF to System.out
//			 * - set to file path: write to that file
//			 * - set to folder path: write data to that folder */
//			else if ("-o".equalsIgnoreCase(args[a]) && ((a+1) < args.length)) {
//				outPath = args[a+1];
//				a += 2;
//			}
//		}
		
		//	read parameters
		Properties argsMap = SlaveRuntimeUtils.parseArguments(args);
		
		//	set up communication with master (before logging tampers with output streams)
		MasterProcessInterface mpi = new MasterProcessInterface();
		mpi.start();
		
		//	check parameters before investing effort in loading data
		String sourceType = argsMap.getProperty("DATATYPE", "G");
		if (("GDSM".indexOf(sourceType) == -1) || (sourceType.length() != 1)) {
			mpi.sendError("Invalid source type '" + sourceType + "'");
			return;
		}
		String fontMode = argsMap.getProperty("FONTMODE", "V");
		if (("DVURQ".indexOf(fontMode) == -1) || (fontMode.length() != 1)) {
			mpi.sendError("Invalid font decoding mode '" + fontMode + "'");
			return;
		}
		String fontCharSet = argsMap.getProperty("FONTCS", "U");
		if (("UFEBC".indexOf(fontCharSet) == -1) || (fontCharSet.length() != 1)) {
			mpi.sendError("Invalid font decoding charset '" + fontCharSet + "'");
			return;
		}
		String fontCharSetPath = argsMap.getProperty("FONTCSPATH");
		if ("C".equals(fontCharSet) && (fontCharSetPath == null)) {
			mpi.sendError("Missing font decoding charset path for charset '" + fontCharSet + "'");
			return;
		}
		String scanDecodeFlagsHex = argsMap.getProperty("SCANFLAGS");
		int scanDecodeFlags = -1;
		if ((scanDecodeFlagsHex != null) && ("GSM".indexOf(sourceType) != -1)) try {
			scanDecodeFlags = Integer.parseInt(scanDecodeFlagsHex, 16);
			if ("G".equals(sourceType))
				sourceType = (((scanDecodeFlags & PdfExtractor.META_PAGES) == 0) ? "S" : "M");
		}
		catch (NumberFormatException nfe) {
			mpi.sendError("Invalid scan decoding flags '" + scanDecodeFlagsHex + "'");
			return;
		}
		
		//	create input source
		BufferedInputStream pdfIn;
		String sourcePath = argsMap.getProperty(DATA_PATH_PARAMETER);
		if (sourcePath == null)
			pdfIn = new BufferedInputStream(System.in);
		else {
			File sourceFile = new File(sourcePath);
			if (!sourceFile.exists()) {
				mpi.sendError("Invalid input file '" + sourcePath + "'");
				return;
			}
			pdfIn = new BufferedInputStream(new FileInputStream(sourceFile));
		}
		
		//	create output destination
		String outPath = argsMap.getProperty(RESULT_PATH_PARAMETER);
		File outFile = new File(outPath);
		if (!outFile.exists() || !outFile.isDirectory()) {
			mpi.sendError("Invalid output destination '" + outPath + "'");
			return;
		}
		
		//	create document style provider
		String docStylePath = argsMap.getProperty("DSPATH");
		String docStyleListUrl = argsMap.getProperty("DSURL");
		if (docStylePath != null) try {
			DocumentStyleProvider dsp = new DocumentStyleProvider(docStyleListUrl, new File(docStylePath), ".docStyle") {
				protected DocumentStyle wrapDocumentStyleData(Data data) {
					return new ImDocumentStyle(data);
				}
			};
			dsp.init();
		}
		catch (IOException ioe) {
			mpi.sendError("Could not load document style templates: " + ioe.getMessage());
		}
		
		//	read input PDF
		byte[] pdfByteBuffer = new byte[1024];
		ByteArrayOutputStream pdfByteCollector = new ByteArrayOutputStream();
		for (int r; (r = pdfIn.read(pdfByteBuffer, 0, pdfByteBuffer.length)) != -1;)
			pdfByteCollector.write(pdfByteBuffer, 0, r);
		pdfIn.close();
		byte[] pdfBytes = pdfByteCollector.toByteArray();
		
		//	set up logging (if we have a folder)
		SlaveRuntimeUtils.setUpLogFiles(argsMap, "ImiPdfDecoder");
		
		//	get charset
		FontDecoderCharset useFontCharSet;
		if ("Q".equals(fontMode))
			useFontCharSet = PdfFontDecoder.NO_DECODING;
		else if ("R".equals(fontMode))
			useFontCharSet = PdfFontDecoder.RENDER_ONLY;
		else {
			if ("U".equals(fontCharSet))
				useFontCharSet = PdfFontDecoder.UNICODE;
			else if ("F".equals(fontCharSet))
				useFontCharSet = PdfFontDecoder.LATIN_FULL;
			else if ("E".equals(fontCharSet))
				useFontCharSet = PdfFontDecoder.LATIN;
			else if ("B".equals(fontCharSet))
				useFontCharSet = PdfFontDecoder.LATIN_BASIC;
			else if ("C".equals(fontCharSet))
				useFontCharSet = readCustomCharSet(fontCharSetPath);
			else return;
			
			//	add font mode if required
			if ("U".equals(fontMode))
				useFontCharSet = FontDecoderCharset.union(useFontCharSet, PdfFontDecoder.DECODE_UNMAPPED);
			else if ("V".equals(fontMode))
				useFontCharSet = FontDecoderCharset.union(useFontCharSet, PdfFontDecoder.VERIFY_MAPPED);
		}
		
		//	create page image store
		String cacheBasePath = argsMap.getProperty(CACHE_PATH_PARAMETER, ".");
		ImageIO.setUseCache(false);
		PageImageStore pis = new PisPageImageStore(new File(cacheBasePath + "/PageImages/"));
		PageImage.addPageImageSource(pis);
		
		//	switch parallel jobs to linear or limited parallel execution if requested to
//		if ("M".equals(cpuMode)) {}
//		else if ("S".equals(cpuMode))
//			ParallelJobRunner.setLinear(true);
//		else ParallelJobRunner.setMaxCores(Integer.parseInt(cpuMode));
		SlaveRuntimeUtils.setUpMaxCores(argsMap);
		
		//	create PDF extractor
		final File supplementFolder = new File(cacheBasePath + "/Supplements/");
		if (!supplementFolder.exists())
			supplementFolder.mkdirs();
		PdfExtractor pdfExtractor = new PisPdfExtractor(new File("."), new File(cacheBasePath), pis, true, supplementFolder);
		
		//	decode input PDF
		ProgressMonitor pm = mpi.createProgressMonitor();
		ImDocument imDoc;
		if ("G".equals(sourceType))
			imDoc = pdfExtractor.loadGenericPdf(pdfBytes, pm);
		else if ("D".equals(sourceType))
			imDoc = pdfExtractor.loadTextPdf(pdfBytes, useFontCharSet, pm);
		else if (scanDecodeFlags != -1)
			imDoc = pdfExtractor.loadImagePdf(pdfBytes, scanDecodeFlags, pm);
		else if ("S".equals(sourceType))
			imDoc = pdfExtractor.loadImagePdf(pdfBytes, false, pm);
		else if ("M".equals(sourceType))
			imDoc = pdfExtractor.loadImagePdf(pdfBytes, true, pm);
		else imDoc = null; // never gonna happen, as we exit further upstream if none of the types match ...
		
		//	shut down PDF extractor
		pdfExtractor.shutdown();
		
		//	write output to folder
		ImDocumentIO.storeDocument(imDoc, outFile, pm);
//		
//		//	shut down whatever threads are left
//		System.exit(0);
	}
	
	private static FontDecoderCharset readCustomCharSet(String path) throws IOException {
		BufferedReader fdcIn;
		if (path.startsWith("http://") || path.startsWith("https://"))
			fdcIn = new BufferedReader(new InputStreamReader((new URL(path)).openStream(), "UTF-8"));
		else if (path.startsWith("/") || (path.indexOf(":\\") != -1))
			fdcIn = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path)), "UTF-8"));
		else throw new IllegalArgumentException("Inavlid charset path " + path);
		return CustomFontDecoderCharset.readCharSet("Custom", fdcIn);
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
		private ImSupplementCache supplementCache;
		PisImDocument(String docId, File supplementFolder) {
			super(docId);
			this.supplementCache = new ImSupplementCache(this, supplementFolder, maxInMemorySupplementBytes);
		}
		public ImSupplement addSupplement(ImSupplement ims) {
			ims = this.supplementCache.cacheSupplement(ims);
			return super.addSupplement(ims);
		}
		public void removeSupplement(ImSupplement ims) {
			this.supplementCache.deleteSupplement(ims);
			super.removeSupplement(ims);
		}
	}
}