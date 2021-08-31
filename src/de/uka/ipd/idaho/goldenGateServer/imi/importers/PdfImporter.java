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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.TreeMap;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.easyIO.util.HashUtils.MD5;
import de.uka.ipd.idaho.easyIO.util.RandomByteSource;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent.ComponentAction;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent.ComponentActionConsole;
import de.uka.ipd.idaho.goldenGateServer.imi.GoldenGateIMI.ImiDocumentImport;
import de.uka.ipd.idaho.goldenGateServer.imi.ImiDocumentImporter;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveErrorRecorder;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveInstallerUtils;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveJob;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveProcessInterface;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.pdf.PdfExtractor;
import de.uka.ipd.idaho.im.util.ImDocumentIO;

/**
 * Importer for PDF documents, decoding them synchronously, but in a slave JVM.
 * 
 * @author sautter
 */
public class PdfImporter extends ImiDocumentImporter {
	private String name = "SyncPDF";
	private boolean isMainDecoder = true;
	
	private int maxSlaveMemory = 4096;
	private int maxSlaveCores = 1;
	private File logFolder;
	private String docStyleListUrl;
	private File docStyleFolder;
	
	private String fontMode = "U";
	private String fontCharset = "U";
	private String fontCharsetPath;
	
	private Process decoderRun = null;
	private SyncPdfSlaveProcessInterface decoderInterface;
	private String decodingDocId = null;
	private String decodingParams = null;
	private long decodingStart = -1;
	private String decodingStep = null;
	private long decodingStepStart = -1;
	private String decodingInfo = null;
	private long decodingInfoStart = -1;
	private int decodingProgress = -1;
	private boolean decodeVerbose = false;
	
	/** the usual zero-argument constructor for class loading */
	public PdfImporter() {}
	
	//	clone constructor for runtime clones in parallel decoding
	private PdfImporter(PdfImporter original, String name) {
		super(original);
		this.name = name;
		this.isMainDecoder = false;
		
		this.maxSlaveMemory = original.maxSlaveMemory;
		this.maxSlaveCores = original.maxSlaveCores;
		this.logFolder = original.logFolder;
		this.docStyleListUrl = original.docStyleListUrl;
		this.docStyleFolder = original.docStyleFolder;
		
		this.fontMode = original.fontMode;
		this.fontCharset = original.fontCharset;
		this.fontCharsetPath = original.fontCharsetPath;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.imi.ImiDocumentImporter#getName()
	 */
	public String getName() {
		return this.name;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.imi.ImiDocumentImporter#getDescription()
	 */
	public String[] getDescription() {
		String[] decription = {
				(this.getName() + " - synchronous import of PDF documents (MIME type */pdf):"),
				"- overwriteExisting: set to 'true' or 'yes' to re-import a previously imported PDF (optional)",
				"- bornDigital: set to 'true' or 'yes' to indicate a born-digital PDF, 'false' or 'no' to indicate a scanned PDF (optional)",
				"- fontMode: set to 'D' for full decoding, 'V' for Unicode mapping verification, 'U' for Unicode mapping completion, 'R' for only rendering glyphs, or 'Q' to skip font decoding altogether (optional, implies 'bornDigital=true' if present, ignored if 'bornDigital=false')",
				"- fontCharset: set to 'U' for Unicode, 'F' for Full Latin, 'E' for Extended Latin, 'B' for Basic Latin, or 'C' for to use the custom character set (optional, implies 'bornDigital=true' if present, ignored if 'bornDigital=false')",
				"- scanDecodeFlags: provide detailed control flags, in HEX, for the import of a scanned PDF (optional, implies 'bornDigital=false' if present, ignored if 'bornDigital=true')",
				"- verbose: set to 'true' to loop full log output from decoder to server console",
			};
		return decription;
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
		
		//	set up folder for logging
		this.logFolder = new File(this.workingFolder, "Logs");
		this.logFolder.mkdirs();
		
		//	make document style templates available (we can do without, however)
		this.docStyleListUrl = config.getSetting("docStyleListUrl");
		this.docStyleFolder = new File(this.workingFolder, "DocStyles");
		this.docStyleFolder.mkdirs();
		
		//	read font mode and charset name
		this.fontMode = checkCharParameter(config.getSetting("fontMode", this.fontMode), "DVURQ", this.fontMode);
		this.fontCharset = checkCharParameter(config.getSetting("fontCharset", this.fontCharset), "UFEBC", this.fontCharset);
		
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
		
		//	set up collecting of errors from our slave processes
		String slaveErrorPath = this.host.getServerProperty("SlaveProcessErrorPath");
		if (slaveErrorPath != null)
			SlaveErrorRecorder.setErrorPath(slaveErrorPath);
	}
	
	private static final String IMPORT_STATUS_COMMAND = "status";
	private static final String IMPORT_THREADS_COMMAND = "threads";
	private static final String IMPORT_THREAD_GROUPS_COMMAND = "threadGroups";
	private static final String IMPORT_STACK_COMMAND = "stack";
	private static final String IMPORT_WAKE_COMMAND = "wake";
	private static final String IMPORT_KILL_COMMAND = "kill";
	private static final String SET_VERBOSE_COMMAND = "setVerbose";
	private static final String SET_QUIET_COMMAND = "setQuiet";
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.imi.ImiDocumentImporter#getActions()
	 */
	public ComponentActionConsole[] getActions() {
		ArrayList cal = new ArrayList();
		ComponentAction ca;
		
		//	check processing status of a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return IMPORT_STATUS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						IMPORT_STATUS_COMMAND,
						"Show the status of a document that is importing"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					reportImportStatus(this, true);
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	list the threads of a batch processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return IMPORT_THREADS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						IMPORT_THREADS_COMMAND,
						"Show the threads of the import of a document"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				if (decodingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				decoderInterface.setReportTo(this);
				decoderInterface.listThreads();
				decoderInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	list the thread groups of a batch processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return IMPORT_THREAD_GROUPS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						IMPORT_THREAD_GROUPS_COMMAND,
						"Show the thread groups of the import of a document"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				if (decodingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				decoderInterface.setReportTo(this);
				decoderInterface.listThreadGroups();
				decoderInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	check the stack of a batch processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return IMPORT_STACK_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						IMPORT_STACK_COMMAND + " <threadName>",
						"Show the stack trace of the import of a document:",
						"- <threadName>: The name of the thread whose stack to show (optional, omitting targets main thread)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length > 1) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify at most the name of the target thread.");
					return;
				}
				if (decodingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				decoderInterface.setReportTo(this);
				decoderInterface.printThreadStack((arguments.length == 0) ? null : arguments[0]);
				decoderInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	wake a batch processing a document, or a thread therein
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return IMPORT_WAKE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						IMPORT_WAKE_COMMAND + " <threadName>",
						"Wake the import of a document, or a thread therein:",
						"- <threadName>: The name of the thread to wake (optional, omitting targets main thread)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length > 1) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				if (decodingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				decoderInterface.setReportTo(this);
				decoderInterface.wakeThread((arguments.length == 0) ? null : arguments[0]);
				decoderInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	kill a batch processing a document, or a thread therein
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return IMPORT_KILL_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						IMPORT_KILL_COMMAND + " <threadName>",
						"Kill the import of a document, or a thread therein:",
						"- <threadName>: The name of the thread to kill (optional, omitting targets main thread)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length > 1) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				if (decodingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				decoderInterface.setReportTo(this);
				decoderInterface.killThread((arguments.length == 0) ? null : arguments[0]);
				decoderInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	setting verbosity makes sense only between decoding runs, not in runtime clones
		if (this.isMainDecoder) {
			
			//	activate verbose decoding
			ca = new ComponentActionConsole() {
				public String getActionCommand() {
					return SET_VERBOSE_COMMAND;
				}
				public String[] getExplanation() {
					String[] explanation = {
							SET_VERBOSE_COMMAND,
							"Set decoding mode to verbose (does not work on running decodings)"
						};
					return explanation;
				}
				public void performActionConsole(String[] arguments) {
					if (arguments.length == 0)
						decodeVerbose = true;
					else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
				}
			};
			cal.add(ca);
			
			//	deactivate verbose decoding
			ca = new ComponentActionConsole() {
				public String getActionCommand() {
					return SET_QUIET_COMMAND;
				}
				public String[] getExplanation() {
					String[] explanation = {
							SET_QUIET_COMMAND,
							"Set decoding mode to quiet (does not work on running decodings)"
						};
					return explanation;
				}
				public void performActionConsole(String[] arguments) {
					if (arguments.length == 0)
						decodeVerbose = false;
					else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
				}
			};
			cal.add(ca);
		}
		
		//	finally ...
		return ((ComponentActionConsole[]) cal.toArray(new ComponentActionConsole[cal.size()]));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.imi.ImiDocumentImporter#reportImportStatus(de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent.ComponentActionConsole)
	 */
	public void reportImportStatus(ComponentActionConsole cac) {
		if (this.decodingStart == -1)
			cac.reportResult("There is no document importing at the moment");
		else this.reportImportStatus(cac, false);
	}
	void reportImportStatus(ComponentActionConsole cac, boolean internal) {
		if (this.decodingStart == -1)
			cac.reportResult("There is no document importing at the moment");
		else {
			long time = System.currentTimeMillis();
			cac.reportResult(((this.isMainDecoder && internal) ? "Importing" : (this.name + ": importing")) + " document " + this.decodingDocId + " (started " + (time - this.decodingStart) + "ms ago, at " + this.decodingProgress + "%)");
			cac.reportResult(" - params are " + this.decodingParams);
			cac.reportResult(" - current step is " + this.decodingStep + " (since " + (time - this.decodingStepStart) + "ms)");
			cac.reportResult(" - current info is " + this.decodingInfo + " (since " + (time - this.decodingInfoStart) + "ms)");
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.imi.ImiDocumentImporter#getRuntimeClone(java.lang.String)
	 */
	public ImiDocumentImporter getRuntimeClone(String name) {
		if (this.isMainDecoder)
			return new PdfImporter(this, name);
		else throw new IllegalStateException("Runtime clones cannot be cloned");
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
			this.decodingStart = System.currentTimeMillis();
			this.doHandleImport(idi);
		}
		catch (Exception e) {
			idi.setError(e);
		}
		catch (Error e) {
			idi.setError(e);
		}
		finally {
			this.decoderRun = null;
			this.decoderInterface = null;
			this.decodingDocId = null;
			this.decodingParams = null;
			this.decodingStart = -1;
			this.decodingStep = null;
			this.decodingStepStart = -1;
			this.decodingInfo = null;
			this.decodingInfoStart = -1;
			this.decodingProgress = -1;
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
		
		//	hash input file first to check whether or not all the decoding hassle makes sense
		String docInFileHash = null;
		boolean docExists = false;
		try {
			docInFileHash = getDocInFileChecksum(docInFile);
			if (this.parent.checkDocumentExists(docInFileHash))
				docExists = true;
		}
		catch (IOException ioe) {
			System.out.println("Error computing hash of import file '" + docInFile.getAbsolutePath() + "': " + ioe.getMessage());
			ioe.printStackTrace(System.out);
		}
		if ((idi.removeAttribute("overwriteExisting") == null) && docExists)
			throw new IOException("Document '" + docInFileHash + "' imported before.");
		
		//	retain parameters in case of error
		TreeMap idiParams = new TreeMap();
		String[] idiParamNames = idi.getAttributeNames();
		for (int n = 0; n < idiParamNames.length; n++)
			idiParams.put(idiParamNames[n], idi.getAttribute(idiParamNames[n]));
		String idiParamString = idiParams.toString();
		
		//	check if we know born-digital or scanned (assume meta pages in the latter case for good measures)
		String bornDigital = ((String) idi.removeAttribute("isBornDigital"));
		String pdfType = "G";
		if ("true".equalsIgnoreCase(bornDigital) || "yes".equalsIgnoreCase(bornDigital))
			pdfType = "D";
		else if ("false".equalsIgnoreCase(bornDigital) || "no".equalsIgnoreCase(bornDigital))
			pdfType = "M";
		
		//	read font mode
		String fontMode = ((String) idi.removeAttribute("fontMode"));
		String fontCharset = ((String) idi.removeAttribute("fontCharset"));
		if ((fontMode != null) || (fontCharset != null)) {
			fontMode = checkCharParameter(fontMode, "DVURQ", this.fontMode);
			fontCharset = checkCharParameter(fontCharset, "UFEBC", this.fontCharset);
			if ("G".equals(pdfType))
				pdfType = "D";
		}
		else if ("D".equals(pdfType)) {
			fontMode = this.fontMode;
			fontCharset = this.fontCharset;
		}
		
		//	read scan decoding flags
		String scanDecodeFlagsHex = ((String) idi.removeAttribute("scanDecodeFlags"));
		int scanDecodeFlags = -1;
		if ((scanDecodeFlagsHex != null) && !"D".equals(pdfType)) try {
			scanDecodeFlags = Integer.parseInt(scanDecodeFlagsHex, 16);
			if ("G".equals(pdfType))
				pdfType = (((scanDecodeFlags & PdfExtractor.META_PAGES) == 0) ? "S" : "M");
		}
		catch (NumberFormatException nfe) {
			this.parent.logError("Invalid scan decoding flags '" + scanDecodeFlagsHex + "'");
		}
		
		//	read verbose flag
		boolean verbose = ((idi.removeAttribute("verbose") != null) || this.decodeVerbose);
		
		//	assemble decoder slave job
		SyncPdfSlaveJob spsj = new SyncPdfSlaveJob(docInFileHash);
		spsj.setDataPath(docInFile.getAbsolutePath());
		spsj.setProperty("DATATYPE", pdfType);
		if ("D".equals(pdfType)) {
			spsj.setProperty("FONTMODE", fontMode);
			if ("C".equals(fontCharset)) {
				spsj.setProperty("FONTCS", "C");
				spsj.setProperty("FONTCSPATH", this.fontCharsetPath);
			}
			else spsj.setProperty("FONTCS", fontCharset);
		}
		else if (scanDecodeFlags != -1)
			spsj.setProperty("SCANFLAGS", Integer.toString(scanDecodeFlags, 16).toUpperCase());
		if (verbose)
			spsj.setProperty(SlaveJob.VERBOSE_PARAMETER);
		spsj.setResultPath(docOutFolder.getAbsolutePath());
		
		//	start document decoder slave process
		this.decoderRun = Runtime.getRuntime().exec(spsj.getCommand(docCacheFolder.getAbsolutePath()), new String[0], this.workingFolder);
		this.decodingDocId = docInFileHash;
		this.decodingParams = idiParamString;
		
		//	set up slave process communication
		this.decoderInterface = new SyncPdfSlaveProcessInterface(this.decoderRun, ("SyncPdf" + idi.hashCode()), docInFileHash, docInFile.getName(), idiParamString, verbose);
		this.decoderInterface.setProgressMonitor(new ProgressMonitor() {
			public void setStep(String step) {
				host.logInfo(getName() + ": " + step);
				decodingStep = step;
				decodingStepStart = System.currentTimeMillis();
			}
			public void setInfo(String info) {
				host.logDebug(getName() + ": " + info);
				decodingInfo = info;
				decodingInfoStart = System.currentTimeMillis();
			}
			private int baseProgress = 0;
			private int maxProgress = 0;
			public void setBaseProgress(int baseProgress) {
				this.baseProgress = baseProgress;
			}
			public void setMaxProgress(int maxProgress) {
				this.maxProgress = maxProgress;
			}
			public void setProgress(int progress) {
				decodingProgress = (this.baseProgress + (((this.maxProgress - this.baseProgress) * progress) / 100));
			}
		});
		this.decoderInterface.start();
		
		//	wait for decoder to finish
		while (true) try {
			this.decoderRun.waitFor();
			break;
		} catch (InterruptedException ie) {}
		
		try {
			
			//	create document on top of import result
			ImDocument doc = ImDocumentIO.loadDocument(docOutFolder, new ProgressMonitor() {
				public void setStep(String step) {
					host.logInfo(getName() + ": " + step);
				}
				public void setInfo(String info) {
					host.logDebug(getName() + ": " + info);
				}
				public void setBaseProgress(int baseProgress) {}
				public void setMaxProgress(int maxProgress) {}
				public void setProgress(int progress) {}
			});
			
			//	hand document back to caller via idi.setDocument()
			idi.setDocument(doc);
		}
		
		//	report whatever error might have occurred
		catch (IOException ioe) {
			this.host.logError(this.getName() + ": failed to load import result - " + ioe.getMessage());
			this.host.logError(ioe);
			
			//	report as slave process error (need to go via strings to fit parameters)
			StackTraceElement[] stes = ioe.getStackTrace();
			String[] errorStackTrace = new String[stes.length];
			for (int e = 0; e < stes.length; e++)
				errorStackTrace[e] = ("\tat " + stes[e]);
			SlaveErrorRecorder.recordError((this.parent.getLetterCode() + "." + this.getName()), docInFileHash, ioe.getClass().getName(), (ioe.getMessage() + " (in " + docInFile.getName() + ", params " + idiParamString + ")"), errorStackTrace);
		}
		
		//	clean up cache and document data
		finally {
			cleanupFile(docCacheFolder);
			cleanupFile(docOutFolder);
		}
	}
	
	private static String getDocInFileChecksum(File docInFile) throws IOException {
		MD5 checksumDigester = new MD5(); // uses instance pool inside, and takes care of all initialization, resetting, etc.
		InputStream docInFileIn = new BufferedInputStream(new FileInputStream(docInFile));
		byte[] buffer = new byte[1024];
		for (int r; (r = docInFileIn.read(buffer, 0, buffer.length)) != -1;)
			checksumDigester.update(buffer, 0, r);
		docInFileIn.close();
		byte[] checksumBytes = checksumDigester.digest();
		String checksum = new String(RandomByteSource.getHexCode(checksumBytes));
		return checksum;
	}
	
	private static String checkCharParameter(String setValue, String permittedValues, String defValue) {
		if (setValue == null)
			return defValue;
		if (setValue.length() != 1)
			return defValue;
		if (permittedValues.indexOf(setValue) == -1)
			return defValue;
		return setValue;
	}
	
	private static void cleanupFile(File file) {
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (int f = 0; f < files.length; f++)
				cleanupFile(files[f]);
		}
		file.delete();
	}
	
	private class SyncPdfSlaveJob extends SlaveJob {
		SyncPdfSlaveJob(String slaveJobId) {
			super(slaveJobId, "PdfImporterSlave.jar");
			if (maxSlaveMemory > 512)
				this.setMaxMemory(maxSlaveMemory);
			int maxCores = maxSlaveCores;
			if (maxCores < 1)
				maxCores = 65536;
			if ((maxCores * 4) > Runtime.getRuntime().availableProcessors())
				maxCores = (Runtime.getRuntime().availableProcessors() / 4);
			this.setMaxCores(maxCores);
			this.setLogPath(logFolder.getAbsolutePath());
			if (docStyleFolder != null)
				this.setProperty("DSPATH", docStyleFolder.getAbsolutePath());
			if (docStyleListUrl != null)
				this.setProperty("DSURL", docStyleListUrl);
		}
	}
	
	private class SyncPdfSlaveProcessInterface extends SlaveProcessInterface {
		private ComponentActionConsole reportTo = null;
		private String docId;
		private String pdfFileName;
		private String importParams;
		private boolean verbose;
		SyncPdfSlaveProcessInterface(Process slave, String slaveName, String docId, String pdfFileName, String importParams, boolean verbose) {
			super(slave, slaveName);
			this.docId = docId;
			this.pdfFileName = pdfFileName;
			this.importParams = importParams;
			this.verbose = verbose;
		}
		void setReportTo(ComponentActionConsole reportTo) {
			this.reportTo = reportTo;
		}
		protected void handleInput(String input) {
			if (this.verbose)
				host.logInfo(getName() + ": " + input);
		}
		protected void handleResult(String result) {
			ComponentActionConsole cac = this.reportTo;
			if (cac == null)
				host.logInfo(getName() + ": " + result);
			else cac.reportResult(result);
		}
		private ArrayList outStackTrace = new ArrayList();
		protected void handleStackTrace(String stackTraceLine) {
			if (stackTraceLine.trim().length() == 0)
				this.reportError(this.outStackTrace);
			else {
				this.outStackTrace.add(stackTraceLine);
				super.handleStackTrace(stackTraceLine);
			}
		}
		protected void finalizeSystemOut() {
			this.reportError(this.outStackTrace);
		}
		private ArrayList errStackTrace = new ArrayList();
		protected void handleError(String error, boolean fromSysErr) {
			ComponentActionConsole cac = this.reportTo;
			if (fromSysErr && (cac == null)) {
				if (error.startsWith("CR\t") || error.startsWith("LA\t") || error.startsWith("Stale "))
					return; // TODO remove this once server fixed
				if (error.matches("(Im|Gamta)Document(Root)?Guard\\:.*"))
					return; // TODO remove this once server fixed
				if (error.startsWith("Font 'Free") && error.endsWith("' loaded successfully."))
					return;
				if (error.endsWith(" org.icepdf.core.pobjects.Document <clinit>"))
					return;
				if (error.equals("WARNING: PDF write support was not found on the class path"))
					return;
			}
			if (cac == null) {
				if (fromSysErr)
					this.errStackTrace.add(error);
				host.logError(getName() + ": " + error);
			}
			else cac.reportError(error);
		}
		protected void finalizeSystemErr() {
			this.reportError(this.errStackTrace);
		}
		private void reportError(ArrayList stackTrace) {
			if (stackTrace.size() == 0)
				return;
			String classAndMessge = ((String) stackTrace.get(0));
			String errorClassName;
			String errorMessage;
			if (classAndMessge.indexOf(":") == -1) {
				errorClassName = classAndMessge;
				errorMessage = "";
			}
			else {
				errorClassName = classAndMessge.substring(0, classAndMessge.indexOf(":")).trim();
				errorMessage = classAndMessge.substring(classAndMessge.indexOf(":") + ":".length()).trim();
			}
			String[] errorStackTrace = ((String[]) stackTrace.toArray(new String[stackTrace.size()]));
			stackTrace.clear();
			SlaveErrorRecorder.recordError((parent.getLetterCode() + "." + getName()), this.docId, errorClassName, (errorMessage + " (in " + this.pdfFileName + ", params " + this.importParams + ")"), errorStackTrace);
		}
	}
}