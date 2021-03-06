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
package de.uka.ipd.idaho.goldenGateServer.ims.util;

import java.io.File;
import java.io.IOException;

import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
import de.uka.ipd.idaho.gamta.util.transfer.DocumentListBuffer;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS;
import de.uka.ipd.idaho.goldenGateServer.util.AsynchronousConsoleAction;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * Convenience console action for updating a GoldenGATE Server Component's data
 * based on the document collection hosted in a GoldenGATE IMS. This class loads
 * the IMS document list and loops through it, observing all the required
 * runtime control methods. Sub classes have to implement solely the update()
 * method, which is meant to deal with individual documents.
 * 
 * @author sautter
 */
public abstract class AsynchronousImsAction extends AsynchronousConsoleAction implements LiteratureConstants {
	private GoldenGateIMS ims;
	private String label;
	
	/**
	 * Constructor
	 * @param command the action command (defaults to 'update' if null or the
	 *            empty string is specified)
	 * @param ims the backing GoldenGATE IMS (must not be null)
	 * @param explanation the explanation string to go into the second line of
	 *            the explanation displayed in the console (must not be null)
	 * @param label the label of the action performed, namely a name for what
	 *            the action updates (must not be null)
	 * @param logFolder the folder to write update log files to (specifying null
	 *            disables logging)
	 * @param logFilePrefix the prefix for log file names (specifying null
	 *            disables logging)
	 */
	public AsynchronousImsAction(String command, GoldenGateIMS ims, String explanation, String label, File logFolder, String logFilePrefix) {
		super(command, explanation, label, logFolder, logFilePrefix);
		this.ims = ims;
		this.label = label;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.util.AsynchronousConsoleAction#performAction(java.lang.String[])
	 */
	protected void performAction(String[] arguments) throws Exception {
		
		//	get document list
		DocumentListBuffer dlb = new DocumentListBuffer(this.ims.getDocumentListFull());
		
		//	notify loop round end (this is fine, as loading the document list might already have been considerable effort)
		this.enteringMainLoop("0 of " + dlb.size() + " documents done");
		this.log("Updating " + label + " from " + dlb.size() + " IMS documents");
		
		//	update documents one by one
		for (int d = 0; this.continueAction() && (d < dlb.size()); d++) {
			StringTupel docData = dlb.get(d);
			String docName = docData.getValue(DOCUMENT_NAME_ATTRIBUTE);
			this.log("  - updating for document '" + docName + "' (" + (d+1) + " of " + dlb.size() + ")");
			try {
				
				//	do sub class specific update
				update(docData, arguments);
				
				//	done
				this.log("    - document '" + docName + "' (" + (d+1) + " of " + dlb.size() + ") done");
			}
			catch (Exception e) {
				this.log(("  ==> Error running update for document '" + docName + "': " + e.getMessage()), e);
			}
			
			//	notify loop round end
			this.loopRoundComplete((d+1) + " of " + dlb.size() + " documents done");
		}
	}
	
	/**
	 * Do the actual (sub class specific) update work.
	 * @param docData the current document's meta data
	 * @param arguments the arguments specified when starting the asynchronous action
	 * @throws IOException
	 */
	protected abstract void update(StringTupel docData, String[] arguments) throws IOException;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.util.AsynchronousConsoleAction#getActionName()
	 */
	protected String getActionName() {
		return (this.ims.getLetterCode() + "." + super.getActionName());
	}
	
	/**
	 * Retrieve an actual document from the backing GoldenGATE IMS.
	 * @param docId the ID of the document to retrieve
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	protected final ImDocument getDocument(String docId) throws IOException {
		return this.ims.getDocument(docId);
	}
}