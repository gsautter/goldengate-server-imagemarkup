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
package de.uka.ipd.idaho.goldenGateServer.ims.connectors;

import java.io.IOException;
import java.util.ArrayList;

import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentRegistry;
import de.uka.ipd.idaho.goldenGateServer.els.GoldenGateELS.LinkWriter;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateIMS;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent;
import de.uka.ipd.idaho.goldenGateServer.ims.GoldenGateImsConstants.ImsDocumentEvent.ImsDocumentEventListener;
import de.uka.ipd.idaho.im.ImAnnotation;
import de.uka.ipd.idaho.im.ImDocument;
import de.uka.ipd.idaho.im.ImDocument.ImDocumentListener;
import de.uka.ipd.idaho.im.ImFont;
import de.uka.ipd.idaho.im.ImObject;
import de.uka.ipd.idaho.im.ImRegion;
import de.uka.ipd.idaho.im.ImSupplement;
import de.uka.ipd.idaho.im.gamta.ImDocumentRoot;

/**
 * @author sautter
 */
public class GoldenGateElsImsWriter extends LinkWriter {
	private GoldenGateIMS ims;
	private ImsDocumentEventListener imsListener;
	
	/** zero-argument default constructor for class loading */
	public GoldenGateElsImsWriter() {}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#init()
	 */
	protected void init() {
		
		//	link up to IMS
		this.ims = ((GoldenGateIMS) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateIMS.class.getName()));
		if (this.ims == null)
			throw new RuntimeException("GoldenGateElsIMS: cannot work without image document store");
		
		//	provide release notifications
		this.imsListener = new ImsDocumentEventListener() {
			public void documentUpdated(ImsDocumentEvent dse) {}
			public void documentReleased(ImsDocumentEvent dse) {
				GoldenGateElsImsWriter.this.dataObjectUnlocked(dse.dataId);
			}
			public void documentDeleted(ImsDocumentEvent dse) {}
			public void documentCheckedOut(ImsDocumentEvent dse) {}
		};
		this.ims.addDocumentEventListener(this.imsListener);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#exit()
	 */
	protected void exit() {
		this.ims.removeDocumentEventListener(this.imsListener);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#getPriority()
	 */
	public int getPriority() {
		return 10; // we're pretty much the root ...
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#canHandleLinks(java.lang.String)
	 */
	public boolean canHandleLinks(String dataId) {
		return this.ims.isDocumentAvailable(dataId);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#canWriteLinks(java.lang.String)
	 */
	public boolean canWriteLinks(String dataId) {
		return this.ims.isDocumentEditable(dataId, UPDATE_USER_NAME);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#providesUnlockNotifications()
	 */
	protected boolean providesUnlockNotifications() {
		return true; // yes, we do
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#writeLinks(java.lang.String, de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateElsConstants.ExternalLink[], de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateElsConstants.ExternalLinkHandler[])
	 */
	public ExternalLink[] writeLinks(String dataId, ExternalLink[] links, ExternalLinkHandler[] handlers) {
		
		//	check out document
		ImDocument doc;
		ImDocumentChangeTracker docTracker;
		try {
			doc = this.ims.checkoutDocument(UPDATE_USER_NAME, dataId);
			docTracker = new ImDocumentChangeTracker();
			doc.addDocumentListener(docTracker);
		}
		catch (IOException ioe) {
			this.host.logError("Could not check out docment '" + dataId + "': " + ioe.getMessage());
			this.host.logError(ioe);
			return links; // this will re-schedule
		}
		
		//	handle links
		try {
			
			//	try handling on IMF document first
			if (links.length != 0)
				links = this.writeLinks(doc, links, handlers);
			
			//	try handling on XML wrapper as alternative
			if (links.length != 0)
				links = this.writeLinks(new ImDocumentRoot(doc, (ImDocumentRoot.NORMALIZE_CHARACTERS | ImDocumentRoot.NORMALIZATION_LEVEL_STREAMS)), links, handlers);
			
			//	store any changes
			if (docTracker.docChanged)
				this.ims.updateDocument(this.getUpdateUserName(), UPDATE_USER_NAME, doc, this);
			
			//	return whatever is left
			return links;
		}
		
		catch (Exception e) {
			this.host.logError("Could not handle links on docment '" + dataId + "': " + e.getMessage());
			this.host.logError(e);
			return links; // this will re-schedule
		}
		
		//	make sure to remove listener and release document under all circumstances
		finally {
			doc.removeDocumentListener(docTracker);
			doc.dispose();
			this.ims.releaseDocument(UPDATE_USER_NAME, dataId);
		}
	}
	
	private static class ImDocumentChangeTracker implements ImDocumentListener {
		boolean docChanged = false;
		public void typeChanged(ImObject object, String oldType) {
			this.docChanged = true;
		}
		public void attributeChanged(ImObject object, String attributeName, Object oldValue) {
			if ((object instanceof ImDocument) && attributeName.startsWith("ID-") && object.hasAttribute(attributeName)) {
				object.setAttribute(("mods:" + attributeName), object.getAttribute(attributeName));
				object.removeAttribute(attributeName);
			}
			this.docChanged = true;
		}
		public void supplementChanged(String supplementId, ImSupplement oldValue) {
			this.docChanged = true;
		}
		public void fontChanged(String fontName, ImFont oldValue) {
			this.docChanged = true;
		}
		public void regionRemoved(ImRegion region) {
			this.docChanged = true;
		}
		public void regionAdded(ImRegion region) {
			this.docChanged = true;
		}
		public void annotationRemoved(ImAnnotation annotation) {
			this.docChanged = true;
		}
		public void annotationAdded(ImAnnotation annotation) {
			this.docChanged = true;
		}
	}
	
	private ExternalLink[] writeLinks(ImDocument doc, ExternalLink[] links, ExternalLinkHandler[] handlers) {
		ArrayList remainingLinks = new ArrayList();
		
		//	try handling on IMF document first
		this.host.logInfo("Handling " + links.length + " links on IMF document:");
		for (int l = 0; l < links.length; l++) {
			ExternalLink link = links[l];
			for (int h = 0; h < handlers.length; h++)
				if (handlers[h].handleLink(doc, link)) {
					link = null;
					break;
				}
			if (link == null)
				this.host.logInfo(" - handled " + links[l].type + " " + links[l].link);
			else remainingLinks.add(link);
		}
		
		//	return remaining links
		if (remainingLinks.size() < links.length)
			links = ((ExternalLink[]) remainingLinks.toArray(new ExternalLink[remainingLinks.size()]));
		this.host.logInfo(" ==> " + links.length + " links remaining");
		return links;
	}
	
	private ExternalLink[] writeLinks(ImDocumentRoot doc, ExternalLink[] links, ExternalLinkHandler[] handlers) {
		ArrayList remainingLinks = new ArrayList();
		
		//	try handling on IMF document first
		this.host.logInfo("Handling " + links.length + " links on XML wrapper:");
		for (int l = 0; l < links.length; l++) {
			ExternalLink link = links[l];
			for (int h = 0; h < handlers.length; h++)
				if (handlers[h].handleLink(doc, link)) {
					link = null;
					break;
				}
			if (link == null)
				this.host.logInfo(" - handled " + links[l].type + " " + links[l].link);
			else remainingLinks.add(link);
		}
		
		//	return remaining links
		if (remainingLinks.size() < links.length)
			links = ((ExternalLink[]) remainingLinks.toArray(new ExternalLink[remainingLinks.size()]));
		this.host.logInfo(" ==> " + links.length + " links remaining");
		return links;
	}
}
