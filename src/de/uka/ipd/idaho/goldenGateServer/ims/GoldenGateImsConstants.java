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
package de.uka.ipd.idaho.goldenGateServer.ims;

import de.uka.ipd.idaho.gamta.util.imaging.ImagingConstants;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants;
import de.uka.ipd.idaho.im.util.ImDocumentData;

/**
 * Constant bearer for GoldenGATE Image Markup Storage facility.
 * 
 * @author sautter
 */
public interface GoldenGateImsConstants extends GoldenGateServerConstants, ImagingConstants {
	
	/** the command for loading a document */
	public static final String GET_DOCUMENT = "IMS_GET_DOCUMENT";
	
	/** the command for loading document entries */
	public static final String GET_DOCUMENT_ENTRIES = "IMS_GET_DOCUMENT_ENTRIES";
	
	/** the command for uploading a document, i.e. adding a new document to the collection */
	public static final String UPLOAD_DOCUMENT = "IMS_UPLOAD_DOCUMENT";
	
	/** the command for checking out a document from a storage */
	public static final String CHECKOUT_DOCUMENT = "IMS_CHECKOUT_DOCUMENT";
	
	/** the command for updating a document, i.e. uploading a new version */
	public static final String UPDATE_DOCUMENT = "IMS_UPDATE_DOCUMENT";
	
	/** the command for loading document entries */
	public static final String UPDATE_DOCUMENT_ENTRIES = "IMS_UPDATE_DOCUMENT_ENTRIES";
	
	/** the command for deleting a document */
	public static final String DELETE_DOCUMENT = "IMS_DELETE_DOCUMENT";
	
	/** the command for releasing a document that was previously checked out */
	public static final String RELEASE_DOCUMENT = "IMS_RELEASE_DOCUMENT";
	
	/** the command for loading a list of all documents in the IMS */
	public static final String GET_DOCUMENT_LIST = "IMS_GET_DOCUMENT_LIST";
	
	/** the command for retrieving the update protocol of document, i.e. messages that describe which other modifications the new version incurred throughout the server (only modifications that happen synchronously on update notification, though) */
	public static final String GET_UPDATE_PROTOCOL = "IMS_GET_UPDATE_PROTOCOL";
	
	/** the last entry in a document update protocol, indicating that the update is fully propagated through the server */
	public static final String UPDATE_COMPLETE = "Document update complete";
	
	/** the last entry in a document update protocol belonging to a deletion, indicating that the update is fully propagated through the server */
	public static final String DELETION_COMPLETE = "Document deletion complete";
	
	
	/** the permission for uploading new documents to the IMS */
	public static final String UPLOAD_DOCUMENT_PERMISSION = "IMS.UploadDocument";
	
	/** the permission for updating existing documents in the IMS */
	public static final String UPDATE_DOCUMENT_PERMISSION = "IMS.UpdateDocument";
	
	/** the permission for deleting documents from the IMS */
	public static final String DELETE_DOCUMENT_PERMISSION = "IMS.DeleteDocument";
	
	
	/** the attribute holding the name of the user who uploaded a document */
	public static final String CHECKIN_USER_ATTRIBUTE = "checkinUser";

	/** the attribute holding the time when a document was uploaded */
	public static final String CHECKIN_TIME_ATTRIBUTE = "checkinTime";

	/** the attribute holding the name of the user who last updated a document */
	public static final String UPDATE_USER_ATTRIBUTE = "updateUser";

	/** the attribute holding the time when a document was last updated */
	public static final String UPDATE_TIME_ATTRIBUTE = "updateTime";
	
	/** the attribute holding the name of a document */
	public static final String DOCUMENT_VERSION_ATTRIBUTE = "docVersion";
	
	/** the attribute holding the name of the user who has currently checked out a document for working */
	public static final String CHECKOUT_USER_ATTRIBUTE = "checkoutUser";
	
	/** the attribute holding the time when a document was checked out */
	public static final String CHECKOUT_TIME_ATTRIBUTE = "checkoutTime";
	
	
	/**
	 * GoldenGATE IMS specific document storage event, adding types for
	 * checkout and release.
	 * 
	 * @author sautter
	 */
	public static class ImsDocumentEvent extends GoldenGateServerEvent {
		public static final int UPDATE_TYPE = 0;
		public static final int DELETE_TYPE = 1;
		public static final int CHECKOUT_TYPE = 2;
		public static final int RELEASE_TYPE = 4;
		
		/**
		 * Specialized storage listener for GoldenGATE IMS, receiving notifications of
		 * document checkout and release operations, besides update and delete
		 * operations.
		 * 
		 * @author sautter
		 */
		public static abstract class ImsDocumentEventListener extends GoldenGateServerEventListener {
			
			/* (non-Javadoc)
			 * @see de.uka.ipd.idaho.goldenGateServer.events.GoldenGateServerEventListener#notify(de.uka.ipd.idaho.goldenGateServer.events.GoldenGateServerEvent)
			 */
			public void notify(GoldenGateServerEvent gse) {
				if (gse instanceof ImsDocumentEvent) {
					ImsDocumentEvent dse = ((ImsDocumentEvent) gse);
					if (dse.type == CHECKOUT_TYPE)
						this.documentCheckedOut(dse);
					else if (dse.type == RELEASE_TYPE)
						this.documentReleased(dse);
					else if (dse.type == UPDATE_TYPE)
						this.documentUpdated(dse);
					else if (dse.type == DELETE_TYPE)
						this.documentDeleted(dse);
				}
			}
			
			/**
			 * Receive notification that a document was checked out by a user. The
			 * actual document will be null in this type of notification, and the
			 * version number will be -1.
			 * @param dse the DocumentStorageEvent providing detail information on the
			 *            checkout
			 */
			public abstract void documentCheckedOut(ImsDocumentEvent dse);
			
			/**
			 * Receive notification that a document was updated (can be both a new
			 * document or an updated version of an existing document)
			 * @param dse the DocumentStorageEvent providing detail information on the
			 *            update
			 */
			public abstract void documentUpdated(ImsDocumentEvent dse);
			
			/**
			 * Receive notification that a document was deleted
			 * @param dse the DocumentStorageEvent providing detail information on the
			 *            deletion
			 */
			public abstract void documentDeleted(ImsDocumentEvent dse);
			
			/**
			 * Receive notification that a document was released by a user. The actual
			 * document will be null in this type of notification, and the version
			 * number will be -1.
			 * @param dse the DocumentStorageEvent providing detail information on the
			 *            release
			 */
			public abstract void documentReleased(ImsDocumentEvent dse);
		}
		
		
		/** The name of the user who caused the event */
		public final String user;
		
		/** The ID of the document affected by the event */
		public final String documentId;
		
//		/**
//		 * The document affected by the event, null for deletion events. This
//		 * document is strictly read-only, any attempt of modification will
//		 * result in a RuntimException being thrown.
//		 */
//		public final ImDocument document;
		/**
		 * The data of document affected by the event, null for deletion events.
		 * This document data is strictly read-only, any attempt of modification
		 * will result in a RuntimException being thrown.
		 */
		public final ImDocumentData documentData;
		
		/**
		 * The current version number of the document affected by this event, -1
		 * for deletion events
		 */
		public final int version;
		
//		/**
//		 * Constructor for update events
//		 * @param user the name of the user who caused the event
//		 * @param documentId the ID of the document that was updated
//		 * @param document the actual document that was updated
//		 * @param version the current version number of the document (after the
//		 *            update)
//		 * @param sourceClassName the class name of the component issuing the event
//		 * @param logger a DocumentStorageLogger to collect log messages while the
//		 *            event is being processed in listeners
//		 */
//		public ImsDocumentEvent(String user, String documentId, ImDocument document, int version, String sourceClassName, long eventTime, EventLogger logger) {
//			this(user, documentId, document, version, UPDATE_TYPE, sourceClassName, eventTime, logger);
//		}
		/**
		 * Constructor for update events
		 * @param user the name of the user who caused the event
		 * @param documentId the ID of the document that was updated
		 * @param documentData the data of the document that was updated
		 * @param version the current version number of the document (after the
		 *            update)
		 * @param sourceClassName the class name of the component issuing the event
		 * @param logger a DocumentStorageLogger to collect log messages while the
		 *            event is being processed in listeners
		 */
		public ImsDocumentEvent(String user, String documentId, ImDocumentData documentData, int version, String sourceClassName, long eventTime, EventLogger logger) {
			this(user, documentId, documentData, version, UPDATE_TYPE, sourceClassName, eventTime, logger);
		}
		
		/**
		 * Constructor for deletion events
		 * @param user the name of the user who caused the event
		 * @param documentId the ID of the document that was deleted
		 * @param sourceClassName the class name of the component issuing the event
		 * @param logger a DocumentStorageLogger to collect log messages while the
		 *            event is being processed in listeners
		 */
		public ImsDocumentEvent(String user, String documentId, String sourceClassName, long eventTime, EventLogger logger) {
			this(user, documentId, null, -1, DELETE_TYPE, sourceClassName, eventTime, logger);
		}
//		
//		/**
//		 * Constructor for custom-type events
//		 * @param user the name of the user who caused the event
//		 * @param documentId the ID of the document that was updated
//		 * @param document the actual document that was updated
//		 * @param version the current version number of the document (after the
//		 *            update)
//		 * @param sourceClassName the class name of the component issuing the event
//		 * @param logger a DocumentStorageLogger to collect log messages while the
//		 *            event is being processed in listeners
//		 * @param type the event type (used for dispatching)
//		 */
//		public ImsDocumentEvent(String user, String documentId, ImDocument document, int version, int type, String sourceClassName, long eventTime, EventLogger logger) {
//			super(type, sourceClassName, eventTime, (documentId + "-" + eventTime), logger);
//			this.user = user;
//			this.documentId = documentId;
//			this.document = document;
//			this.version = version;
//		}
		
		/**
		 * Constructor for custom-type events
		 * @param user the name of the user who caused the event
		 * @param documentId the ID of the document that was updated
		 * @param documentData the data of the document that was updated
		 * @param version the current version number of the document (after the
		 *            update)
		 * @param sourceClassName the class name of the component issuing the event
		 * @param logger a DocumentStorageLogger to collect log messages while the
		 *            event is being processed in listeners
		 * @param type the event type (used for dispatching)
		 */
		public ImsDocumentEvent(String user, String documentId, ImDocumentData documentData, int version, int type, String sourceClassName, long eventTime, EventLogger logger) {
			super(type, sourceClassName, eventTime, (documentId + "-" + eventTime), logger);
			this.user = user;
			this.documentId = documentId;
			this.documentData = documentData;
			this.version = version;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent#getParameterString()
		 */
		public String getParameterString() {
			return (super.getParameterString() + " " + this.user + " " + this.documentId + " " + this.version);
		}
		
		/**
		 * Parse a document event from its string representation returned by the
		 * getParameterString() method.
		 * @param data the string to parse
		 * @return a document event created from the specified data
		 */
		public static ImsDocumentEvent parseEvent(String data) {
			String[] dataItems = data.split("\\s");
			return new ImsDocumentEvent(dataItems[4], dataItems[5], null, Integer.parseInt(dataItems[6]), Integer.parseInt(dataItems[0]), dataItems[1], Long.parseLong(dataItems[2]), null);
		}
	}
}