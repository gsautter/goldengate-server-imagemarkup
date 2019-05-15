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
package de.uka.ipd.idaho.goldenGateServer.ime;

import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants;

/**
 * Constant bearer for GoldenGATE Image Markup Error protocol service.
 * 
 * @author sautter
 */
public interface GoldenGateImeConstants extends GoldenGateServerConstants, LiteratureConstants {
	
	/** the attribute holding the total number of errors in a document list entry retrieved from IME */
	public static final String ERROR_COUNT_ATTRIBUTE = "errorCount";
	
	/** the attribute holding the total number of error categories in a document list entry retrieved from IME */
	public static final String ERROR_CATEGORY_COUNT_ATTRIBUTE = "errorCategories";
	
	/** the attribute holding the total number of error types in a document list entry retrieved from IME */
	public static final String ERROR_TYPE_COUNT_ATTRIBUTE = "errorTypes";
	
	/** the attribute holding the total number of blocker errors in a document list entry retrieved from IME */
	public static final String BLOCKER_ERROR_COUNT_ATTRIBUTE = "blockerCount";
	
	/** the attribute holding the total number of critical errors in a document list entry retrieved from IME */
	public static final String CRITICAL_ERROR_COUNT_ATTRIBUTE = "criticalCount";
	
	/** the attribute holding the total number of major errors in a document list entry retrieved from IME */
	public static final String MAJOR_ERROR_COUNT_ATTRIBUTE = "majorCount";
	
	/** the attribute holding the total number of minor errors in a document list entry retrieved from IME */
	public static final String MINOR_ERROR_COUNT_ATTRIBUTE = "minorCount";
	
	
	/** the command for loading a list of all documents in the IME, i.e., all the documents that have errors */
	public static final String GET_DOCUMENT_LIST = "IME_GET_DOCUMENT_LIST";
	
	/** the command for loading the error summary for a document */
	public static final String GET_ERROR_SUMMARY = "IME_GET_ERROR_SUMMARY";
	
	/** the command for loading the error protocol for a document */
	public static final String GET_ERROR_PROTOCOL = "IME_GET_ERROR_PROTOCOL";
}
