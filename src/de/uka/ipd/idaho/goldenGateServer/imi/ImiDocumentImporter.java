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
package de.uka.ipd.idaho.goldenGateServer.imi;

import java.io.File;

import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentHost;
import de.uka.ipd.idaho.goldenGateServer.imi.GoldenGateIMI.ImiDocumentImport;

/**
 * Importer for a specific type of binary documents.
 * 
 * @author sautter
 */
public abstract class ImiDocumentImporter {
	
	/** The data folder of the importer. This folder is the location where the
	 * importer should have data it reads on initialization, but not the place
	 * to store temporary data during an import. Rather, the cache folder is
	 * intended for the latter purpose. Also, if the importer installs any
	 * binaries and runs them via <code>Runtime.exec()</code>, that should be
	 * done in the working folder. */
	protected File dataPath;
	
	/** the component's host, providing access to the shared database */
	protected GoldenGateServerComponentHost host;
	
	/** The working folder of the importer. This folder is the location where
	 * the importer should install any binaries it uses and run sub processes
	 * in via <code>Runtime.exec()</code>. */
	protected File workingFolder;
	
	/** The cache folder of the importer. This folder is the location where the
	 * importer should store temporary data during an import, including data of
	 * imported documents it hands back to an import job via either of the
	 * <code>setDocument()</code> or <code>setDocumentData()</code> methods. */
	protected File cacheFolder;
	
	/** Constructor
	 */
	protected ImiDocumentImporter() {}
	
	/**
	 * Get the name of the importer. The name returned by this method must be a
	 * valid file name. It best consists of letters, digits, underscores, and
	 * dashes only.
	 * @return the importer name
	 */
	public abstract String getName();
	
	/**
	 * Get the priority of the importer, on a 0-10 scale. Higher priority
	 * importers will be asked to handle a given import before lower priority
	 * ones. Thus, importers that handle a very specific type of imports only,
	 * e.g. documents with specific metadata attributes, should return a higher
	 * value from this method, whereas more generic importers should return a
	 * lower value. This default implementation returns 0, sub classes are
	 * welcome to overwrite it as needed.
	 * @return the priority of the importer.
	 */
	public int getPrority() {
		return 0;
	}
	
	/**
	 * Set the component host, providing access to a database, etc.
	 * @param host the host
	 */
	public void setHost(GoldenGateServerComponentHost host) {
		this.host = host;
	}
	
	/**
	 * Set the data folder of the importer. This folder is the location where
	 * the importer should store data it reads on initialization, but not the
	 * place to store temporary data during an import. Rather, the cache folder
	 * is intended for the latter purpose. Also, if the importer installs any
	 * binaries and runs them via <code>Runtime.exec()</code>, that should be
	 * done in the working folder.
	 * @param dataPath the data path
	 */
	public void setDataPath(File dataPath) {
		this.dataPath = dataPath;
	}
	
	/**
	 * Set the working folder of the importer. This folder is the location
	 * where the importer should install any binaries it uses and run sub
	 * processes in via <code>Runtime.exec()</code>.
	 * @param workingFolder the working folder
	 */
	public void setWorkingFolder(File workingFolder) {
		this.workingFolder = workingFolder;
	}
	
	/**
	 * Set the cache folder of the importer. This folder is the location where
	 * the importer should store temporary data during an import.
	 * @param cacheFolder the cache folder
	 */
	public void setCacheFolder(File cacheFolder) {
		this.cacheFolder = cacheFolder;
	}
	
	/**
	 * Initialize the importer. This method is called after host, data path,
	 * and working folder are set. This default implementation does nothing,
	 * sub classes are welcome to overwrite it as needed.
	 */
	public void init() {}
	
	/**
	 * Shut down the importer. This default implementation does nothing, sub
	 * classes are welcome to overwrite it as needed.
	 */
	public void exit() {}
	
	/**
	 * Test if the importer can handle a specific import. This judgment should
	 * mostly be made based on the MIME type of the import data.
	 * @param idi the import to inspect
	 * @return true if the import can be handled
	 */
	public abstract boolean canHandleImport(ImiDocumentImport idi);
	
	/**
	 * Handle an import. Any exception occurring in implementations of this
	 * method should be handed to the argument import via the
	 * <code>setError()</code> method, rather than thrown or wrapped and thrown.
	 * Once the import is complete, the resulting document or document data
	 * object should be handed to the argument import via either of the
	 * <code>setDocument()</code> or <code>setDocumentData()</code> methods.
	 * Implementations may decide to return right away and handle the import
	 * asynchronously. However, this likely results in this method being called
	 * again before an import is finished. Thus, only implementations that do
	 * not cause much load on local CPUs should use the asynchronous approach.
	 * @param idi the import to handle
	 */
	public abstract void handleImport(ImiDocumentImport idi);
}