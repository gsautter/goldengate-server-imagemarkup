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
package de.uka.ipd.idaho.goldenGateServer.imp;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import de.uka.ipd.idaho.easyIO.util.JsonParser;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants;

/**
 * Constant bearer for GoldenGATE Image Markup Processing service.
 * 
 * @author sautter
 */
public interface GoldenGateImpConstants extends GoldenGateServerConstants {
	
	/** the command for retrieving the descriptors of all available batches */
	public static final String GET_BATCH_DESCRIPTORS = "IMP_GET_BATCH_DESCRIPTORS";
	
	/** the command for retrieving the detail description of a batch */
	public static final String GET_BATCH_DESCRIPTION = "IMP_GET_BATCH_DESCRIPTION";
	
	/** the command for scheduling processing of a document through a specific batch */
	public static final String SCHEDULE_BATCH_PROCESSING = "IMP_SCHEDULE_BATCH_PROCESSING";
	
	/**
	 * Descriptor of a batch available in the backing IMP.
	 * 
	 * @author sautter
	 */
	public static class ImpBatchDescriptor {
		
		/** the name of the batch, to use as a parameter on scheduling */
		public final String name;
		
		/** a label (nice name) for the batch, for use in UI elements */
		public final String label;
		
		/** a somewhat more detailed description of the batch, for use in UI tooltips (can contain HTML) */
		public final String description;
		
		/**
		 * @param name the name of the batch
		 * @param label the label of the batch
		 * @param description the description of the batch
		 */
		ImpBatchDescriptor(String name, String label, String description) {
			this.name = name;
			this.label = label;
			this.description = description;
		}
		
		/**
		 * Write the batch descriptor to a given writer as a JSON object. This
		 * method creates an object with the three properties of this class.
		 * @param out the writer to write to
		 * @throws IOException
		 */
		public void writeJson(Writer out) throws IOException {
			out.write("{");
			if (this.description != null)
				out.write("\"description\": \"" + JsonParser.escape(this.description) + "\",");
			out.write("\"label\": \"" + JsonParser.escape(this.label) + "\",");
			out.write("\"name\": \"" + JsonParser.escape(this.name) + "\"");
			out.write("}");
		}
		
		/**
		 * Instantiate a batch descriptor from its JSON representation, as
		 * output by the <code>writeJson()</code> method. If the argument
		 * object lacks either of the <code>name</code> or <code>label</code>
		 * property, this method returns null;
		 * @param ibdObject the JSON object containing the data
		 * @return the batch descriptor
		 */
		public static ImpBatchDescriptor readJson(Map ibdObject) {
			String name = JsonParser.getString(ibdObject, "name");
			String label = JsonParser.getString(ibdObject, "label");
			return (((name == null) || (label == null)) ? null : new ImpBatchDescriptor(name, label, JsonParser.getString(ibdObject, "description")));
		}
		
		/**
		 * Read an array of batch descriptors from its JSON representation.
		 * This method expects a JSON encoded list of batch descriptor objects.
		 * @param in the reader to read from
		 * @return an array holding the batch descriptors
		 * @throws IOException
		 */
		public static ImpBatchDescriptor[] readJson(Reader in) throws IOException {
			Object ibdsObj = JsonParser.parseJson(in);
			if (ibdsObj instanceof List) {}
			else if (ibdsObj instanceof Map) {
				ImpBatchDescriptor ibd = readJson((Map) ibdsObj);
				if (ibd == null)
					return null;
				ImpBatchDescriptor[] ibds = {ibd};
				return ibds;
			}
			else return null;
			List ibdsArray = ((List) ibdsObj);
			List ibdsList = new ArrayList(ibdsArray.size());
			for (int d = 0; d < ibdsArray.size(); d++) {
				Map ibdObject = JsonParser.getObject(ibdsArray, d);
				if (ibdObject == null)
					continue;
				ImpBatchDescriptor ibd = readJson(ibdObject);
				if (ibd != null)
					ibdsList.add(ibd);
			}
			return ((ImpBatchDescriptor[]) ibdsList.toArray(new ImpBatchDescriptor[ibdsList.size()]));
		}
	}
}
