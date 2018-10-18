/**
 * Copyright (c) 2017-2018, FinancialForce.com, inc
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 *   are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *      this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice,
 *      this list of conditions and the following disclaimer in the documentation
 *      and/or other materials provided with the distribution.
 * - Neither the name of the FinancialForce.com, inc nor the names of its contributors
 *      may be used to endorse or promote products derived from this software without
 *      specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 *  OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 *  THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 *  OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 *  OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

package com.financialforce.orizuru;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import com.financialforce.orizuru.exception.publisher.OrizuruPublisherException;
import com.financialforce.orizuru.exception.publisher.encode.EncodeTransportException;
import com.financialforce.orizuru.interfaces.IPublisher;
import com.financialforce.orizuru.message.Context;
import com.financialforce.orizuru.message.Message;
import com.financialforce.orizuru.transport.Transport;

/**
 * Sends a message containing a FinancialForce Orizuru Avro Transport schema.
 * <p>
 * The Transport schema is in the form:
 * 
 * <pre>
 * <code>
 * {
 *     "namespace": "com.financialforce.orizuru",
 *     "name": "Transport",
 *     "type": "record",
 *     "fields": [
 * 	       { "name": "contextSchema", "type": "string" },
 * 		   { "name": "contextBuffer", "type": "bytes" },
 * 		   { "name": "messageSchema", "type": "string" },
 * 		   { "name": "messageBuffer", "type": "bytes" }
 * 	    ]
 * }
 * </code>
 * </pre>
 */
public abstract class AbstractPublisher<O extends GenericContainer> implements IPublisher<O> {

	protected String queueName;

	public AbstractPublisher(String queueName) {
		this.queueName = queueName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.financialforce.orizuru.interfaces.IPublisher#publish(com.financialforce.
	 * orizuru.message.Context, org.apache.avro.generic.GenericContainer)
	 */
	@Override
	public byte[] publish(Context context, O message) throws OrizuruPublisherException {

		try {

			CharSequence contextSchema = context.getSchema().toString();
			ByteBuffer contextBuffer = context.getDataBuffer();

			Message outgoingMessage = new Message();
			outgoingMessage.encode(message);

			CharSequence messageSchema = outgoingMessage.getSchema().toString();
			ByteBuffer messageBuffer = outgoingMessage.getDataBuffer();

			return writeTransport(contextSchema, contextBuffer, messageSchema, messageBuffer);

		} catch (OrizuruPublisherException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new OrizuruPublisherException(ex);
		}

	}

	@Override
	public String getQueueName() {
		return queueName;
	}

	// private methods

	private byte[] writeTransport(CharSequence contextSchema, ByteBuffer contextBuffer, CharSequence messageSchema,
			ByteBuffer messageBuffer) throws EncodeTransportException {

		try {

			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			DatumWriter<Transport> transportDatumWriter = new SpecificDatumWriter<Transport>(Transport.class);
			BinaryEncoder transportEncoder = EncoderFactory.get().binaryEncoder(baos, null);

			Transport transport = new Transport(contextSchema, contextBuffer, messageSchema, messageBuffer);
			transportDatumWriter.write(transport, transportEncoder);
			transportEncoder.flush();

			return baos.toByteArray();

		} catch (Exception ex) {
			throw new EncodeTransportException(ex);
		}

	}

}
