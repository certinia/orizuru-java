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

package com.financialforce.orizuru.message;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.financialforce.orizuru.exception.consumer.OrizuruConsumerException;
import com.financialforce.orizuru.exception.consumer.decode.DecodeMessageContentException;
import com.financialforce.orizuru.exception.consumer.decode.DecodeMessageException;
import com.financialforce.orizuru.exception.publisher.encode.EncodeMessageContentException;
import com.financialforce.orizuru.transport.Transport;

/**
 * Wraps the message part of the FinancialForce Orizuru Avro Transport schema.
 */
public class Message {

	protected Schema schema;
	protected byte[] data;

	/**
	 * Constructs a new empty Avro message.
	 */
	public Message() {
	}

	/**
	 * Constructs an Avro message containing the schema and the message data.
	 * 
	 * @param schema The FinancialForce Orizuru Avro Message schema.
	 * @param data The FinancialForce Orizuru Avro Message data.
	 */
	public Message(Schema schema, byte[] data) {
		this.schema = schema;
		this.data = data;
	}

	/**
	 * Encode the message data provided.
	 * 
	 * @param <O> The type of the data to encode.
	 * @param data The message data.
	 * @throws EncodeMessageContentException Exception thrown if encoding the message content fails.
	 */
	public <O extends GenericContainer> void encode(O data) throws EncodeMessageContentException {

		try {

			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			this.schema = data.getSchema();
			DatumWriter<O> outputDatumWriter = new SpecificDatumWriter<O>(this.schema);
			BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
			outputDatumWriter.write(data, encoder);
			encoder.flush();

			this.data = baos.toByteArray();

		} catch (Exception ex) {
			throw new EncodeMessageContentException(ex);
		}

	}

	/**
	 * Decode the message from the transport.
	 * 
	 * @param input The FinancialForce Orizuru Avro Transport message from which to decode the message.
	 * @throws OrizuruConsumerException Exception thrown if decoding the message fails.
	 */
	public void decodeFromTransport(Transport input) throws OrizuruConsumerException {

		try {

			String messageSchemaName = input.getMessageSchemaName().toString();

			Class<?> avroClass = Class.forName(messageSchemaName);
			Constructor<?> constructor = avroClass.getConstructor();
			GenericContainer container = (GenericContainer) constructor.newInstance();
			this.schema = container.getSchema();

			ByteBuffer messageBuffer = input.getMessageBuffer();
			this.data = messageBuffer.array();

		} catch (Exception ex) {
			throw new DecodeMessageException(ex);
		}

	}

	/**
	 * Decode the message content.
	 * 
	 * @param <I> The type of the data that is decoded.
	 * @return The message data.
	 * @throws DecodeMessageContentException Exception thrown if decoding the message content fails.
	 */
	public <I extends GenericContainer> I decode() throws DecodeMessageContentException {

		try {

			DatumReader<I> messageDatumReader = new SpecificDatumReader<I>(schema);
			BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
			return messageDatumReader.read(null, decoder);

		} catch (Exception ex) {
			throw new DecodeMessageContentException(ex);
		}

	}

	/**
	 * @return the schema
	 */
	public Schema getSchema() {
		return schema;
	}

	/**
	 * @return the data
	 */
	public byte[] getData() {
		return data;
	}

	/**
	 * @return the schema name
	 */
	public CharSequence getSchemaName() {
		return schema.getFullName();
	}

	/**
	 * @return the data as a bytebuffer
	 */
	public ByteBuffer getDataBuffer() {
		return ByteBuffer.wrap(data);
	}

}
