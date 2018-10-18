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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Base64;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.financialforce.orizuru.exception.consumer.decode.DecodeMessageContentException;
import com.financialforce.orizuru.exception.consumer.decode.DecodeMessageException;
import com.financialforce.orizuru.exception.publisher.encode.EncodeMessageContentException;
import com.financialforce.orizuru.transport.Transport;
import com.financialforce.orizuru.util.TestMessage;

public class MessageTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void decode_shouldThrowADecodeMessageContentExceptionIfTheSchemaIsNull() throws Exception {

		// expect
		exception.expect(DecodeMessageContentException.class);
		exception.expectMessage("Failed to consume message: Failed to decode message content");
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(NullPointerException.class));

		// given
		Message message = new Message();

		// when
		message.decode();

	}

	@Test
	public void decode_shouldDecodeTheMessageContent() throws Exception {

		// given
		CharSequence expectedName = "testName";

		TestMessage testMessage = new TestMessage();
		Schema schema = testMessage.getSchema();

		byte[] data = Base64.getDecoder().decode("EHRlc3ROYW1l");

		Message message = new Message(schema, data);

		// when
		TestMessage avroMessage = message.decode();

		// then
		assertEquals(expectedName, avroMessage.getName().toString());

	}

	@Test
	public void decodeFromTransport_shouldThrowADecodeMessageExceptionIfTheTransportIsNull() throws Exception {

		// expect
		exception.expect(DecodeMessageException.class);
		exception.expectMessage("Failed to consume message: Failed to decode message");
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(NullPointerException.class));

		// given
		Message message = new Message();

		// when
		message.decodeFromTransport(null);

	}

	@Test
	public void decodeFromTransport_shouldThrowADecodeMessageExceptionWithASchemaParseExceptionIfTheSchemaIsInvalid()
			throws Exception {

		// expect
		exception.expect(DecodeMessageException.class);
		exception.expectMessage("Failed to consume message: Failed to decode message");
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(SchemaParseException.class));

		// given
		Transport transport = new Transport();
		transport.setMessageSchema("invalid");

		Message message = new Message();

		// when
		message.decodeFromTransport(transport);

	}

	@Test
	public void decodeFromTransport_shouldDecodeTheMessage() throws Exception {

		// given
		ByteBuffer expectedMessage = ByteBuffer.wrap("{\"name\":\"testName\"}".getBytes());

		String schema = "{\"type\":\"record\",\"name\":\"TestSchema\",\"namespace\":\"com.financialforce.orizuru.AbstractConsumerTest\",\"fields\":[{\"name\":\"testString\",\"type\":\"string\"}]}";
		
		Transport transport = new Transport();
		transport.setMessageSchema(schema);
		transport.setMessageBuffer(expectedMessage);

		Message message = new Message();

		// when
		message.decodeFromTransport(transport);

		// then
		assertEquals(expectedMessage, message.getDataBuffer());

	}

	@Test
	public void encode_shouldThrowAnEncodeMessageContentExceptionIfTheMessageDataIsNull() throws Exception {

		// expect
		exception.expect(EncodeMessageContentException.class);
		exception.expectMessage("Failed to publish message: Failed to encode message content");
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(NullPointerException.class));

		// given
		Message message = new Message();

		// when
		message.encode(null);

	}

	@Test
	public void encode_shouldCorrectlyEncodeAnAvroObject() throws Exception {

		// given
		String expectedData = "EHRlc3ROYW1l";

		TestMessage testMessage = new TestMessage();

		GenericRecordBuilder builder = new GenericRecordBuilder(testMessage.getSchema());
		builder.set("name", "testName");
		Record record = builder.build();

		Message message = new Message();

		// when
		message.encode(record);

		// then
		assertEquals(expectedData, Base64.getEncoder().encodeToString(message.getData()));

	}

	@Test
	public void getSchema_shouldReturnTheMessageSchema() throws Exception {

		// given
		TestMessage testMessage = new TestMessage();
		Schema schema = testMessage.getSchema();

		Message message = new Message(schema, null);

		// when/then
		assertEquals(schema, message.getSchema());

	}

}
