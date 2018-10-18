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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.financialforce.orizuru.exception.publisher.OrizuruPublisherException;
import com.financialforce.orizuru.exception.publisher.encode.EncodeMessageContentException;
import com.financialforce.orizuru.exception.publisher.encode.EncodeTransportException;
import com.financialforce.orizuru.message.Context;

public class AbstractPublisherTest {

	private static final String QUEUE_NAME = "testQueue";

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	private Publisher publisher;
	private Schema schema;

	@Before
	public void doBefore() {

		schema = SchemaBuilder.record("TestSchema").namespace("com.financialforce.orizuru.AbstractConsumerTest")
				.fields().name("testString").type().stringType().noDefault().endRecord();

		publisher = new Publisher();
	}

	@Test
	public void getQueueName_shouldReturnTheQueueName() {

		// when
		String queueName = publisher.getQueueName();

		// then
		assertTrue(queueName.equals(QUEUE_NAME));
	}

	@Test
	public void publish_shouldThrowAnOrizuruPublisherExceptionForANullMessage() throws Exception {

		// expect
		exception.expect(OrizuruPublisherException.class);
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(NullPointerException.class));
		exception.expectMessage("Failed to publish message");

		// when
		publisher.publish(null, null);

	}

	@Test
	public void publish_shouldThrowAnEncodeTransportExceptionForAnInvalidTransportMessage() throws Exception {

		// expect
		exception.expect(EncodeMessageContentException.class);
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(ClassCastException.class));
		exception.expectMessage("Failed to publish message: Failed to encode message content");

		// given
		Context context = mock(Context.class);
		when(context.getSchema())
				.thenReturn(new Schema.Parser().parse("{\"name\":\"test\",\"type\":\"record\",\"fields\":[]}"));
		when(context.getDataBuffer()).thenReturn(ByteBuffer.wrap("{}".getBytes()));

		GenericRecordBuilder builder = new GenericRecordBuilder(schema);
		builder.set("testString", Boolean.TRUE);
		Record record = builder.build();

		// when
		publisher.publish(context, record);

	}

	@Test
	public void publish_shouldThrowAnEncodeTransportExceptionForAnInvalidTransportContext() throws Exception {

		// expect
		exception.expect(EncodeTransportException.class);
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(NullPointerException.class));
		exception.expectMessage("Failed to publish message: Failed to encode transport");

		// given
		Context context = mock(Context.class);
		when(context.getSchema())
				.thenReturn(new Schema.Parser().parse("{\"name\":\"test\",\"type\":\"record\",\"fields\":[]}"));
		when(context.getDataBuffer()).thenReturn(null);

		GenericRecordBuilder builder = new GenericRecordBuilder(schema);
		builder.set("testString", "testString");
		Record record = builder.build();

		// when
		publisher.publish(context, record);

	}

	@Test
	public void publish_shouldPublishTheMessage() throws Exception {

		// given
		Context context = mock(Context.class);
		when(context.getSchema())
				.thenReturn(new Schema.Parser().parse("{\"name\":\"test\",\"type\":\"record\",\"fields\":[]}"));
		when(context.getDataBuffer()).thenReturn(ByteBuffer.wrap("{}".getBytes()));

		GenericRecordBuilder builder = new GenericRecordBuilder(schema);
		builder.set("testString", "test");
		Record record = builder.build();

		// when
		publisher.publish(context, record);

	}

	private class Publisher extends AbstractPublisher<GenericContainer> {

		public Publisher() {
			super(QUEUE_NAME);
		}

	}
}
