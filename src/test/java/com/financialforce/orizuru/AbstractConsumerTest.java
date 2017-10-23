/**
 * Copyright (c) 2017, FinancialForce.com, inc
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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.financialforce.orizuru.exception.OrizuruException;
import com.financialforce.orizuru.exception.consumer.decode.DecodeTransportException;
import com.financialforce.orizuru.exception.consumer.handler.HandleMessageException;
import com.financialforce.orizuru.interfaces.IConsumer;
import com.financialforce.orizuru.interfaces.IPublisher;
import com.financialforce.orizuru.message.Context;

public class AbstractConsumerTest {

	private static final String VALID_MESSAGE = "V{\"name\":\"test\",\"type\":\"record\",\"fields\":[]}{}tcom.financialforce.orizuru.AbstractConsumerTest$TestSchematestData";

	private static final String QUEUE_NAME = "testQueue";

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void getQueueName_shouldReturnTheQueueName() {

		// given
		IConsumer consumer = new Consumer(QUEUE_NAME);

		// when
		String queueName = consumer.getQueueName();

		// then
		assertTrue(queueName.equals(QUEUE_NAME));
	}

	@Test
	public void consume_callsThePublishMethodIfAPublisherIsDefined() throws Exception {

		// given
		IPublisher<GenericContainer> publisher = mock(IPublisher.class);
		Consumer consumer = new Consumer(QUEUE_NAME);
		consumer.setPublisher(publisher);

		byte[] body = VALID_MESSAGE.getBytes();

		// when
		consumer.consume(body);

		// then
		verify(publisher, times(1)).publish(any(), any());

	}

	@Test
	public void consume_shouldDecodeTheTransport() throws Exception {

		// given
		byte[] body = VALID_MESSAGE.getBytes();

		IConsumer consumer = new Consumer(QUEUE_NAME);

		// when
		byte[] outgoingMessage = consumer.consume(body);

		// then
		assertNull(outgoingMessage);

	}

	@Test
	public void consume_throwsDecodeTransportExceptionForNullBody() throws OrizuruException {

		// expect
		exception.expect(DecodeTransportException.class);
		exception.expectMessage("Failed to consume message: Failed to decode transport");
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(NullPointerException.class));

		// given
		IConsumer consumer = new Consumer(QUEUE_NAME);

		// when
		consumer.consume(null);

	}

	@Test
	public void consume_throwsAHandleMessageExceptionForAnInvalidMessage() throws Exception {

		// expect
		exception.expect(HandleMessageException.class);
		exception.expectMessage("Failed to consume message: Failed to handle message");
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(NullPointerException.class));

		// given
		byte[] body = VALID_MESSAGE.getBytes();

		IConsumer consumer = new ErrorConsumer(QUEUE_NAME);

		// when
		consumer.consume(body);

	}

	@Test
	public void consume_throwsAHandleMessageExceptionWithAnAlteredInvalidMessage() throws Exception {

		// expect
		exception.expect(HandleMessageException.class);
		exception.expectMessage("Failed to consume message: Failed to handle message: test");
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(NullPointerException.class));

		// given
		byte[] body = VALID_MESSAGE.getBytes();

		IConsumer consumer = new ErrorConsumer2(QUEUE_NAME);

		// when
		consumer.consume(body);

	}

	public static class TestSchema implements GenericContainer {

		public TestSchema() {
		}

		@Override
		public Schema getSchema() {
			return new Schema.Parser().parse(
					"{\"type\":\"record\",\"name\":\"TestSchema\",\"namespace\":\"com.financialforce.orizuru.AbstractConsumerTest\",\"fields\":[]}");
		}

	}

	private class Consumer extends AbstractConsumer<GenericContainer, GenericContainer> {

		public Consumer(String queueName) {
			super(queueName);
		}

		@Override
		public GenericContainer handleMessage(Context context, GenericContainer input) throws HandleMessageException {
			return null;
		}

		public void setPublisher(IPublisher<GenericContainer> publisher) {
			this.publisher = publisher;
		}

	}

	private class ErrorConsumer extends AbstractConsumer<GenericContainer, GenericContainer> {

		public ErrorConsumer(String queueName) {
			super(queueName);
		}

		@Override
		public GenericContainer handleMessage(Context context, GenericContainer input) throws HandleMessageException {
			throw new HandleMessageException(new NullPointerException());
		}

	}

	private class ErrorConsumer2 extends AbstractConsumer<GenericContainer, GenericContainer> {

		public ErrorConsumer2(String queueName) {
			super(queueName);
		}

		@Override
		public GenericContainer handleMessage(Context context, GenericContainer input) throws HandleMessageException {
			throw new HandleMessageException("test", new NullPointerException());
		}

	}

}
