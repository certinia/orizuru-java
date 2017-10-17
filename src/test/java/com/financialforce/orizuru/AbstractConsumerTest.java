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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;

import org.apache.avro.generic.GenericContainer;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.financialforce.orizuru.exception.OrizuruException;
import com.financialforce.orizuru.exception.consumer.OrizuruConsumerException;
import com.financialforce.orizuru.exception.consumer.decode.DecodeContextException;
import com.financialforce.orizuru.exception.consumer.decode.DecodeTransportException;
import com.financialforce.orizuru.exception.consumer.handler.HandleMessageException;
import com.financialforce.orizuru.interfaces.IConsumer;
import com.financialforce.orizuru.interfaces.IPublisher;
import com.financialforce.orizuru.message.Context;

public class AbstractConsumerTest {

	private static final String QUEUE_NAME = "testQueue";

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void getQueueName_shouldReturnTheQueueName() {

		// given
		IConsumer consumer = new Consumer(QUEUE_NAME, null);

		// when
		String queueName = consumer.getQueueName();

		// then
		assertTrue(queueName.equals(QUEUE_NAME));
	}

	@Test
	public void getOutgoingMessagePublisher_shouldReturnNull() {

		// given
		Consumer consumer = new Consumer(QUEUE_NAME, null);

		// when
		IPublisher<GenericContainer> publisher = consumer.getOutgoingMessagePublisher();

		// then
		assertNull(publisher);

	}

	@Test
	public void consume_callsThePublishMethodIfAPublisherIsDefined() throws Exception {

		// given
		IPublisher<GenericContainer> publisher = mock(IPublisher.class);
		IConsumer consumer = new Consumer(QUEUE_NAME, publisher);

		byte[] body = Base64.getDecoder().decode(getFileContents("validTransport.txt"));

		// when
		consumer.consume(body);

		// then
		verify(publisher, times(1)).publish(any(), any());

	}

	@Test
	public void consume_shouldDecodeTheTransport() throws Exception {

		// given
		byte[] body = Base64.getDecoder().decode(getFileContents("validTransport.txt"));

		IConsumer consumer = new Consumer(QUEUE_NAME, null);

		// when
		byte[] outgoingMessage = consumer.consume(body);

		// then
		assertNull(outgoingMessage);

	}

	@Test
	public void consume_throwsDecodeTransportExceptionForNullBody() throws OrizuruException {

		// given
		IConsumer consumer = new Consumer(QUEUE_NAME, null);

		// expect
		exception.expect(OrizuruConsumerException.class);
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(DecodeTransportException.class));

		// when
		consumer.consume(null);

	}

	@Test
	public void consume_throwsDecodeContextExceptionForInvalidContextSchema() throws Exception {

		// given
		byte[] body = Base64.getDecoder().decode(getFileContents("invalidContextSchema.txt"));

		IConsumer consumer = new Consumer(QUEUE_NAME, null);

		// expect
		exception.expect(OrizuruConsumerException.class);
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(DecodeContextException.class));

		// when
		consumer.consume(body);

	}

	@Test
	public void consume_throwsAHandleMessageExceptionForAnInvalidMessage() throws Exception {

		// given
		byte[] body = Base64.getDecoder().decode(getFileContents("validTransport.txt"));

		IConsumer consumer = new ErrorConsumer(QUEUE_NAME, null);

		// expect
		exception.expect(OrizuruConsumerException.class);
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(HandleMessageException.class));

		// when
		consumer.consume(body);

	}
	
	@Test
	public void consume_throwsAHandleMessageExceptionWithAnAlteredInvalidMessage() throws Exception {

		// given
		byte[] body = Base64.getDecoder().decode(getFileContents("validTransport.txt"));

		IConsumer consumer = new ErrorConsumer2(QUEUE_NAME, null);

		// expect
		exception.expect(OrizuruConsumerException.class);
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(HandleMessageException.class));

		// when
		consumer.consume(body);

	}
	
	private byte[] getFileContents(String fileName) throws IOException {

		ByteArrayOutputStream output = null;

		try {

			InputStream input = getClass().getResourceAsStream(fileName);

			output = new ByteArrayOutputStream();

			byte[] buffer = new byte[8192];
			int n = 0;
			while (-1 != (n = input.read(buffer))) {
				output.write(buffer, 0, n);
			}

			return output.toByteArray();

		} catch (IOException ioe) {
			if (output != null) {
				output.close();
			}
		}
		
		return null;
	}

	private class Consumer extends AbstractConsumer<GenericContainer, GenericContainer> {

		public Consumer(String queueName, IPublisher<GenericContainer> publisher) {
			super(queueName, publisher);
		}

		@Override
		public GenericContainer handleMessage(Context context, GenericContainer input) throws HandleMessageException {
			return null;
		}

	}

	private class ErrorConsumer extends AbstractConsumer<GenericContainer, GenericContainer> {

		public ErrorConsumer(String queueName, IPublisher<GenericContainer> publisher) {
			super(queueName, publisher);
		}

		@Override
		public GenericContainer handleMessage(Context context, GenericContainer input) throws HandleMessageException {
			throw new HandleMessageException(new NullPointerException());
		}

	}
	
	private class ErrorConsumer2 extends AbstractConsumer<GenericContainer, GenericContainer> {

		public ErrorConsumer2(String queueName, IPublisher<GenericContainer> publisher) {
			super(queueName, publisher);
		}

		@Override
		public GenericContainer handleMessage(Context context, GenericContainer input) throws HandleMessageException {
			throw new HandleMessageException("test", new NullPointerException());
		}

	}

}
