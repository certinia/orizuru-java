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

package com.financialforce.orizuru.message;

import java.nio.ByteBuffer;

import org.apache.avro.Schema;

import com.financialforce.orizuru.exception.consumer.OrizuruConsumerException;
import com.financialforce.orizuru.exception.consumer.decode.DecodeContextException;
import com.financialforce.orizuru.transport.Transport;

/**
 * Wraps the context part of the FinancialForce Orizuru Avro Transport schema.
 */
public class Context extends Message {

	/**
	 * Constructs a new empty Avro context.
	 */
	public Context() {
		super();
	}

	/**
	 * Decode the context from the transport.
	 * 
	 * @param input The FinancialForce Orizuru Avro Transport message from which to decode the context.
	 * @throws OrizuruConsumerException Exception thrown if decoding the context fails.
	 */
	@Override
	public void decodeFromTransport(Transport input) throws OrizuruConsumerException {

		try {

			String contextSchemaStr = input.getContextSchema().toString();
			Schema.Parser parser = new Schema.Parser();
			this.schema = parser.parse(contextSchemaStr);

			ByteBuffer contextBuffer = input.getContextBuffer();
			this.data = contextBuffer.array();

		} catch (Exception ex) {
			throw new DecodeContextException(ex);
		}

	}

}
