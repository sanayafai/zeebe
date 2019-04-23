/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.msgpack.spec;

import static io.zeebe.msgpack.spec.MsgPackCodes.ARRAY32;
import static io.zeebe.msgpack.spec.MsgPackCodes.BIN32;
import static io.zeebe.msgpack.spec.MsgPackCodes.FIXSTR_PREFIX;
import static io.zeebe.msgpack.spec.MsgPackCodes.MAP32;
import static io.zeebe.msgpack.spec.MsgPackCodes.STR32;
import static io.zeebe.msgpack.spec.MsgPackCodes.UINT64;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.matchesPattern;

import java.util.Arrays;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class MsgPackReadingBoundaryCheckingExceptionTest {

  protected static final Matcher<String> NEGATIVE_BUF_SIZE_EXCEPTION_MSG =
      containsString(
          "Negative value should not be accepted by size value and unsigned 64bit integer");

  protected static final Matcher<String> WRAPPING_PAST_BUFFER_LIMIT =
      matchesPattern("Reading \\d+ bytes past buffer capacity\\(\\d+\\) in range \\[\\d+:\\d+\\)");

  @Rule public ExpectedException exception = ExpectedException.none();

  @Parameters(name = "{0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {
            new byte[] {(byte) ARRAY32, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff},
            codeUnderTest((r) -> r.readArrayHeader()),
            NEGATIVE_BUF_SIZE_EXCEPTION_MSG
          },
          {
            new byte[] {(byte) BIN32, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff},
            codeUnderTest((r) -> r.readBinaryLength()),
            NEGATIVE_BUF_SIZE_EXCEPTION_MSG
          },
          {
            new byte[] {(byte) MAP32, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff},
            codeUnderTest((r) -> r.readMapHeader()),
            NEGATIVE_BUF_SIZE_EXCEPTION_MSG
          },
          {
            new byte[] {(byte) STR32, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff},
            codeUnderTest((r) -> r.readStringLength()),
            NEGATIVE_BUF_SIZE_EXCEPTION_MSG
          },
          {
            new byte[] {(byte) FIXSTR_PREFIX | (byte) 0x01},
            codeUnderTest((r) -> r.readToken()),
            WRAPPING_PAST_BUFFER_LIMIT
          },
          {
            new byte[] {(byte) FIXSTR_PREFIX | (byte) 0x10, (byte) 0x00},
            codeUnderTest((r) -> r.readToken()),
            WRAPPING_PAST_BUFFER_LIMIT
          },
          {
            new byte[] {
              (byte) UINT64,
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff
            },
            codeUnderTest((r) -> r.readInteger()),
            NEGATIVE_BUF_SIZE_EXCEPTION_MSG
          }
        });
  }

  @Parameter(0)
  public byte[] testingBuf;

  @Parameter(1)
  public Consumer<MsgPackReader> codeUnderTest;

  @Parameter(2)
  public Matcher<String> exceptionMessage;

  protected MsgPackReader reader;

  @Before
  public void setUp() {
    reader = new MsgPackReader();
  }

  @Test
  public void shouldNotReadNegativeValue() {
    // given
    final DirectBuffer negativeTestingBuf = new UnsafeBuffer(testingBuf);
    reader.wrap(negativeTestingBuf, 0, negativeTestingBuf.capacity());

    // then
    exception.expect(MsgpackReaderException.class);
    exception.expectMessage(exceptionMessage);

    // when
    codeUnderTest.accept(reader);
  }

  protected static Consumer<MsgPackReader> codeUnderTest(Consumer<MsgPackReader> arg) {
    return arg;
  }
}
