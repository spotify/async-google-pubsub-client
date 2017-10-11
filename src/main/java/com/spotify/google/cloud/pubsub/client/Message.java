/*-
 * -\-\-
 * async-google-pubsub-client
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

/*
 * Copyright (c) 2011-2015 Spotify AB
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

package com.spotify.google.cloud.pubsub.client;

import com.google.common.base.CharMatcher;
import com.google.common.io.BaseEncoding;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import io.norberg.automatter.AutoMatter;

import static java.nio.charset.StandardCharsets.UTF_8;

@AutoMatter
public interface Message {

  CharMatcher BASE64_MATCHER = CharMatcher.anyOf("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=");

  String data();

  Map<String, String> attributes();

  Optional<String> messageId();

  Optional<Instant> publishTime();

  static MessageBuilder builder() {
    return new MessageBuilder();
  }

  static Message of(final String data) {
    return builder().data(data).build();
  }

  static Message ofEncoded(final CharSequence data) {
    return of(encode(data));
  }

  static String encode(final CharSequence data) {
    return encode(CharBuffer.wrap(data));
  }

  static String encode(final CharSequence data, final int start, final int end) {
    return encode(CharBuffer.wrap(data, start, end));
  }

  static String encode(final CharBuffer data) {
    return encode(UTF_8.encode(data));
  }

  static String encode(final char[] data) {
    return encode(UTF_8.encode(CharBuffer.wrap(data)));
  }

  static String encode(final ByteBuffer data) {
    if (data.hasArray()) {
      return encode(data.array(), data.arrayOffset(), data.arrayOffset() + data.remaining());
    }
    final byte[] bytes = new byte[data.remaining()];
    final int mark = data.position();
    data.get(bytes);
    data.position(mark);
    return encode(bytes);
  }

  static String encode(final byte[] data, final int offset, final int length) {
    if (offset == 0 && data.length == length) {
      return encode(data);
    }
    return BaseEncoding.base64().encode(data, offset, length);
  }

  static String encode(final byte[] data) {
    return Base64.getEncoder().encodeToString(data);
  }

  default byte[] decodedData() {
    return Base64.getDecoder().decode(data());
  }

  default CharSequence decodedDataUTF8() {
    return UTF_8.decode(ByteBuffer.wrap(decodedData()));
  }

  static boolean isEncoded(Message message) {
    return BASE64_MATCHER.matchesAllOf(message.data());
  }
}
