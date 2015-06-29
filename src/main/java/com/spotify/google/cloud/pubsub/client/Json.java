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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import java.io.IOException;

import io.norberg.automatter.jackson.AutoMatterModule;
import io.vertx.core.buffer.Buffer;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;

class Json {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .setSerializationInclusion(NON_EMPTY)
      .registerModule(new AutoMatterModule())
      .registerModule(new Jdk8Module());

  private static final ObjectReader READER = MAPPER.reader();

  private static final ObjectWriter WRITER = MAPPER.writer();

  static ObjectReader reader() {
    return READER;
  }

  static ObjectWriter writer() {
    return WRITER;
  }

  static <T> T read(final Buffer buffer, final Class<T> cls) throws IOException {
    return MAPPER.readValue(new BufferInputStream(buffer), cls);
  }
}
