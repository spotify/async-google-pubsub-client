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

import java.io.IOException;
import java.io.OutputStream;

import io.vertx.core.buffer.Buffer;

import static java.util.Objects.requireNonNull;

class BufferOutputStream extends OutputStream {

  private final Buffer buffer;

  BufferOutputStream(final Buffer buffer) {
    this.buffer = requireNonNull(buffer, "buffer");
  }

  @Override
  public void write(final int b) throws IOException {
    buffer.appendByte((byte) b);
  }
}
