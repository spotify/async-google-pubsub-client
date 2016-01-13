/*
 * Copyright (c) 2011-2016 Spotify AB
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

import com.google.api.client.repackaged.com.google.common.base.Throwables;

import com.ning.http.client.AsyncHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;

import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_ENCODING;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.GZIP;

public class AsyncHttpClientTransport implements Transport {

  private static final String VERSION = "1.0.0";
  private static final String USER_AGENT =
      "Spotify-Google-Pubsub-Java-Client/" + VERSION + " (gzip)";
  private static final String APPLICATION_JSON_UTF8 = "application/json; charset=UTF-8";

  private final AsyncHttpClient client;
  private final String baseUri;
  private final Supplier<String> accessToken;
  private final int compressionLevel;

  public AsyncHttpClientTransport(final AsyncHttpClient client, final String baseUri,
                                  final Supplier<String> accessToken, final int compressionLevel) {
    this.client = client;
    this.baseUri = baseUri;
    this.accessToken = accessToken;
    this.compressionLevel = compressionLevel;
  }

  @Override
  public <T> PubsubFuture<T> request(final String operation, final HttpMethod method, final String path,
                                     final Class<T> responseClass, final Object payload) {
    final String uri = baseUri + path;
    final RequestBuilder builder = new RequestBuilder()
        .setUrl(uri)
        .setMethod(method.toString())
        .setHeader("Authorization", "Bearer " + accessToken.get())
        .setHeader("User-Agent", USER_AGENT);

    final long payloadSize;
    if (payload != NO_PAYLOAD) {
      final byte[] json = gzipJson(payload);
      payloadSize = json.length;
      builder.setHeader(CONTENT_ENCODING, GZIP);
      builder.setHeader(CONTENT_LENGTH, String.valueOf(json.length));
      builder.setHeader(CONTENT_TYPE, APPLICATION_JSON_UTF8);
      builder.setBody(json);
    } else {
      payloadSize = 0;
    }

    final Request request = builder.build();

    final PubsubFuture<T> future = new PubsubFuture<>(operation, method.toString(), uri, payloadSize);
    client.executeRequest(request, new AsyncHandler<Void>() {
      private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

      @Override
      public void onThrowable(final Throwable t) {
        future.fail(t);
      }

      @Override
      public STATE onBodyPartReceived(final HttpResponseBodyPart bodyPart) throws Exception {
        bytes.write(bodyPart.getBodyPartBytes());
        return STATE.CONTINUE;
      }

      @Override
      public STATE onStatusReceived(final HttpResponseStatus status) throws Exception {

        // Return null for 404'd GET & DELETE requests
        if (status.getStatusCode() == 404 && method == HttpMethod.GET || method == HttpMethod.DELETE) {
          future.succeed(null);
          return STATE.ABORT;
        }

        // Fail on non-2xx responses
        final int statusCode = status.getStatusCode();
        if (!(statusCode >= 200 && statusCode < 300)) {
          future.fail(new RequestFailedException(status.getStatusCode(), status.getStatusText()));
          return STATE.ABORT;
        }

        if (responseClass == Void.class) {
          future.succeed(null);
          return STATE.ABORT;
        }

        return STATE.CONTINUE;
      }

      @Override
      public STATE onHeadersReceived(final HttpResponseHeaders headers) throws Exception {
        return STATE.CONTINUE;
      }

      @Override
      public Void onCompleted() throws Exception {
        if (future.isDone()) {
          return null;
        }
        try {
          future.succeed(Json.read(bytes.toByteArray(), responseClass));
        } catch (IOException e) {
          future.fail(e);
        }
        return null;
      }
    });

    return future;

  }

  private byte[] gzipJson(final Object payload) {
    // TODO (dano): cache and reuse deflater
    try (
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final GZIPOutputStream gzip = new GZIPOutputStream(bytes) {
          {
            this.def.setLevel(compressionLevel);
          }
        }
    ) {
      Json.write(gzip, payload);
      return bytes.toByteArray();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
