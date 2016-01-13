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

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.repackaged.com.google.common.base.Throwables;

import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;

import static com.google.common.util.concurrent.MoreExecutors.getExitingExecutorService;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.GZIP;

class HttpUrlConnectionTransport implements Transport {

  private static final String VERSION = "1.0.0";
  private static final String USER_AGENT = "Spotify-Google-Pubsub-Java-Client/" + VERSION + " (gzip)";
  private static final String APPLICATION_JSON_UTF8 = "application/json; charset=UTF-8";

  private final String baseUri;
  private final Supplier<String> accessToken;
  private final int compressionLevel;

  private final ExecutorService executor = getExitingExecutorService(
      (ThreadPoolExecutor) Executors.newCachedThreadPool());

  HttpUrlConnectionTransport(final String baseUri, final Supplier<String> accessToken,
                                    final int compressionLevel) {
    this.baseUri = baseUri;
    this.accessToken = accessToken;
    this.compressionLevel = compressionLevel;
  }

  @Override
  public <T> PubsubFuture<T> request(final String operation, final HttpMethod method, final String path,
                                     final Class<T> responseClass, final Object payload) {
    final NetHttpTransport transport = new NetHttpTransport();
    final HttpRequestFactory requestFactory = transport.createRequestFactory();

    final String uri = baseUri + path;

    final HttpHeaders headers = new HttpHeaders();
    final HttpRequest request;
    try {
      request = requestFactory.buildRequest(method.getName(), new GenericUrl(URI.create(uri)), null);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    headers.setAuthorization("Bearer " + accessToken.get());
    headers.setUserAgent(USER_AGENT);

    final long payloadSize;
    if (payload != NO_PAYLOAD) {
      final byte[] json = gzipJson(payload);
      payloadSize = json.length;
      headers.setContentEncoding(GZIP);
      headers.setContentLength((long) json.length);
      headers.setContentType(APPLICATION_JSON_UTF8);
      request.setContent(new ByteArrayContent(APPLICATION_JSON_UTF8, json));
    } else {
      payloadSize = 0;
    }

    request.setHeaders(headers);

    final PubsubFuture<T> future = new PubsubFuture<>(operation, method.toString(), uri, payloadSize);

    executor.execute(() -> {
      final HttpResponse response;
      try {
        response = request.execute();
      } catch (IOException e) {
        future.fail(e);
        return;
      }

      // Return null for 404'd GET & DELETE requests
      if (response.getStatusCode() == 404 && method == HttpMethod.GET || method == HttpMethod.DELETE) {
        future.succeed(null);
        return;
      }

      // Fail on non-2xx responses
      final int statusCode = response.getStatusCode();
      if (!(statusCode >= 200 && statusCode < 300)) {
        future.fail(new RequestFailedException(response.getStatusCode(), response.getStatusMessage()));
        return;
      }

      if (responseClass == Void.class) {
        future.succeed(null);
        return;
      }

      try {
        future.succeed(Json.read(response.getContent(), responseClass));
      } catch (IOException e) {
        future.fail(e);
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
