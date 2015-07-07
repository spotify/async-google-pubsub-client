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

import com.google.common.net.HttpHeaders;

import com.ning.http.client.AsyncHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;

import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

class Service implements Closeable {

  private static final String VERSION = "1.0.0";

  private static final String USER_AGENT =
      "Spotify-Google-Pubsub-Java-Client/" + VERSION + " (gzip)";

  private static final Object NO_PAYLOAD = new Object();

  private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
  private final String uri;
  private final Supplier<String> accessToken;
  private final AsyncHttpClient client;

  Service(final AsyncHttpClientConfig config, final URI uri, final Supplier<String> accessToken) {
    this.uri = stripTrailingSlash(uri.toString());
    this.accessToken = accessToken;
    this.client = new AsyncHttpClient(config);
  }

  private String stripTrailingSlash(final String path) {
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }

  /**
   * Make a GET request.
   */
  <T> CompletableFuture<T> get(final String path, final Class<T> responseClass) {
    return request(HttpMethod.GET, path, responseClass);
  }

  /**
   * Make a POST request.
   */
  <T> CompletableFuture<T> post(final String path, final Object payload,
                                final Class<T> responseClass) {
    return request(HttpMethod.POST, path, responseClass, payload);
  }

  /**
   * Make a PUT request.
   */
  <T> CompletableFuture<T> put(final String path, final Object payload,
                               final Class<T> responseClass) {
    return request(HttpMethod.PUT, path, responseClass, payload);
  }

  /**
   * Make a DELETE request.
   */
  CompletableFuture<Void> delete(final String path) {
    return request(HttpMethod.DELETE, path, Void.class);
  }

  /**
   * Make an HTTP request.
   */
  private <T> CompletableFuture<T> request(final HttpMethod method, final String path,
                                           final Class<T> responseClass) {
    return request(method, path, responseClass, NO_PAYLOAD);
  }

  /**
   * Make an HTTP request.
   */
  <T> CompletableFuture<T> request(final HttpMethod method, final String path,
                                   final Class<T> responseClass, final Object payload) {

    final RequestBuilder builder = new RequestBuilder()
        .setUrl(uri + path)
        .setMethod(method.toString())
        .setHeader("Authorization", "Bearer " + accessToken)
        .setHeader("User-Agent", USER_AGENT)
        .setHeader("Accept-Encoding", "gzip");

    if (payload != NO_PAYLOAD) {
      final byte[] json = Json.write(payload);
      builder.setHeader(HttpHeaders.CONTENT_LENGTH, String.valueOf(json.length));
      builder.setBody(json);
    }

    final Request request = builder.build();

    final CompletableFuture<T> future = new CompletableFuture<>();
    client.executeRequest(request, new AsyncHandler<Void>() {
      private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

      @Override
      public void onThrowable(final Throwable t) {
        future.completeExceptionally(t);
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
          future.complete(null);
          return STATE.ABORT;
        }

        // Fail on non-2xx responses
        final int statusCode = status.getStatusCode();
        if (!(statusCode >= 200 && statusCode < 300)) {
          future.completeExceptionally(
              new RequestFailedException(status.getStatusCode(), status.getStatusText()));
          return STATE.ABORT;
        }

        if (responseClass == Void.class) {
          future.complete(null);
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
        try {
          future.complete(Json.read(bytes.toByteArray(), responseClass));
        } catch (IOException e) {
          future.completeExceptionally(e);
        }
        return null;
      }
    });

    return future;
  }

  public CompletableFuture<Void> closeFuture() {
    return closeFuture.thenApply(ignore -> null);
  }

  public void close() {

  }

  public interface Factory {

    Service createService(AsyncHttpClientConfig config, URI uri, Supplier<String> accessToken);
  }
}
