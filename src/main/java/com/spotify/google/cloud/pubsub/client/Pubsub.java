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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.repackaged.com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;

import static com.google.common.util.concurrent.MoreExecutors.getExitingScheduledExecutorService;
import static io.vertx.core.http.HttpMethod.DELETE;
import static io.vertx.core.http.HttpMethod.GET;
import static io.vertx.core.http.HttpMethod.POST;
import static io.vertx.core.http.HttpMethod.PUT;

/**
 * The Datastore class encapsulates the Cloud Datastore API and handles
 * calling the datastore backend.
 * <p>
 * To create a Datastore object, call the static method {@code Datastore.create()}
 * passing configuration. A scheduled task will begin that automatically refreshes
 * the API access token for you.
 * <p>
 * Call {@code close()} to perform all necessary clean up.
 */
public class Pubsub implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(Pubsub.class);

  private static final String VERSION = "1.0.0";
  private static final String USER_AGENT =
      "Spotify-Google-Pubsub-Java-Client/" + VERSION + " (gzip)";

  private static final Object NO_PAYLOAD = new Object();

  private final HttpClient client;

  private final ScheduledExecutorService executor = getExitingScheduledExecutorService(
      new ScheduledThreadPoolExecutor(1));

  private final String baseUri;

  private volatile String accessToken;

  private static final Vertx vertx = Vertx.vertx();
  private final Credential credential;

  private Pubsub(final Builder builder) {
    this.client = vertx.createHttpClient(
        new HttpClientOptions()
            .setKeepAlive(true)
            .setDefaultHost(builder.uri.getHost())
            .setDefaultPort(defaultPort(builder.uri))
            .setConnectTimeout(builder.connectTimeout)
            .setMaxPoolSize(builder.maxConnections)
            .setTryUseCompression(true)
            .setSsl(true));

    if (builder.credential == null) {
      this.credential = defaultCredential();
    } else {
      this.credential = builder.credential;
    }

    this.baseUri = stripTrailingSlash(builder.uri.getRawPath());

    // Get initial access token
    refreshAccessToken();
    if (accessToken == null) {
      throw new RuntimeException("Failed to get access token");
    }

    // Wake up every 10 seconds to check if access token has expired
    executor.scheduleAtFixedRate(this::refreshAccessToken, 10, 10, TimeUnit.SECONDS);
  }

  private static Credential defaultCredential() {
    try {
      return GoogleCredential.getApplicationDefault(
          Utils.getDefaultTransport(), Utils.getDefaultJsonFactory());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private String stripTrailingSlash(final String path) {
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }

  private static int defaultPort(final URI uri) {
    if (uri.getPort() != -1) {
      return uri.getPort();
    }
    switch (uri.getScheme()) {
      case "https":
        return 443;
      case "http":
        return 80;
      default:
        throw new IllegalArgumentException("Illegal uri: " + uri);
    }
  }

  @Override
  public void close() {
    executor.shutdown();
    client.close();
  }

  private void refreshAccessToken() {
    final Long expiresIn = credential.getExpiresInSeconds();

    // trigger refresh if token is about to expire
    String accessToken = credential.getAccessToken();
    if (accessToken == null || expiresIn != null && expiresIn <= 60) {
      try {
        credential.refreshToken();
        accessToken = credential.getAccessToken();
      } catch (final IOException e) {
        log.error("Failed to fetch access token", e);
      }
    }
    if (accessToken != null) {
      this.accessToken = accessToken;
    }
  }

  private boolean isSuccessful(final int statusCode) {
    return statusCode >= 200 && statusCode < 300;
  }

  public CompletableFuture<TopicList> listTopics(final String project) {
    final String uri = baseUri + "/projects/" + project + "/topics";
    return get(uri, TopicList.class);
  }

  public CompletableFuture<TopicList> listTopics(final String project,
                                                 final String pageToken) {
    final StringBuilder uri = new StringBuilder().append(baseUri)
        .append("/projects/").append(project).append("/topics");
    if (pageToken != null) {
      uri.append("?pageToken=").append(pageToken);
    }
    return get(uri.toString(), TopicList.class);
  }

  public CompletableFuture<TopicList> listTopics(final String project,
                                                 final int pageSize,
                                                 final String pageToken) {
    final StringBuilder uri = new StringBuilder().append(baseUri)
        .append("/projects/").append(project).append("/topics")
        .append("?pageSize=").append(pageSize);
    if (pageToken != null) {
      uri.append("pageToken=").append(pageToken);
    }
    return get(uri.toString(), TopicList.class);
  }

  public CompletableFuture<Topic> createTopic(final String project,
                                              final String topic) {
    return createTopic(project, topic, Topic.of(topic));
  }

  public CompletableFuture<Topic> createTopic(final String project,
                                              final String topic,
                                              final Topic req) {
    final String uri = baseUri + "/projects/" + project + "/topics/" + topic;
    return put(uri, req, Topic.class);
  }

  public CompletableFuture<Topic> getTopic(final String project, final String topic) {
    final String uri = baseUri + "/projects/" + project + "/topics/" + topic;
    return get(uri, Topic.class);
  }

  public CompletableFuture<Void> deleteTopic(final String project,
                                             final String topic) {
    final String uri = baseUri + "/projects/" + project + "/topics/" + topic;
    return delete(uri, Void.class);
  }

  public CompletableFuture<PublishResponse> publish(final String project, final String topic,
                                                    final PublishRequest req) {
    final String uri = baseUri + "/projects/" + project + "/topics/" + topic + ":publish";
    return post(uri, req, PublishResponse.class);
  }

  private <T> CompletableFuture<T> get(final String uri, final Class<T> responseClass) {
    return request(GET, uri, responseClass);
  }

  private <T> CompletableFuture<T> post(final String uri, final Object payload,
                                        final Class<T> responseClass) {
    return request(POST, uri, responseClass, payload);
  }

  private <T> CompletableFuture<T> put(final String uri, final Object payload,
                                       final Class<T> responseClass) {
    return request(PUT, uri, responseClass, payload);
  }

  private <T> CompletableFuture<T> delete(final String uri, final Class<T> responseClass) {
    return request(DELETE, uri, responseClass);
  }

  private <T> CompletableFuture<T> request(final HttpMethod method, final String uri,
                                           final Class<T> responseClass) {
    return request(method, uri, responseClass, NO_PAYLOAD);
  }

  private <T> CompletableFuture<T> request(final HttpMethod method, final String uri,
                                           final Class<T> responseClass, final Object payload) {

    log.debug("{} {}", method, uri);

    final HttpClientRequest request = client.request(method, uri);

    request.putHeader("Authorization", "Bearer " + accessToken);
    request.putHeader("User-Agent", USER_AGENT);
    request.putHeader("Accept-Encoding", "gzip");

    final CompletableFuture<T> future = new CompletableFuture<>();
    request.handler(response -> {

      // Return null for 404'd GET requests
      if (response.statusCode() == 404 && method == GET) {
        future.complete(null);
        return;
      }

      // Fail on non-2xx responses
      if (!isSuccessful(response.statusCode())) {
        future.completeExceptionally(
            new RequestFailedException(response.statusCode(), response.statusMessage()));
        return;
      }

      if (responseClass == Void.class) {
        future.complete(null);
        return;
      }

      // Parse response body
      response.bodyHandler(body -> {
        try {
          future.complete(Json.read(body, responseClass));
        } catch (IOException e) {
          future.completeExceptionally(e);
        }
      });
    });

    // Write JSON payload
    if (payload != NO_PAYLOAD) {
      final Buffer json = json(payload);
      if (log.isDebugEnabled()) {
        log.debug(json.toString("UTF-8"));
      }
      request.putHeader(HttpHeaders.CONTENT_LENGTH, String.valueOf(json.length()));
      request.write(json);
    }

    request.end();

    return future;
  }

  private Buffer json(final Object value) {
    final Buffer body = Buffer.buffer();
    try {
      Json.writer().writeValue(new BufferOutputStream(body), value);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return body;
  }

  public static Pubsub create() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private static final int DEFAULT_CONNECT_TIMEOUT = 5000;
    private static final int DEFAULT_MAX_CONNECTIONS = 5;

    private static final URI DEFAULT_URI = URI.create("https://pubsub.googleapis.com/v1/");

    private Integer connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private Integer maxConnections = DEFAULT_MAX_CONNECTIONS;
    private Credential credential;
    private URI uri = DEFAULT_URI;

    private Builder() {
    }

    /**
     * Creates a new {@code Pubsub}.
     */
    public Pubsub build() {
      return new Pubsub(this);
    }

    /**
     * Set the maximum time in milliseconds the client will can wait
     * when connecting to a remote host.
     *
     * @param connectTimeout the maximum time in milliseconds.
     * @return this config builder.
     */
    public Builder connectTimeout(final int connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    /**
     * Set the maximum number of connections client will open.
     *
     * @param maxConnections the maximum number of connections.
     * @return this config builder.
     */
    public Builder maxConnections(final int maxConnections) {
      this.maxConnections = maxConnections;
      return this;
    }

    /**
     * Set Pubsub credentials to ues when requesting an access token.
     * <p>
     * Credentials can be generated by calling
     * {@code PubsubHelper.getComputeEngineCredential} or
     * {@code PubsubHelper.getServiceAccountCredential}
     *
     * @param credential the credentials used to authenticate.
     * @return this config builder.
     */
    public Builder credential(final Credential credential) {
      this.credential = credential;
      return this;
    }

    /**
     * The Pubsub service URI. By default, this is the Google
     * Pubsub provider, however you may run a local Developer Server.
     *
     * @param uri the service to connect to.
     * @return this config builder.
     */
    public Builder uri(final URI uri) {
      this.uri = uri;
      return this;
    }
  }

}
