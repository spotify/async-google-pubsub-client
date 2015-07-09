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

import com.ning.http.client.AsyncHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.zip.GZIPOutputStream;

import static com.google.common.util.concurrent.MoreExecutors.getExitingScheduledExecutorService;
import static com.spotify.google.cloud.pubsub.client.Topic.canonicalTopic;
import static com.spotify.google.cloud.pubsub.client.Topic.validateCanonicalTopic;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_ENCODING;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.GZIP;

/**
 * An async low-level Google Cloud Pub/Sub client.
 */
public class Pubsub implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(Pubsub.class);

  private static final String VERSION = "1.0.0";
  private static final String USER_AGENT =
      "Spotify-Google-Pubsub-Java-Client/" + VERSION + " (gzip)";

  private static final Object NO_PAYLOAD = new Object();

  private final AsyncHttpClient client;
  private final String baseUri;
  private final Credential credential;
  private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

  private final ScheduledExecutorService executor = getExitingScheduledExecutorService(
      new ScheduledThreadPoolExecutor(1));

  private volatile String accessToken;

  private Pubsub(final Builder builder) {
    final AsyncHttpClientConfig config = builder.clientConfig.build();

    log.debug("creating new pubsub client with config:");
    log.debug("uri: {}", builder.uri);
    log.debug("connect timeout: {}", config.getConnectTimeout());
    log.debug("read timeout: {}", config.getReadTimeout());
    log.debug("request timeout: {}", config.getRequestTimeout());
    log.debug("max connections: {}", config.getMaxConnections());
    log.debug("max connections per host: {}", config.getMaxConnectionsPerHost());
    log.debug("enabled cipher suites: {}", Arrays.toString(config.getEnabledCipherSuites()));
    log.debug("compression enforced: {}", config.isCompressionEnforced());
    log.debug("accept any certificate: {}", config.isAcceptAnyCertificate());
    log.debug("follows redirect: {}", config.isFollowRedirect());
    log.debug("pooled connection TTL: {}", config.getConnectionTTL());
    log.debug("pooled connection idle timeout: {}", config.getPooledConnectionIdleTimeout());
    log.debug("pooling connections: {}", config.isAllowPoolingConnections());
    log.debug("pooling SSL connections: {}", config.isAllowPoolingSslConnections());
    log.debug("user agent: {}", config.getUserAgent());
    log.debug("max request retry: {}", config.getMaxRequestRetry());

    this.client = new AsyncHttpClient(config);

    if (builder.credential == null) {
      this.credential = defaultCredential();
    } else {
      this.credential = builder.credential;
    }

    this.baseUri = stripTrailingSlash(builder.uri.toString());

    // Get initial access token
    refreshAccessToken();
    if (accessToken == null) {
      throw new RuntimeException("Failed to get access token");
    }

    // Wake up every 10 seconds to check if access token has expired
    executor.scheduleAtFixedRate(this::refreshAccessToken, 10, 10, SECONDS);
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

  /**
   * Close this {@link Pubsub} client.
   */
  @Override
  public void close() {
    executor.shutdown();
    client.close();
  }

  /**
   * Get a future that is completed when this {@link Pubsub} client is closed.
   */
  public CompletableFuture<Void> closeFuture() {
    return closeFuture.thenApply(ignore -> null);
  }

  /**
   * Refresh the Google Cloud API access token, if necessary.
   */
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

  /**
   * List the Pub/Sub topics in a project. This will get the first page of topics. To enumerate all topics you might
   * have to make further calls to {@link #listTopics(String, String)} with the page token in order to get further
   * pages.
   *
   * @param project The Google Cloud project.
   * @return A future that is completed when this request is completed.
   */
  public CompletableFuture<TopicList> listTopics(final String project) {
    final String uri = baseUri + "/projects/" + project + "/topics";
    return get(uri, TopicList.class);
  }

  /**
   * Get a page of Pub/Sub topics in a project using a specified page token.
   *
   * @param project   The Google Cloud project.
   * @param pageToken A token for the page of topics to get.
   * @return A future that is completed when this request is completed.
   */
  public CompletableFuture<TopicList> listTopics(final String project,
                                                 final String pageToken) {
    final StringBuilder uri = new StringBuilder().append(baseUri)
        .append("/projects/").append(project).append("/topics");
    if (pageToken != null) {
      uri.append("?pageToken=").append(pageToken);
    }
    return get(uri.toString(), TopicList.class);
  }

  /**
   * Create a Pub/Sub topic.
   *
   * @param project The Google Cloud project.
   * @param topic   The name of the topic to create.
   * @return A future that is completed when this request is completed.
   */
  public CompletableFuture<Topic> createTopic(final String project,
                                              final String topic) {
    return createTopic(canonicalTopic(project, topic));
  }

  /**
   * Create a Pub/Sub topic.
   *
   * @param canonicalTopic The canonical (including project) name of the topic to create.
   * @return A future that is completed when this request is completed.
   */
  public CompletableFuture<Topic> createTopic(final String canonicalTopic) {
    return createTopic(canonicalTopic, Topic.of(canonicalTopic));
  }

  /**
   * Create a Pub/Sub topic.
   *
   * @param canonicalTopic The canonical (including project) name of the topic to create.
   * @param req            The payload of the create request. This seems to be ignore in the current API version.
   * @return A future that is completed when this request is completed.
   */
  private CompletableFuture<Topic> createTopic(final String canonicalTopic,
                                               final Topic req) {
    validateCanonicalTopic(canonicalTopic);
    final String uri = baseUri + "/" + canonicalTopic;
    return put(uri, req, Topic.class);
  }

  /**
   * Get a Pub/Sub topic.
   *
   * @param project The Google Cloud project.
   * @param topic   The name of the topic to get.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public CompletableFuture<Topic> getTopic(final String project, final String topic) {
    final String uri = baseUri + "/" + canonicalTopic(project, topic);
    return get(uri, Topic.class);
  }

  /**
   * Get a Pub/Sub topic.
   *
   * @param canonicalTopic The canonical (including project) name of the topic to get.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public CompletableFuture<Topic> getTopic(final String canonicalTopic) {
    validateCanonicalTopic(canonicalTopic);
    final String uri = baseUri + "/" + canonicalTopic;
    return get(uri, Topic.class);
  }

  /**
   * Delete a Pub/Sub topic.
   *
   * @param project The Google Cloud project.
   * @param topic   The name of the topic to delete.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public CompletableFuture<Void> deleteTopic(final String project,
                                             final String topic) {
    return deleteTopic(canonicalTopic(project, topic));
  }

  /**
   * Delete a Pub/Sub topic.
   *
   * @param canonicalTopic The canonical (including project) name of the topic to delete.
   * @return A future that is completed when this request is completed. The future will be completed with {@code null}
   * if the response is 404.
   */
  public CompletableFuture<Void> deleteTopic(final String canonicalTopic) {
    validateCanonicalTopic(canonicalTopic);
    final String uri = baseUri + "/" + canonicalTopic;
    return delete(uri, Void.class);
  }

  /**
   * Publish a batch of messages.
   *
   * @param project  The Google Cloud project.
   * @param topic    The topic to publish on.
   * @param messages The batch of messages.
   * @return a future that is completed with a list of message ID's for the published messages.
   */
  public CompletableFuture<List<String>> publish(final String project, final String topic,
                                                 final Message... messages) {
    return publish(project, topic, asList(messages));
  }

  /**
   * Publish a batch of messages.
   *
   * @param project  The Google Cloud project.
   * @param topic    The topic to publish on.
   * @param messages The batch of messages.
   * @return a future that is completed with a list of message ID's for the published messages.
   */
  public CompletableFuture<List<String>> publish(final String project, final String topic,
                                                 final List<Message> messages) {
    final String uri = baseUri + "/projects/" + project + "/topics/" + topic + ":publish";
    return post(uri, PublishRequest.of(messages), PublishResponse.class)
        .thenApply(PublishResponse::messageIds);
  }

  /**
   * Make a GET request.
   */
  private <T> CompletableFuture<T> get(final String uri, final Class<T> responseClass) {
    return request(HttpMethod.GET, uri, responseClass);
  }

  /**
   * Make a POST request.
   */
  private <T> CompletableFuture<T> post(final String uri, final Object payload,
                                        final Class<T> responseClass) {
    return request(HttpMethod.POST, uri, responseClass, payload);
  }

  /**
   * Make a PUT request.
   */
  private <T> CompletableFuture<T> put(final String uri, final Object payload,
                                       final Class<T> responseClass) {
    return request(HttpMethod.PUT, uri, responseClass, payload);
  }

  /**
   * Make a DELETE request.
   */
  private <T> CompletableFuture<T> delete(final String uri, final Class<T> responseClass) {
    return request(HttpMethod.DELETE, uri, responseClass);
  }

  /**
   * Make an HTTP request.
   */
  private <T> CompletableFuture<T> request(final HttpMethod method, final String uri,
                                           final Class<T> responseClass) {
    return request(method, uri, responseClass, NO_PAYLOAD);
  }

  /**
   * Make an HTTP request.
   */
  private <T> CompletableFuture<T> request(final HttpMethod method, final String uri,
                                           final Class<T> responseClass, final Object payload) {

    final RequestBuilder builder = new RequestBuilder()
        .setUrl(uri)
        .setMethod(method.toString())
        .setHeader("Authorization", "Bearer " + accessToken)
        .setHeader("User-Agent", USER_AGENT);

    if (payload != NO_PAYLOAD) {
      final byte[] json = gzipJson(payload);
      builder.setHeader(CONTENT_ENCODING, GZIP);
      builder.setHeader(CONTENT_LENGTH, String.valueOf(json.length));
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

  private byte[] gzipJson(final Object payload) {
    try (
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final GZIPOutputStream gzip = new GZIPOutputStream(bytes);
    ) {
      Json.write(gzip, payload);
      return bytes.toByteArray();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Create a new {@link Pubsub} client with default configuration.
   */
  public static Pubsub create() {
    return builder().build();
  }

  /**
   * Create a new {@link Builder} that can be used to build a new {@link Pubsub} client.
   */
  public static Builder builder() {
    return new Builder();
  }


  /**
   * A {@link Builder} that can be used to build a new {@link Pubsub} client.
   */
  public static class Builder {

    private static final URI DEFAULT_URI = URI.create("https://pubsub.googleapis.com/v1/");

    private static final int DEFAULT_REQUEST_TIMEOUT_MS = 30000;

    private final AsyncHttpClientConfig.Builder clientConfig = new AsyncHttpClientConfig.Builder()
        .setCompressionEnforced(true)
        .setUseProxySelector(true)
        .setRequestTimeout(DEFAULT_REQUEST_TIMEOUT_MS)
        .setReadTimeout(DEFAULT_REQUEST_TIMEOUT_MS);

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
     * Set the maximum time in milliseconds the client will can wait when connecting to a remote host.
     *
     * @param connectTimeout the connect timeout in milliseconds.
     * @return this config builder.
     */
    public Builder connectTimeout(final int connectTimeout) {
      clientConfig.setConnectTimeout(connectTimeout);
      return this;
    }

    /**
     * Set the connection read timeout in milliseconds.
     *
     * @param readTimeout the read timeout in milliseconds.
     * @return this config builder.
     */
    public Builder readTimeout(final int readTimeout) {
      clientConfig.setReadTimeout(readTimeout);
      return this;
    }

    /**
     * Set the request timeout in milliseconds.
     *
     * @param requestTimeout the maximum time in milliseconds.
     * @return this config builder.
     */
    public Builder requestTimeout(final int requestTimeout) {
      clientConfig.setRequestTimeout(requestTimeout);
      return this;
    }

    /**
     * Set the maximum number of connections client will open.
     *
     * @param maxConnections the maximum number of connections.
     * @return this config builder.
     */
    public Builder maxConnections(final int maxConnections) {
      clientConfig.setMaxConnections(maxConnections);
      return this;
    }

    /**
     * Set the maximum number of milliseconds a pooled connection will be reused. -1 for no limit.
     *
     * @param pooledConnectionTTL the maximum time in milliseconds.
     * @return this config builder.
     */
    public Builder pooledConnectionTTL(final int pooledConnectionTTL) {
      clientConfig.setConnectionTTL(pooledConnectionTTL);
      return this;
    }

    /**
     * Set the maximum number of milliseconds an idle pooled connection will be be kept.
     *
     * @param pooledConnectionIdleTimeout the timeout in milliseconds.
     * @return this config builder.
     */
    public Builder pooledConnectionIdleTimeout(final int pooledConnectionIdleTimeout) {
      clientConfig.setPooledConnectionIdleTimeout(pooledConnectionIdleTimeout);
      return this;
    }

    /**
     * Set whether to allow connection pooling or not. Default is true.
     *
     * @param allowPoolingConnections the maximum number of connections.
     * @return this config builder.
     */
    public Builder allowPoolingConnections(final boolean allowPoolingConnections) {
      clientConfig.setAllowPoolingConnections(allowPoolingConnections);
      return this;
    }

    /**
     * Set Google Cloud API credentials to use.
     *
     * @param credential the credentials used to authenticate.
     */
    public Builder credential(final Credential credential) {
      this.credential = credential;
      return this;
    }

    /**
     * Set cipher suites to enable for SSL/TLS.
     *
     * @param enabledCipherSuites The cipher suites to enable.
     */
    public Builder enabledCipherSuites(final String... enabledCipherSuites) {
      clientConfig.setEnabledCipherSuites(enabledCipherSuites);
      return this;
    }

    /**
     * Set cipher suites to enable for SSL/TLS.
     *
     * @param enabledCipherSuites The cipher suites to enable.
     */
    public Builder enabledCipherSuites(final List<String> enabledCipherSuites) {
      clientConfig.setEnabledCipherSuites(enabledCipherSuites.toArray(new String[enabledCipherSuites.size()]));
      return this;
    }

    /**
     * The Google Cloud Pub/Sub API URI. By default, this is the Google Cloud Pub/Sub provider, however you may run a
     * local Developer Server.
     *
     * @param uri the service to connect to.
     */
    public Builder uri(final URI uri) {
      this.uri = uri;
      return this;
    }
  }
}
