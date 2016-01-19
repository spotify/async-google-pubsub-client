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

package com.spotify.google.cloud.pubsub.client.integration;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.pubsub.PubsubScopes;

import com.spotify.google.cloud.pubsub.client.Pubsub;
import com.spotify.google.cloud.pubsub.client.ReceivedMessage;
import com.spotify.logging.LoggingConfigurator;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.Deflater;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static com.spotify.logging.LoggingConfigurator.Level.WARN;

public class PullerStress {

  private static final int PULLER_CONCURRENCY = 32;

  private static class MockPubsub extends AbstractHandler {

    @Override
    public void handle(final String s, final Request baseRequest, final HttpServletRequest request,
                       final HttpServletResponse response) throws IOException, ServletException {
      response.setContentType("text/html;charset=utf-8");
      response.setStatus(HttpServletResponse.SC_OK);
      baseRequest.setHandled(true);
      response.getWriter().println("<h1>Hello World</h1>");
    }
  }

  public static void main(final String... args) throws Exception {

    LoggingConfigurator.configureDefaults("puller-stress", WARN);

    Server server = new Server();

    ServerConnector connector = new ServerConnector(server);
    connector.setPort(9999);

    HttpConfiguration https = new HttpConfiguration();
    https.addCustomizer(new SecureRequestCustomizer());

    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(PullerStress.class.getResource("/keystore.jks").toExternalForm());
    sslContextFactory.setKeyStorePassword("pubsub");
    sslContextFactory.setKeyManagerPassword("pubsub");
    ServerConnector sslConnector = new ServerConnector(server,
                                                       new SslConnectionFactory(sslContextFactory, "http/1.1"),
                                                       new HttpConnectionFactory(https));
    sslConnector.setPort(9998);
    server.setConnectors(new Connector[]{connector, sslConnector});

    server.setHandler(new MockPubsub());

    server.start();
    server.join();

    if (System.getenv("nope") == null) {
      return;
    }

    final String project = Util.defaultProject();

    GoogleCredential credential;

    // Use credentials from file if available
    try {
      credential = GoogleCredential
          .fromStream(new FileInputStream("credentials.json"))
          .createScoped(PubsubScopes.all());
    } catch (IOException e) {
      credential = GoogleCredential.getApplicationDefault()
          .createScoped(PubsubScopes.all());
    }

    final Pubsub pubsub = Pubsub.builder()
        .credential(credential)
        .compressionLevel(Deflater.BEST_SPEED)
        .enabledCipherSuites(Util.nonGcmCiphers())
        .build();

    LoggingConfigurator.configureDefaults("benchmark", WARN);

    final String subscription = System.getenv("GOOGLE_PUBSUB_SUBSCRIPTION");
    if (subscription == null) {
      System.err.println("Please specify a subscription using the GOOGLE_PUBSUB_SUBSCRIPTION environment variable.");
      System.exit(1);
    }

    System.out.println("Consuming from GOOGLE_PUBSUB_SUBSCRIPTION='" + subscription + "'");

    final ProgressMeter meter = new ProgressMeter();
    final ProgressMeter.Metric receives = meter.group("operations").metric("receives", "messages");

    // Pull concurrently
    for (int i = 0; i < PULLER_CONCURRENCY; i++) {
      pull(project, pubsub, subscription, receives);
    }
  }

  private static void pull(final String project, final Pubsub pubsub, final String subscription,
                           final ProgressMeter.Metric receives) {

    final long start = System.nanoTime();

    pubsub.pull(project, subscription, false, 1000)
        .whenComplete((messages, ex) -> {
          if (ex != null) {
            ex.printStackTrace();
            return;
          }
          // Immediately kick off another pull
          pull(project, pubsub, subscription, receives);

          // Ack received messages
          final String[] ackIds = messages.stream().map(ReceivedMessage::ackId).toArray(String[]::new);
          pubsub.acknowledge(project, subscription, ackIds);

          final long end = System.nanoTime();
          final long latency = end - start;

          receives.add(messages.size(), latency);
        });
  }

}
