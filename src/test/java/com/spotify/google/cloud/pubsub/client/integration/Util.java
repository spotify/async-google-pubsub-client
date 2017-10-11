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

package com.spotify.google.cloud.pubsub.client.integration;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;

class Util {

  static final String TEST_NAME_PREFIX = "async-google-pubsub-client-test-";

  private static String defaultProject = System.getenv("GOOGLE_CLOUD_PROJECT");

  static String defaultProject() {

    if (defaultProject != null) {
      return defaultProject;
    }

    // Try asking gcloud
    try {
      Process process = Runtime.getRuntime().exec("gcloud config get-value project");
      defaultProject = CharStreams.toString(new InputStreamReader(process.getInputStream())).trim();
    } catch (IOException e) {
      throw new RuntimeException("failed to get default project", e);
    }

    if (defaultProject.isEmpty()) {
      throw new RuntimeException("got empty default project");
    }

    return defaultProject;
  }

  /**
   * Return a list of non-GCM ciphers. GCM performance in Java 8 (pre 8u60) is unusably bad and currently worse than
   * CBC in >= 8u60.
   *
   * https://bugs.openjdk.java.net/browse/JDK-8069072
   */
  static String[] nonGcmCiphers() {
    final SSLContext sslContext;
    try {
      sslContext = SSLContext.getDefault();
    } catch (NoSuchAlgorithmException e) {
      throw Throwables.propagate(e);
    }

    final String[] defaultCiphers = sslContext.getDefaultSSLParameters().getCipherSuites();

    return Stream.of(defaultCiphers)
        .filter(cipher -> !cipher.contains("GCM"))
        .toArray(String[]::new);
  }
}
