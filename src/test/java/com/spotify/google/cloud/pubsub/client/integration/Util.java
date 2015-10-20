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
import com.google.common.base.Splitter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;

import static com.google.common.base.CharMatcher.WHITESPACE;
import static java.lang.System.getProperty;

class Util {

  static final String TEST_NAME_PREFIX = "test-";

  private static File CONFIG_PATH = new File(getProperty("user.home"), ".config");
  private static File GCLOUD_CONFIG_PATH = new File(CONFIG_PATH, "gcloud");
  private static File PROPERTIES_PATH = new File(GCLOUD_CONFIG_PATH, "properties");

  private static String defaultProject = System.getenv("GOOGLE_CLOUD_PROJECT");

  static String defaultProject() {

    if (defaultProject != null) {
      return defaultProject;
    }

    // Try reading $HOME/.config/gcloud/properties

    final List<String> lines;
    try {
      lines = Files.readAllLines(PROPERTIES_PATH.toPath());
    } catch (IOException e) {
      throw new RuntimeException("failed to get default project");
    }

    defaultProject = lines.stream()
        .filter(line -> line.contains("project")).findFirst()
        .map(line -> Splitter.on(WHITESPACE).splitToList(line))
        .map(tokens -> tokens.size() > 2 ? tokens.get(2) : "").orElse("");

    if (defaultProject == null) {
      throw new RuntimeException("failed to get default project");
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
