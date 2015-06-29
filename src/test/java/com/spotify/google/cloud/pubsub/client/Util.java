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

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.io.CharStreams;

import java.io.IOException;
import java.io.InputStreamReader;

import static com.google.api.client.repackaged.com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.CharMatcher.WHITESPACE;

class Util {

  public static final String TEST_TOPIC_PREFIX = "test-topic-";

  private static String defaultProject;

  public static String defaultProject() {
    if (defaultProject == null) {
      defaultProject = shell("gcloud config list | grep project | cut -d ' ' -f 3-");
      if (isNullOrEmpty(defaultProject) || WHITESPACE.matchesAnyOf(defaultProject())) {
        throw new RuntimeException("failed to get default project");
      }
    }
    return defaultProject;
  }

  private static String shell(final String command) {
    try {
      final Process p = new ProcessBuilder().command("/bin/sh", "-c", command).start();
      final String result = CharStreams.toString(new InputStreamReader(p.getInputStream())).trim();
      if (p.waitFor() != 0) {
        throw new RuntimeException("Exit code != 0: " + command);
      }
      return result;
    } catch (IOException | InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }
}
