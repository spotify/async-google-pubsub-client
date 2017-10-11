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

package com.spotify.google.cloud.pubsub.client;

import java.util.regex.Pattern;

import io.norberg.automatter.AutoMatter;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

@AutoMatter
public interface Topic {

  Pattern PATTERN = Pattern.compile("^projects/[^/]*/topics/[^/]*$");

  String PROJECTS = "projects";
  String TOPICS = "topics";

  String name();

  static TopicBuilder builder() {
    return new TopicBuilder();
  }

  static Topic of(String project, String topic) {
    return of(canonicalTopic(project, topic));
  }

  static Topic of(String canonicalTopic) {
    return builder().name(canonicalTopic).build();
  }

  static String canonicalTopic(final String project, final String topic) {
    checkArgument(!isNullOrEmpty(project) && !project.contains("/"), "illegal project: %s", project);
    checkArgument(!isNullOrEmpty(topic) && !topic.contains("/"), "illegal topic: %s", topic);
    return PROJECTS + '/' + project + '/' + TOPICS + '/' + topic;
  }

  static void validateCanonicalTopic(final String canonicalTopic) {
    checkArgument(PATTERN.matcher(canonicalTopic).matches(), "malformed topic: %s", canonicalTopic);
  }
}
