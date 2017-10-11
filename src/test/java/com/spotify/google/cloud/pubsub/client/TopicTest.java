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

import com.google.common.collect.ImmutableMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TopicTest {

  private static final String PROJECT = "test-project";
  private static final String TOPIC = "test-topic";

  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void testOf() throws Exception {
    final Topic topic = Topic.of(PROJECT, TOPIC);
    final String canonical = Topic.canonicalTopic(PROJECT, TOPIC);
    assertThat(topic.name(), is(canonical));
  }

  @Test
  public void testOfCanonical() throws Exception {
    final String canonical = Topic.canonicalTopic(PROJECT, TOPIC);
    final Topic topic = Topic.of(canonical);
    assertThat(topic.name(), is(canonical));
  }

  @Test
  public void testCanonicalTopic() throws Exception {
    final String canonical = Topic.canonicalTopic(PROJECT, TOPIC);
    assertThat(canonical, is("projects/" + PROJECT + "/topics/" + TOPIC));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateCanonicalTopic() throws Exception {
    Topic.validateCanonicalTopic("foo/bar/");
  }

  @Test
  public void testBuilder() throws Exception {
    final String canonicalTopic = Topic.canonicalTopic(PROJECT, TOPIC);
    final TopicBuilder builder = Topic.builder();
    assertThat(builder.name(), is(nullValue()));
    builder.name(canonicalTopic);
    assertThat(builder.name(), is(canonicalTopic));
    final Topic topic = builder.build();
    assertThat(topic.name(), is(canonicalTopic));
  }

  @Test
  public void testWriteJson() {
    final String canonicalTopic = Topic.canonicalTopic(PROJECT, TOPIC);
    final byte[] expected = Json.write(ImmutableMap.of("name", canonicalTopic));
    final byte[] json = Json.write(Topic.of(PROJECT, TOPIC));
    assertTrue(Arrays.equals(json, expected));
  }

  @Test
  public void testReadJson() throws IOException {
    final Topic expected = Topic.of(PROJECT, TOPIC);
    final byte[] json = Json.write(ImmutableMap.of("name", expected.name()));
    final Topic topic = Json.read(json, Topic.class);
  }

}