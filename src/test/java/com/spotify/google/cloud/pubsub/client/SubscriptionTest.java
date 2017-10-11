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

public class SubscriptionTest {

  private static final String PROJECT = "test-project";
  private static final String TOPIC = "test-topic";
  private static final String SUBSCRIPTION = "test-subscription";

  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void testOf() throws Exception {
    final Subscription subscription = Subscription.of(PROJECT, SUBSCRIPTION, TOPIC);
    final String canonical = Subscription.canonicalSubscription(PROJECT, SUBSCRIPTION);
    assertThat(subscription.name(), is(canonical));
  }

  @Test
  public void testOfCanonical() throws Exception {
    final String canonicalSubscription = Subscription.canonicalSubscription(PROJECT, SUBSCRIPTION);
    final String canonicalTopic = Topic.canonicalTopic(PROJECT, TOPIC);
    final Subscription subscription = Subscription.of(canonicalSubscription, canonicalTopic);
    assertThat(subscription.name(), is(canonicalSubscription));
  }

  @Test
  public void testCanonicalTopic() throws Exception {
    final String canonical = Subscription.canonicalSubscription(PROJECT, SUBSCRIPTION);
    assertThat(canonical, is("projects/" + PROJECT + "/subscriptions/" + SUBSCRIPTION));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateCanonicalTopic() throws Exception {
    Subscription.validateCanonicalSubscription("foo/bar/");
  }

  @Test
  public void testBuilder() throws Exception {
    final String canonicalSubscription = Subscription.canonicalSubscription(PROJECT, SUBSCRIPTION);
    final SubscriptionBuilder builder = Subscription.builder();
    assertThat(builder.name(), is(nullValue()));
    builder.name(canonicalSubscription);
    assertThat(builder.name(), is(canonicalSubscription));
    final String canonicalTopic = Topic.canonicalTopic(PROJECT, TOPIC);
    builder.topic(canonicalTopic);
    assertThat(builder.topic(), is(canonicalTopic));
    final Subscription subscription = builder.build();
    assertThat(subscription.name(), is(canonicalSubscription));
    assertThat(subscription.topic(), is(canonicalTopic));
  }

  @Test
  public void testWriteJson() {
    final String canonicalSubscription = Subscription.canonicalSubscription(PROJECT, SUBSCRIPTION);
    final String canonicalTopic = Topic.canonicalTopic(PROJECT, TOPIC);
    final byte[] expected = Json.write(ImmutableMap.of("name", canonicalSubscription,
                                                       "topic", canonicalTopic));
    final byte[] json = Json.write(Subscription.of(PROJECT, SUBSCRIPTION, TOPIC));
    assertTrue(Arrays.equals(json, expected));
  }

  @Test
  public void testReadJson() throws IOException {
    final Subscription expected = Subscription.of(PROJECT, SUBSCRIPTION, TOPIC);
    final byte[] json = Json.write(ImmutableMap.of("name", expected.name(), "topic", expected.topic()));
    final Subscription subscription = Json.read(json, Subscription.class);
  }
}