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

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import io.norberg.automatter.AutoMatter;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class JsonTest {

  @Test
  public void testReadGuava() throws Exception {
    final Map<String, String> expected = ImmutableMap.of("foo", "bar");
    final ImmutableMap<?, ?> value = Json.read("{\"foo\":\"bar\"}".getBytes("UTF-8"), ImmutableMap.class);
    assertThat(value, is(expected));
  }

  @AutoMatter
  public interface ValueWithOptional {

    Optional<String> foo();
  }

  @Test
  public void testReadEmptyOptional() throws Exception {
    final ValueWithOptional expected = new ValueWithOptionalBuilder().build();
    final ValueWithOptional value = Json.read("{}".getBytes("UTF-8"), ValueWithOptional.class);
    assertThat(value, is(expected));
  }

  @Test
  public void testReadPresentOptional() throws Exception {
    final ValueWithOptional expected = new ValueWithOptionalBuilder().foo("bar").build();
    final ValueWithOptional value = Json.read("{\"foo\":\"bar\"}".getBytes("UTF-8"), ValueWithOptional.class);
    assertThat(value, is(expected));
  }

  @Test
  public void testWriteGuava() throws Exception {
    final byte[] expected = "{\"foo\":\"bar\"}".getBytes("UTF-8");
    final ImmutableMap<String, String> value = ImmutableMap.of("foo", "bar");
    testWrite(expected, value);
  }

  @Test
  public void testWritePresentOptional() throws Exception {
    final byte[] expected = "{\"foo\":\"bar\"}".getBytes("UTF-8");
    final ValueWithOptional value = new ValueWithOptionalBuilder().foo("bar").build();
    testWrite(expected, value);
  }

  @Test
  public void testWriteEmptyOptional() throws Exception {
    final byte[] expected = "{}".getBytes("UTF-8");
    final ValueWithOptional value = new ValueWithOptionalBuilder().build();
    testWrite(expected, value);
  }

  private void testWrite(final byte[] expected, final Object value) throws IOException {
    final byte[] json = Json.write(value);
    assertTrue(Arrays.equals(json, expected));

    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    Json.write(stream, value);
    assertTrue(Arrays.equals(stream.toByteArray(), expected));
  }
}