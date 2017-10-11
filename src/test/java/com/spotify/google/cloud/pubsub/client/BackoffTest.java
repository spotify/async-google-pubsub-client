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
 * Copyright (c) 2011-2017 Spotify AB
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.util.MockSleeper;
import com.google.api.client.util.Sleeper;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BackoffTest {

  @Mock
  private Sleeper sleeper;
  @Mock
  private Random random;

  @InjectMocks
  public Backoff backoff = Backoff.builder().initialInterval(111).maxBackoffMultiplier(5).build();
  @InjectMocks
  public Backoff noBackoff = Backoff.builder().initialInterval(111).maxBackoffMultiplier(0).build();

  @Test
  public void testBackoff() throws Exception {
    when(random.nextDouble()).thenReturn(0.5);

    backoff.reset();
    backoff.sleep();
    backoff.sleep();
    backoff.sleep();
    backoff.sleep();
    backoff.sleep(); // max is reached
    backoff.sleep();
    backoff.reset(); // reset back
    backoff.sleep();
    backoff.sleep();

    InOrder inOrder = inOrder(sleeper);
    inOrder.verify(sleeper).sleep(eq(111L));
    inOrder.verify(sleeper).sleep(eq(222L));
    inOrder.verify(sleeper).sleep(eq(333L));
    inOrder.verify(sleeper).sleep(eq(444L));
    inOrder.verify(sleeper, times(2)).sleep(eq(555L));
    inOrder.verify(sleeper).sleep(eq(111L));
    inOrder.verify(sleeper).sleep(eq(222L));

    verifyNoMoreInteractions(sleeper);
  }

  @Test
  public void testBackoffRandomizer() throws Exception {
    when(random.nextDouble()).thenReturn(0.5, 0.1, 0.9);

    backoff.reset();
    backoff.sleep();
    backoff.reset();
    backoff.sleep();
    backoff.reset();
    backoff.sleep();

    InOrder inOrder = inOrder(sleeper);
    inOrder.verify(sleeper).sleep(eq(111L));
    inOrder.verify(sleeper).sleep(eq(102L));
    inOrder.verify(sleeper).sleep(eq(119L));

    verifyNoMoreInteractions(sleeper);
  }

  @Test
   public void testNoBackoff() throws Exception {
    when(random.nextDouble()).thenReturn(0.5);
    noBackoff.reset();
    noBackoff.sleep();
    noBackoff.sleep();
    verifyZeroInteractions(sleeper);
  }
}
