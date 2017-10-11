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

import com.google.api.client.util.Sleeper;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread safe backoff. Assumes threads that use the same backoff instance all share the
 * same rate limit, and throttles best effort until reset isn't called
 */
public class Backoff {

  // non-final for testing only
  private Sleeper sleeper = Sleeper.DEFAULT;
  // non-final for testing only
  private Random random = new Random();

  private final long initialInterval;
  private final int maxBackoffMultiplier;

  private final AtomicInteger backoffMultiplier = new AtomicInteger(1);

  private Backoff(Builder builder) {
    this.initialInterval = builder.initialInterval;
    this.maxBackoffMultiplier = builder.maxBackoffMultiplier;
  }

  public void sleep() {
    // maxBackoffMultiplier of 0 means no backoff
    if (maxBackoffMultiplier == 0) {
      return;
    }

    try {
      int backoff = backoffMultiplier.get();

      if (backoff < maxBackoffMultiplier) {
        // if some other thread updated the backoff already, we don't care
        backoffMultiplier.compareAndSet(backoff, backoff + 1);
      }
      sleeper.sleep((long) (initialInterval * backoff * getRandomizationFactor()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void reset() {
    backoffMultiplier.set(1);
  }

  /**
   * Return a random factor between 0.9 and 1.1
   */
  private double getRandomizationFactor() {
    return 0.9 + random.nextDouble() * 0.2;
  }

  /**
   * Create a builder that can be used to build an {@link Acker}.
   */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private long initialInterval = 0;
    private int maxBackoffMultiplier = 0;

    /**
     * The initial interval in milliseconds between calls
     */
    public Builder initialInterval(final long initialInterval) {
      this.initialInterval = initialInterval;
      return this;
    }

    /**
     * The maximum interval multiplier for backoff (0 is no backoff)
     */
    public Builder maxBackoffMultiplier(final int maxBackoffMultiplier) {
      this.maxBackoffMultiplier = maxBackoffMultiplier;
      return this;
    }

    public Backoff build() {
      return new Backoff(this);
    }
  }
}
