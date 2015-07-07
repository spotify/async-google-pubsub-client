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

import com.google.api.client.auth.oauth2.Credential;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.MoreExecutors.getExitingScheduledExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;

class AccessTokenSupplier implements Supplier<String> {

  private static final Logger log = LoggerFactory.getLogger(AccessTokenSupplier.class);

  private volatile String accessToken;

  private final ScheduledExecutorService executor = getExitingScheduledExecutorService(
      new ScheduledThreadPoolExecutor(1));

  private final Credential credential;

  AccessTokenSupplier(final Credential credential) {
    this.credential = Objects.requireNonNull(credential, "credential");

    // Get initial access token
    refreshAccessToken();
    if (accessToken == null) {
      throw new RuntimeException("Failed to get access token");
    }

    // Wake up every 10 seconds to check if access token has expired
    executor.scheduleAtFixedRate(this::refreshAccessToken, 10, 10, SECONDS);
  }

  /**
   * Refresh the Google Cloud API access token, if necessary.
   */
  private void refreshAccessToken() {
    final Long expiresIn = credential.getExpiresInSeconds();

    // trigger refresh if token is about to expire
    String accessToken = credential.getAccessToken();
    if (accessToken == null || expiresIn != null && expiresIn <= 60) {
      try {
        credential.refreshToken();
        accessToken = credential.getAccessToken();
      } catch (final IOException e) {
        log.error("Failed to fetch access token", e);
      }
    }
    if (accessToken != null) {
      this.accessToken = accessToken;
    }
  }

  @Override
  public String get() {
    return accessToken;
  }
}
