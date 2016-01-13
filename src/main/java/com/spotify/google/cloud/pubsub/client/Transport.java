/*
 * Copyright (c) 2011-2016 Spotify AB
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

import org.jboss.netty.handler.codec.http.HttpMethod;

interface Transport {

  Object NO_PAYLOAD = new Object();

  /**
   * Make a GET request.
   */
  default <T> PubsubFuture<T> get(final String operation, final String path, final Class<T> responseClass) {
    return request(operation, HttpMethod.GET, path, responseClass);
  }

  /**
   * Make a POST request.
   */
  default <T> PubsubFuture<T> post(final String operation, final String path, final Object payload,
                                   final Class<T> responseClass) {
    return request(operation, HttpMethod.POST, path, responseClass, payload);
  }

  /**
   * Make a PUT request.
   */
  default <T> PubsubFuture<T> put(final String operation, final String path, final Object payload,
                                  final Class<T> responseClass) {
    return request(operation, HttpMethod.PUT, path, responseClass, payload);
  }

  /**
   * Make a DELETE request.
   */
  default <T> PubsubFuture<T> delete(final String operation, final String path, final Class<T> responseClass) {
    return request(operation, HttpMethod.DELETE, path, responseClass);
  }

  /**
   * Make an HTTP request.
   */
  default <T> PubsubFuture<T> request(final String operation, final HttpMethod method, final String path,
                                      final Class<T> responseClass) {
    return request(operation, method, path, responseClass, NO_PAYLOAD);
  }

  <T> PubsubFuture<T> request(final String operation, final HttpMethod method, final String path,
                              final Class<T> responseClass, final Object payload);
}
