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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class PubsubFuture<T> extends CompletableFuture<T> {

  private final RequestInfo requestInfo;

  PubsubFuture(final RequestInfo requestInfo) {
    this.requestInfo = requestInfo;
  }

  public String operation() {
    return requestInfo.operation();
  }

  public String method() {
    return requestInfo.method();
  }

  public String uri() {
    return requestInfo.uri();
  }

  public long payloadSize() {
    return requestInfo.payloadSize();
  }

  @Override
  public <U> PubsubFuture<U> thenApply(final Function<? super T, ? extends U> fn) {
    return wrap(super.thenApply(fn));
  }

  @Override
  public <U> PubsubFuture<U> thenApplyAsync(final Function<? super T, ? extends U> fn) {
    return wrap(super.thenApplyAsync(fn));
  }

  @Override
  public <U> PubsubFuture<U> thenApplyAsync(final Function<? super T, ? extends U> fn, final Executor executor) {
    return wrap(super.thenApplyAsync(fn, executor));
  }

  @Override
  public PubsubFuture<Void> thenAccept(final Consumer<? super T> action) {
    return wrap(super.thenAccept(action));
  }

  @Override
  public PubsubFuture<Void> thenAcceptAsync(final Consumer<? super T> action) {
    return wrap(super.thenAcceptAsync(action));
  }

  @Override
  public PubsubFuture<Void> thenAcceptAsync(final Consumer<? super T> action, final Executor executor) {
    return wrap(super.thenAcceptAsync(action, executor));
  }

  @Override
  public PubsubFuture<Void> thenRun(final Runnable action) {
    return wrap(super.thenRun(action));
  }

  @Override
  public PubsubFuture<Void> thenRunAsync(final Runnable action) {
    return wrap(super.thenRunAsync(action));
  }

  @Override
  public PubsubFuture<Void> thenRunAsync(final Runnable action, final Executor executor) {
    return wrap(super.thenRunAsync(action, executor));
  }

  @Override
  public <U, V> PubsubFuture<V> thenCombine(final CompletionStage<? extends U> other,
                                                 final BiFunction<? super T, ? super U, ? extends V> fn) {
    return wrap(super.thenCombine(other, fn));
  }

  @Override
  public <U, V> PubsubFuture<V> thenCombineAsync(final CompletionStage<? extends U> other,
                                                      final BiFunction<? super T, ? super U, ? extends V> fn) {
    return wrap(super.thenCombineAsync(other, fn));
  }

  @Override
  public <U, V> PubsubFuture<V> thenCombineAsync(final CompletionStage<? extends U> other,
                                                      final BiFunction<? super T, ? super U, ? extends V> fn,
                                                      final Executor executor) {
    return wrap(super.thenCombineAsync(other, fn, executor));
  }

  @Override
  public <U> PubsubFuture<Void> thenAcceptBoth(final CompletionStage<? extends U> other,
                                                    final BiConsumer<? super T, ? super U> action) {
    return wrap(super.thenAcceptBoth(other, action));
  }

  @Override
  public <U> PubsubFuture<Void> thenAcceptBothAsync(final CompletionStage<? extends U> other,
                                                         final BiConsumer<? super T, ? super U> action) {
    return wrap(super.thenAcceptBothAsync(other, action));
  }

  @Override
  public <U> PubsubFuture<Void> thenAcceptBothAsync(final CompletionStage<? extends U> other,
                                                         final BiConsumer<? super T, ? super U> action,
                                                         final Executor executor) {
    return wrap(super.thenAcceptBothAsync(other, action, executor));
  }

  @Override
  public PubsubFuture<Void> runAfterBoth(final CompletionStage<?> other, final Runnable action) {
    return wrap(super.runAfterBoth(other, action));
  }

  @Override
  public PubsubFuture<Void> runAfterBothAsync(final CompletionStage<?> other, final Runnable action) {
    return wrap(super.runAfterBothAsync(other, action));
  }

  @Override
  public PubsubFuture<Void> runAfterBothAsync(final CompletionStage<?> other, final Runnable action,
                                              final Executor executor) {
    return wrap(super.runAfterBothAsync(other, action, executor));
  }

  @Override
  public <U> PubsubFuture<U> applyToEither(final CompletionStage<? extends T> other,
                                           final Function<? super T, U> fn) {
    return wrap(super.applyToEither(other, fn));
  }

  @Override
  public <U> PubsubFuture<U> applyToEitherAsync(final CompletionStage<? extends T> other,
                                                final Function<? super T, U> fn) {
    return wrap(super.applyToEitherAsync(other, fn));
  }

  @Override
  public <U> PubsubFuture<U> applyToEitherAsync(final CompletionStage<? extends T> other,
                                                final Function<? super T, U> fn,
                                                final Executor executor) {
    return wrap(super.applyToEitherAsync(other, fn, executor));
  }

  @Override
  public PubsubFuture<Void> acceptEither(final CompletionStage<? extends T> other,
                                         final Consumer<? super T> action) {
    return wrap(super.acceptEither(other, action));
  }

  @Override
  public PubsubFuture<Void> acceptEitherAsync(final CompletionStage<? extends T> other,
                                              final Consumer<? super T> action) {
    return wrap(super.acceptEitherAsync(other, action));
  }

  @Override
  public PubsubFuture<Void> acceptEitherAsync(final CompletionStage<? extends T> other,
                                              final Consumer<? super T> action,
                                              final Executor executor) {
    return wrap(super.acceptEitherAsync(other, action, executor));
  }

  @Override
  public PubsubFuture<Void> runAfterEither(final CompletionStage<?> other, final Runnable action) {
    return wrap(super.runAfterEither(other, action));
  }

  @Override
  public PubsubFuture<Void> runAfterEitherAsync(final CompletionStage<?> other, final Runnable action) {
    return wrap(super.runAfterEitherAsync(other, action));
  }

  @Override
  public PubsubFuture<Void> runAfterEitherAsync(final CompletionStage<?> other, final Runnable action,
                                                final Executor executor) {
    return wrap(super.runAfterEitherAsync(other, action, executor));
  }

  @Override
  public <U> PubsubFuture<U> thenCompose(final Function<? super T, ? extends CompletionStage<U>> fn) {
    return wrap(super.thenCompose(fn));
  }

  @Override
  public <U> PubsubFuture<U> thenComposeAsync(final Function<? super T, ? extends CompletionStage<U>> fn) {
    return wrap(super.thenComposeAsync(fn));
  }

  @Override
  public <U> PubsubFuture<U> thenComposeAsync(final Function<? super T, ? extends CompletionStage<U>> fn,
                                              final Executor executor) {
    return wrap(super.thenComposeAsync(fn, executor));
  }

  @Override
  public PubsubFuture<T> whenComplete(final BiConsumer<? super T, ? super Throwable> action) {
    return wrap(super.whenComplete(action));
  }

  @Override
  public PubsubFuture<T> whenCompleteAsync(final BiConsumer<? super T, ? super Throwable> action) {
    return wrap(super.whenCompleteAsync(action));
  }

  @Override
  public PubsubFuture<T> whenCompleteAsync(final BiConsumer<? super T, ? super Throwable> action,
                                           final Executor executor) {
    return wrap(super.whenCompleteAsync(action, executor));
  }

  @Override
  public <U> PubsubFuture<U> handle(final BiFunction<? super T, Throwable, ? extends U> fn) {
    return wrap(super.handle(fn));
  }

  @Override
  public <U> PubsubFuture<U> handleAsync(final BiFunction<? super T, Throwable, ? extends U> fn) {
    return wrap(super.handleAsync(fn));
  }

  @Override
  public <U> PubsubFuture<U> handleAsync(final BiFunction<? super T, Throwable, ? extends U> fn,
                                         final Executor executor) {
    return wrap(super.handleAsync(fn, executor));
  }

  @Override
  public PubsubFuture<T> toCompletableFuture() {
    return this;
  }

  @Override
  public PubsubFuture<T> exceptionally(final Function<Throwable, ? extends T> fn) {
    return wrap(super.exceptionally(fn));
  }

  @Override
  public boolean complete(final T value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean completeExceptionally(final Throwable ex) {
    throw new UnsupportedOperationException();
  }

  private <U> PubsubFuture<U> wrap(final CompletableFuture<U> future) {
    final PubsubFuture<U> pubsubFuture = new PubsubFuture<>(requestInfo);
    future.whenComplete((v, t) -> {
      if (t != null) {
        pubsubFuture.fail(t);
      } else {
        pubsubFuture.succeed(v);
      }
    });
    return pubsubFuture;
  }

  public boolean succeed(final T value) {
    return super.complete(value);
  }

  public boolean fail(final Throwable ex) {
    return super.completeExceptionally(ex);
  }

  public static <T> PubsubFuture<T> of(final RequestInfo requestInfo) {
    return new PubsubFuture<>(requestInfo);
  }

  public static <T> PubsubFuture<T> succeededFuture(final RequestInfo requestInfo, final T value) {
    final PubsubFuture<T> future = new PubsubFuture<>(requestInfo);
    future.succeed(value);
    return future;
  }

  public static <T> PubsubFuture<T> failedFuture(final RequestInfo requestInfo, final Throwable t) {
    final PubsubFuture<T> future = new PubsubFuture<>(requestInfo);
    future.fail(t);
    return future;
  }
}
