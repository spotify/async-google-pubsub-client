/**
 * Copyright (C) 2016 Spotify AB
 */

package com.spotify.google.cloud.pubsub.client;

import java.util.concurrent.CompletionStage;

public interface MessageHandler {

  CompletionStage<String> handleMessage(final Message message);
}
