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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

class ConfigurableSSLSocketFactory extends SSLSocketFactory {

  private final String[] enabledCipherSuites;
  private final SSLSocketFactory sslSocketFactory;

  ConfigurableSSLSocketFactory(final String[] enabledCipherSuites, final SSLSocketFactory sslSocketFactory) {
    this.enabledCipherSuites = enabledCipherSuites;
    this.sslSocketFactory = sslSocketFactory;
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return enabledCipherSuites;
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return enabledCipherSuites;
  }

  @Override
  public Socket createSocket(final Socket sock, final String host, final int port, final boolean autoClose)
      throws IOException {
    final SSLSocket s = (SSLSocket) sslSocketFactory.createSocket(sock, host, port, autoClose);
    if (enabledCipherSuites != null) {
      s.setEnabledCipherSuites(enabledCipherSuites);
    }
    return s;
  }

  @Override
  public Socket createSocket(final String host, final int port) throws IOException {
    final SSLSocket s = (SSLSocket) sslSocketFactory.createSocket(host, port);
    if (enabledCipherSuites != null) {
      s.setEnabledCipherSuites(enabledCipherSuites);
    }
    return s;
  }

  @Override
  public Socket createSocket(final String host, final int port, final InetAddress localAddress, final int localPort)
      throws IOException {
    final SSLSocket s = (SSLSocket) sslSocketFactory.createSocket(host, port, localAddress, localPort);
    if (enabledCipherSuites != null) {
      s.setEnabledCipherSuites(enabledCipherSuites);
    }
    return s;
  }

  @Override
  public Socket createSocket(final InetAddress host, final int port) throws IOException {
    final SSLSocket s = (SSLSocket) sslSocketFactory.createSocket(host, port);
    if (enabledCipherSuites != null) {
      s.setEnabledCipherSuites(enabledCipherSuites);
    }
    return s;
  }

  @Override
  public Socket createSocket(final InetAddress host, final int port, final InetAddress localAddress,
                             final int localPort)
      throws IOException {
    final SSLSocket s = (SSLSocket) sslSocketFactory.createSocket(host, port, localAddress, localPort);
    if (enabledCipherSuites != null) {
      s.setEnabledCipherSuites(enabledCipherSuites);
    }
    return s;
  }
}
