/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.apex;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import org.apache.hadoop.net.NetUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.netlet.DefaultEventLoop;


public class AddressResolutionTest
{
  private static final Logger LOG = LoggerFactory.getLogger(AddressResolutionTest.class);

  static DefaultEventLoop eventloopServer;
  static DefaultEventLoop eventloopClient;

  @BeforeClass
  public static void setupServerAndClients() throws Exception
  {
    try {
      eventloopServer = DefaultEventLoop.createEventLoop("server");
      eventloopClient = DefaultEventLoop.createEventLoop("client");
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    eventloopServer.start();
    eventloopClient.start();

    Server instance = new Server(0, 4096,8);
    InetSocketAddress address = instance.run(eventloopServer);
    Assert.assertTrue(address instanceof InetSocketAddress);
    Assert.assertFalse(address.isUnresolved());

  }

  @Test
  public void test() throws IOException {
    String bufferServerHost;
    int bufferServerPort;

    // start buffer server, if it was not set externally
    Server bufferServer = new Server(0, 4096,8);
    //bufferServer.setAuthToken(ctx.getValue(StreamingContainerContext.BUFFER_SERVER_TOKEN));
    //if (ctx.getValue(Context.DAGContext.BUFFER_SPOOLING)) {
    //  bufferServer.setSpoolStorage(new DiskStorage());
    //}
    SocketAddress bindAddr = bufferServer.run(eventloopServer);
    LOG.debug("Buffer server started: {}", bindAddr);
    InetSocketAddress bufferServerAddress = NetUtils.getConnectAddress(((InetSocketAddress)bindAddr));

    //if (bufferServerAddress != null) {
      bufferServerHost = bufferServerAddress.getHostName();
      bufferServerPort = bufferServerAddress.getPort();
    //}

    // publisher
    InetSocketAddress pubBufferServerAddress;
    pubBufferServerAddress = (InetSocketAddress.createUnresolved(bufferServerHost, bufferServerPort));
    //bssc.put(StreamContext.BUFFER_SERVER_TOKEN, nodi.bufferServerToken);
    InetAddress pubInetAddress = pubBufferServerAddress.getAddress();
    if (pubInetAddress != null && NetUtils.isLocalAddress(pubInetAddress)) {
      pubBufferServerAddress = (new InetSocketAddress(InetAddress.getByName(null), bufferServerPort));
    }
    LOG.info("Publisher buffer server address: {}", pubBufferServerAddress);

    //stram
    InetSocketAddress amAddress = InetSocketAddress.createUnresolved(bufferServerHost, bufferServerPort);
    // capture dynamically assigned address from container
    //if (sca.container.bufferServerAddress == null && heartbeat.bufferServerHost != null) {
    //  sca.container.bufferServerAddress = InetSocketAddress.createUnresolved(heartbeat.bufferServerHost,
    //      heartbeat.bufferServerPort);
      LOG.info("Address in AM {}", amAddress);
    //}

    //deploy
    String portInfo_bufferServerHost = amAddress.getHostName();
    int portInfo_bufferServerPort = amAddress.getPort();

    //subscriber
    {
      InetSocketAddress subBufferServerAddress = (InetSocketAddress.createUnresolved(portInfo_bufferServerHost, portInfo_bufferServerPort));
      InetAddress subInetAddress = subBufferServerAddress.getAddress();
      if (subInetAddress != null && NetUtils.isLocalAddress(subInetAddress)) {
        subBufferServerAddress = new InetSocketAddress(InetAddress.getByName(null), portInfo_bufferServerPort);
      }
      LOG.info("Subscriber buffer server address: {}", subBufferServerAddress);
    }


    Socket socket = null;

    try {
      LOG.info("Writing to {} {}", "localhost", portInfo_bufferServerPort);
      socket = new Socket("localhost", portInfo_bufferServerPort);
      socket.getOutputStream().write("hello".getBytes());
      socket.getOutputStream().flush();
    } finally {
      if (socket != null)
        socket.close();
    }

    try {
      LOG.info("Writing to {} {}", portInfo_bufferServerHost, portInfo_bufferServerPort);
      socket = new Socket(portInfo_bufferServerHost, portInfo_bufferServerPort);
      socket.getOutputStream().write("hello".getBytes());
      socket.getOutputStream().flush();
    } finally {
      if (socket != null)
        socket.close();
    }


  }


}
