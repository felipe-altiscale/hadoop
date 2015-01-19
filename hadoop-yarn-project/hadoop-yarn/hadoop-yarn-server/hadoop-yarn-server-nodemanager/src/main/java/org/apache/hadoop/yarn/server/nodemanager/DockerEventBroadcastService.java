/**
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

package org.apache.hadoop.yarn.server.nodemanager;

import com.google.common.eventbus.EventBus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


class DockerEventBroadcastService extends AbstractService {
private static final Log LOG = LogFactory
        .getLog(DockerEventBroadcastService.class);
  private String dockerExecutor;
  private final ExecutorService executorService;
  private final EventBus eventBus;

  public DockerEventBroadcastService(String dockerExecutor, EventBus eventBus) {
    super(DockerEventBroadcastService.class.getName());
    this.dockerExecutor = dockerExecutor;
    this.eventBus = eventBus;
    this.executorService = Executors.newSingleThreadExecutor();
  }

@Override
protected void serviceStart() throws Exception {
  final String dockerEventsCmd = dockerExecutor + " events";

  executorService.submit(new Callable<Void>() {
    @Override
    public Void call() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("started call " + dockerEventsCmd);
      }
      ProcessBuilder pb = new ProcessBuilder(dockerEventsCmd.split(" "));
      pb.redirectErrorStream(true);

      final Process process = pb.start();
      try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line = br.readLine();
      while (line != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("line " + line);
        }
        if (line != null) {
          String[] words = line.split(" ");
          String cid = words[1].substring(0, words[1].length() - 1);
          switch (words[4]) {
            case "create":
              eventBus.post(new DockerEvent.DockerContainerCreatedEvent(cid));
            case "start":
              LOG.debug("Launched an event " + cid);
              eventBus.post(new DockerEvent.DockerContainerStartedEvent(cid));
              break;
            case "die":
              eventBus.post(new DockerEvent.DockerContainerKilledEvent(cid));
              break;
            default:
              LOG.warn("unknown event: " + words[4]);
          }
        }
        line = br.readLine();
      }

      }
      return null;

    }
  });

}

@Override
protected void serviceStop() throws Exception {
  if(executorService != null)        {
    executorService.shutdown();
  }
}

}
