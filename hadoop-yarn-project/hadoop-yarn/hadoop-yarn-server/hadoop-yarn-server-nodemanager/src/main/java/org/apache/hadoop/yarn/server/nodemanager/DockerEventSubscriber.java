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

import com.google.common.eventbus.Subscribe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class DockerEventSubscriber {
  private static final Log LOG = LogFactory
          .getLog(DockerEventBroadcastService.class);
  private final String dockerExecutor;
  private final Path pidFile;
  private final String cid;

  public DockerEventSubscriber(String dockerExecutor, Path pidFile, String cid) {
    this.dockerExecutor = dockerExecutor;
    this.pidFile = pidFile;
    this.cid = cid;
  }

  @Subscribe
  public void handleStartEvent(DockerEvent.DockerContainerStartedEvent event) throws IOException, InterruptedException {
    if (!event.dockerContainerId.equals(cid)) {
      LOG.debug("Not equal: " + event.dockerContainerId.equals(cid));
      return;
    }
    final String dockerPidScript = dockerExecutor + " inspect --format {{.State.Pid}} " + cid;
    LOG.debug("pid script: " + dockerPidScript);
    ProcessBuilder processBuilder = new ProcessBuilder(dockerPidScript.split(" "));

    Process process = processBuilder.start();
    final BufferedReader errReader =
            new BufferedReader(new InputStreamReader(
                    process.getErrorStream(), Charset.defaultCharset()));
    final StringBuffer errMsg = new StringBuffer();
    ExecutorService service = Executors.newSingleThreadExecutor();
    Future<Void> f = service.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {

        String line = errReader.readLine();
        while (line != null) {
          errMsg.append(line);
          errMsg.append(System.getProperty("line.separator"));
          line = errReader.readLine();
        }
        return null;
      }
    });

    try {
      f.get();
    } catch (ExecutionException e) {
      LOG.error("Error getting error out: ", e);
    } finally {
      if (errReader != null) {
        errReader.close();
      }
    }
    try(BufferedReader inReader =
            new BufferedReader(new InputStreamReader(
                    process.getInputStream(), Charset.defaultCharset()))) {
      String line = inReader.readLine();

      int exitCode = process.waitFor();

      if (exitCode != 0) {
        throw new Shell.ExitCodeException(exitCode, "Error: " + dockerPidScript + " error msg: " + errMsg);
      }

      try (OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream(pidFile.toString()),
              Charset.forName("UTF-8").newEncoder())) {
        fw.write(line);
        if (LOG.isDebugEnabled()) {
          LOG.debug("wrote pid: " + line + " file: " + pidFile);
        }
      }
    }
  }
}
