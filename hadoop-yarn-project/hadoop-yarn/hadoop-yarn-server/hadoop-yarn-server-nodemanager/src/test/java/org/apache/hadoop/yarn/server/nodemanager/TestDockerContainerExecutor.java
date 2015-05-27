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

import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This is intended to test the DockerContainerExecutor code, but it requires docker
 * to be installed.
 * <br><ol>
 * <li>Compile the code with container-executor.conf.dir set to the location you
 * want for testing.
 * <br><pre><code>
 * > mvn clean install -Pnative -Dcontainer-executor.conf.dir=/etc/hadoop
 *                          -DskipTests
 * </code></pre>
 *
 * <li>Set up <code>${container-executor.conf.dir}/container-executor.cfg</code>
 * container-executor.cfg needs to be owned by root and have in it the proper
 * config values.
 * <br><pre><code>
 * > cat /etc/hadoop/container-executor.cfg
 * yarn.nodemanager.linux-container-executor.group=yarn
 * #depending on the user id of the application.submitter option
 * min.user.id=1
 * > sudo chown root:root /etc/hadoop/container-executor.cfg
 * > sudo chmod 444 /etc/hadoop/container-executor.cfg
 * </code></pre>
 *
 * <li>Move the binary and set proper permissions on it. It needs to be owned
 * by root, the group needs to be the group configured in container-executor.cfg,
 * and it needs the setuid bit set. (The build will also overwrite it so you
 * need to move it to a place that you can support it.
 * <br><pre><code>
 * > cp ./hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/c/container-executor/container-executor /tmp/
 * > sudo chown root:yarn /tmp/container-executor
 * > sudo chmod 4550 /tmp/container-executor
 * </code></pre>
 *
 * <li>Run the tests with the execution enabled (The user you run the tests as
 * needs to be part of the group from the config.
 * <li>Install docker, and Compile the code with docker-service-url set to the host and port
 * where docker service is running.
 * <br><pre><code>
 * mvn test -Dtest=TestDockerContainerExecutor -Dapplication.submitter=nobody -Dcontainer-executor.path=/tmp/container-executor -Ddocker-service-url=tcp://0.0.0.0:4243
 * </code></pre>
 * </ol>
 */
public class TestDockerContainerExecutor {
private static final Log LOG = LogFactory
        .getLog(TestDockerContainerExecutor.class);
private static File workSpace = null;
private DockerContainerExecutor exec = null;
private LocalDirsHandlerService dirsHandler;
private FileContext files;
private String yarnImage;

private int id = 0;
private String appSubmitter;
private Configuration conf;
private String testImage = "centos";
static {
  String basedir = System.getProperty("workspace.dir");
  if(basedir == null || basedir.isEmpty()) {
    basedir = "target";
  }
  workSpace = new File(basedir,
          TestLinuxContainerExecutor.class.getName() + "-workSpace");
}
private ContainerId getNextContainerId() {
  ContainerId cId = mock(ContainerId.class, RETURNS_DEEP_STUBS);
  String id = "CONTAINER_" + System.currentTimeMillis();
  when(cId.toString()).thenReturn(id);
  return cId;
}

@Before
public void setup() throws Exception {
  yarnImage = "yarn-test-image";
  files = FileContext.getLocalFSFileContext();
  Path workSpacePath = new Path(workSpace.getAbsolutePath());
  files.mkdir(workSpacePath, null, true);
  FileUtil.chmod(workSpace.getAbsolutePath(), "777");
  File localDir = new File(workSpace.getAbsoluteFile(), "localDir");
  files.mkdir(new Path(localDir.getAbsolutePath()), new FsPermission("777"),
          false);
  File logDir = new File(workSpace.getAbsoluteFile(), "logDir");
  files.mkdir(new Path(logDir.getAbsolutePath()), new FsPermission("777"),
          false);
  String exec_path = System.getProperty("container-executor.path");
  if (exec_path != null && !exec_path.isEmpty()) {
    conf = new Configuration(false);
    conf.setClass("fs.AbstractFileSystem.file.impl",
            org.apache.hadoop.fs.local.LocalFs.class,
            org.apache.hadoop.fs.AbstractFileSystem.class);

    appSubmitter = System.getProperty("application.submitter");
    if (appSubmitter == null || appSubmitter.isEmpty()) {
      appSubmitter = "nobody";
    }

    conf.set(YarnConfiguration.NM_NONSECURE_MODE_LOCAL_USER_KEY, appSubmitter);
    conf.set(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, yarnImage);

    LOG.info("Setting " + YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH
            + "=" + exec_path);
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, exec_path);
    exec = new DockerContainerExecutor();
    exec.setConf(conf);

    dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    List<String> localDirs = dirsHandler.getLocalDirs();
    for (String dir : localDirs) {
      Path userDir = new Path(dir, ContainerLocalizer.USERCACHE);
      files.mkdir(userDir, new FsPermission("777"), false);
      // $local/filecache
      Path fileDir = new Path(dir, ContainerLocalizer.FILECACHE);
      files.mkdir(fileDir, new FsPermission("777"), false);
    }
    shellExec("docker pull " + testImage);
    exec.init();
  }




}

private Shell.ShellCommandExecutor shellExec(String command) {
  try {

    Shell.ShellCommandExecutor shExec = new Shell.ShellCommandExecutor(
            command.split("\\s+"),
            new File(workSpace.getAbsolutePath()),
            System.getenv());
    shExec.execute();
    return shExec;
  } catch (IOException e) {
    throw new RuntimeException(e);
  }
}

private boolean shouldRun() {
  return exec != null;
}

private int runAndBlock(ContainerId cId, Map<String, String> launchCtxEnv, String... cmd) throws IOException {
  String appId = "APP_" + System.currentTimeMillis();
  Container container = mock(Container.class);
  ContainerLaunchContext context = mock(ContainerLaunchContext.class);

  when(container.getContainerId()).thenReturn(cId);
  when(container.getLaunchContext()).thenReturn(context);
  when(cId.getApplicationAttemptId().getApplicationId().toString()).thenReturn(appId);
  when(context.getEnvironment()).thenReturn(launchCtxEnv);

  String script = writeScriptFile(launchCtxEnv, cmd);

  Path scriptPath = new Path(script);
  Path tokensPath = new Path("/dev/null");
  Path workDir = new Path(workSpace.getAbsolutePath());
  Path pidFile = new Path(workDir, "pid.txt");

  exec.activateContainer(cId, pidFile);
  return exec.launchContainer(container, scriptPath, tokensPath,
          appSubmitter, appId, workDir, dirsHandler.getLocalDirs(),
          dirsHandler.getLogDirs());
}

private String writeScriptFile(Map<String, String> launchCtxEnv, String... cmd) throws IOException {
  File f = File.createTempFile("TestDockerContainerExecutor", ".sh");
  f.deleteOnExit();
  ByteArrayOutputStream baos = new ByteArrayOutputStream();
  PrintWriter p = new PrintWriter(new FileOutputStream(f));
  PrintWriter q = new PrintWriter(baos);
  Set<String> exclusionSet = new HashSet<String>();
  exclusionSet.add(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);
  exclusionSet.add(ApplicationConstants.Environment.HADOOP_YARN_HOME.name());
  exclusionSet.add(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name());
  exclusionSet.add(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name());
  exclusionSet.add(ApplicationConstants.Environment.HADOOP_CONF_DIR.name());
  exclusionSet.add(ApplicationConstants.Environment.JAVA_HOME.name());
  for(Map.Entry<String, String> entry: launchCtxEnv.entrySet()) {
    if (!exclusionSet.contains(entry.getKey())) {
      p.println("export " + entry.getKey() + "=\"" + entry.getValue() + "\"");
      q.println("export " + entry.getKey() + "=\"" + entry.getValue() + "\"");
    }
  }
  for (String part : cmd) {
    p.print(part.replace("\\", "\\\\").replace("'", "\\'"));
    p.print(" ");
    q.print(part.replace("\\", "\\\\").replace("'", "\\'"));
    q.print(" ");
  }
  p.println();
  p.close();
  q.println();
  q.close();
  if (LOG.isDebugEnabled()) {
    LOG.debug("Launch script: " + baos.toString("UTF-8"));
  }
  return f.getAbsolutePath();
}

@After
public void tearDown() {
  try {
    files.delete(new Path(workSpace.getAbsolutePath()), true);
  } catch (IOException e) {
    throw new RuntimeException(e);
  }
}

@Test
public void testLaunchContainer() throws IOException {
  if (!shouldRun()) {
    LOG.warn("Docker not installed, aborting test.");
    return;
  }

  Map<String, String> env = new HashMap<String, String>();
  env.put(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, testImage);
  String touchFileName = "touch-file-" + System.currentTimeMillis();
  ContainerId cId = getNextContainerId();
  int ret = runAndBlock(
          cId, env, "touch", "/tmp/" + touchFileName);

  assertEquals(0, ret);
}
}
