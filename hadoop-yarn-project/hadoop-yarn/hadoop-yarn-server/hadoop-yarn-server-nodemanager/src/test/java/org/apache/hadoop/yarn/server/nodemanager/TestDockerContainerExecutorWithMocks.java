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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Mock tests for docker container executor
 */
public class TestDockerContainerExecutorWithMocks {

  private static final Log LOG = LogFactory
      .getLog(TestDockerContainerExecutorWithMocks.class);
  public static final String DOCKER_URL = "localhost:4243";
  private DockerContainerExecutor dockerContainerExecutor = null;
  private final File mockParamFile = new File("./params.txt");
  private LocalDirsHandlerService dirsHandler;
  private Path workDir;
  private FileContext lfs;
  private String yarnImage;

  @Before
  public void setup() {
    assumeTrue(Shell.LINUX);
    File f = new File("./src/test/resources/mock-container-executor");
    if(!FileUtil.canExecute(f)) {
      FileUtil.setExecutable(f, true);
    }
    String executorPath = f.getAbsolutePath();
    Configuration conf = new Configuration();
    yarnImage = "yarnImage";
    long time = System.currentTimeMillis();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, executorPath);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, "/tmp/nm-local-dir" + time);
    conf.set(YarnConfiguration.NM_LOG_DIRS, "/tmp/userlogs" + time);
    conf.set(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, yarnImage);
    conf.set(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_DOCKER_URL, DOCKER_URL);
    dockerContainerExecutor = new DockerContainerExecutor();
    dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    dockerContainerExecutor.setConf(conf);
    lfs = null;
    try {
      lfs = FileContext.getLocalFSFileContext();
      workDir = new Path("/tmp/temp-"+ System.currentTimeMillis());
      lfs.mkdir(workDir, FsPermission.getDirDefault(), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @After
  public void tearDown() {
    try {
      if (lfs != null) {
        lfs.delete(workDir, true);
      }
      deleteMockParamFile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testContainerInitSecure() throws IOException {
    dockerContainerExecutor.getConf().set(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    dockerContainerExecutor.init();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testContainerLaunchNullImage() throws IOException {
    String appSubmitter = "nobody";
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";
    String testImage = "";

    Container container = mock(Container.class, RETURNS_DEEP_STUBS);
    ContainerId cId = mock(ContainerId.class, RETURNS_DEEP_STUBS);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String,String>();

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);
    when(cId.getApplicationAttemptId().getApplicationId().toString()).thenReturn(appId);
    when(cId.toString()).thenReturn(containerId);

    when(context.getEnvironment()).thenReturn(env);
    env.put(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, testImage);
    dockerContainerExecutor.getConf()
        .set(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, testImage);
    Path scriptPath = new Path("file:///bin/echo");
    Path tokensPath = new Path("file:///dev/null");

    Path pidFile = new Path(workDir, "pid.txt");

    dockerContainerExecutor.activateContainer(cId, pidFile);
    dockerContainerExecutor.launchContainer(container, scriptPath, tokensPath,
        appSubmitter, appId, workDir, dirsHandler.getLocalDirs(),
        dirsHandler.getLogDirs());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testContainerLaunchInvalidImage() throws IOException {
    String appSubmitter = "nobody";
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";
    String testImage = "testrepo.com/test-image rm -rf $HADOOP_PREFIX/*";

    Container container = mock(Container.class, RETURNS_DEEP_STUBS);
    ContainerId cId = mock(ContainerId.class, RETURNS_DEEP_STUBS);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String,String>();

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);
    when(cId.getApplicationAttemptId().getApplicationId().toString()).thenReturn(appId);
    when(cId.toString()).thenReturn(containerId);

    when(context.getEnvironment()).thenReturn(env);
    env.put(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, testImage);
    dockerContainerExecutor.getConf()
      .set(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, testImage);
    Path scriptPath = new Path("file:///bin/echo");
    Path tokensPath = new Path("file:///dev/null");

    Path pidFile = new Path(workDir, "pid.txt");

    dockerContainerExecutor.activateContainer(cId, pidFile);
    dockerContainerExecutor.launchContainer(container, scriptPath, tokensPath,
      appSubmitter, appId, workDir, dirsHandler.getLocalDirs(),
      dirsHandler.getLogDirs());
  }

  @Test
  public void testContainerLaunch() throws IOException {
    String appSubmitter = "nobody";
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";
    String testImage = "\"sequenceiq/hadoop-docker:2.4.1\"";

    Container container = mock(Container.class, RETURNS_DEEP_STUBS);
    ContainerId cId = mock(ContainerId.class, RETURNS_DEEP_STUBS);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String,String>();

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);
    when(cId.getApplicationAttemptId().getApplicationId().toString()).thenReturn(appId);
    when(cId.toString()).thenReturn(containerId);

    when(context.getEnvironment()).thenReturn(env);
    env.put(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, testImage);
    Path scriptPath = new Path("file:///bin/echo");
    Path tokensPath = new Path("file:///dev/null");

    Path pidFile = new Path(workDir, "pid");
    dockerContainerExecutor.activateContainer(cId, pidFile);
    Shell.ShellCommandExecutor shellExec = dockerContainerExecutor.createShellExec(container, workDir, dirsHandler.getLocalDirs(),
            dirsHandler.getLogDirs(), scriptPath, tokensPath,
            appSubmitter, appId);

    List<String> localDirs = dirsToMount(dirsHandler.getLocalDirs());
    List<String> logDirs = dirsToMount(dirsHandler.getLogDirs());
    List<String> expectedParams =  new ArrayList<String>(
        Arrays.asList(appSubmitter, appSubmitter, LinuxContainerExecutor.Commands.CREATE_DOCKER_CONTAINER.getValue() + "",
                appId, containerId, workDir.toUri().getPath(), scriptPath.toUri().getPath(),
                tokensPath.toUri().getPath()));
    expectedParams.addAll(dirsHandler.getLocalDirs());
    expectedParams.addAll(dirsHandler.getLogDirs());
    expectedParams.addAll(Arrays.asList(
            "docker", "-H", DOCKER_URL, "create", "--net", "host",  "--name",
                containerId, "--user", "nobody", "--workdir", workDir.toUri().getPath(),
                "-v", "/etc/passwd:/etc/passwd:ro"));
    expectedParams.addAll(localDirs);
    expectedParams.addAll(logDirs);
    String shellScript =  workDir + "/launch_container.sh";

    expectedParams.addAll(Arrays.asList(testImage.replaceAll("['\"]", ""), "bash", shellScript));

    assertEquals(expectedParams, readMockParams());
  }
  private List<String> readMockParams() throws IOException {
    LinkedList<String> ret = new LinkedList<String>();
    LineNumberReader reader = new LineNumberReader(new FileReader(
            mockParamFile));
    String line;
    while((line = reader.readLine()) != null) {
      ret.add(line);
    }
    reader.close();
    return ret;
  }
  private void deleteMockParamFile() {
    if(mockParamFile.exists()) {
      mockParamFile.delete();
    }
  }

  private List<String> dirsToMount(List<String> dirs) {
    List<String> localDirs = new ArrayList<String>();
    for(String dir: dirs){
      localDirs.add("-v");
      localDirs.add(dir + ":" + dir);
    }
    return localDirs;
  }
}
