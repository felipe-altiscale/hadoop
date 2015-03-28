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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * This executor will launch a docker container and run the task inside the container.
 */
public class DockerContainerExecutor extends LinuxContainerExecutor {

enum Mode {
  dce, lce, both;
}
private static final Log LOG = LogFactory
        .getLog(DockerContainerExecutor.class);
// This validates that the image is a proper docker image and would not crash docker.
public static final String DOCKER_IMAGE_PATTERN = "^(([\\w\\.-]+)(:\\d+)*\\/)?[\\w\\.:-]+$";


private final FileContext lfs;
private final Pattern dockerImagePattern;
private final EventBus eventBus;
private Mode mode;
public DockerContainerExecutor() {
  try {
    this.lfs = FileContext.getLocalFSFileContext();
    this.dockerImagePattern = Pattern.compile(DOCKER_IMAGE_PATTERN);
    this.eventBus = new EventBus();
  } catch (UnsupportedFileSystemException e) {
    throw new RuntimeException(e);
  }
}

@Override
public void init() throws IOException {
  super.init();
  mode = Mode.valueOf(getConf().get(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_MODE,
          YarnConfiguration.NM_DEFAULT_DOCKER_CONTAINER_EXECUTOR_MODE));
  if (mode == Mode.lce) {
    return;
  }
  String auth = getConf().get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);
  if (auth != null && !auth.equals("simple")) {
    throw new IllegalStateException("DockerContainerExecutor only works with simple authentication mode");
  }
  String dockerUrl = getConf().get(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_DOCKER_URL,
          YarnConfiguration.NM_DEFAULT_DOCKER_CONTAINER_EXECUTOR_DOCKER_URL);
  if (LOG.isDebugEnabled()) {
    LOG.debug("dockerUrl: " + dockerUrl);
  }

  if (Strings.isNullOrEmpty(dockerUrl)) {
    throw new IllegalStateException("DockerUrl must be configured");
  }
}

@Override
public int launchContainer(Container container,
                           Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
                           String userName, String appId, Path containerWorkDir,
                           List<String> localDirs, List<String> logDirs) throws IOException {
  if (isModeLCE(container.getLaunchContext().getEnvironment())) {
    return super.launchContainer(container, nmPrivateContainerScriptPath, nmPrivateTokensPath,
            userName, appId, containerWorkDir, localDirs, logDirs);
  }
  Info info = new Info(container, containerWorkDir, localDirs, logDirs).invoke();
  ShellCommandExecutor shExec = null;

  try {
    if (isContainerActive(info.containerId)) {
      shExec = createShellExec(info, nmPrivateContainerScriptPath, nmPrivateTokensPath, userName, appId);
      String cid = shExec.getOutput();
      if (cid.length() > 1) {
        cid = cid.substring(0, cid.length() - 1);
      }
      shExec = startShellExec(info, nmPrivateContainerScriptPath, nmPrivateTokensPath, userName, appId, cid);
      return shExec.getExitCode();
    } else {
      LOG.info("Container " + info.containerIdStr +
              " was marked as inactive. Returning terminated error");
      return ExitCode.TERMINATED.getExitCode();
    }
  } finally {
    if (shExec != null) {
      shExec.close();
    }
  }
}

private void handleShellException(Info info, ShellCommandExecutor shExec, IOException e) {
  int exitCode = shExec.getExitCode();
  LOG.warn("Exit code from container " + info.containerId + " is : " + exitCode);
  if (exitCode != ExitCode.FORCE_KILLED.getExitCode()
          && exitCode != ExitCode.TERMINATED.getExitCode()) {
    LOG.warn("Exception from container-launch with container ID: "
            + info.containerId + " and exit code: " + exitCode , e);

    StringBuilder builder = new StringBuilder();
    builder.append("Exception from container-launch.\n");
    builder.append("Container id: " + info.containerId + "\n");
    builder.append("Exit code: " + exitCode + "\n");
    if (!Optional.fromNullable(e.getMessage()).or("").isEmpty()) {
      builder.append("Exception message: " + e.getMessage() + "\n");
    }
    builder.append("Stack trace: "
            + StringUtils.stringifyException(e) + "\n");
    if (!shExec.getOutput().isEmpty()) {
      builder.append("Shell output: " + shExec.getOutput() + "\n");
    }
    String diagnostics = builder.toString();
    logOutput(diagnostics);
    info.container.handle(new ContainerDiagnosticsUpdateEvent(info.containerId,
            diagnostics));
  } else {
    info.container.handle(new ContainerDiagnosticsUpdateEvent(info.containerId,
            "Container killed on request. Exit code is " + exitCode));
  }
}

ShellCommandExecutor createShellExec(Container container, Path containerWorkDir, List<String> localDirs, List<String> logDirs, Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath, String userName, String appId) throws IOException {
  Info info = new Info(container, containerWorkDir, localDirs, logDirs).invoke();
  return createShellExec(info, nmPrivateContainerScriptPath, nmPrivateTokensPath, userName, appId);
}

private ShellCommandExecutor createShellExec(Info info, Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath, String userName, String appId) throws IOException {
  List<String> commandStr = Lists.newArrayList("docker", "-H", info.dockerUrl, "create",
          "--net", "host", "--name", info.containerIdStr, "--user", userName, "--workdir",
          info.containerWorkDir.toUri().getPath(), "-v", "/etc/passwd:/etc/passwd:ro");
  commandStr.addAll(Arrays.asList(info.localMounts));
  commandStr.addAll(Arrays.asList(info.logMounts));
  commandStr.add(info.containerImageName.trim());
  commandStr.add("bash");
  commandStr.add(info.launchDst.toUri().getPath());
  // Setup command to run
  if (LOG.isDebugEnabled()) {
    LOG.debug("createContainer: " + Joiner.on(" ").join(commandStr));
  }
  List<String> createContainerCommand = Arrays.asList(containerExecutorExe, userName, userName, Integer
                  .toString(Commands.CREATE_DOCKER_CONTAINER.getValue()), appId,
          info.containerIdStr, info.containerWorkDir.toString(),
          nmPrivateContainerScriptPath.toUri().getPath().toString(),
          nmPrivateTokensPath.toUri().getPath().toString(),
          StringUtils.join(",", info.localDirs),
          StringUtils.join(",", info.logDirs));
  List<String> command = new ArrayList<String>();
  command.addAll(createContainerCommand);
  command.addAll(commandStr);
  ShellCommandExecutor shExec = new ShellCommandExecutor(command.toArray(new String[command.size()])
          , null, // NM's cwd
          info.container.getLaunchContext().getEnvironment()); // sanitized env
  if (LOG.isDebugEnabled()) {
    LOG.debug("command: " + command);
  }
  return executeShell(info, shExec);
}

private ShellCommandExecutor executeShell(Info info, ShellCommandExecutor shExec) {
  try {
    shExec.execute();
  } catch (IOException e) {
    handleShellException(info, shExec, e);
  }  finally {
    if (shExec != null) {
      if (LOG.isDebugEnabled()) {
        logOutput(shExec.getOutput());
      }
      shExec.close();
    }
  }
  return shExec;
}

private ShellCommandExecutor startShellExec(Info info, Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath, String userName, String appId, String cid) throws IOException {
  List<String> manageContainerCommand = Arrays.asList(containerExecutorExe, userName, userName, Integer
                  .toString(Commands.MANAGE_DOCKER_CONTAINER.getValue()), appId,
          info.containerIdStr, info.containerWorkDir.toString(),
          nmPrivateContainerScriptPath.toUri().getPath().toString(),
          nmPrivateTokensPath.toUri().getPath().toString(),
          StringUtils.join(",", info.localDirs),
          StringUtils.join(",", info.logDirs));
  DockerEventSubscriber subscriber = new DockerEventSubscriber(
          Joiner.on(" ").join(Lists.newArrayList("docker", "-H", info.dockerUrl)),
          getPidFilePath(info.containerId),
          cid);
  eventBus.register(subscriber);
  List<String> containerStartCommand = new ArrayList<>(manageContainerCommand);

  List<String> dockerStartScript = Arrays.asList("docker", "-H", info.dockerUrl, "start", "-a", info.containerIdStr);
  containerStartCommand.addAll(dockerStartScript);
  ShellCommandExecutor shExec = new ShellCommandExecutor(
          containerStartCommand.toArray(new String[containerStartCommand.size()]),
          null, // NM's cwd
          info.container.getLaunchContext().getEnvironment()); // sanitized env
  if (LOG.isDebugEnabled()) {
    LOG.debug("startContainer: " + Joiner.on(" ").join(dockerStartScript));
  }
//  try {
//    shExec.execute();
//  } finally {
//    if (LOG.isDebugEnabled()) {
//      logOutput(shExec.getOutput());
//    }
//  }
//  List<String> containerRmCommand = new ArrayList<>(manageContainerCommand);
//  List<String> dockerRmScript = Arrays.asList("docker", "-H", info.dockerUrl, "rm", info.containerIdStr);
//  containerRmCommand.addAll(dockerRmScript);
//  shExec = new ShellCommandExecutor(
//          containerRmCommand.toArray(new String[containerRmCommand.size()]),
//          null, // NM's cwd
//          info.container.getLaunchContext().getEnvironment()); // sanitized env
  return executeShell(info, shExec);
}

public boolean isModeLCE(Map<String, String> environment) {
  Mode userMode = null;
  if (mode == Mode.lce) {

    return true;
  }
  if (mode != Mode.both) {
    return false;
  }
  if(environment.containsKey(YarnConfiguration.NM_DOCKER_CONTAINER_USER_MODE)) {
    try {
      userMode = Mode.valueOf(environment.get(YarnConfiguration.NM_DOCKER_CONTAINER_USER_MODE));
    } catch (IllegalArgumentException ie) {
      LOG.warn("User provided illegal mode: " + environment.get(YarnConfiguration.NM_DOCKER_CONTAINER_USER_MODE), ie);
    }
  }
  LOG.debug("Mode_is_set_to " + userMode);
  return userMode == Mode.lce;
}

@Override
public void writeLaunchEnv(OutputStream out, Map<String, String> environment,
                           Map<Path, List<String>> resources, List<String> command)
        throws IOException {
  if (isModeLCE(environment)) {
    super.writeLaunchEnv(out, environment, resources, command);
    return;
  }
  ContainerLaunch.ShellScriptBuilder sb = ContainerLaunch.ShellScriptBuilder.create();

  Set<String> exclusionSet = new HashSet<String>();
  exclusionSet.add(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);
  exclusionSet.add(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_MODE);
  exclusionSet.add(YarnConfiguration.NM_DOCKER_CONTAINER_USER_MODE);
  exclusionSet.add(ApplicationConstants.Environment.HADOOP_YARN_HOME.name());
  exclusionSet.add(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name());
  exclusionSet.add(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name());
  exclusionSet.add(ApplicationConstants.Environment.HADOOP_CONF_DIR.name());
  exclusionSet.add(ApplicationConstants.Environment.JAVA_HOME.name());

  if (environment != null) {
    for (Map.Entry<String, String> env : environment.entrySet()) {
      if (!exclusionSet.contains(env.getKey())) {
        sb.env(env.getKey().toString(), env.getValue().toString());
      }
    }
  }
  if (resources != null) {
    for (Map.Entry<Path, List<String>> entry : resources.entrySet()) {
      for (String linkName : entry.getValue()) {
        sb.symlink(entry.getKey(), new Path(linkName));
      }
    }
  }

  sb.command(command);

  PrintStream pout = null;
  PrintStream ps = null;
  ByteArrayOutputStream baos = new ByteArrayOutputStream();
  try {
    pout = new PrintStream(out, false, "UTF-8");
    if (LOG.isDebugEnabled()) {
      ps = new PrintStream(baos, false, "UTF-8");
      sb.write(ps);
    }
    sb.write(pout);

  } finally {
    if (out != null) {
      out.close();
    }
    if (ps != null) {
      ps.close();
    }
  }
  if (LOG.isDebugEnabled()) {
    LOG.debug("Script: " + baos.toString("UTF-8"));
  }
}

@Override
public void deleteAsUser(String user, Path dir, Path... baseDirs) {
//  super.deleteAsUser(user, dir, baseDirs);
}

private class Info {
  private Container container;
  private Path containerWorkDir;
  private List<String> localDirs;
  private List<String> logDirs;
  private String containerImageName;
  private String dockerUrl;
  private ContainerId containerId;
  private String containerIdStr;
  private Path launchDst;
  private String[] localMounts;
  private String[] logMounts;

  public Info(Container container, Path containerWorkDir, List<String> localDirs, List<String> logDirs) {
    this.container = container;
    this.containerWorkDir = containerWorkDir;
    this.localDirs = localDirs;
    this.logDirs = logDirs;
  }

  public Info invoke() {
    containerImageName = container.getLaunchContext().getEnvironment()
            .get(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);
    containerImageName = Strings.isNullOrEmpty(containerImageName) ?
            getConf().get(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME) : containerImageName;
    if (LOG.isDebugEnabled()) {
      LOG.debug("containerImageName from launchContext: " + containerImageName);
    }
    Preconditions.checkArgument(!Strings.isNullOrEmpty(containerImageName), "Container image must not be null");
    containerImageName = containerImageName.replaceAll("['\"]", "");

    Preconditions.checkArgument(saneDockerImage(containerImageName), "Image: " + containerImageName + " is not a proper docker image");
    dockerUrl = getConf().get(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_DOCKER_URL,
            YarnConfiguration.NM_DEFAULT_DOCKER_CONTAINER_EXECUTOR_DOCKER_URL);

    containerId = container.getContainerId();
    containerIdStr = ConverterUtils.toString(containerId);

    launchDst = new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);

    String localDirMount = toMount(localDirs);
    String logDirMount = toMount(logDirs);
    localMounts = localDirMount.trim().split("\\s+");
    logMounts = logDirMount.trim().split("\\s+");
    return this;
  }

  private boolean saneDockerImage(String containerImageName) {
    return dockerImagePattern.matcher(containerImageName).matches();
  }

  /**
   * Converts a directory list to a docker mount string
   *
   * @param dirs
   * @return a string of mounts for docker
   */
  private String toMount(List<String> dirs) {
    StringBuilder builder = new StringBuilder();
    for (String dir : dirs) {
      builder.append(" -v " + dir + ":" + dir);
    }
    return builder.toString();
  }
}
}