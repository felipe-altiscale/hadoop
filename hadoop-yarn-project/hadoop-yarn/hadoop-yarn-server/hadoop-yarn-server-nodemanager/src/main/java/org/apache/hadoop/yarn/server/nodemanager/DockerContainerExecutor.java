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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;
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

private static final Log LOG = LogFactory
        .getLog(DockerContainerExecutor.class);
// This validates that the image is a proper docker image and would not crash docker.
public static final String DOCKER_IMAGE_PATTERN = "^(([\\w\\.-]+)(:\\d+)*\\/)?[\\w\\.:-]+$";
private static final String TMP_FILE_PREFIX = "dc";
private static final String TMP_FILE_SUFFIX = ".cmds";

private final FileContext lfs;
private final Pattern dockerImagePattern;
private String tmpDirPath;

public DockerContainerExecutor() {
  try {
    this.lfs = FileContext.getLocalFSFileContext();
    this.dockerImagePattern = Pattern.compile(DOCKER_IMAGE_PATTERN);
  } catch (UnsupportedFileSystemException e) {
    throw new RuntimeException(e);
  }
}

protected void copyFile(Path src, Path dst, String owner) throws IOException {
  lfs.util().copy(src, dst);
}

@Override
public void init() throws IOException {
  super.init();
  String auth = getConf().get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);
  if (auth != null && !auth.equals("simple")) {
    throw new IllegalStateException("DockerContainerExecutor only works with simple authentication mode");
  }

  String tmpDirBase = getConf().get("hadoop.tmp.dir");
  if (tmpDirBase == null) {
    throw new IllegalStateException("hadoop.tmp.dir not set!");
  }
  tmpDirPath = tmpDirBase + "/nm-dc-command";
  File tmpDir = new File(tmpDirPath);
  if (!(tmpDir.exists() || tmpDir.mkdirs())) {
    LOG.warn("Unable to create directory: " + tmpDirPath);
    throw new RuntimeException("Unable to create directory: " +
            tmpDirPath);
  }
}


@Override
public int launchContainer(Container container,
                           Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
                           String userName, String appId, Path containerWorkDir,
                           List<String> localDirs, List<String> logDirs) throws IOException {
  String containerImageName = container.getLaunchContext().getEnvironment()
          .get(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);
  if (LOG.isDebugEnabled()) {
    LOG.debug("containerImageName from launchContext: " + containerImageName);
  }
  Preconditions.checkArgument(!Strings.isNullOrEmpty(containerImageName), "Container image must not be null");
  containerImageName = containerImageName.replaceAll("['\"]", "");

  Preconditions.checkArgument(saneDockerImage(containerImageName), "Image: " + containerImageName + " is not a proper docker image");

  ContainerId containerId = container.getContainerId();

  String containerIdStr = ConverterUtils.toString(containerId);

  Path launchDst =
          new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);
  List<String> localWithWorkDir = new ArrayList<>(localDirs);
  localWithWorkDir.add(containerWorkDir.toUri().getPath());
  String localDirMount = toMount(localWithWorkDir);
  String logDirMount = toMount(logDirs);

  String[] localMounts = localDirMount.trim().split("\\s+");
  String[] logMounts = logDirMount.trim().split("\\s+");
  List<String> commandStr = Lists.newArrayList("run", "--workdir",
          containerWorkDir.toUri().getPath(),
          "--net", "host", "--name", containerIdStr, "--user", userName,
          "-v", "/etc/passwd:/etc/passwd:ro");
  commandStr.addAll(Arrays.asList(localMounts));
  commandStr.addAll(Arrays.asList(logMounts));

  commandStr.add(containerImageName.trim());
  commandStr.add("bash");
  commandStr.add(launchDst.toUri().getPath());
  // write it out to a file. has to be the same docker command file
  String commandFilePath = writeCommandFile(Joiner.on(" ").join(commandStr), containerIdStr);
  ShellCommandExecutor shExec = null;
  try {
    // Setup command to run
    if (LOG.isDebugEnabled()) {
      LOG.debug("launchContainer: " + Joiner.on(" ").join(commandStr));
    }
    String runAsUser = getRunAsUser(userName);
    List<String> launchDocker = new ArrayList<String>();
    Path pidFilePath = getPidFilePath(containerId);
    launchDocker.addAll(Arrays.asList(
            containerExecutorExe, runAsUser, userName, Integer
                    .toString(Commands.LAUNCH_DOCKER_CONTAINER.getValue()), appId,
            containerIdStr, containerWorkDir.toString(),
            nmPrivateContainerScriptPath.toUri().getPath().toString(),
            nmPrivateTokensPath.toUri().getPath().toString(),
            pidFilePath.toString(),
            StringUtils.join(",", localDirs),
            StringUtils.join(",", logDirs)));
    launchDocker.add(commandFilePath);
    shExec = new ShellCommandExecutor(launchDocker.toArray(new String[launchDocker.size()])
            , null, // NM's cwd
            container.getLaunchContext().getEnvironment()); // sanitized env
    if (LOG.isDebugEnabled()) {
      LOG.debug("launchDocker: " + launchDocker);
    }
    if (isContainerActive(containerId)) {
      shExec.execute();
      if (LOG.isDebugEnabled()) {
        logOutput(shExec.getOutput());
      }
    } else {
      LOG.info("Container " + containerIdStr +
              " was marked as inactive. Returning terminated error");
      return ExitCode.TERMINATED.getExitCode();
    }
  } catch (IOException e) {
    int exitCode = shExec.getExitCode();
    LOG.warn("Exit code from container " + containerId + " is : " + exitCode);
    // 143 (SIGTERM) and 137 (SIGKILL) exit codes means the container was
    // terminated/killed forcefully. In all other cases, log the
    // container-executor's output
    if (exitCode != ExitCode.FORCE_KILLED.getExitCode()
            && exitCode != ExitCode.TERMINATED.getExitCode()) {
      LOG.warn("Exception from container-launch with container ID: "
              + containerId + " and exit code: " + exitCode, e);
      logOutput(shExec.getOutput());
      String diagnostics = "Exception from container-launch: \n"
              + StringUtils.stringifyException(e) + "\n" + shExec.getOutput();
      container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
              diagnostics));
    } else {
      container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
              "Container killed on request. Exit code is " + exitCode));
    }
    return exitCode;
  } finally {
    if (shExec != null) {
      shExec.close();
    }
  }
  return 0;
}

private String writeCommandFile(String dockerCommand, String containerId) throws IOException {
  try {
    LOG.info("tmpDirPath " + tmpDirPath + " containerId " + containerId);
    File dcCmds = File.createTempFile(TMP_FILE_PREFIX + "-" + containerId + ".", TMP_FILE_SUFFIX, new
            File(tmpDirPath));
    LOG.info("writing dockerCommandFile to: " +dcCmds.getAbsolutePath());
    Writer writer = new OutputStreamWriter(new FileOutputStream(dcCmds),
            "UTF-8");
    PrintWriter printWriter = new PrintWriter(writer);
    printWriter.println(dockerCommand);
    printWriter.close();

    return dcCmds.getAbsolutePath();
  } catch (IOException e) {
    LOG.error("Failed to create or write to temporary file in dir: " +
            tmpDirPath);
    throw e;
  }
} //end BatchBuilder

@Override
public void writeLaunchEnv(OutputStream out, Map<String, String> environment, Map<Path, List<String>> resources, List<String> command) throws IOException {
  ContainerLaunch.ShellScriptBuilder sb = ContainerLaunch.ShellScriptBuilder.create();

  Set<String> exclusionSet = new HashSet<String>();
  exclusionSet.add(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);
  exclusionSet.add(ApplicationConstants.Environment.HADOOP_YARN_HOME.name());
  exclusionSet.add(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name());
  exclusionSet.add(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name());
  exclusionSet.add(ApplicationConstants.Environment.HADOOP_CONF_DIR.name());
  exclusionSet.add(ApplicationConstants.Environment.JAVA_HOME.name());

  if (environment != null) {
    for (Map.Entry<String,String> env : environment.entrySet()) {
      if (!exclusionSet.contains(env.getKey())) {
        sb.env(env.getKey().toString(), env.getValue().toString());
      }
    }
  }
  if (resources != null) {
    for (Map.Entry<Path,List<String>> entry : resources.entrySet()) {
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

private boolean saneDockerImage(String containerImageName) {
  return dockerImagePattern.matcher(containerImageName).matches();
}

/**
 * Converts a directory list to a docker mount string
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