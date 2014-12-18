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

import com.google.common.base.Optional;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

public class MultiVerseContainerExecutor extends ContainerExecutor {

  private static final Log LOG = LogFactory
      .getLog(MultiVerseContainerExecutor.class);

  private static final int WIN_MAX_PATH = 260;

  //TODO : THIS SHOULD COME FROM config
  private static final String DEFAULT_MULTIVERSE_CONTAINER_EXECUTOR = "org.apache.hadoop.yarn.server.nodemanager.DockerContainerExecutor";

  protected final FileContext lfs;

  protected Map<String, ContainerExecutor> execs = new HashMap<String, ContainerExecutor>();
  public MultiVerseContainerExecutor() {
    try {
      this.lfs = FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  MultiVerseContainerExecutor(FileContext lfs) {
    this.lfs = lfs;
  }

  public void setChildren(Map<String, ContainerExecutor> execs) {
    this.execs.putAll(execs);
  }
  protected void copyFile(Path src, Path dst, String owner) throws IOException {
    lfs.util().copy(src, dst);
  }
  
  protected void setScriptExecutable(Path script, String owner) throws IOException {
    lfs.setPermission(script, ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);
  }

  @Override
  public void init() throws IOException {
    // nothing to do or verify here
    Configuration conf = getConf();
    // parsing and add in map
    String containerExecutorString = getConf().get(YarnConfiguration.NM_MULTIVERSE_CONTAINER_EXECUTOR_CLASSES);
    StringTokenizer containerSplit = new StringTokenizer(containerExecutorString, ",");
    while (containerSplit.hasMoreElements()) {
      String containerSubString = containerSplit.nextToken().trim();
      if (containerSubString.length() != 0) {
          ContainerExecutor exec;
        try {
            exec = (ContainerExecutor) ReflectionUtils.newInstance(
                  conf.getClassByName(containerSubString), conf);
            LOG.info("containerSubString " + containerSubString);
            execs.put(containerSubString, exec);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
      }
    }

    //launch init of list of containers
    for (Map.Entry<String, ContainerExecutor> entry: execs.entrySet()) {
      entry.getValue().init();
    }

  }

  @Override
  public void startLocalizer(Path nmPrivateContainerTokensPath,
      InetSocketAddress nmAddr, String user, String appId, String locId,
      LocalDirsHandlerService dirsHandler)
      throws IOException, InterruptedException {
      throw new UnsupportedOperationException("This is only supported for children");

  }

  /** Returns the ContainerExecutor (e.g. LinuxContainerExecutor /
   * DockerContainerExecutor) to use
   *
   * @param env The LaunchContext environment
   */
  public ContainerExecutor getContainerExecutorToPick(Map<String, String> env) {
    String containerExecutorString = getConf().get(YarnConfiguration.NM_MULTIVERSE_CONTAINER_EXECUTOR);
    containerExecutorString = (containerExecutorString == null || containerExecutorString.length() == 0) ?
      env.get(DEFAULT_MULTIVERSE_CONTAINER_EXECUTOR) : containerExecutorString;
    return execs.get(containerExecutorString);
  }

  @Override
  public int launchContainer(Container container,
      Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
      String user, String appId, Path containerWorkDir,
      List<String> localDirs, List<String> logDirs) throws IOException {
    // Simply pick the container executor and call its launchContainer
    return getContainerExecutorToPick(container.getLaunchContext().getEnvironment())
      .launchContainer(container, nmPrivateContainerScriptPath,
        nmPrivateTokensPath, user, appId, containerWorkDir, localDirs, logDirs);
  }


  @Override
  public boolean signalContainer(String user, String pid, Signal signal)
      throws IOException {
    LOG.debug("Sending signal " + signal.getValue() + " to pid " + pid
        + " as user " + user);
    if (!containerIsAlive(pid)) {
      return false;
    }
    try {
      killContainer(pid, signal);
    } catch (IOException e) {
      if (!containerIsAlive(pid)) {
        return false;
      }
      throw e;
    }
    return true;
  }

  @Override
  public boolean isContainerProcessAlive(String user, String pid)
      throws IOException {

    for (ContainerExecutor exec: execs.values()){
      if (exec.isContainerProcessAlive(user, pid)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if the process with the specified pid is alive.
   *
   * @param pid String pid
   * @return boolean true if the process is alive
   */
  @VisibleForTesting
  public static boolean containerIsAlive(String pid) throws IOException {
    try {
      new ShellCommandExecutor(Shell.getCheckProcessIsAliveCommand(pid))
        .execute();
      // successful execution means process is alive
      return true;
    }
    catch (ExitCodeException e) {
      // failure (non-zero exit code) means process is not alive
      return false;
    }
  }

  /**
   * Send a specified signal to the specified pid
   *
   * @param pid the pid of the process [group] to signal.
   * @param signal signal to send
   * (for logging).
   */
  protected void killContainer(String pid, Signal signal) throws IOException {
    new ShellCommandExecutor(Shell.getSignalKillCommand(signal.getValue(), pid))
      .execute();
  }

  @Override
  public void deleteAsUser(String user, Path subDir, Path... baseDirs)
      throws IOException, InterruptedException {
      for (Map.Entry<String, ContainerExecutor> entry: execs.entrySet()) {
          entry.getValue().deleteAsUser(user, subDir, baseDirs);
      }
  }

  /** Permissions for user dir.
   * $local.dir/usercache/$user */
  static final short USER_PERM = (short)0750;
  /** Permissions for user appcache dir.
   * $local.dir/usercache/$user/appcache */
  static final short APPCACHE_PERM = (short)0710;
  /** Permissions for user filecache dir.
   * $local.dir/usercache/$user/filecache */
  static final short FILECACHE_PERM = (short)0710;
  /** Permissions for user app dir.
   * $local.dir/usercache/$user/appcache/$appId */
  static final short APPDIR_PERM = (short)0710;
  /** Permissions for user log dir.
   * $logdir/$user/$appId */
  static final short LOGDIR_PERM = (short)0710;


}
