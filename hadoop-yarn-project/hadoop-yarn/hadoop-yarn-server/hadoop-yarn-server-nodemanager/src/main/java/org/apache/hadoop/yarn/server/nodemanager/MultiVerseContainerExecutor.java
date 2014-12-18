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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

public class MultiVerseContainerExecutor extends ContainerExecutor {

  private static final Log LOG = LogFactory
      .getLog(MultiVerseContainerExecutor.class);

  private static final int WIN_MAX_PATH = 260;

  //TODO : THIS SHOULD COME FROM config
  private static final String DEFAULT_MULTIVERSE_CONTAINER_EXECUTOR = "org.apache.hadoop.yarn.server.nodemanager.DockerContainerExecutor";

  protected final FileContext lfs;

  protected Map<String, ContainerExecutor> execs = new ConcurrentHashMap<String, ContainerExecutor>();
  protected Map<ContainerId, ContainerExecutor> containersToExecs = new ConcurrentHashMap<ContainerId, ContainerExecutor>();

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
  @Override
  public ContainerExecutor getContainerExecutorToPick(Map<String, String> env) {
    String containerExecutorString = env.get(YarnConfiguration.NM_MULTIVERSE_CONTAINER_EXECUTOR);
    containerExecutorString = (containerExecutorString == null || containerExecutorString.length() == 0) ?
      DEFAULT_MULTIVERSE_CONTAINER_EXECUTOR : containerExecutorString;
    LOG.debug("Env: "+env +" Table: " + execs + " key: " + containerExecutorString + " value: " + execs.get(containerExecutorString));
    return execs.get(containerExecutorString);
  }

  @Override
  public int launchContainer(Container container,
      Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
      String user, String appId, Path containerWorkDir,
      List<String> localDirs, List<String> logDirs) throws IOException {
    LOG.debug("container: " + container);
    containersToExecs.put(container.getContainerId(), getContainerExecutorToPick(container.getLaunchContext().getEnvironment()));
    // Simply pick the container executor and call its launchContainer
    return getContainerExecutorToPick(container.getLaunchContext().getEnvironment())
      .launchContainer(container, nmPrivateContainerScriptPath,
        nmPrivateTokensPath, user, appId, containerWorkDir, localDirs, logDirs);
  }

  private ContainerExecutor getExec(String containerId) {
    if (containersToExecs.containsKey(containerId)) {
      return execs.get(containersToExecs.get(containerId));
    }
    return null;
  }

  @Override
  public boolean signalContainer(String user, String pid, Signal signal)
      throws IOException {
    LOG.debug("signalContainer: " + user + " " + pid + " " + signal);
    //iterate through all containers to find the pid
    for(Map.Entry<ContainerId, ContainerExecutor> entry: containersToExecs.entrySet()) {
      if(pid.equals(getProcessId(entry.getKey()))){
        return entry.getValue().signalContainer(user, pid, signal);
      }
    }

    return false;
  }

  @Override
  public boolean isContainerProcessAlive(String user, String pid)
      throws IOException {
    LOG.debug("isAlive: " + user + " " + pid);
    for (ContainerExecutor exec: execs.values()){
      if (exec.isContainerProcessAlive(user, pid)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void deleteAsUser(String user, Path subDir, Path... baseDirs)
      throws IOException, InterruptedException {
    LOG.debug("deleteAsUser: " + user + " " + subDir);
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
