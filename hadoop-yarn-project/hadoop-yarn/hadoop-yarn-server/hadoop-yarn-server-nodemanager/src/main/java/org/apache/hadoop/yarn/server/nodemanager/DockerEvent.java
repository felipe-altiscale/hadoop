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

import org.apache.hadoop.yarn.event.AbstractEvent;

abstract  class DockerEvent extends AbstractEvent<DockerEventType> {
final String dockerContainerId;

public DockerEvent(String dockerContainerId, DockerEventType dockerEventType) {
  super(dockerEventType);
  this.dockerContainerId = dockerContainerId;
}
static class DockerContainerStartedEvent extends DockerEvent {
  public DockerContainerStartedEvent(String dockerContainerId) {
    super(dockerContainerId, DockerEventType.STARTED);
  }
}
static class DockerContainerCreatedEvent extends DockerEvent {
  public DockerContainerCreatedEvent(String dockerContainerId) {
    super(dockerContainerId, DockerEventType.CREATED);
  }
}
static class DockerContainerKilledEvent extends DockerEvent {
  public DockerContainerKilledEvent(String dockerContainerId) {
    super(dockerContainerId, DockerEventType.KILLED);
  }
}
}
