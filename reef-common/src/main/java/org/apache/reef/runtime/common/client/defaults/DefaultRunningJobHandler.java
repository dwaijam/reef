/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.reef.runtime.common.client.defaults;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.RunningJob;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default event handler for RunningJob: Logging it.
 */
@Provided
@ClientSide
public final class DefaultRunningJobHandler implements EventHandler<RunningJob> {

  private static final Logger LOG = Logger.getLogger(DefaultRunningJobHandler.class.getName());

  @Inject
  private DefaultRunningJobHandler() {
  }

  @Override
  public void onNext(final RunningJob job) {
    LOG.log(Level.INFO, "Job is running: {0}", job);
  }
}