/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.concurrent.LinkedBlockingDeque

import org.apache.kafka.clients.{KafkaClient, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time

/**
 * This class uses the controller.connect configuration to to find and connect to the controller and uses
 * MetadataRequest to identify the active controller.
 */
class Kip500BrokerToControllerChannelManager(time: Time,
                                             metrics: Metrics,
                                             config: KafkaConfig,
                                             threadNamePrefix: Option[String] = None) extends
  AbstractBrokerToControllerChannelManager(time, metrics, config, threadNamePrefix) {
  val _brokerToControllerListenerName = new ListenerName("CONTROLLER") // TODO: where will we define this?
  val _brokerToControllerSecurityProtocol = SecurityProtocol.PLAINTEXT  // TODO: where will we define this?
  val _brokerToControllerSaslMechanism = null // TODO: where will we define this?

  val _requestThread = newRequestThread
  override protected def requestThread = _requestThread

  override protected def brokerToControllerListenerName = _brokerToControllerListenerName

  override protected def brokerToControllerSecurityProtocol = _brokerToControllerSecurityProtocol

  override protected def brokerToControllerSaslMechanism = _brokerToControllerSaslMechanism

  override protected def instantiateRequestThread(networkClient: NetworkClient,
                                                  brokerToControllerListenerName: ListenerName,
                                                  threadName: String) = {
    new Kip500BrokerToControllerRequestThread(networkClient, manualMetadataUpdater, requestQueue, config,
      brokerToControllerListenerName, time, threadName)
  }
}

class Kip500BrokerToControllerRequestThread(networkClient: KafkaClient,
                                            metadataUpdater: ManualMetadataUpdater,
                                            requestQueue: LinkedBlockingDeque[BrokerToControllerQueueItem],
                                            config: KafkaConfig,
                                            listenerName: ListenerName,
                                            time: Time,
                                            threadName: String)
  extends BrokerToControllerRequestThread(networkClient, requestQueue, config, time, threadName) {

  override def doWork(): Unit = {
    if (activeController.isDefined) {
      super.doWork()
    } else {
//      debug("Controller isn't cached, looking for local metadata changes")
//      val controllerOpt = metadataCache.getControllerId.flatMap(metadataCache.getAliveBroker)
//      if (controllerOpt.isDefined) {
//        if (activeController.isEmpty || activeController.exists(_.id != controllerOpt.get.id))
//          info(s"Recorded new controller, from now on will use broker ${controllerOpt.get.id}")
//        activeController = Option(controllerOpt.get.node(listenerName))
//        metadataUpdater.setNodes(metadataCache.getAliveBrokers.map(_.node(listenerName)).asJava)
//      } else {
//        // need to backoff to avoid tight loops
//        debug("No controller defined in metadata cache, retrying after backoff")
//        backoff()
//      }
    }
  }
}