/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.{File, IOException}
import java.util
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import kafka.cluster.{Broker, EndPoint}
import kafka.common.InconsistentBrokerMetadataException
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.{ProducerIdMgr, TransactionCoordinator}
import kafka.log.LogManager
import kafka.metrics.{KafkaMetricsReporter, KafkaYammerMetrics}
import kafka.network.SocketServer
import kafka.security.CredentialProvider
import kafka.server.KafkaBroker.{metricsPrefix, notifyClusterListeners}
import kafka.server.metadata.BrokerMetadataListener
import kafka.utils.{CoreUtils, KafkaScheduler, Mx4jLoader}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.message.BrokerRegistrationRequestData.{FeatureCollection, Listener, ListenerCollection}
import org.apache.kafka.common.{Endpoint, KafkaException}
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, MetricsReporter}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time}
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.authorizer.Authorizer

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, Seq, mutable}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

/**
 * A KIP-500 Kafka broker.
 */
class Kip500Broker(val config: KafkaConfig,
                   time: Time,
                   threadNamePrefix: Option[String],
                   kafkaMetricsReporters: Seq[KafkaMetricsReporter]) extends KafkaBroker {
  import kafka.server.KafkaServerManager._

  private val isShuttingDown = new AtomicBoolean(false)

  val lock = new ReentrantLock()
  val awaitShutdownCond = lock.newCondition()
  var status: ProcessStatus = KafkaServerManager.SHUTDOWN

  private var logContext: LogContext = null

  var kafkaYammerMetrics: KafkaYammerMetrics = null
  var metrics: Metrics = null

  var dataPlaneRequestProcessor: KafkaApis = null
  var controlPlaneRequestProcessor: KafkaApis = null

  var authorizer: Option[Authorizer] = None
  var socketServer: SocketServer = null
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = null
  var controlPlaneRequestHandlerPool: KafkaRequestHandlerPool = null

  var logDirFailureChannel: LogDirFailureChannel = null
  var logManager: LogManager = null

  var tokenManager: DelegationTokenManager = null

  var replicaManager: ReplicaManager = null

  var credentialProvider: CredentialProvider = null
  var tokenCache: DelegationTokenCache = null

  var groupCoordinator: GroupCoordinator = null

  var transactionCoordinator: TransactionCoordinator = null

  var brokerToControllerChannelManager: BrokerToControllerChannelManager = null

  var kafkaScheduler: KafkaScheduler = null

  var metadataCache: MetadataCache = null
  var quotaManagers: QuotaFactory.QuotaManagers = null

  val brokerMetaPropsFile = "meta.properties"
  val brokerMetadataCheckpoints = config.logDirs.map(logDir => (logDir, new BrokerMetadataCheckpoint(new File(logDir + File.separator + brokerMetaPropsFile)))).toMap

  private var _clusterId: String = null
  private var _brokerTopicStats: BrokerTopicStats = null

  val brokerFeatures: BrokerFeatures = BrokerFeatures.createDefault()

  val featureCache: FinalizedFeatureCache = new FinalizedFeatureCache(brokerFeatures)

  def clusterId(): String = _clusterId

  var brokerMetadataListener: BrokerMetadataListener = null

  var brokerLifecycleManager: BrokerLifecycleManager = null

  private[kafka] def brokerTopicStats = _brokerTopicStats

  private def maybeChangeStatus(from: ProcessStatus, to: ProcessStatus): Boolean = {
    lock.lock()
    try {
      if (status != from) return false
      status = to
      if (to == SHUTTING_DOWN) {
        isShuttingDown.set(true)
      } else if (to == SHUTDOWN) {
        isShuttingDown.set(false)
        awaitShutdownCond.signalAll()
      }
    } finally {
      lock.unlock()
    }
    true
  }

  override def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    try {
      maybeChangeStatus(STARTING, STARTED)

      val (loadedClusterId, loadedBrokerId, initialOfflineDirs) = loadClusterIdBrokerIdAndOfflineDirs
      _clusterId = loadedClusterId
      info(s"Cluster ID = ${_clusterId}")

      config.brokerId = loadedBrokerId
      logContext = new LogContext(s"[KafkaBroker id=${config.brokerId}] ")
      this.logIdent = logContext.logPrefix

      // initialize dynamic broker configs from static config. Any updates will be
      // applied as we process the metadata log.
      config.dynamicConfig.initialize() // Currently we don't wait for catch-up on the metadata log.  TODO?

      /* start scheduler */
      kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
      kafkaScheduler.startup()

      /* create and configure metrics */
      kafkaYammerMetrics = KafkaYammerMetrics.INSTANCE
      kafkaYammerMetrics.configure(config.originals)

      val jmxReporter = new JmxReporter()
      jmxReporter.configure(config.originals)

      val reporters = new util.ArrayList[MetricsReporter]
      reporters.add(jmxReporter)

      val metricConfig = KafkaBroker.metricConfig(config)
      val metricsContext = KafkaBroker.createKafkaMetricsContext(_clusterId, config)
      metrics = new Metrics(metricConfig, reporters, time, true, metricsContext)

      /* register broker metrics */
      _brokerTopicStats = new BrokerTopicStats

      quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))
      notifyClusterListeners(_clusterId, kafkaMetricsReporters ++ metrics.reporters.asScala)

      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

      /* start log manager */
      var cleanShutdown: Boolean = false
      logManager = LogManager(config, initialOfflineDirs, wasCleanShutdown => cleanShutdown = wasCleanShutdown, kafkaScheduler, time, brokerTopicStats, logDirFailureChannel)
      logManager.startup()

      metadataCache = new MetadataCache(config.brokerId)
      // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
      // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

      // Create and start the socket server acceptor threads so that the bound port is known.
      // Delay starting processors until the end of the initialization sequence to ensure
      // that credentials have been loaded before processing authentications.
      socketServer = new SocketServer(config, metrics, time, credentialProvider)
      socketServer.startup(startProcessingRequests = false)

      /* start replica manager */
      brokerToControllerChannelManager = new BrokerToControllerChannelManagerImpl(metadataCache, time, metrics, config, threadNamePrefix)
      replicaManager = createReplicaManager(isShuttingDown)
      replicaManager.startup()
      brokerToControllerChannelManager.start()

      /* start token manager */
      if (config.tokenAuthEnabled) {
        throw new UnsupportedOperationException("Delegation tokens are not supported")
      }
      tokenManager = new DelegationTokenManager(config, tokenCache, time , null)
      tokenManager.startup() // does nothing, we just need a token manager in order to compile right now...

      /* start group coordinator */
      // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
      groupCoordinator = GroupCoordinator(config, () => getGroupMetadataTopicPartitionCount(), replicaManager, Time.SYSTEM, metrics)
      groupCoordinator.startup()

      /* start transaction coordinator, with a separate background thread scheduler for transaction expiration and log loading */
      // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
      transactionCoordinator = TransactionCoordinator(config, replicaManager, new KafkaScheduler(threads = 1, threadNamePrefix = "transaction-log-manager-"),
        createTemporaryProducerIdManager(), getTransactionTopicPartitionCount,
        metrics, metadataCache, Time.SYSTEM)
      transactionCoordinator.startup()

      /* Add all reconfigurables for config change notification before starting the metadata listener */
      config.dynamicConfig.addReconfigurables(this.asInstanceOf[Kip500Broker])

      brokerMetadataListener = new BrokerMetadataListener(
        config, time,
        BrokerMetadataListener.defaultProcessors(
          config, _clusterId, metadataCache, groupCoordinator, quotaManagers, replicaManager, transactionCoordinator,
          logManager))
      brokerMetadataListener.start()

      brokerLifecycleManager = new BrokerLifecycleManagerImpl(brokerMetadataListener, config,
        brokerToControllerChannelManager, kafkaScheduler, time, config.brokerId, config.rack.getOrElse(""),
        brokerMetadataListener.currentMetadataOffset, brokerMetadataListener.brokerEpochNow)
      val listeners = new ListenerCollection()
      config.advertisedListeners.foreach { ep =>
        listeners.add(new Listener().setHost(ep.host).setName(ep.listenerName.value()).setPort(ep.port.shortValue())
          .setSecurityProtocol(ep.securityProtocol.id))
      }
      val features = new FeatureCollection()
      brokerLifecycleManager.start(listeners, features, !cleanShutdown)

      val endPoints = new ArrayBuffer[EndPoint](listeners.size())
      listeners.iterator().forEachRemaining(listener => {
        endPoints += new EndPoint(listener.host(), listener.port(), new ListenerName(listener.name()),
          SecurityProtocol.forId(listener.securityProtocol()))
      })
      val supportedFeaturesMap = mutable.Map[String, SupportedVersionRange]()
      features.iterator().forEachRemaining(feature => {
        supportedFeaturesMap(feature.name()) = new SupportedVersionRange(feature.minSupportedVersion(), feature.maxSupportedVersion())
      })
      val broker = Broker(config.brokerId, endPoints, config.rack, Features.supportedFeatures(supportedFeaturesMap.asJava))
      /* Get the authorizer and initialize it if one is specified.*/
      authorizer = config.authorizer
      authorizer.foreach(_.configure(config.originals))
      val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
        case Some(authZ) =>
          authZ.start(broker.toServerInfo(_clusterId, config)).asScala.map { case (ep, cs) =>
            ep -> cs.toCompletableFuture
          }
        case None =>
          broker.endPoints.map { ep =>
            ep.toJava -> CompletableFuture.completedFuture[Void](null)
          }.toMap
      }

      val fetchManager = new FetchManager(Time.SYSTEM,
        new FetchSessionCache(config.maxIncrementalFetchSessionCacheSlots,
          KafkaBroker.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS))

      /* start processing requests */
      val adminManager: LegacyAdminManager = null
      val zkClient: KafkaZkClient = null
      val kafkaController: KafkaController = null
      dataPlaneRequestProcessor = new KafkaApis(socketServer.dataPlaneRequestChannel,
        replicaManager, adminManager, groupCoordinator, transactionCoordinator,
        kafkaController, zkClient, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
        fetchManager, brokerTopicStats, _clusterId, time, tokenManager, brokerFeatures, featureCache)

      dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
        config.numIoThreads, s"${SocketServer.DataPlaneMetricPrefix}RequestHandlerAvgIdlePercent", SocketServer.DataPlaneThreadPrefix)

      socketServer.controlPlaneRequestChannelOpt.foreach { controlPlaneRequestChannel =>
        controlPlaneRequestProcessor = new KafkaApis(controlPlaneRequestChannel,
          replicaManager, adminManager, groupCoordinator, transactionCoordinator,
          kafkaController, zkClient, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
          fetchManager, brokerTopicStats, _clusterId, time, tokenManager, brokerFeatures, featureCache)

        controlPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.controlPlaneRequestChannelOpt.get, controlPlaneRequestProcessor, time,
          1, s"${SocketServer.ControlPlaneMetricPrefix}RequestHandlerAvgIdlePercent", SocketServer.ControlPlaneThreadPrefix)
      }

      Mx4jLoader.maybeLoad()

      socketServer.startProcessingRequests(authorizerFutures)

      AppInfoParser.registerAppInfo(metricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
      info("started")
    } catch {
      case e: Throwable =>
        maybeChangeStatus(STARTING, STARTED)
        fatal("Fatal error during controller startup. Prepare to shutdown", e)
        shutdown()
        throw e
    }
  }

  class TemporaryProducerIdManager() extends ProducerIdMgr {
    val maxProducerIdsPerBrokerEpoch = 1000000
    var currentOffset = -1
    override def generateProducerId(): Long = {
      currentOffset = currentOffset + 1
      if (currentOffset >= maxProducerIdsPerBrokerEpoch) {
        fatal(s"Exhausted all demo/temporary producerIds as the next one will has extend past the block size of $maxProducerIdsPerBrokerEpoch")
        throw new KafkaException("Have exhausted all demo/temporary producerIds.")
      }
      brokerMetadataListener.brokerEpochFuture().get() * maxProducerIdsPerBrokerEpoch + currentOffset
    }
  }

  def createTemporaryProducerIdManager(): ProducerIdMgr = {
    new TemporaryProducerIdManager()
  }

  protected def createReplicaManager(isShuttingDown: AtomicBoolean): ReplicaManager = {
    val alterIsrManager = new AlterIsrManagerImpl(brokerToControllerChannelManager, kafkaScheduler,
      time, config.brokerId, () => brokerMetadataListener.brokerEpochFuture().get())
    new ReplicaManager(config, metrics, time, None, kafkaScheduler, logManager, isShuttingDown, quotaManagers,
      brokerTopicStats, metadataCache, logDirFailureChannel, alterIsrManager, None)
  }

  /**
   * Gets the partition count of the group metadata topic from the metadata log.
   * If the topic does not exist, the configured partition count is returned.
   */
  def getGroupMetadataTopicPartitionCount(): Int = {
    // TODO: don't return a result until the broker catches up; for now, just give the configured partition count
    // brokerMetadataListener.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(config.offsetsTopicPartitions)
    config.offsetsTopicPartitions
  }

  /**
   * Gets the partition count of the transaction log topic from the metadata log.
   * If the topic does not exist, the default partition count is returned.
   */
  def getTransactionTopicPartitionCount(): Int = {
    // TODO: don't return a result until the broker catches up; for now, just give the configured partition count
    // brokerMetadataListener.getTopicPartitionCount(Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(config.transactionTopicPartitions)
    config.transactionTopicPartitions
  }

  /**
   * Performs controlled shutdown
   */
  private def controlledShutdown(): Unit = {

    if(config.controlledShutdownEnable) {
      // We request the heartbeat to initiate a controlled shutdown.
      info("Requesting controlled shutdown via broker heartbeat")

      if (brokerLifecycleManager != null) {
        brokerLifecycleManager.enqueue(BrokerState.PENDING_CONTROLLED_SHUTDOWN).future.onComplete {
          case Success(_) => info("Controlled shutdown succeeded")
          case Failure(_) => warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")
        }
      }
    }
  }

   def shutdown(): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      info("shutting down")

      CoreUtils.swallow(controlledShutdown(), this)

      // Stop socket server to stop accepting any more connections and requests.
      // Socket server will be shutdown towards the end of the sequence.
      if (socketServer != null)
        CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
      if (dataPlaneRequestHandlerPool != null)
        CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
      if (controlPlaneRequestHandlerPool != null)
        CoreUtils.swallow(controlPlaneRequestHandlerPool.shutdown(), this)
      if (kafkaScheduler != null)
        CoreUtils.swallow(kafkaScheduler.shutdown(), this)

      if (dataPlaneRequestProcessor != null)
        CoreUtils.swallow(dataPlaneRequestProcessor.close(), this)
      if (controlPlaneRequestProcessor != null)
        CoreUtils.swallow(controlPlaneRequestProcessor.close(), this)
      CoreUtils.swallow(authorizer.foreach(_.close()), this)

      if (transactionCoordinator != null)
        CoreUtils.swallow(transactionCoordinator.shutdown(), this)
      if (groupCoordinator != null)
        CoreUtils.swallow(groupCoordinator.shutdown(), this)

      if (tokenManager != null)
        CoreUtils.swallow(tokenManager.shutdown(), this)

      if (replicaManager != null)
        CoreUtils.swallow(replicaManager.shutdown(), this)

      if (brokerToControllerChannelManager != null)
        CoreUtils.swallow(brokerToControllerChannelManager.shutdown(), this)

      if (logManager != null)
        CoreUtils.swallow(logManager.shutdown(), this)

      if (quotaManagers != null)
        CoreUtils.swallow(quotaManagers.shutdown(), this)

      if (socketServer != null)
        CoreUtils.swallow(socketServer.shutdown(), this)
      if (metrics != null)
        CoreUtils.swallow(metrics.close(), this)
      if (brokerTopicStats != null)
        CoreUtils.swallow(brokerTopicStats.close(), this)

      // Clear all reconfigurable instances stored in DynamicBrokerConfig
      config.dynamicConfig.clear()

      isShuttingDown.set(false)
      CoreUtils.swallow(AppInfoParser.unregisterAppInfo(metricsPrefix, config.brokerId.toString, metrics), this)
      info("shut down completed")
    } catch {
      case e: Throwable =>
        fatal("Fatal error during broker shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
  }

  override def awaitShutdown(): Unit = {
    lock.lock()
    try {
      while (true) {
        if (status == SHUTDOWN) return
        awaitShutdownCond.awaitUninterruptibly()
      }
    } finally {
      lock.unlock()
    }
  }

  def getLogManager: LogManager = logManager

  override def currentState(): BrokerState = brokerLifecycleManager.brokerState

  def loadClusterIdBrokerIdAndOfflineDirs: (String, Int, Seq[String]) = {
    /* load metadata */
    val (loadedBrokerMetadata, initialOfflineDirs) = getBrokerMetadataAndOfflineDirs

    /* check brokerId */
    val loadedBrokerId = config.brokerId // TODO: grab loadedBrokerMetadata.brokerId when available in meta.properties
    if (config.brokerId != loadedBrokerId)
      throw new InconsistentBrokerMetadataException(
        s"The configured Broker ID ${config.brokerId} doesn't match stored broker.id ${loadedBrokerId} in meta.properties. " +
          s"The broker is trying to use the wrong log directory. Configured log.dirs may be wrong.")

    (loadedBrokerMetadata.clusterId.toString, loadedBrokerId, initialOfflineDirs)
  }

  /**
   * Reads the BrokerMetadata. If the BrokerMetadata doesn't match in all the log.dirs, InconsistentBrokerMetadataException is
   * thrown.
   *
   * The log directories whose meta.properties can not be accessed due to IOException will be returned to the caller
   *
   * @return A 2-tuple containing the brokerMetadata and a sequence of offline log directories.
   */
  private def getBrokerMetadataAndOfflineDirs: (MetaProperties, Seq[String]) = {
    val brokerMetadataMap = mutable.HashMap[String, MetaProperties]()
    val brokerMetadataSet = mutable.HashSet[MetaProperties]()
    val offlineDirs = mutable.ArrayBuffer.empty[String]

    for (logDir <- config.logDirs) {
      try {
        brokerMetadataCheckpoints(logDir).read().foreach(properties => {
          val brokerMetadata = MetaProperties(properties)
          brokerMetadataMap += (logDir -> brokerMetadata)
          brokerMetadataSet += brokerMetadata
        })
      } catch {
        case e: IOException =>
          offlineDirs += logDir
          error(s"Fail to read $brokerMetaPropsFile under log directory $logDir", e)
      }
    }

    if (brokerMetadataSet.size > 1) {
      val builder = new StringBuilder

      for ((logDir, brokerMetadata) <- brokerMetadataMap)
        builder ++= s"- $logDir -> $brokerMetadata\n"

      throw new InconsistentBrokerMetadataException(
        s"BrokerMetadata is not consistent across log.dirs. This could happen if multiple brokers shared a log directory (log.dirs) " +
          s"or partial data was manually copied from another broker. Found:\n${builder.toString()}"
      )
    } else if (brokerMetadataSet.size == 1)
      (brokerMetadataSet.last, offlineDirs)
    else {
      throw new IOException("All log dirs are offline; unable to read any meta.properties file.")
    }
  }
}
