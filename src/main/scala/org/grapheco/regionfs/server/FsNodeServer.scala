package org.grapheco.regionfs.server

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import io.netty.buffer.Unpooled
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEnvServerConfig}
import org.apache.commons.io.IOUtils
import org.grapheco.commons.util.{ConfigurationEx, Logging, ProcessUtils}
import org.grapheco.hippo.{ChunkedStream, CompleteStream, HippoRpcHandler, ReceiveContext}
import org.grapheco.regionfs._
import org.grapheco.regionfs.client._
import org.grapheco.regionfs.util.{CrcUtils, ParsedChildNodeEventHandler, RegionFsException, ZooKeeperClient}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2019/8/22.
  */
/**
  * FsNodeServer factory
  */
object FsNodeServer {
  /**
    * create a FsNodeServer with a configuration file, e.g. node1.conf
    */
  private def create(conf: ConfigurationEx, baseDir: File): FsNodeServer = {
    val storeDir = conf.get("data.storeDir").asFile(baseDir).
      getCanonicalFile.getAbsoluteFile

    if (!storeDir.exists())
      throw new StoreDirNotExistsException(storeDir)

    val lockFile = new File(storeDir, ".lock")
    if (lockFile.exists()) {
      val fis = new FileInputStream(lockFile)
      val pid = IOUtils.toString(fis).toInt
      fis.close()

      throw new StoreLockedException(storeDir, pid)
    }

    //TODO: use a leader node: manages all regions
    new FsNodeServer(
      conf.get("zookeeper.address").asString,
      conf.get("node.id").asInt,
      storeDir,
      conf.get("server.host").withDefault("localhost").asString,
      conf.get("server.port").withDefault(1224).asInt
    )
  }

  def create(props: Map[String, String], baseDir: File = null): FsNodeServer = {
    create(new ConfigurationEx(props), baseDir)
  }

  def create(configFile: File): FsNodeServer = {
    create(new ConfigurationEx(configFile), configFile.getParentFile)
  }
}

/**
  * a FsNodeServer responds blob save/read requests
  */
class FsNodeServer(zks: String, nodeId: Int, storeDir: File, host: String, port: Int) extends Logging {
  logger.debug(s"nodeId: ${nodeId}")
  logger.debug(s"storeDir: ${storeDir.getCanonicalFile.getAbsolutePath}")

  val zookeeper = ZooKeeperClient.create(zks)
  val (env, address) = createRpcEnv(zookeeper)
  val globalSetting = zookeeper.loadGlobalSetting()

  val localRegionManager = new RegionManager(nodeId, storeDir, globalSetting, new RegionEventListener {
    override def handleRegionEvent(event: RegionEvent): Unit = {
      event match {
        case CreateRegionEvent(region) => {
          //registered
        }

        case WriteRegionEvent(region) => {
          //
        }
      }
    }
  })

  val clientFactory = new FsNodeClientFactory(globalSetting);

  //get neighbour nodes
  val cachedClients = mutable.Map[Int, FsNodeClient]()
  val mapNeighbourNodeWithAddress = mutable.Map[Int, RpcAddress]()
  val neighbourNodesWatcher = zookeeper.watchNodeList(
    new ParsedChildNodeEventHandler[(Int, RpcAddress)] {
      override def onCreated(t: (Int, RpcAddress)): Unit = {
        mapNeighbourNodeWithAddress += t
      }

      override def onDeleted(t: (Int, RpcAddress)): Unit = {
        mapNeighbourNodeWithAddress -= t._1
      }

      override def accepts(t: (Int, RpcAddress)): Boolean = {
        nodeId != t._1
      }
    })

  val primaryRegionWatcher: Option[PrimaryRegionWatcher] = {
    if (globalSetting.consistencyStrategy == Constants.CONSISTENCY_STRATEGY_EVENTUAL) {
      Some(new PrimaryRegionWatcher(zookeeper, globalSetting, nodeId, localRegionManager, clientOf(_)).start)
    }
    else {
      None
    }
  }

  //get regions in neighbour nodes
  //32768->(1,2), 32769->(1), ...
  var mapNeighbourRegionWithNodes = mutable.Map[Long, ArrayBuffer[Int]]()
  var mapNeighbourNodeWithRegionCount = mutable.ListMap[Int, AtomicInteger]()

  val neighbourRegionsWatcher = zookeeper.watchRegionList(
    new ParsedChildNodeEventHandler[(Long, Int)] {
      override def onCreated(t: (Long, Int)): Unit = {
        mapNeighbourNodeWithRegionCount.getOrElseUpdate(t._2, new AtomicInteger(0)).incrementAndGet()
        mapNeighbourRegionWithNodes.getOrElseUpdate(t._1, ArrayBuffer()) += t._2
      }

      override def onDeleted(t: (Long, Int)): Unit = {
        mapNeighbourNodeWithRegionCount(t._2).decrementAndGet()
        mapNeighbourRegionWithNodes(t._1) -= t._2
      }

      override def accepts(t: (Long, Int)): Boolean = {
        nodeId != t._2
      }
    })

  var alive: Boolean = true
  val endpoint = new FileRpcEndpoint(env)
  env.setupEndpoint("regionfs-service", endpoint)
  env.setRpcHandler(endpoint)
  writeLockFile(new File(storeDir, ".lock"))

  def awaitTermination(): Unit = {
    println(IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("logo.txt"), "utf-8"))
    println(s"starting node server on ${address}, nodeId=${nodeId}, storeDir=${storeDir.getAbsoluteFile.getCanonicalPath}")

    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = {
        shutdown();
      }
    })

    env.awaitTermination()
  }

  def shutdown(): Unit = {
    if (alive) {
      clientFactory.close()
      neighbourNodesWatcher.close()
      neighbourRegionsWatcher.close()

      new File(storeDir, ".lock").delete();
      primaryRegionWatcher.foreach(_.stop())
      env.shutdown()
      zookeeper.close()
      println(s"shutdown node server on ${address}, nodeId=${nodeId}")
      alive = false;
    }
  }

  private def clientOf(nodeId: Int): FsNodeClient = {
    cachedClients.getOrElseUpdate(nodeId,
      clientFactory.of(mapNeighbourNodeWithAddress(nodeId)))
  }

  def cleanData(): Unit = {
    throw new NotImplementedError();
  }

  private def writeLockFile(lockFile: File): Unit = {
    val pid = ProcessUtils.getCurrentPid();
    val fos = new FileOutputStream(lockFile);
    fos.write(pid.toString.getBytes())
    fos.close()
  }

  private def createRpcEnv(zookeeper: ZooKeeperClient): (HippoRpcEnv, RpcAddress) = {
    val env = HippoRpcEnvFactory.create(
      RpcEnvServerConfig(new RpcConf(), "regionfs-server", host, port))

    val address = env.address
    val path = s"/regionfs/nodes/${nodeId}_${address.host}_${address.port}"
    zookeeper.assertPathNotExists(path) {
      env.shutdown()
    }
    env -> address;
  }

  class FileRpcEndpoint(override val rpcEnv: HippoRpcEnv)
    extends RpcEndpoint
      with HippoRpcHandler
      with Logging {

    val traffic = new AtomicInteger(0);

    //NOTE: register only on started up
    override def onStart(): Unit = {
      //register this node and regions
      zookeeper.createNodeNode(nodeId, address);
      localRegionManager.regions.foreach(x => zookeeper.createRegionNode(nodeId, x._2))
    }

    private def createNewRegion(): (Region, Array[Int]) = {
      val region = localRegionManager.createNew()
      val regionId = region.regionId

      val nodeIds: Array[Int] = {
        if (globalSetting.replicaNum <= 1) {
          Array[Int]()
        }
        else {
          if (mapNeighbourNodeWithAddress.size < globalSetting.replicaNum - 1)
            throw new InsufficientNodeServerException(mapNeighbourNodeWithAddress.size, globalSetting.replicaNum);

          //notify neighbours
          //find thinnest neighbour which has least regions

          //TODO: very very time costing
          val thinNodeIds = mapNeighbourNodeWithAddress.map(
            x => x._1 -> mapNeighbourNodeWithRegionCount.getOrElse(x._1, new AtomicInteger(0)).get).
            toList.sortBy(_._2).takeRight(globalSetting.replicaNum - 1).map(_._1)

          if (logger.isTraceEnabled()) {
            logger.trace(s"chosen thin nodes: ${thinNodeIds.mkString(",")}");
          }

          val futures = thinNodeIds.map(clientOf(_).endPointRef.ask[CreateRegionResponse](
            CreateRegionRequest(regionId)))

          //hello, pls create a new region with id=regionId
          futures.foreach(Await.result(_, Duration.Inf))
          thinNodeIds.toArray
        }
      }

      //ok, now I register this region
      zookeeper.createRegionNode(nodeId, region)
      region -> nodeIds
    }

    private def chooseRegionForWrite(): (Region, Array[Int]) = {
      localRegionManager.synchronized {
        val writableRegions = localRegionManager.regions.values.toArray.filter(x => x.isWritable).sortBy(_.length);
        //too few regions
        if (writableRegions.size < globalSetting.minWritableRegions) {
          createNewRegion()
        }
        else {
          val region = writableRegions(0)
          region -> mapNeighbourRegionWithNodes.get(region.regionId).map(_.toArray).getOrElse(Array[Int]())
        }
      }
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case x: Any => {
        traffic.incrementAndGet()
        val t = receiveAndReplyInternal(context).apply(x)
        traffic.decrementAndGet()
        t
      }
    }

    override def onStop(): Unit = {
      logger.info("stop endpoint")
    }

    override def openCompleteStream(): PartialFunction[Any, CompleteStream] = {
      case x: Any => {
        traffic.incrementAndGet()
        val t = openCompleteStreamInternal.apply(x)
        traffic.decrementAndGet()
        t
      }
    }

    override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
      case x: Any => {
        traffic.incrementAndGet()
        val t = openChunkedStreamInternal.apply(x)
        traffic.decrementAndGet()
        t
      }
    }

    override def receiveWithStream(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
      case x: Any => {
        traffic.incrementAndGet()
        val t = receiveWithStreamInternal(extraInput, context).apply(x)
        traffic.decrementAndGet()
        t
      }
    }

    private def receiveAndReplyInternal(ctx: RpcCallContext): PartialFunction[Any, Unit] = {
      case GetNodeStatRequest() => {
        val nodeStat = NodeStat(nodeId, address,
          localRegionManager.regions.map { kv =>
            RegionStat(kv._1, kv._2.fileCount, kv._2.length)
          }.toList)

        ctx.reply(GetNodeStatResponse(nodeStat))
      }

      case GetRegionStatusRequest(regionIds: Array[Long]) => {
        val status = regionIds.map(regionId => {
          val region = localRegionManager.regions(regionId)
          region.status
        })

        ctx.reply(GetRegionStatusResponse(status))
      }

      //create region as replica
      case CreateRegionRequest(regionId: Long) => {
        val region = localRegionManager.createNewReplica(regionId)
        zookeeper.createRegionNode(nodeId, region)
        ctx.reply(CreateRegionResponse(regionId))
      }

      case PrepareToWriteFileRequest(fileSize: Long) => {
        val (region: Region, nodeIds: Array[Int]) = chooseRegionForWrite()
        ctx.reply(PrepareToWriteFileResponse(
          region.regionId,
          region.peekNextFileId(),
          (Set(region.nodeId) ++ nodeIds).toArray)
        )
      }

      case ShutdownRequest() => {
        ctx.reply(ShutdownResponse(address))
        shutdown()
      }

      case CleanDataRequest() => {
        cleanData()
        ctx.reply(CleanDataResponse(address))
      }

      case DeleteFileRequest(regionId: Long, localId: Long) => {
        val maybeRegion = localRegionManager.get(regionId)
        if (maybeRegion.isEmpty) {
          throw new WrongRegionIdException(regionId);
        }

        try {
          maybeRegion.get.delete(localId)
          //notify neigbours
          //TODO: filter(ownsNewVersion)
          val futures = mapNeighbourRegionWithNodes(regionId).filter(_ => true).map(clientOf(_).endPointRef.ask[CreateRegionResponse](
            DeleteFileRequest(regionId, localId)))

          //TODO: if fail?
          futures.foreach(Await.result(_, Duration("4s")))
          ctx.reply(DeleteFileResponse(true, null))
        }
        catch {
          case e: Throwable =>
            ctx.reply(DeleteFileResponse(false, e.getMessage))
        }
      }

      case GreetingRequest(msg: String) => {
        println(s"node-${nodeId}($address): \u001b[31;47;4m${msg}\u0007\u001b[0m")
        ctx.reply(GreetingResponse(address))
      }
    }

    private def openCompleteStreamInternal(): PartialFunction[Any, CompleteStream] = {
      case ReadFileRequest(regionId: Long, localId: Long) => {
        val maybeRegion = localRegionManager.get(regionId)

        if (maybeRegion.isEmpty) {
          throw new WrongRegionIdException(regionId);
        }

        val maybeBuffer = maybeRegion.get.read(localId)
        if (maybeBuffer.isEmpty) {
          throw new WrongLocalIdException(regionId, localId);
        }

        CompleteStream.fromByteBuffer(maybeBuffer.get)
      }

      case GetRegionPatchRequest(regionId: Long, since: Long) => {
        val maybeRegion = localRegionManager.get(regionId)
        if (maybeRegion.isEmpty) {
          throw new WrongRegionIdException(regionId);
        }

        if (traffic.get() > Constants.MAX_BUSY_TRAFFIC) {
          CompleteStream.fromByteBuffer(Unpooled.buffer(1024).writeByte(Constants.MARK_GET_REGION_PATCH_SERVER_IS_BUSY))
        }
        else {
          val buf = maybeRegion.get.offerPatch(since)
          CompleteStream.fromByteBuffer(buf)
        }
      }
    }

    private def openChunkedStreamInternal(): PartialFunction[Any, ChunkedStream] = {
      case ListFileRequest() =>
        ChunkedStream.pooled[ListFileResponseDetail](1024, (pool) => {
          localRegionManager.regions.values.foreach { x =>
            val it = x.listFiles()
            it.foreach(x => pool.push(ListFileResponseDetail(x)))
          }
        })
    }

    private def receiveWithStreamInternal(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
      case SendFileRequest(regionId: Long, fileId: FileId, totalLength: Long, crc32: Long) =>
        //primary region
        val region = localRegionManager.get(regionId).get
        val clone = extraInput.duplicate()

        if (globalSetting.enableCrc && CrcUtils.computeCrc32(clone) != crc32) {
          throw new ReceiveTimeMismatchedCheckSumException();
        }

        val localId = region.write(extraInput, crc32)
        context.reply(SendFileResponse(FileId.make(regionId, localId)))
    }
  }

}

class RegionFsServerException(msg: String, cause: Throwable = null) extends
  RegionFsException(msg, cause) {
}

class InsufficientNodeServerException(actual: Int, required: Int) extends
  RegionFsServerException(s"insufficient node server for replica: actual: ${actual}, required: ${required}") {

}

class StoreLockedException(storeDir: File, pid: Int) extends
  RegionFsServerException(s"store is locked by another node server: node server pid=${pid}, storeDir=${storeDir.getPath}") {

}

class StoreDirNotExistsException(storeDir: File) extends
  RegionFsServerException(s"store dir does not exist: ${storeDir.getPath}") {

}

class WrongLocalIdException(regionId: Long, localId: Long) extends
  RegionFsServerException(s"file #${localId} not exist in region #${regionId}") {

}

class WrongRegionIdException(regionId: Long) extends
  RegionFsServerException(s"region not exist: ${regionId}") {

}

class ReceiveTimeMismatchedCheckSumException extends
  RegionFsServerException(s"mismatched checksum exception on receive time") {

}

class PrimaryRegionWatcher(zookeeper: ZooKeeperClient,
                           conf: GlobalSetting,
                           nodeId: Int,
                           localRegionManager: RegionManager,
                           clientOf: (Int) => FsNodeClient)
  extends Logging {
  var stopped: Boolean = false;
  val thread: Thread = new Thread(new Runnable() {
    override def run(): Unit = {
      while (!stopped) {
        Thread.sleep(conf.regionVersionCheckInterval)
        val secondaryRegions = localRegionManager.regions.values.filter(!_.isPrimary).groupBy(x => (x.regionId >> 16).toInt)
        secondaryRegions.foreach {
          x => {
            try {
              val regionIds = x._2.map(_.regionId).toArray
              val statusList = Await.result(clientOf(x._1).getRegionStatus(regionIds), Duration("2s"))
              statusList.foreach(status => {
                val localRegion = localRegionManager.regions(status.regionId)
                //local is old
                val targetRevision: Long = status.revision
                val localRevision: Long = localRegion.revision
                if (targetRevision > localRevision) {
                  if (logger.isTraceEnabled())
                    logger.trace(s"[region-${localRegion.regionId}@${nodeId}] found new version : ${targetRevision}>${localRevision}");

                  val is = clientOf(x._1).getPatchInputStream(
                    localRegion.regionId, localRevision, Duration("10s"))

                  val updated = localRegion.applyPatch(is);
                  is.close();

                  if (updated) {
                    val updatedRegion = localRegionManager.update(localRegion)
                    if (logger.isTraceEnabled())
                      logger.trace(s"[region-${localRegion.regionId}@${nodeId}] updated: ${localRevision}->${updatedRegion.revision}");
                  }
                }
              })
            }
            catch {
              case t: Throwable =>
                if (logger.isWarnEnabled())
                  logger.warn(t.getMessage)
            }
          }
        }
      }
    }
  });

  def start(): PrimaryRegionWatcher = {
    thread.start();
    this;
  }

  def stop(): Unit = {
    stopped = true;
    thread.join()
  }
}