package cn.regionfs.jraft.rpc

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import io.netty.buffer.Unpooled
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint}
import net.neoremind.kraps.rpc.netty.HippoRpcEnv
import org.grapheco.commons.util.Logging
import org.grapheco.hippo.{ChunkedStream, HippoRpcHandler, PooledMessageStream, ReceiveContext}
import org.grapheco.regionfs.client.{NodeStat, RegionStat}
import org.grapheco.regionfs.server.{FileNotFoundException, InsufficientNodeServerException, ReceivedMismatchedStreamException, Region, RegionInfo, RegionNotFoundOnNodeServerException}
import org.grapheco.regionfs.util.{Atomic, CrcUtils, RetryStrategy, Rollbackable, TransactionRunner}
import org.grapheco.regionfs.{CleanDataRequest, CleanDataResponse, Constants, CreateFileRequest, CreateFileResponse, CreateSecondaryFileRequest, CreateSecondaryFileResponse, CreateSecondaryRegionRequest, CreateSecondaryRegionResponse, DeleteFileRequest, DeleteFileResponse, DeleteSeconaryFileRequest, DeleteSeconaryFileResponse, FileId, GetHelloRequest, GetHelloResponse, GetNodeStatRequest, GetNodeStatResponse, GetRegionInfoRequest, GetRegionInfoResponse, GetRegionOwnerNodesRequest, GetRegionOwnerNodesResponse, GetRegionPatchRequest, GetRegionsOnNodeRequest, GetRegionsOnNodeResponse, GreetingRequest, GreetingResponse, ListFileRequest, ListFileResponseDetail, MarkSecondaryFileWrittenRequest, MarkSecondaryFileWrittenResponse, ProcessFilesRequest, ProcessFilesResponse, ReadFileRequest, ReadFileResponseHead, RegisterSeconaryRegionsRequest, ShutdownRequest, ShutdownResponse}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration


class FileRpcEndpoint(override val rpcEnv: HippoRpcEnv) extends RpcEndpoint with HippoRpcHandler with Logging {

    Unpooled.buffer(1024)

    private val traffic = new AtomicInteger(0)

    //NOTE: register only on started up
    override def onStart(): Unit = {
    //register this node
    //todo set node info
    //zookeeper.createNodeNode(nodeId, address, localRegionManager)
  }

  /*  private def createNewRegion(): (Region, Array[RegionInfo]) = {
    val localRegion = localRegionManager.createNew()
    val regionId = localRegion.regionId

    val neighbourRegions: Array[RegionInfo] = {
      if (globalSetting.replicaNum <= 1) {
        Array()
      }
      else {
        if (mapNeighbourNodeWithAddress.size < globalSetting.replicaNum - 1)
          throw new InsufficientNodeServerException(mapNeighbourNodeWithAddress.size, globalSetting.replicaNum - 1)

        //sort neighbour nodes by region count
        val thinNodeIds = mapNeighbourNodeWithRegionCount.toArray.sortBy(_._2).take(globalSetting.replicaNum - 1).map(_._1)
        val futures = thinNodeIds.map(clientOf(_).createSecondaryRegion(regionId))

        //hello, pls create a new localRegion with id=regionId
        val neighbourResults = futures.map(Await.result(_, Duration.Inf).info)
        //zookeeper.updateNodeData(nodeId, address, localRegionManager)
        //todo update node server
        remoteRegionWatcher.cacheRemoteSeconaryRegions(neighbourResults)
        neighbourResults
      }
    }

    localRegion -> neighbourRegions
  }*/

  /*  private def chooseRegionForWrite(): (Region, Array[RegionInfo]) = {
    val writableRegions = localRegionManager.regions.values.toArray.filter(x => x.isWritable && x.isPrimary)

    //too few writable regions
    if (writableRegions.length < globalSetting.minWritableRegions) {
      createNewRegion()
    }
    else {
      val localRegion = localRegionManager.synchronized {
        localRegionManager.regions(localRegionManager.ring.take(x =>
          writableRegions.exists(_.regionId == x)).get)
      }

      localRegion -> remoteRegionWatcher.getSecondaryRegions(localRegion.regionId)
    }
  }*/

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case x: Any =>
      try {
        traffic.incrementAndGet()
        val t = receiveAndReplyInternal(context).apply(x)
        traffic.decrementAndGet()
        t
      }
      catch {
        case e: Throwable =>
          context.sendFailure(e)
      }
      finally {
        traffic.decrementAndGet()
      }
  }

    override def onStop(): Unit = {
    logger.info("stop endpoint")
  }

    override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    case x: Any =>
      traffic.incrementAndGet()
      val t = openChunkedStreamInternal.apply(x)
      traffic.decrementAndGet()
      t
  }

    override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
    case x: Any =>
      try {
        traffic.incrementAndGet()
        val t = receiveWithStreamInternal(extraInput, context).apply(x)
        t
      }
      catch {
        case e: Throwable =>
          context.sendFailure(e)
      }
      finally {
        traffic.decrementAndGet()
      }
  }

    private def receiveAndReplyInternal(ctx: RpcCallContext): PartialFunction[Any, Unit] = {
    //TODO: cancelable task
    case GetHelloRequest() => {
      println("=========hello msg")
      ctx.reply(GetHelloResponse("hello world"))
    }
/*    case ProcessFilesRequest(process) => {
      val results = process(localRegionManager.regions.values.filter(_.isPrimary).flatMap { region => region.listFiles()})

      ctx.reply(ProcessFilesResponse(results))
    }*/

   /* case GetRegionOwnerNodesRequest(regionId: Long) =>
      ctx.reply(GetRegionOwnerNodesResponse(localRegionManager.regions.get(regionId).map(_.info).toArray
        ++ remoteRegionWatcher.cachedRemoteSecondaryRegions(regionId)))

    case GetRegionsOnNodeRequest() =>
      ctx.reply(GetRegionsOnNodeResponse(localRegionManager.regions.values.map(_.info).toArray))

    case GetNodeStatRequest() =>
      val nodeStat = NodeStat(nodeId, address,
        localRegionManager.regions.values.filter(_.isPrimary).map { region =>
          RegionStat(region.regionId, region.fileCount, region.bodyLength)
        }.toList)

      ctx.reply(GetNodeStatResponse(nodeStat))

    case GetRegionInfoRequest(regionIds: Array[Long]) =>
      val infos = regionIds.map(regionId => {
        val region = localRegionManager.regions(regionId)
        region.info
      })

      ctx.reply(GetRegionInfoResponse(infos))

    //create region as replica
    case CreateSecondaryRegionRequest(regionId: Long) =>
      val region = localRegionManager.createSecondaryRegion(regionId)
      //zookeeper.updateNodeData(nodeId, address, localRegionManager)
      //todo update server
      ctx.reply(CreateSecondaryRegionResponse(region.info))

    case ShutdownRequest() =>
      ctx.reply(ShutdownResponse(address))
      shutdown()

    case CleanDataRequest() =>
      cleanData()
      ctx.reply(CleanDataResponse(address))

    case RegisterSeconaryRegionsRequest(regions: Array[RegionInfo]) =>
      remoteRegionWatcher.cacheRemoteSeconaryRegions(regions)

    case DeleteSeconaryFileRequest(fileId: FileId) =>
      handleDeleteSeconaryFileRequest(fileId, ctx)

    case DeleteFileRequest(fileId: FileId) =>
      handleDeleteFileRequest(fileId, ctx)

    case GreetingRequest(msg: String) =>
      println(s"node-$nodeId($address): \u001b[31;47;4m${msg}\u0007\u001b[0m")
      ctx.reply(GreetingResponse(address))*/
  }

  /*  private def handleDeleteFileRequest(fileId: FileId, ctx: RpcCallContext): Unit = {
    val maybeRegion = localRegionManager.get(fileId.regionId)
    if (maybeRegion.isEmpty) {
      throw new RegionNotFoundOnNodeServerException(nodeId, fileId.regionId)
    }

    try {
      val localRegion: Region = maybeRegion.get
      if (localRegion.revision <= fileId.localId)
        throw new FileNotFoundException(nodeId, fileId)

      val success = localRegion.delete(fileId.localId)
      val regions =
      //is a primary region?
        if (globalSetting.replicaNum > 1 && (fileId.regionId >> 16) == nodeId) {
          //notify secondary regions
          val futures = remoteRegionWatcher.getSecondaryRegions(fileId.regionId).map(x =>
            clientOf(x.nodeId).endPointRef.ask[DeleteSeconaryFileResponse](
              DeleteSeconaryFileRequest(fileId)))

          //TODO: if fail?
          (Set(localRegion.info) ++ futures.map(Await.result(_, Duration.Inf).info)).toArray
        }
        else {
          Array(localRegion.info)
        }

      remoteRegionWatcher.cacheRemoteSeconaryRegions(regions)
      ctx.reply(DeleteFileResponse(success, null, regions))
    }
    catch {
      case e: Throwable =>
        ctx.reply(DeleteFileResponse(false, e.getMessage, Array()))
    }
  }*/

  /*  private def handleDeleteSeconaryFileRequest(fileId: FileId, ctx: RpcCallContext): Unit = {
    val maybeRegion = localRegionManager.get(fileId.regionId)
    if (maybeRegion.isEmpty) {
      throw new RegionNotFoundOnNodeServerException(nodeId, fileId.regionId)
    }

    try {
      if (maybeRegion.get.revision <= fileId.localId)
        throw new FileNotFoundException(nodeId, fileId)

      val success = maybeRegion.get.delete(fileId.localId)
      ctx.reply(DeleteSeconaryFileResponse(success, null, maybeRegion.get.info))
    }
    catch {
      case e: Throwable =>
        ctx.reply(DeleteSeconaryFileResponse(false, e.getMessage, maybeRegion.get.info))
    }
  }*/

    private def openChunkedStreamInternal(): PartialFunction[Any, ChunkedStream] = {
    case ListFileRequest() =>
      handleListFileRequest()
  }

    private def handleListFileRequest(): PooledMessageStream[ListFileResponseDetail] = {
  /*  ChunkedStream.pooled[ListFileResponseDetail](1024, (pool) => {
      localRegionManager.regions.values.filter(_.isPrimary).foreach { x =>
        val it = x.listFiles().map(entry => entry.id -> entry.length)
        it.foreach(x => pool.push(ListFileResponseDetail(x)))
      }
    })*/
      null
  }

    private def receiveWithStreamInternal(extraInput: ByteBuffer, ctx: ReceiveContext): PartialFunction[Any, Unit] = {
    case GetRegionPatchRequest(regionId: Long, since: Long) =>
      handleGetRegionPatchRequest(regionId, since, ctx)

    case ReadFileRequest(fileId: FileId) =>
      handleReadFileRequest(fileId, ctx)

    case CreateFileRequest(totalLength: Long, crc32: Long) =>
      //handleCreateFileRequest(totalLength, crc32, extraInput, ctx)

    case CreateSecondaryFileRequest(regionId: Long, localId: Long, totalLength: Long, crc32: Long) =>
      handleCreateSecondaryFileRequest(regionId, localId, totalLength, crc32, extraInput, ctx)

    case MarkSecondaryFileWrittenRequest(regionId: Long, localId: Long, totalLength: Long) =>
      handleMarkSecondaryFileWrittenRequest(regionId, localId, totalLength, ctx)
  }

    private def handleGetRegionPatchRequest(regionId: Long, since: Long, ctx: ReceiveContext): Unit = {
/*    val maybeRegion = localRegionManager.get(regionId)
    if (maybeRegion.isEmpty) {
      throw new RegionNotFoundOnNodeServerException(nodeId, regionId)
    }

    if (traffic.get() > Constants.MAX_BUSY_TRAFFIC) {
      ctx.replyBuffer(Unpooled.buffer(1024).writeByte(Constants.MARK_GET_REGION_PATCH_SERVER_IS_BUSY))
    }
    else {
      ctx.replyBuffer(maybeRegion.get.offerPatch(since))
    }*/
  }

    private def handleReadFileRequest(fileId: FileId, ctx: ReceiveContext): Unit = {
/*    val maybeRegion = localRegionManager.get(fileId.regionId)

    if (maybeRegion.isEmpty) {
      throw new RegionNotFoundOnNodeServerException(nodeId, fileId.regionId)
    }

    val localRegion: Region = maybeRegion.get
    val maybeBuffer = localRegion.read(fileId.localId)
    if (maybeBuffer.isEmpty) {
      throw new FileNotFoundException(nodeId, fileId)
    }

    val body = Unpooled.wrappedBuffer(maybeBuffer.get.duplicate())
    val crc32 = CrcUtils.computeCrc32(maybeBuffer.get.duplicate())

    val buf = Unpooled.buffer()
    buf.writeObject(ReadFileResponseHead(body.readableBytes(), crc32,
      (Set(localRegion.info) ++ remoteRegionWatcher.getSecondaryRegions(fileId.regionId)).toArray))

    buf.writeBytes(body)
    ctx.replyBuffer(buf)*/
  }

    private def handleMarkSecondaryFileWrittenRequest(regionId: Long, localId: Long, totalLength: Long, ctx: ReceiveContext): Unit = {
  /*  assert(totalLength >= 0)
    val region = localRegionManager.get(regionId).get
    region.markGlobalWriten(localId, totalLength)

    ctx.reply(MarkSecondaryFileWrittenResponse(regionId, localId, region.info))*/
  }

    private def handleCreateSecondaryFileRequest(regionId: Long, localId: Long, totalLength: Long, crc32: Long, extraInput: ByteBuffer, ctx: ReceiveContext): Unit = {
    assert(totalLength >= 0)

    if (CrcUtils.computeCrc32(extraInput.duplicate()) != crc32) {
      throw new ReceivedMismatchedStreamException()
    }

   /* val region = localRegionManager.get(regionId).get
    region.saveLocalFile(localId, extraInput.duplicate(), crc32)
    region.markLocalWriten(localId)*/

    ctx.reply(CreateSecondaryFileResponse(regionId, localId))
  }

/*    private def handleCreateFileRequest(totalLength: Long, crc32: Long, extraInput: ByteBuffer, ctx: ReceiveContext): Unit = {
    //primary region
    if (CrcUtils.computeCrc32(extraInput.duplicate()) != crc32) {
      throw new ReceivedMismatchedStreamException()
    }

    val (localRegion: Region, neighbourRegions: Array[RegionInfo]) = chooseRegionForWrite()
    val regionId = localRegion.regionId

    //todo lock write
    //val mutex = zookeeper.createRegionWriteLock(regionId)
    // mutex.acquire()

    try {
      //i am a primary region
      if (globalSetting.replicaNum > 1 &&
        globalSetting.consistencyStrategy == Constants.CONSISTENCY_STRATEGY_STRONG) {

        //create secondary files
        val tx = Atomic("create local id") {
          case _ =>
            localRegion.createLocalId()
        } --> Atomic("request to create secondary file") {
          case localId: Long =>
            Rollbackable.success(localId -> neighbourRegions.map(x =>
              clientOf(x.nodeId).createSecondaryFile(regionId, localId, totalLength, crc32,
                extraInput.duplicate()))) {}
        } --> Atomic("save local file") {
          case (localId: Long, futures: Array[Future[CreateSecondaryFileResponse]]) =>
            localRegion.saveLocalFile(localId, extraInput.duplicate(), crc32) map {
              case _ =>
                Rollbackable.success(localId -> futures) {}
            }
        } --> Atomic("waits all secondary file creation response") {
          case (localId: Long, futures: Array[Future[CreateSecondaryFileResponse]]) =>
            futures.foreach(Await.result(_, Duration.Inf))

            Rollbackable.success(localId) {}
        } --> Atomic("mark local written") {
          case (localId: Long) =>
            localRegion.markLocalWriten(localId)
        } --> Atomic("mark global written") {
          case (localId: Long) =>
            val futures =
              neighbourRegions.map(x => clientOf(x.nodeId).markSecondaryFileWritten(regionId, localId, totalLength))

            val neighbourResults = futures.map(Await.result(_, Duration.Inf)).map(x => x.info)

            localRegion.markGlobalWriten(localId, totalLength)
            remoteRegionWatcher.cacheRemoteSeconaryRegions(neighbourResults)

            val fid = FileId.make(regionId, localId)
            ctx.reply(CreateFileResponse(fid, (Set(localRegion.info) ++ neighbourResults).toArray))

            Rollbackable.success(fid) {
            }
        }

        TransactionRunner.perform(tx, regionId, RetryStrategy.FOR_TIMES(globalSetting.maxWriteRetryTimes))
      }
      else {
        val tx = Atomic("create local id") {
          case _ =>
            localRegion.createLocalId()
        } --> Atomic("save local file") {
          case localId: Long =>
            localRegion.saveLocalFile(localId, extraInput.duplicate(), crc32)
        } --> Atomic("mark global written") {
          case (localId: Long) =>
            localRegion.markGlobalWriten(localId, totalLength)
        } --> Atomic("response") {
          case localId: Long =>
            val fid = FileId.make(regionId, localId)

            ctx.reply(CreateFileResponse(fid, Array(localRegion.info)))
            Rollbackable.success(fid) {}
        }

        TransactionRunner.perform(tx, regionId, RetryStrategy.FOR_TIMES(globalSetting.maxWriteRetryTimes))
      }
    }
    finally {
      //if (mutex.isAcquiredInThisProcess) {
      //  mutex.release()
      //}
    }
  }*/
}
