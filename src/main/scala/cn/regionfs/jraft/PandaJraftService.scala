package cn.regionfs.jraft

import java.nio.ByteBuffer

import com.alipay.sofa.jraft.Lifecycle
import com.alipay.sofa.jraft.entity.PeerId
import org.grapheco.commons.util.Logging


class PandaJraftService extends Lifecycle[String] with Logging {
  var jraftServer: PandaJraftServer = null
  //val pandaConfig: PandaConfig = PandaRuntimeContext.contextGet[PandaConfig]()

  //PandaRuntimeContext.contextPut[PandaJraftService](this)

/*  def commitWriteOpeartions(ops: WriteOperations): Unit = {
    if (!jraftServer.isLeader || ops.size == 0) {
      return
    }

    try {
      val task = new Task
      task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(ops)))
      jraftServer.getNode.apply(task)
    } catch {
      case e: CodecException =>
        val errorMsg = "Fail to encode tx operation"
        logger.error(errorMsg, e)
    }
  }*/

  def isLeader(): Boolean = {
    jraftServer.isLeader
  }

  def init(): Unit = {
/*    if (jraftServer == null) {
      val dataPath: String = pandaConfig.jraftDataPath
      val serverId: String = pandaConfig.jraftServerId
      val groupId: String = pandaConfig.jraftGroupId
      val peers: String = pandaConfig.jraftPeerIds
      jraftServer = new PandaJraftServer(dataPath, groupId, serverId, peers)
    }*/
    logger.info("==== jraft server init ====")
  }

  def start(): Unit = {
    if (jraftServer == null) {
      init()
    }
    jraftServer.start()
    logger.info("==== jraft server started ====")
  }

  def stop(): Unit = {
    logger.info("==== jraft server stop ====")
    shutdown()
  }

  override def shutdown(): Unit = {
    jraftServer.shutdown()
    logger.info("==== jraft server shutdown ====")
  }

  def getPeers(): Set[PeerId] = {
    jraftServer.getPeers()
  }

  def getLeader(): PeerId = {
    jraftServer.getLeader()
  }

  def isStarted(): Boolean = {
    jraftServer!=null && jraftServer.isStarted()
  }

  def appliedTxLogIndex(): Long = {
    if (jraftServer!=null) {
      jraftServer.getFsm.logIndexFile.load()
    } else {
      -1
    }
  }

  override def init(t: String): Boolean = ???
}
