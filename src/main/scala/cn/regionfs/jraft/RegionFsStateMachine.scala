package cn.regionfs.jraft

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong

import cn.regionfs.jraft.rpc.FsServerEvent
import cn.regionfs.jraft.snapshot.{LogIndexFile, PandaGraphSnapshotFile}
import com.alipay.remoting.exception.CodecException
import com.alipay.remoting.serialization.SerializerManager
import com.alipay.sofa.jraft.core.StateMachineAdapter
import com.alipay.sofa.jraft.error.RaftException
import com.alipay.sofa.jraft.storage.snapshot.{SnapshotReader, SnapshotWriter}
import com.alipay.sofa.jraft.util.Utils
import org.grapheco.commons.util.Logging
import com.alipay.sofa.jraft.{Closure, Status, Iterator => SofaIterator}
import org.grapheco.regionfs.server.FsNodeServer

class RegionFsStateMachine(path: String) extends StateMachineAdapter with Logging{

  /**
    * Leader term
    */
  private val leaderTerm = new AtomicLong(-1)

  var fs: FsNodeServer = null

  def setFsNodeServer(server: FsNodeServer): Unit = {
    fs = server
  }

  val logIndexFile: LogIndexFile = new LogIndexFile(getLogIndexPath())

  def isLeader: Boolean = this.leaderTerm.get > 0

  def getLogIndexPath(): String = {
    Paths.get(getDataPath(), "logIndex").toString
  }

  override def onApply(iter: SofaIterator): Unit = {
  /*  PandaRuntimeContext.setSnapshotLoaded(true)
    while (PandaRuntimeContext.contextGetOption[GraphDatabaseService]().isEmpty) {
      logger.info("wait for GraphDatabaseService created")
      Thread.sleep(500)
    }*/
    //val neo4jDB = PandaRuntimeContext.contextGet[GraphDatabaseService]()
 /*   while (!neo4jDB.isAvailable(1000)) {
      logger.info("wait for GraphDatabaseService available")
    }*/
    var logIndex: Long = logIndexFile.load()
    //println("task come!!!!")
    while ( iter.hasNext() && iter.getIndex > logIndex) {
      var fsEvent: FsServerEvent = null
      // leader not apply task
      if (!isLeader) { // Have to parse FetchAddRequest from this user log.
        val data = iter.getData
        try {
          fsEvent = SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(data.array, classOf[FsServerEvent].getName)
        }
        catch {
          case e: CodecException =>
            logger.error("Fail to decode WriteOperations", e)
        }
        println(s"${fsEvent.getEvent().toString}")
        while(this.fs == null) {
          println("fs is null =====")
          Thread.sleep(500)
        }

        fsEvent.handelEvent(this.fs)
        //}
      }
      iter.next
    }
    val logIndexNew = iter.getIndex.toInt - 1
    if (logIndexNew > logIndex) logIndex = logIndexNew
    logIndexFile.save(logIndex)
  }

  def getDataPath(): String = {
    //val pandaConfig: PandaConfig = PandaRuntimeContext.contextGet[PandaConfig]()
    //pandaConfig.dataPath
    path
  }

  def getActiveDatabase(): String = {
    //val pandaConfig: PandaConfig = PandaRuntimeContext.contextGet[PandaConfig]()
    //pandaConfig.activeDatabase
    null
  }
  override def onSnapshotSave(writer: SnapshotWriter, done: Closure): Unit = {
    logger.info("save snapshot.")
    val snap = new PandaGraphSnapshotFile
    Utils.runInThread(new Runnable {
      override def run(): Unit = {
        val dataPathFile = Paths.get("ef", "databases" + File.separator + "5645").toFile
        if (dataPathFile.exists()) {
          val dataPath = dataPathFile.getAbsoluteFile.toString
          snap.save(dataPath, writer.getPath)
          if (writer.addFile("backup.zip")) done.run(Status.OK())
        }
      }
    })
  }

  override def onError(e: RaftException): Unit = {
    logger.error("Raft error: {}", e)
  }

  override def onSnapshotLoad(reader: SnapshotReader): Boolean = {
    if (isLeader) {
      logger.warn("Leader is not supposed to load snapshot")
      return false
    }
    if (reader.getFileMeta("backup.zip") == null) {
      logger.error("Fail to find data file in {}", reader.getPath)
      return false
    }
    logger.info("load snapshot.")
    val snap = new PandaGraphSnapshotFile
    val loadDirectory = new File(reader.getPath)
    val dataPath = Paths.get(getDataPath(), "databases").toString
    var ret = false
    if (loadDirectory.isDirectory) {
      val files = loadDirectory.listFiles()
      if (files.length == 0) {
        logger.error("snapshot file is not existed.")
      }
      else {
        files.foreach(f => {
          if (f.getName.endsWith("zip")) snap.load(f.getAbsolutePath, dataPath)
        })
        ret = true
      }
    }
    else {
      logger.error("snapshot file save directory is not existed.")
    }
    //PandaRuntimeContext.setSnapshotLoaded(true)
    ret
  }

  override def onLeaderStart(term: Long): Unit = {
    this.leaderTerm.set(term)
    super.onLeaderStart(term)
  }

  override def onLeaderStop(status: Status): Unit = {
    this.leaderTerm.set(-1)
    super.onLeaderStop(status)
  }
}
