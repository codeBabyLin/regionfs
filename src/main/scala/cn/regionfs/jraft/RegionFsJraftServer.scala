package cn.regionfs.jraft

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util.Properties

import scala.collection.JavaConverters._
import cn.regionfs.jraft.rpc.{FsNodeCreateRequest, FsNodeCreateRequestProcessor, FsNodeUpdateRequest, FsNodeUpdateRequestProcessor, FsServerEvent, GetAllNodesInfoRequestProcessor, GetNodeIDReuqestProcessor, LockRegionIdRequest, LockRegionIdRequestProcessor, LockRegionIdResponse, NodeCreateEvent, NodeDeleteEvent, NodeInfo, NodeUpdateEvent, NodesInfoResponse, UnLockRegionIdRequest, UnLockRegionIdRequestProcessor}
import com.alipay.remoting.exception.CodecException
import com.alipay.remoting.serialization.SerializerManager
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.core.Replicator.ReplicatorStateListener
import com.alipay.sofa.jraft.entity.{PeerId, Task}
import com.alipay.sofa.jraft.option.{CliOptions, NodeOptions}
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import com.alipay.sofa.jraft.rpc.{RaftRpcServerFactory, RpcClient, RpcServer}
import com.alipay.sofa.jraft.util.Endpoint
import com.alipay.sofa.jraft.{JRaftUtils, Node, RaftGroupService, RouteTable, Status}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvServerConfig}
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.grapheco.commons.util.{ConfigurationEx, Logging, ProcessUtils}
import org.grapheco.hippo.{HippoRpcHandler, HippoServer, ReceiveContext}
import org.grapheco.regionfs.{Constants, GetHelloRequest, GetHelloResponse, GlobalSetting}
import org.grapheco.regionfs.server.{FsNodeServer, LocalRegionManager, NodeServerInfo, RegionEvent, RegionEventListener, RegionFsServerNullException}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
class RegionFsJraftServer(dataPath: String,
                          groupId: String,
                          serverIdStr: String,
                          initConfStr: String,
                          confPath: String) extends Logging{

  private var raftGroupService: RaftGroupService = null
  private var node: Node = null
  private var fsm: RegionFsStateMachine = null
  val conf = getconf()
  var fsNodeServer : FsNodeServer = null
  var fsNodeServerIsStart: Boolean = conf.get(Constants.PARAMETER_KEY_NODE_ISSTART).asBoolean
  var lockRegionIds =  mutable.Map[Int, Int]()

  // parse args
  val serverId: PeerId = new PeerId()
  if (!serverId.parse(serverIdStr)) throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr)
  val initConf = new Configuration()
  if (!initConf.parse(initConfStr)) throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr)


  def init(): this.type = {
    // init file directory
    FileUtils.forceMkdir(new File(dataPath))

    // add business RPC service
    // (Here, the raft RPC and the business RPC use the same RPC server)
    val rpcServer: RpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint)
    //val rc = HippoServer.create("testHip", Map(), )
    //val rpcServer2 =
    // add business RPC processor
    rpcServer.registerProcessor(new GetAllNodesInfoRequestProcessor(this))
    rpcServer.registerProcessor(new FsNodeCreateRequestProcessor(this))
    rpcServer.registerProcessor(new FsNodeUpdateRequestProcessor(this))
    rpcServer.registerProcessor(new LockRegionIdRequestProcessor(this))
    rpcServer.registerProcessor(new UnLockRegionIdRequestProcessor(this))
    rpcServer.registerProcessor(new GetNodeIDReuqestProcessor(this))


    // init state machine
    this.fsm = new RegionFsStateMachine(dataPath, fsNodeServerIsStart)
    // set NodeOption
    val nodeOptions = new NodeOptions
    // init configuration
    nodeOptions.setInitialConf(initConf)
    // set leader election timeout
    nodeOptions.setElectionTimeoutMs(2000)
    // dialbel CLI
    nodeOptions.setDisableCli(false)
    // set snapshot save period
    nodeOptions.setSnapshotIntervalSecs(30)
    // set state machine args
    nodeOptions.setFsm(this.fsm)
    // set log save path (required)
    nodeOptions.setLogUri(dataPath + File.separator + "log")
    // set meta save path (required)
    nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta")
    // set snapshot save path (Optional)
   // if (useSnapshot()) nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot")
    // init raft group
    this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer)
    this.node = this.raftGroupService.start
    //logger.info("Started PandaJraftServer at port:" + this.node.getNodeId.getPeerId.getPort)
    val bool = true
    while(this.node.getLeaderId ==null){
      println("wait for leader!!!")
      Thread.sleep(500)
    }
    println("leader is " + this.node.getLeaderId.getIp + ":" +this.node.getLeaderId.getPort)
    addReplicaListen()
    //startHippoServer()
    //println(this.node.addReplicatorStateListener())
    this
  }

  def getconf(): ConfigurationEx = {
    val conf = new ConfigurationEx(new File(this.confPath))
    conf
  }

  def getEndpoint(): Endpoint = {
    //val conf = getconf()
    new Endpoint(getServerHost, getServerPort)
  }

  def getRpcAddress(): RpcAddress = {
    new RpcAddress(getServerHost, getServerPort)
  }
  def getNodeId(): Int = conf.get(Constants.PARAMETER_KEY_NODE_ID).asInt

  def getStoreDir(): File = {
    val storeDir = Paths.get(conf.get(Constants.PARAMETER_KEY_DATA_STORE_DIR).asString, "").toFile
    if (!storeDir.exists()) {
      storeDir.mkdirs()
    }
    val lockFile = new File(storeDir, ".lock")
    if (lockFile.exists()) {
      val fis = new FileInputStream(lockFile)
      val pid = IOUtils.toString(fis).toInt
      fis.close()

      //throw new RegionStoreLockedException(storeDir, pid)
      println(s"the last pid is ${pid} at path ${storeDir.getAbsolutePath}")
    }
    storeDir
  }
  def getServerHost: String =  conf.get(Constants.PARAMETER_KEY_SERVER_HOST).withDefault(Constants.DEFAULT_SERVER_HOST).asString
  def getServerPort: Int = conf.get(Constants.PARAMETER_KEY_SERVER_PORT).withDefault(Constants.DEFAULT_SERVER_PORT).asInt
  def getRegionCount(): Int ={
    if (this.fsNodeServer == null) throw new RegionFsServerNullException
    else this.fsNodeServer.localRegionManager.regions.size
  }
  def startFsNodeServer(): Unit = {
    if (fsNodeServerIsStart){
      this.fsNodeServer = new FsNodeServer(this, getNodeId(), getStoreDir(), getServerHost, getServerPort)
      this.fsNodeServer.start()
      this.fsm.setFsNodeServer(this.fsNodeServer)
      this.fsNodeServer.awaitTermination()
    }
    //FsNodeServer.create(new File(confPath), this).awaitTermination()
  }

  def shutdown(): Unit = {
    this.node.shutdown()
    //this.started = false
  }

  def getAllnodesInfo(): NodesInfoResponse = {
    val nodesInfo: ArrayBuffer[NodeInfo] = new ArrayBuffer[NodeInfo]()
    fsNodeServer.mapNeighbourNodeWithAddress.foreach(u => {
      nodesInfo += new NodeInfo(u._1, u._2, fsNodeServer.mapNeighbourNodeWithRegionCount.get(u._1).get)
    })
    nodesInfo += new NodeInfo(fsNodeServer.nodeId, fsNodeServer.address, fsNodeServer.localRegionManager.regions.size)
    val response = new NodesInfoResponse(nodesInfo.toArray)
    response
  }

  def loadGlobalSetting(): GlobalSetting = {
    //val conf = getconf()
    val props = new Properties()
    props.setProperty(Constants.PARAMETER_KEY_REPLICA_NUM, conf.get(Constants.PARAMETER_KEY_REPLICA_NUM).asString)
    new GlobalSetting(props)
    //todo set or load props from jraft
  }
  //def isStarted(): Boolean = this.started

  def compareRpcAddress(peerId: PeerId, address: RpcAddress): Boolean = {
    if (peerId.getIp == address.host && peerId.getPort == address.port) true
    else false
  }
  def addReplicaListen(): Unit = {
    val node = this.node
    val listen = new ReplicatorStateListener {
      override def onCreated(peerId: PeerId): Unit = {
        //addReplicaListen()
        println(s"the peerId: ${peerId.getPort} get on line")
      }

      override def onError(peerId: PeerId, status: Status): Unit = {
        //addReplicaListen()
        val ips = fsNodeServer.mapNeighbourNodeWithAddress
        var nodeInfo: NodeServerInfo = null
        var hasIp = false
        ips.foreach(u => {
          if (compareRpcAddress(peerId, u._2)) {
            hasIp = true
            nodeInfo = NodeServerInfo(u._1, u._2, 0)
          }
        })
        if (hasIp) {
          broadNodeDelete(nodeInfo)
        }
        println(s"the peerId: ${peerId.getPort} get on erroe: ${status.getErrorMsg}")
      }

      override def onDestroyed(peerId: PeerId): Unit = {
        //addReplicaListen()
        println(s"the peerId: ${peerId.getPort} get off line")
      }
    }
    node.addReplicatorStateListener(listen)
  }

  def getFsm: RegionFsStateMachine = this.fsm


  def getRaftNode: Node = this.node

  def isLeader: Boolean = this.getRaftNode.isLeader

  def getRaftGroupService: RaftGroupService = this.raftGroupService




  def getPeers(): Set[PeerId] = {
    val uri = this.serverId.getIp + ":" + this.serverId.getPort
    val conf = JRaftUtils.getConfiguration(uri)
    val cliClientService = new CliClientServiceImpl
    cliClientService.init(new CliOptions())
    RouteTable.getInstance().updateConfiguration(this.groupId, conf)
    RouteTable.getInstance().refreshConfiguration(cliClientService, this.groupId, 10000)
    RouteTable.getInstance().getConfiguration(this.groupId).getPeerSet.asScala.toSet
  }

  def getLeader(): PeerId = {
    val uri = this.serverId.getIp + ":" + this.serverId.getPort
    val conf = JRaftUtils.getConfiguration(uri)
    val cliClientService = new CliClientServiceImpl
    cliClientService.init(new CliOptions())
    RouteTable.getInstance().updateConfiguration(this.groupId, conf)
    RouteTable.getInstance().refreshConfiguration(cliClientService, this.groupId, 10000)
    RouteTable.getInstance().selectLeader(this.groupId)
  }

  def getRpcClient(): RpcClient = {
    val uri = this.serverId.getIp + ":" + this.serverId.getPort
    val conf = JRaftUtils.getConfiguration(uri)
    val cliClientService = new CliClientServiceImpl
    cliClientService.init(new CliOptions())
    RouteTable.getInstance().updateConfiguration(this.groupId, conf)
    RouteTable.getInstance().refreshConfiguration(cliClientService, this.groupId, 10000)
    cliClientService.getRpcClient
  }

  def LockRegionId(regionId: Int, nodeId: Int): Boolean = {
    if (this.isLeader) {
      if (this.lockRegionIds.get(regionId).isEmpty){
        this.lockRegionIds += regionId -> nodeId
        true
      }
      else false
    }
    else {
      val request = new LockRegionIdRequest(regionId, nodeId)
      getRpcClient().invokeSync(this.node.getLeaderId.getEndpoint, request, 5000).asInstanceOf[LockRegionIdResponse].isAcquire
    }
  }
  def UnlockRegionId(regionId: Int, nodeId: Int): Boolean = {
    if (this.isLeader) {
      if (!this.lockRegionIds.get(regionId).isEmpty && nodeId == this.fsNodeServer.nodeId){
        this.lockRegionIds -= regionId
        true
      }
      else false
    }
    else {
      val request = new UnLockRegionIdRequest(regionId, nodeId)
      getRpcClient().invokeSync(this.node.getLeaderId.getEndpoint, request, 5000).asInstanceOf[LockRegionIdResponse].isAcquire
    }
  }

  def sendNodeCreatetoLeader(nodeInfo: NodeServerInfo): Unit = {
    println("====send create====")
    if (this.isLeader) broadNodeCreate(nodeInfo)
    else {
      val request = new FsNodeCreateRequest(nodeInfo)
      getRpcClient().invokeSync(this.node.getLeaderId.getEndpoint, request, 1000)
    }
  }

  def sendNodeUpdatetoLeader(nodeInfo: NodeServerInfo): Unit = {
    println("****send update*****")
    if (this.isLeader) broadNodeUpdate(nodeInfo)
    else {
      val request = new FsNodeUpdateRequest(nodeInfo)
      getRpcClient().invokeSync(this.node.getLeaderId.getEndpoint, request, 1000)
    }
  }

  def broadNodeCreate(nodeInfo: NodeServerInfo): Unit = {
    //val nodeInfo = NodeServerInfo(getNodeId(), getRpcAddress(), getRegionCount())
    val event = NodeCreateEvent(nodeInfo)
    val fse = new FsServerEvent(event)
    //fse.setEvent(event)
    broadcastMessage(fse)
  }
  def broadNodeUpdate(nodeInfo: NodeServerInfo): Unit = {
    //val nodeInfo = NodeServerInfo(getNodeId(), getRpcAddress(), getRegionCount())
    val event = NodeUpdateEvent(nodeInfo)
    val fse = new FsServerEvent(event)
    //fse.setEvent(event)
    broadcastMessage(fse)
  }

  def broadNodeDelete(nodeInfo: NodeServerInfo): Unit = {
    //val nodeInfo = NodeServerInfo(getNodeId(), getRpcAddress(), getRegionCount())
    val event = NodeDeleteEvent(nodeInfo)
    val fse = new FsServerEvent(event)
    //fse.setEvent(event)
    broadcastMessage(fse)
  }

  def broadcastMessage(fse: FsServerEvent): Unit = {
    if (!this.isLeader) return
    while(this.fsNodeServer == null){
      Thread.sleep(500)
    }
    fse.handelEvent(this.fsNodeServer)
    println(fse.event.toString)
    try {
      val task = new Task
      task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(fse)))
      this.node.apply(task)
    } catch {
      case e: CodecException =>
        val errorMsg = "Fail to encode tx operation"
        logger.error(errorMsg, e)
    }
  }

}


