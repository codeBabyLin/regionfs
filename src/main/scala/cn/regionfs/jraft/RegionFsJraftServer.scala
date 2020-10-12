package cn.regionfs.jraft

import java.io.File
import java.nio.ByteBuffer
import java.util.Properties

import scala.collection.JavaConverters._
import cn.regionfs.jraft.rpc.{FileRpcEndpoint, GetAllNodesInfoRequestProcessor, NodeInfo, NodesInfoResponse}
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.entity.PeerId
import com.alipay.sofa.jraft.option.{CliOptions, NodeOptions}
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import com.alipay.sofa.jraft.rpc.{RaftRpcServerFactory, RpcServer}
import com.alipay.sofa.jraft.util.Endpoint
import com.alipay.sofa.jraft.{JRaftUtils, Node, RaftGroupService, RouteTable}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvServerConfig}
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import org.apache.commons.io.FileUtils
import org.grapheco.commons.util.{ConfigurationEx, Logging, ProcessUtils}
import org.grapheco.hippo.{HippoRpcHandler, HippoServer, ReceiveContext}
import org.grapheco.regionfs.{Constants, GetHelloRequest, GetHelloResponse, GlobalSetting}
import org.grapheco.regionfs.server.FsNodeServer
class RegionFsJraftServer(dataPath: String,
                          groupId: String,
                          serverIdStr: String,
                          initConfStr: String,
                          confPath: String) {

  private var raftGroupService: RaftGroupService = null
  private var node: Node = null
  private var fsm: RegionFsStateMachine = null
  var serverRpcEnv:HippoRpcEnv = null
  var server: HippoServer = null



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

    // init state machine
    this.fsm = new RegionFsStateMachine()
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
    //startHippoServer()
    this
  }

  def getconf(): ConfigurationEx = {
    val conf = new ConfigurationEx(new File(this.confPath))
    conf
  }

  def startHippoServer(): Unit = {
    val rpcAddress = getRpcAddress()
    this.server = HippoServer.create("test", Map(), new HippoRpcHandler {
      override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
        case GetHelloRequest => context.reply(GetHelloResponse("hello world"))
      }
    }, rpcAddress.getPort, rpcAddress.getIp)
    println("rpc server started!!!!! : " + this.server.getPort())
  }

  def startHippoRpc(): Unit = {
    val rpcAddress = getRpcAddress()
    val serverConfig = RpcEnvServerConfig(new RpcConf(), "hippo-server", rpcAddress.getIp, rpcAddress.getPort)
    this.serverRpcEnv = HippoRpcEnvFactory.create(serverConfig)
    val endpoint = new FileRpcEndpoint(this.serverRpcEnv)
    this.serverRpcEnv.setupEndpoint("hippo-sever1", endpoint)
    this.serverRpcEnv.setRpcHandler(endpoint)
    //this.serverRpcEnv.awaitTermination()
    println("rpc server started!!!!!")
  }

  def getRpcAddress(): Endpoint = {
    val conf = getconf()
    new Endpoint(conf.get(Constants.PARAMETER_KEY_SERVER_HOST).asString, conf.get(Constants.PARAMETER_KEY_SERVER_PORT).asInt)
  }
  def startFsNodeServer(): Unit = {
    FsNodeServer.create(new File(confPath), this).awaitTermination()
  }

  def shutdown(): Unit = {
    this.node.shutdown()
    //this.started = false
  }

  def getAllnodesInfo(): NodesInfoResponse = {
    //val request = new GetAllNodesInfoRequest
    //client.invokeSync(leader.getEndpoint, request, 5000)
    //  .asInstanceOf[NodesInfoResponse]
    val node1 = new NodeInfo(1, new RpcAddress("127.0.0.1", 1224), 3)
    val node2 = new NodeInfo(2, new RpcAddress("127.0.0.1", 1225), 2)
    val node3 = new NodeInfo(3, new RpcAddress("127.0.0.1", 1226), 1)
    val response = new NodesInfoResponse(Array(node1, node2, node3))
    response
    //todo watch node on/off line
  }

  def loadGlobalSetting(): GlobalSetting = {
    val props = new Properties()
    props.setProperty("replica.num", "1")
    new GlobalSetting(props)
    //todo set or load props from jraft
  }
  //def isStarted(): Boolean = this.started

  def getFsm: RegionFsStateMachine = this.fsm


  def getNode: Node = this.node

  def isLeader: Boolean = this.getNode.isLeader

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

}


