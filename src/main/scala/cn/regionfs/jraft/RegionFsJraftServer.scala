package cn.regionfs.jraft

import java.io.File

import scala.collection.JavaConverters._
import cn.regionfs.jraft.rpc.{GetGraphDataStateRequestProcessor, GetNeo4jBoltAddressRequestProcessor}
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.entity.PeerId
import com.alipay.sofa.jraft.option.{CliOptions, NodeOptions}
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import com.alipay.sofa.jraft.rpc.{RaftRpcServerFactory, RpcServer}
import com.alipay.sofa.jraft.{JRaftUtils, Node, RaftGroupService, RouteTable}
import org.apache.commons.io.FileUtils
import org.grapheco.commons.util.{ConfigurationEx, Logging, ProcessUtils}
class RegionFsJraftServer(dataPath: String,
                          groupId: String,
                          serverIdStr: String,
                          initConfStr: String) {

  private var raftGroupService: RaftGroupService = null
  private var node: Node = null
  private var fsm: RegionFsStateMachine = null



  // parse args
  val serverId: PeerId = new PeerId()
  if (!serverId.parse(serverIdStr)) throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr)
  val initConf = new Configuration()
  if (!initConf.parse(initConfStr)) throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr)

  def init(): Unit = {
    // init file directory
    FileUtils.forceMkdir(new File(dataPath))

    // add business RPC service
    // (Here, the raft RPC and the business RPC use the same RPC server)
    val rpcServer: RpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint)
    // add business RPC processor
    //rpcServer.registerProcessor(new GetNeo4jBoltAddressRequestProcessor(this))
    //rpcServer.registerProcessor(new GetGraphDataStateRequestProcessor(this))
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
  }


  def shutdown(): Unit = {
    this.node.shutdown()
    //this.started = false
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
