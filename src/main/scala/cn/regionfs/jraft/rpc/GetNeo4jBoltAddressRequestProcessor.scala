package cn.regionfs.jraft.rpc

import cn.regionfs.jraft.RegionFsJraftServer
import com.alipay.sofa.jraft.rpc.{RpcContext, RpcProcessor}

class GetNeo4jBoltAddressRequestProcessor(pandaJraftServer: RegionFsJraftServer) extends RpcProcessor[cn.regionfs.jraft.rpc.GetNeo4jBoltAddressRequest]{
  def handleRequest(rpcCtx: RpcContext, request: GetNeo4jBoltAddressRequest): Unit = {
    //rpcCtx.sendResponse(new Neo4jBoltAddressValue(pandaJraftServer.getNeo4jBoltServerAddress()))
  }

  override def interest(): String = {
    classOf[GetNeo4jBoltAddressRequest].getName
  }
}
