package cn.regionfs.jraft.rpc

import cn.regionfs.jraft.RegionFsJraftServer
import com.alipay.sofa.jraft.rpc.{RpcContext, RpcProcessor}

class GetNodeIDReuqestProcessor(server: RegionFsJraftServer) extends RpcProcessor[GetNodeIDRequest]{
  override def handleRequest(rpcContext: RpcContext, t: GetNodeIDRequest): Unit = {
    rpcContext.sendResponse(server.getNodeId())
  }

  override def interest(): String = {
    classOf[GetNodeIDRequest].getName
  }
}
