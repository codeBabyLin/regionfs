package cn.regionfs.jraft.rpc

import cn.regionfs.jraft.RegionFsJraftServer
import com.alipay.sofa.jraft.rpc.{RpcContext, RpcProcessor}
import net.neoremind.kraps.rpc.RpcAddress

class GetAllNodesInfoRequestProcessor(server: RegionFsJraftServer) extends RpcProcessor[GetAllNodesInfoRequest]{
  override def handleRequest(rpcContext: RpcContext, t: GetAllNodesInfoRequest): Unit = {
    //val node = new NodeInfo(1, new RpcAddress("127.0.0.1", 9999), 10)
    //val response = new NodesInfoResponse(Array(node))
    rpcContext.sendResponse(server.getAllnodesInfo())
  }

  override def interest(): String = {
    classOf[GetAllNodesInfoRequest].getName
  }
}
