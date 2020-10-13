package cn.regionfs.jraft.rpc

import cn.regionfs.jraft.RegionFsJraftServer
import com.alipay.sofa.jraft.rpc.{RpcContext, RpcProcessor}

class FsNodeUpdateRequestProcessor(rfs: RegionFsJraftServer) extends RpcProcessor[FsNodeUpdateRequest] {
  override def handleRequest(rpcContext: RpcContext, t: FsNodeUpdateRequest): Unit = {

    rpcContext.sendResponse()
    if(rfs.isLeader){
      rfs.broadNodeUpdate(t.nodeInfo)
    }
  }

  override def interest(): String = {
    classOf[FsNodeUpdateRequest].getName
  }
}
