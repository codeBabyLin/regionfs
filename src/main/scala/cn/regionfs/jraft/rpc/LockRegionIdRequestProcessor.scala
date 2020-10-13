package cn.regionfs.jraft.rpc

import cn.regionfs.jraft.RegionFsJraftServer
import com.alipay.sofa.jraft.rpc.{RpcContext, RpcProcessor}

class LockRegionIdRequestProcessor(rfs: RegionFsJraftServer) extends RpcProcessor[LockRegionIdRequest] {
  override def handleRequest(rpcContext: RpcContext, t: LockRegionIdRequest): Unit = {
    if(rfs.lockRegionIds.get(t.regionId).isEmpty) {
      this.synchronized{
        rfs.lockRegionIds += t.regionId -> t.nodeId
      }
      rpcContext.sendResponse(new LockRegionIdResponse(true))
    }
    else rpcContext.sendResponse(new LockRegionIdResponse(false))
  }

  override def interest(): String = {
    classOf[LockRegionIdRequest].getName
  }
}
