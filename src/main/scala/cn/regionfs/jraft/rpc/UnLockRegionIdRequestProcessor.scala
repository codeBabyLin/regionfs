package cn.regionfs.jraft.rpc

import cn.regionfs.jraft.RegionFsJraftServer
import com.alipay.sofa.jraft.rpc.{RpcContext, RpcProcessor}

class UnLockRegionIdRequestProcessor(rfs: RegionFsJraftServer) extends RpcProcessor[UnLockRegionIdRequest] {
  override def handleRequest(rpcContext: RpcContext, t: UnLockRegionIdRequest): Unit = {
    val nodeId = rfs.lockRegionIds.get(t.regionId)
    if(!nodeId.isEmpty && nodeId.get == t.nodeId) {
      this.synchronized{
        rfs.lockRegionIds -= t.regionId
      }
      rpcContext.sendResponse(new LockRegionIdResponse(true))
    }
    else rpcContext.sendResponse(new LockRegionIdResponse(true))
  }

  override def interest(): String = {
    classOf[UnLockRegionIdRequest].getName
  }
}
