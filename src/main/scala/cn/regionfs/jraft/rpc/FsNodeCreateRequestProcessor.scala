package cn.regionfs.jraft.rpc

import cn.regionfs.jraft.RegionFsJraftServer
import com.alipay.sofa.jraft.rpc.{RpcContext, RpcProcessor}

class FsNodeCreateRequestProcessor(rfs: RegionFsJraftServer) extends RpcProcessor[FsNodeCreateRequest]{
  override def handleRequest(rpcContext: RpcContext, t: FsNodeCreateRequest): Unit = {
    rpcContext.sendResponse()
    if(rfs.isLeader){
      //println(s"${t.nodeInfo.address.toString()}: ${t.nodeInfo.regionCount} : ${t.nodeInfo.nodeId}")
      rfs.broadNodeCreate(t.nodeInfo)
    }
  }

  override def interest(): String = {
    classOf[FsNodeCreateRequest].getName
  }
}
