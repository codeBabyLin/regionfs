package cn.regionfs.jraft.rpc

import net.neoremind.kraps.rpc.RpcAddress

class NodesInfoResponse(val array: Array[NodeInfo]) extends Serializable {

}

class NodeInfo(val nodeId: Int, val address: RpcAddress, val regionCount: Int) extends Serializable{

}