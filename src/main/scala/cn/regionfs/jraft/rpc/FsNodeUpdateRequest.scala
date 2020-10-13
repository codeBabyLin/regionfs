package cn.regionfs.jraft.rpc

import org.grapheco.regionfs.server.NodeServerInfo

class FsNodeUpdateRequest(val nodeInfo: NodeServerInfo) extends Serializable{

}
