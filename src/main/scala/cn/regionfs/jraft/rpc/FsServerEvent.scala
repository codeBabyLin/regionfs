package cn.regionfs.jraft.rpc

import org.grapheco.regionfs.server.{FsNodeServer, NodeServerInfo}

class FsServerEvent(val event: NodeServerEvent) extends Serializable {
  //private var event: NodeServerEvent = null
  def getEvent(): NodeServerEvent = {
    this.event
  }
  def handelEvent(fs: FsNodeServer): Unit = {
    event match {
      case NodeCreateEvent(t) => {
        t.asInstanceOf[NodeServerInfo]
        if (t.nodeId != fs.nodeId){
          //println(s"node ${t.nodeId} Create, address: ${t.address.host}-${t.address.port}, regioncnt: ${t.regionCount}")
          fs.mapNeighbourNodeWithAddress += t.nodeId -> t.address
          fs.mapNeighbourNodeWithRegionCount += t.nodeId -> t.regionCount
          fs.remoteRegionWatcher.reportLocalSeconaryRegions(t.nodeId)
        }
      }
      case NodeUpdateEvent(t) => {
        //println(s"node ${t.nodeId} Update, address: ${t.address.host}-${t.address.port}, regioncnt: ${t.regionCount}")
        if (t.nodeId != fs.nodeId) fs.mapNeighbourNodeWithRegionCount += t.nodeId -> t.regionCount
      }
      case NodeDeleteEvent(t) => {
        fs.mapNeighbourNodeWithAddress -= t.nodeId
        fs.mapNeighbourNodeWithRegionCount -= t.nodeId
        fs.remoteRegionWatcher._mapRemoteSecondaryRegions.foreach({
          _._2 -= t.nodeId
        })
      }
    }
  }
}



trait NodeServerEvent extends Serializable{

}

case class NodeCreateEvent(node: NodeServerInfo) extends NodeServerEvent{

}

case class NodeUpdateEvent(node: NodeServerInfo) extends NodeServerEvent{

}

case class NodeDeleteEvent(node: NodeServerInfo) extends NodeServerEvent{

}