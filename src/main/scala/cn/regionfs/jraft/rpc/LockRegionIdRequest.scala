package cn.regionfs.jraft.rpc

class LockRegionIdRequest(val regionId: Int, val nodeId: Int) extends Serializable {

}
