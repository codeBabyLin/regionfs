package cn.regionfs.jraft.rpc

class UnLockRegionIdRequest(val regionId: Int, val nodeId: Int) extends Serializable {

}
