package cn.regionfs.jraft.rpc

class GraphDataStateResponse(val graphDataPathNull: Boolean,
                             val files: Array[String],
                             val directories: Array[String],
                             val appliedTxLogIndex: Long) extends Serializable {
}
