package jraft

import java.io.File
import java.nio.file.Paths
import java.util
import java.util.Optional

import cn.regionfs.jraft.RegionFsJraftServer

object ServerStart {

}

object ServerStart1 {

  def main(args: Array[String]): Unit = {
    val dataPath: String = "./outinput/data1"
    val  groupId: String = "regionfs"
    val serverIdStr: String = "127.0.0.1:8081"
    val initConfStr: String = "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083"

    new RegionFsJraftServer(dataPath, groupId, serverIdStr, initConfStr).init()
  }

}

object ServerStart2 {

  def main(args: Array[String]): Unit = {
    val dataPath: String = "./outinput/data2"
    val  groupId: String = "regionfs"
    val serverIdStr: String = "127.0.0.1:8082"
    val initConfStr: String = "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083"

    new RegionFsJraftServer(dataPath, groupId, serverIdStr, initConfStr).init()
  }

}

object ServerStart3 {

  def main(args: Array[String]): Unit = {
    val dataPath: String = "./outinput/data3"
    val  groupId: String = "regionfs"
    val serverIdStr: String = "127.0.0.1:8083"
    val initConfStr: String = "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083"

    new RegionFsJraftServer(dataPath, groupId, serverIdStr, initConfStr).init()
  }

}