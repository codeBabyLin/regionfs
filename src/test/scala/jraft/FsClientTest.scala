package jraft

import cn.regionfs.jraft.client.FsClient
import org.junit.Test

class FsClientTest {


  val  groupId: String = "regionfs"
  val initConfStr: String = "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083"
  @Test
  def testGetAllnodesInfo(): Unit = {
    val client = new FsClient(groupId, initConfStr)
    val test = client.nodesInfo
    test.foreach(u => println(s"the address is: ${u.address.host}:${u.address.port}"))
  }
}
