package jraft
import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.ByteBuffer

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import org.apache.commons.io.IOUtils
import org.grapheco.hippo.{HippoClientFactory, HippoRpcHandler, HippoServer, ReceiveContext}
import org.grapheco.regionfs.{GetHelloRequest, GetHelloResponse}
import org.grapheco.regionfs.client.FsClient
import org.junit.Test

import scala.concurrent.{Await, CanAwait}
import scala.concurrent.duration.Duration

class FsClientTest {


  val  groupId: String = "regionfs"
  val initConfStr: String = "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083"
  @Test
  def testGetAllnodesInfo(): Unit = {
    val client = new FsClient(groupId, initConfStr)
    val test = client.nodesInfo
    test.foreach(u => println(s"the address is: ${u.address.host}:${u.address.port} and the region count is: ${u.regionCount}"))
  }
  def makeFile(dst: File, length: Long): Unit = {
    val fos = new FileOutputStream(dst)
    var n: Long = 0
    while (n < length) {
      val left: Int = Math.min((length - n).toInt, 10240)
      fos.write((0 to left - 1).map(x => ('a' + x % 26).toByte).toArray)
      n += left
    }

    fos.close()
  }

  def prepareData(): Unit ={
    val BLOB_LENGTH = Array[Long](999, 2048, 9999, 99999, 999999, 9999999)
    for (i <- BLOB_LENGTH) {
      val file = new File(s"./testdata/inputs/$i")
      file.getParentFile.mkdirs()
      file.createNewFile()
      makeFile(file, i)
    }
  }

  @Test
  def testFileWtiteAndRead(): Unit ={
    prepareData()
    val client = new FsClient(groupId, initConfStr)
    val i = 999
    //val id = client.writeFile(new File(s"./testdata/inputs/$i").)
    val id = client.writeFile(ByteBuffer.wrap(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/999")))))
    println(id)
  }

  @Test
  def testnew(): Unit = {
    val rpcEnv: HippoRpcEnv = {
      val rpcConf = new RpcConf()
      val config = RpcEnvClientConfig(rpcConf, "regionfs-client")
      HippoRpcEnvFactory.create(config)
    }
    val endPointRef = rpcEnv.setupEndpointRef(new RpcAddress("127.0.0.1", 1224), "hippo-sever1")
    //val re = endPointRef.ask[GetHelloResponse](GetHelloRequest).result(Duration.Inf)(null)
   // val f = endPointRef.ask[GetHelloResponse](GetHelloRequest)
    val res = Await.result(endPointRef.askWithBuffer[GetHelloResponse](GetHelloRequest), Duration.Inf)
    println(res.msg)
   // f.onComplete {
   //   case scala.util.Success(value) => println(s"Got the result = $value")
  //    case scala.util.Failure(e) => println(s"Got error: $e")
  //  }(null)
  //  Await.result(f, Duration.apply("30s"))
    //println(re.msg)
  }

  @Test
  def testServer(): Unit = {
    val client = HippoClientFactory.create("test", Map()).createClient("127.0.0.1", 1224)
    val res = Await.result(client.askWithBuffer[GetHelloResponse](GetHelloRequest), Duration.Inf)
    println(res.msg)
  }

  @Test
  def testHippo(): Unit = {
    val server =  HippoServer.create("test", Map(), new HippoRpcHandler {
      override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
        case GetHelloRequest => context.reply(GetHelloResponse("hello world"))
      }
    }, 5678, "127.0.0.1")
    val client = HippoClientFactory.create("test", Map()).createClient("127.0.0.1", 5678)
    val res = Await.result(client.askWithBuffer[GetHelloResponse](GetHelloRequest), Duration.Inf)
    println(res.msg)
  }
}
