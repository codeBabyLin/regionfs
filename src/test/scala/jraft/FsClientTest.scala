package jraft
import java.io.{File, FileInputStream, FileOutputStream, FileWriter, InputStream}
import java.nio.ByteBuffer

import cn.regionfs.jraft.rpc.FsNodeCreateRequest
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import org.apache.commons.io.IOUtils
import org.grapheco.hippo.{HippoClientFactory, HippoRpcHandler, HippoServer, ReceiveContext}
import org.grapheco.regionfs.{FileId, GetHelloRequest, GetHelloResponse}
import org.grapheco.regionfs.client.FsClient
import org.grapheco.regionfs.server.NodeServerInfo
import org.junit.{Assert, Test}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, CanAwait}
import scala.concurrent.duration.Duration
import scala.util.Random

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
    val BLOB_LENGTH = Array[Long](999, 2048, 9999, 99999, 999999, 9999999, 1024 *1024)
    for (i <- BLOB_LENGTH) {
      val file = new File(s"./testdata/inputs/$i")
      file.getParentFile.mkdirs()
      file.createNewFile()
      makeFile(file, i)
    }
  }

  @Test
  def testFileWtiteAndRead(): Unit = {
    prepareData()
    val client = new FsClient(groupId, initConfStr)
    val i = 999
    val pathname = "F:/data/1.jpg"

    val id = Await.result(client.writeFile(ByteBuffer.wrap(IOUtils.toByteArray(new FileInputStream(new File(pathname))))), Duration.Inf)

    val res = Await.result(client.readFile(id, (is) => IOUtils.toByteArray(is)), Duration.Inf)

    val data = IOUtils.toByteArray(new FileInputStream(new File(pathname)))

    Assert.assertArrayEquals(res, data)
  }

  @Test
  def testTime(): Unit = {
    val client = new FsClient(groupId, initConfStr)
    val t1 = System.currentTimeMillis()
    val fileIds = ArrayBuffer[FileId]()
    for (i <- 1 to 972){
      val pathname = s"F:/data/${i}.jpg"
      val id = Await.result(client.writeFile(ByteBuffer.wrap(IOUtils.toByteArray(new FileInputStream(new File(pathname))))), Duration.Inf)
      fileIds += id
    }
    val t2 = System.currentTimeMillis()
    println(s"write cost time: ${t2-t1}")
    var fi = 0
    fileIds.foreach(u =>{
      fi = fi + 1
      val pathname = s"F:/data/${fi}.jpg"
      val res = Await.result(client.readFile(u, (is) => IOUtils.toByteArray(is)), Duration.Inf)
      val data = IOUtils.toByteArray(new FileInputStream(new File(pathname)))

      Assert.assertArrayEquals(res, data)
    })
    val t3 = System.currentTimeMillis()
    println(s"read cost time: ${t3-t2}")
  }


  @Test
  def testFileWtiteLatency(): Unit ={
    prepareData()
    val client = new FsClient(groupId, initConfStr)
    val i = 1024 *1024


   // def consum(input: InputStream): =
    //val id = client.writeFile(new File(s"./testdata/inputs/$i").)
    //Await.result(client.askWithBuffer[GetHelloResponse](GetHelloRequest), Duration.Inf)
    //client.writeFile(ByteBuffer.wrap(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/999")))))
    //val array = mutable.Map[Int, Long]()
    val array: ArrayBuffer[Long] = ArrayBuffer[Long]()
    for (iii <- 1 to 10){

      for (ii <- 1 to 100){
        val t1 = System.currentTimeMillis()
        val id = Await.result(client.writeFile(ByteBuffer.wrap(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/${i}"))))), Duration.Inf)
        val t2 = System.currentTimeMillis()
        val t3 = t2 - t1
        array += t3
        println(s"write file cost time: ${t2-t1}")
      }

     // array += iii -> (t2-t1)
    }
    val out = new FileWriter("./time.txt",true)
    array.foreach(u => {
      out.write(u.toString)
      out.write("\n")
    })
    out.close()


   // array.values.foreach(u => {
    //  println(u)
    //})
    //val id = Await.result(client.writeFile(ByteBuffer.wrap(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/${i}"))))), Duration.Inf)
    //val res = Await.result(client.readFile(id, (is) => IOUtils.toByteArray(is)), Duration.Inf)

   // val data = IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/${i}")))

    //Assert.assertArrayEquals(res, data)

    //println(s"regionID:${id.regionId}   localId:${id.localId}")
    //println(id)
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

  @Test
  def randomWalk(): Unit = {
    var x = 0
    var y = 0
    var z = 0
    val loop = 10000000
    val array = Array(11, 12, 21, 22, 31, 32)
    var d1 = 0.0
    var d2 = 0.0
    var d3 = 0.0
    //println(array(Random.nextInt(array.length)))
    for (i <- 1 to loop) {
      val delta = array(Random.nextInt(array.length))
      delta match {
        case 11 => x += 1
        case 12 => x -= 1
        case 21 => y += 1
        case 22 => y -= 1
        case 31 => z += 1
        case 32 => z -= 1
      }
      //x += array(Random.nextInt(array.length))
      //y += array(Random.nextInt(array.length))
      //z += array(Random.nextInt(array.length))
      if (x==0 || y==0 || z==0) d1 += 1
      if ((x==0 && y==0)||(y==0 && z==0)||(x==0 && z==0)) d2 +=1
      if (x==0 && y==0 && z==0) d3 +=1
    }
    println(s"the 1d p is $x: ${d1/loop}")
    println(s"the 2d p is $y: ${d2/loop}")
    println(s"the 3d p is $z: ${d3}")

  }

  @Test
  def testHjl(): Unit = {
    val client = new FsClient(groupId, initConfStr)
    val ed = client.jc.leader
    val ni = new NodeServerInfo(1, null, 2)
    val cr = new FsNodeCreateRequest(ni)
    client.jc.client.invokeSync(ed.getEndpoint, cr, 5000)
  }
}
