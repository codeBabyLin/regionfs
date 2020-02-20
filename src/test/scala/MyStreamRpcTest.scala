import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import cn.regionfs.network._
import cn.regionfs.util.Profiler
import cn.regionfs.util.Profiler._
import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.commons.io.IOUtils
import org.apache.spark.network.buffer.{ManagedBuffer, NettyManagedBuffer}
import org.junit.{Assert, Test}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by bluejoe on 2020/2/18.
  */
case class SayHelloRequest(str: String) {

}

case class SayHelloResponse(str: String) {

}

case class ReadFile(path: String) {

}

case class GetManyResults(times: Int, chunkSize: Int, msg: String) {

}

object MyStreamServer {
  val server = StreamingServer.create("test", new StreamingRpcHandler() {

    override def receive(request: Any, ctx: ReceiveContext): Unit = {
      request match {
        case SayHelloRequest(msg) =>
          ctx.reply(SayHelloResponse(msg.toUpperCase()))
      }
    }

    override def receiveBuffer(request: ByteBuffer, ctx: ReceiveContext): Unit = {
      ctx.reply(request.remaining())
    }

    override def openChunkedStream(request: Any): ChunkedStream = {
      request match {
        case GetManyResults(times, chunkSize, msg) =>
          var count = 0;
          new ChunkedMessageStream[String]() {
            override def hasNext(): Boolean = count < times

            override def nextChunk(): Iterable[String] = {
              count += 1
              (1 to chunkSize).map(_ => msg)
            }

            override def close(): Unit = {}
          }

        case ReadFile(path) =>
          new ChunkedStream() {
            val fis = new FileInputStream(new File(path))
            val length = new File(path).length()
            var count = 0;

            override def hasNext(): Boolean = {
              count < length
            }

            def writeNextChunk(buf: ByteBuf) {
              val written =
                timing(false) {
                  buf.writeBytes(fis, 1024 * 1024)
                }

              count += written
            }

            override def close(): Unit = {
              fis.close()
            }
          }
      }
    }

    override def openStream(request: Any): ManagedBuffer = {
      request match {
        case ReadFile(path) =>
          val fis = new FileInputStream(new File(path))
          val buf = Unpooled.buffer()
          buf.writeBytes(fis.getChannel, new File(path).length().toInt)
          new NettyManagedBuffer(buf)
      }
    }
  }, 1224)
}

class MyStreamRpcTest {
  Profiler.enableTiming = true
  val server = MyStreamServer.server
  val client = StreamingClient.create("test", "localhost", 1224)

  @Test
  def testRpc(): Unit = {
    Await.result(client.ask[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)

    val res1 = timing(true) {
      Await.result(client.ask[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)
    }

    Assert.assertEquals("HELLO", res1.str);
  }

  @Test
  def testPutFiles(): Unit = {
    val res = timing(true, 10) {
      Await.result(client.send[Int]((buf: ByteBuf) => {
        val fos = new FileInputStream(new File("./testdata/inputs/9999999"));
        buf.writeBytes(fos.getChannel, new File("./testdata/inputs/9999999").length().toInt)
        fos.close()
      }), Duration.Inf)
    }

    Assert.assertEquals(new File("./testdata/inputs/9999999").length(), res)
  }

  @Test
  def testGetChunkedTStream(): Unit = {
    Await.result(client.ask[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)

    val results = timing(true, 10) {
      client.getChunkedStream(GetManyResults(100, 10, "hello"))
    }.toArray

    Assert.assertEquals(results(0), "hello")
    Assert.assertEquals(results(100 * 10 - 1), "hello")
    Assert.assertEquals(100 * 10, results.length)
  }

  @Test
  def testGetStream(): Unit = {

    Await.result(client.ask[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)

    timing(true, 10) {
      val is = client.getInputStream(ReadFile("./testdata/inputs/9999999"));
      var read = 0;
      while (read != -1) {
        read = is.read()
      }
    }

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/999"))),
      IOUtils.toByteArray(client.getInputStream(ReadFile("./testdata/inputs/999")))
    );

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/999"))),
      IOUtils.toByteArray(client.getChunkedInputStream(ReadFile("./testdata/inputs/999")))
    );

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))),
      IOUtils.toByteArray(client.getInputStream(ReadFile("./testdata/inputs/9999999")))
    );

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))),
      IOUtils.toByteArray(client.getChunkedInputStream(ReadFile("./testdata/inputs/9999999")))
    );

    for (size <- Array(999, 9999, 99999, 999999, 9999999)) {
      println("=================================")
      println(s"getInputStream(): size=$size")
      timing(true, 10) {
        IOUtils.toByteArray(client.getInputStream(ReadFile(s"./testdata/inputs/$size")))
      }

      println(s"getChunkedInputStream(): size=$size")
      timing(true, 10) {
        IOUtils.toByteArray(client.getChunkedInputStream(ReadFile(s"./testdata/inputs/$size")))
      }
      println("=================================")
    }

    import scala.concurrent.ExecutionContext.Implicits.global

    //378ms
    timing(true) {
      val futures = (1 to 5).map { _ =>
        Future {
          IOUtils.toByteArray(client.getInputStream(ReadFile("./testdata/inputs/9999999")))
        }
      }

      futures.foreach(Await.result(_, Duration.Inf))
    }
  }
}
