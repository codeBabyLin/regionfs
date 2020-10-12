package cn.regionfs.jraft.rpc

import java.util.concurrent.Executor

import com.alipay.remoting
import com.alipay.remoting.rpc.protocol.UserProcessor.ExecutorSelector
import com.alipay.remoting.rpc.protocol.{AsyncUserProcessor, UserProcessor, UserProcessorRegisterHelper}
import com.alipay.remoting.{AsyncContext, BizContext, ConnectionEventListener, ConnectionEventProcessor, ConnectionEventType}
import com.alipay.sofa.jraft.rpc.impl.{BoltRpcServer, ConnectionClosedEventListener}
import com.alipay.sofa.jraft.rpc.{Connection, RpcContext, RpcProcessor, RpcServer}
import net.neoremind.kraps.rpc.netty.HippoRpcEnv
import net.neoremind.kraps.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEnv}

class HanceEndpoint(override val rpcEnv: HippoRpcEnv) extends RpcEndpoint with RpcServer{

  val  connectionEventListener : ConnectionEventListener = new ConnectionEventListener
  var processors: Map[String, UserProcessor[_]] = null
  var processorsP: Map[String, RpcProcessor[_]] = null
  override def receive: PartialFunction[Any, Unit] = {
    case x: Any => processors.get(x.getClass.getName).get.handleRequest(null, x.asInstanceOf)
  }

  //override val rpcEnv: RpcEnv = _

  override def registerConnectionClosedEventListener(lis: ConnectionClosedEventListener): Unit = {
    val etype = ConnectionEventType.CLOSE
    val processor = new ConnectionEventProcessor {
      override def onEvent(s: String, conn: remoting.Connection): Unit = {
        val proxyConn: Connection = {
          if (conn == null) null
          else new Connection {
            override def getAttribute(s: String): AnyRef = conn.getAttribute(s)

            override def setAttribute(s: String, o: Any): Unit = conn.setAttribute(s, o)

            override def close(): Unit = conn.close()
          }
        }
        lis.onClosed(s, proxyConn)
      }
    }
    this.connectionEventListener.addConnectionEventProcessor(etype, processor)
  }

  override def registerProcessor(rpcProcessor: RpcProcessor[_]): Unit = {
    val process = new AsyncUserProcessor[Any] {
      override def handleRequest(bizContext: BizContext, asyncContext: AsyncContext, t: Any): Unit = {
        val rpcContext = new RpcContext {
          override def sendResponse(o: Any): Unit = asyncContext.sendResponse(o)

          override def getConnection: Connection = {
            val conn = bizContext.getConnection
            if (conn == null) null
            else new BoltConnection(conn)
          }

          override def getRemoteAddress: String = bizContext.getRemoteAddress
        }
        rpcProcessor.handleRequest(rpcContext, t.asInstanceOf)
      }

      override def interest(): String = rpcProcessor.interest()

      override def getExecutorSelector: ExecutorSelector = {
        val rs = rpcProcessor.executorSelector()
        if (rs == null) null
        else null
      }

      //def getRs(r: (String, Any) => ExecutorSelector): ExecutorSelector

      override def getExecutor: Executor = rpcProcessor.executor
    }
    //UserProcessorRegisterHelper.registerUserProcessor(process, processors)
    processors += rpcProcessor.interest() -> process
  }

  override def boundPort(): Int = {
    this.rpcEnv.address.port
  }

  override def init(t: Void): Boolean = {
    true
  }

  override def shutdown(): Unit = {

  }

  class BoltConnection(conn: remoting.Connection) extends Connection {
    override def getAttribute(s: String): AnyRef = conn.getAttribute(s)

    override def setAttribute(s: String, o: Any): Unit = conn.setAttribute(s, o)

    override def close(): Unit = conn.close()
  }

  //override def onDisconnected(remoteAddress: RpcAddress): Unit = super.onDisconnected(remoteAddress)
}
