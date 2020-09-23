package cn.regionfs.jraft

import com.alipay.sofa.jraft.rpc.RpcClient
import org.grapheco.commons.util.Logging

object PandaJraftClient extends Logging{

/*  def getOrCreateRpcClient(): RpcClient = {
    if (PandaRuntimeContext.contextGetOption[RpcClient]().isEmpty) {
      val cliClientService = new CliClientServiceImpl
      cliClientService.init(new CliOptions)
      val rpcClient = cliClientService.getRpcClient
      PandaRuntimeContext.contextPut[RpcClient](rpcClient)
    }
    PandaRuntimeContext.contextGet[RpcClient]()
  }

  def getRemoteGraphDataState(peerId: PeerId) : GraphDataStateResponse = {
    val request = new GetGraphDataStateRequest
    getOrCreateRpcClient().invokeSync(peerId.getEndpoint, request, 5000)
                          .asInstanceOf[GraphDataStateResponse]
  }*/

}
