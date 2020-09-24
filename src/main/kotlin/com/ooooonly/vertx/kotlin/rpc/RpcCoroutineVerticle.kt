package com.ooooonly.vertx.kotlin.rpc

import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.coroutines.toChannel
import kotlinx.coroutines.launch
import java.lang.reflect.InvocationTargetException

open class RpcCoroutineVerticle(var channel:String  = ""): CoroutineVerticle() {
    private var serviceInstance = RpcServiceInstance.instance(this)

    protected fun setServiceInstance(service:Any){
        serviceInstance = RpcServiceInstance.instance(service)
    }

    override suspend fun start() {
        if (channel.isBlank()) channel = this::class.simpleName?:""

        launch(vertx.dispatcher()) {
            for (msg in vertx.eventBus().consumer<ByteArray>(channel).toChannel(vertx)) {
                launch(vertx.dispatcher()) {
                    try {
                        val request = msg.body().toRpcRequest()
                        msg.reply(serviceInstance.processRequest(request).toBytes())
                    } catch (e: Throwable) {
                        if(e is InvocationTargetException){
                            msg.fail(1, e.targetException.message)
                        }else{
                            e.printStackTrace()
                            msg.fail(1, e.message)
                        }
                    }
                }
            }
        }
    }
}