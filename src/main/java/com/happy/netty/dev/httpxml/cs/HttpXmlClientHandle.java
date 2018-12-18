package com.happy.netty.dev.httpxml.cs;

import com.happy.netty.dev.httpxml.HttpXmlRequest;
import com.happy.netty.dev.httpxml.HttpXmlResponse;
import com.happy.netty.dev.httpxml.pojo.OrderFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

public class HttpXmlClientHandle extends SimpleChannelInboundHandler<HttpXmlResponse> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpXmlResponse msg) throws Exception {
        System.out.println("response head: " + msg.getHttpResponse().headers().names());
        System.out.println("response body:" + msg.getResult());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        HttpXmlRequest request = new HttpXmlRequest(null, OrderFactory.create(24));
        ctx.writeAndFlush(request);
    }
}
