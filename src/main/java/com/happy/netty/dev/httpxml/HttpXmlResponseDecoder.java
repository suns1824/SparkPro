package com.happy.netty.dev.httpxml;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;

import java.util.List;

public class HttpXmlResponseDecoder extends AbstractHttpXmlDecoder<DefaultFullHttpResponse> {

    public HttpXmlResponseDecoder(Class<?> clazz) {
        this(clazz, false);
    }

    public HttpXmlResponseDecoder(Class<?> clazz, boolean isPrintLog) {
        super(clazz, isPrintLog);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, DefaultFullHttpResponse msg, List<Object> out) throws Exception {
        HttpXmlResponse resHttpXmlResponse = new HttpXmlResponse(msg, decode0(ctx, msg.content()));
        out.add(resHttpXmlResponse);
    }
}
