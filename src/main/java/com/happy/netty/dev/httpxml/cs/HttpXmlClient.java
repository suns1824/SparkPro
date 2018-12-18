package com.happy.netty.dev.httpxml.cs;

import com.happy.netty.dev.httpxml.HttpXmlRequestEncoder;
import com.happy.netty.dev.httpxml.HttpXmlResponseDecoder;
import com.happy.netty.dev.httpxml.pojo.Order;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseDecoder;

import java.net.InetSocketAddress;

public class HttpXmlClient {
    public void connect(int port) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b
                    .group(group).
                    channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast("http-decoder", new HttpResponseDecoder());
                            ch.pipeline().addLast("http-aggregator", new HttpObjectAggregator(65536));
                            ch.pipeline().addLast("xml-decoder", new HttpXmlResponseDecoder(Order.class, true));
                            ch.pipeline().addLast("http-encoder", new HttpXmlRequestEncoder());
                            ch.pipeline().addLast("xmlClientHandler", new HttpXmlClientHandle());
                        }
                    });
            //发起异步链接操作
            ChannelFuture f = b.connect(new InetSocketAddress(port)).sync();
            //等待客户端链路关闭
            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        int port = 8080;
        new HttpXmlClient().connect(port);
    }
}
