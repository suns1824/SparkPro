package com.happy.netty.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;


//Reactor线程的Runnable
public class MultiplexerTimeServer implements Runnable{

    private Selector selector;
    private ServerSocketChannel servChannel;
    private volatile boolean stop;

    public MultiplexerTimeServer(int port) {
        try {
            selector = Selector.open();
            servChannel = ServerSocketChannel.open();
            servChannel.configureBlocking(false);
            servChannel.socket().bind(new InetSocketAddress(port), 1024);
            //将servChannel注册到Selector上，监听accept事件
            servChannel.register(selector, SelectionKey.OP_ACCEPT);
            System.out.println("The time server is start in port: " + port);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void stop() {
        this.stop = true;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                // 轮询准备就绪的key
                selector.select(1000);
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> it = selectionKeys.iterator();
                SelectionKey key = null;
                while (it.hasNext()) {
                    key = it.next();
                    it.remove();
                    try {
                        handleInput(key);
                    } catch (Exception e) {
                        if (key != null) {
                            key.cancel();
                            if (key.channel() != null) {
                                key.channel().close();
                            }
                        }
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        if (selector != null) {
            try {
                selector.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleInput(SelectionKey key) throws IOException {
          if (key.isValid()) {
              if (key.isAcceptable()) {
                  ServerSocketChannel ssc = (ServerSocketChannel)key.channel();
                  SocketChannel sc = ssc.accept();
                  sc.configureBlocking(false);
                  //将这个key对应的channel（也就是新接入的客户端）注册到Reactor线程的多路复用器上，监听读操作
                  sc.register(selector, SelectionKey.OP_READ);
              }
              if (key.isReadable()) {
                  SocketChannel sc = (SocketChannel) key.channel();
                  ByteBuffer readBuffer = ByteBuffer.allocate(1024);
                  //异步读取客户端请求消息到缓冲区
                  int readBytes = sc.read(readBuffer);
                  if (readBytes > 0 ){
                      //将缓存字节数组的指针设置为数组的开始序列即数组下标0。这样就可以从buffer开头，对该buffer进行遍历（读取）了。
                      readBuffer.flip();
                      byte[] bytes = new byte[readBuffer.remaining()];
                      readBuffer.get(bytes);
                      String body = new String(bytes, "UTF-8");
                      System.out.println("The time server receive order: " + body);
                      String currentTime = "QUERY TIME ORDER".equalsIgnoreCase(body) ? new Date(System.currentTimeMillis()).toString() : "BAD ORDER";
                      doWrite(sc, currentTime);
                  } else if (readBytes < 0) {
                      key.cancel();
                      sc.close();
                  } else
                      ;
              }
          }
    }

    private void doWrite(SocketChannel channel, String response) throws IOException{
        // 将应答消息发送给客户端
        if (response != null && response.trim().length() > 0) {
            byte[] bytes = response.getBytes();
            ByteBuffer writeBuffer = ByteBuffer.allocate(bytes.length);
            writeBuffer.put(bytes);
            writeBuffer.flip();
            channel.write(writeBuffer);
        }
    }

}






















