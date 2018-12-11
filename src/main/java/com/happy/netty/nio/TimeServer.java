package com.happy.netty.nio;

public class TimeServer {
    public static void main(String[] args) {
        int port = 8080;
        MultiplexerTimeServer timeServer = new MultiplexerTimeServer(port);
        //创建Reactor线程，并启动线程
        new Thread(timeServer, "NIO-MutiTimeServer-001").start();
    }
}
