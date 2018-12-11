package com.happy.netty.aio;

public class TimeServer {
    public static void main(String[] args) {
        new Thread(new AsyncTimeServerHandler(8080), "AIO-Handler001").start();
    }
}
