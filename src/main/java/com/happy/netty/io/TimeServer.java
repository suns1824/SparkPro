package com.happy.netty.io;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * 通过线程池实现伪异步IO，
 * 缺点：
 *  --read方法：method blocks until input data is available, end of file is detected, or an exception is thrown
 *  --write方法：被阻塞知道所有字节被发送完毕或者发生IO异常
 */

public class TimeServer {
    public static void main(String[] args) throws IOException {
        int port = 8080;
        if (args != null && args.length > 0){
            //port处理
        }
        ServerSocket server = null;
        try {
            server = new ServerSocket(port);
            Socket socket = null;
            TimeServerHandlerExecutePool singleExecutor = new TimeServerHandlerExecutePool(50, 10000);
            while (true) {
                socket = server.accept();
                singleExecutor.execute(new TimeServerHandler(socket));
            }
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
            if (server != null) {
                System.out.println("over");
                server.close();
                server = null;
            }

        }
    }
}
