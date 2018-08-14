package com.atwy.java.com.atwy.java.socket;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class ServiceSocket {
    public static void main(String[] args) {
        try {
            ServerSocket serverSocket = new ServerSocket(9999);
            Socket socket = null;
            while (true) {
                socket = serverSocket.accept();
                while (socket.isConnected()) {
                    // 向服务器端发送数据
                    OutputStream os = socket.getOutputStream();
                    DataOutputStream bos = new DataOutputStream(os);
                    //每隔ms发送一次数据
                    String str = "Connect test sparkstreaming by java socket\n";
                    while (true) {
                        bos.writeUTF(str);
                        bos.flush();
                        System.out.println(str);
                        //20ms发送一次数据
                        try {
                            Thread.sleep(500L);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                //ms检测一次连接
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

