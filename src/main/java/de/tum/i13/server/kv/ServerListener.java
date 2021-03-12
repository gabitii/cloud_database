package de.tum.i13.server.kv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * the class was created for testing, starts the server on port 9339 receives strings and prints it
 */
public class ServerListener {
    private int port;
    private String host;
    private BufferedReader in;
    private ServerSocket serverSocket;

    public ServerListener(int port, String host) throws IOException {
        this.port = port;
        this.host = host;
        this.serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress(host,port));
    }

    public static void main(String[] args) {
        try {
            ServerListener serverListener = new ServerListener(9339,"127.0.0.1");
            while (true){
                Socket client = serverListener.serverSocket.accept();
                serverListener.in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                while (true) {
                    System.out.println(serverListener.in.readLine());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
