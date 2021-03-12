package de.tum.i13.server.kv;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.Metadata;

import javax.crypto.spec.PSource;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * Created by chris on 09.01.15.
 */
public class KVServer {


    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile);
        final ServerSocket serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress(cfg.listenaddr, cfg.port));
        System.out.println(serverSocket.getLocalPort());
        KVStore kvStore = null;
        List<Metadata> metadata = new ArrayList<Metadata>();


        /**
         * Parsing configurations to create a specific KVStore with a specific cache displacement strategy
         */
        switch (cfg.strategy) {
            case "FIFO":
                kvStore = new KVStoreFIFO(cfg.cache, cfg.dataDir, cfg.listenaddr, serverSocket.getLocalPort());
                break;
            case "LFU":
                kvStore = new KVStoreLFU(cfg.cache, cfg.dataDir, cfg.listenaddr, serverSocket.getLocalPort());
                break;
            case "LRU":
                kvStore = new KVStoreLRU(cfg.cache, cfg.dataDir, cfg.listenaddr, serverSocket.getLocalPort());
                break;
            default:
                kvStore = new KVStoreFIFO(cfg.cache, cfg.dataDir, cfg.listenaddr, serverSocket.getLocalPort());
                break;
        }

        /**
         *
         * When server is shut down, all the elements from the cache will be written to a disk with putAll() method
         **/
        KVStore finalKvStore = kvStore;
        List<ConnectionHandleThread > threads = new ArrayList<>();
        PubSubBroker broker = new PubSubBroker(cfg.retentionTime*1000,kvStore);
        broker.start();
        CommandProcessor logic = new KVCommandProcessor(finalKvStore);
        ECSHandler thread = new ECSHandler(cfg.bootstrap.getAddress(), cfg.bootstrap.getPort(), finalKvStore);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {

                long start = System.currentTimeMillis();
                System.out.println("Closing thread per connection kv server");
                finalKvStore.putAll();
                thread.onShutdown();
                try {
                    TimeUnit.MILLISECONDS.sleep(2500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                broker.kill();
                for (ConnectionHandleThread t: threads){
                    t.kill();
                }
                try {
                    serverSocket.close();
                    System.out.println("server is closed");
                } catch (IOException e) {
                    e.printStackTrace();
                }

                /*
                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } */

                System.out.println("Shutdown took " + (System.currentTimeMillis() - start) + "ms");
            }
        });


        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println(clientSocket.toString());
                /**
                 *
                 * Starting a new thread per connection.
                 */
                ConnectionHandleThread th = new ConnectionHandleThread(logic, clientSocket, finalKvStore, broker);

                threads.add(th);
                th.start();
            } catch (SocketException e) {

            }
        }
    }
}
