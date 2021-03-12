package de.tum.i13.server.ecs;

import de.tum.i13.shared.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;



public class ECSServer {

    /**
     *
     * @param args to set a host and a port of the ecs server
     * @throws IOException
     */

    public static void main(String[] args) throws IOException {
        List<Metadata> metadata = new ArrayList<>();
        Config cfg = Config.parseCommandlineArgs(args);  //Do not change this
        LogSetup.setupLogging(cfg.logfile);
        final ServerSocket ecsServerSocket = new ServerSocket();

        /*
         * When server is shut down
         */

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Closing thread per connection ecs server");
                try {
                    ecsServerSocket.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        ecsServerSocket.bind(new InetSocketAddress(cfg.listenaddr, cfg.port));
        ECSAdministrator admin = new ECSAdministrator(ecsServerSocket);
        admin.handleServers();
    }
}


