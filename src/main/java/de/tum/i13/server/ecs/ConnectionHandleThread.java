package de.tum.i13.server.ecs;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Metadata;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicBoolean;

import static de.tum.i13.shared.Config.LOGGER;

/**
 * Connection handle threads for KV servers
 */

public class ConnectionHandleThread extends Thread {

    protected Socket kvSocket;
    private BufferedReader in;
    private PrintWriter out;
    private boolean toUpdateMetadata;
    private ECSAdministrator administrator;
    private Metadata ownMetadata;
    private boolean toHandOff;
    private Metadata joiner;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public PrintWriter getOut() {
        return out;
    }

    public void setOut(PrintWriter out) {
        this.out = out;
    }
    public void setAdministrator(ECSAdministrator administrator) {
        this.administrator = administrator;
    }

    public Metadata getOwnMetadata() {
        return ownMetadata;
    }

    public void setOwnMetadata(Metadata ownMetadata) {
        this.ownMetadata = ownMetadata;
    }

    public void setJoiner(Metadata joiner) {
        this.joiner = joiner;
    }

    public void setToHandOff(boolean toHandOff) {
        this.toHandOff = toHandOff;
    }


    public void setToUpdateMetadata(boolean toUpdateMetadata) {
        this.toUpdateMetadata = toUpdateMetadata;
    }


    /**
     * Constructor for ConnectionHandleThread
     *
     * @param clientSocket socket of a KV server
     *                     global metadata to update
     * @throws IOException
     */
    public ConnectionHandleThread(Socket clientSocket) throws IOException {
        this.kvSocket = clientSocket;
        in = new BufferedReader(new InputStreamReader(kvSocket.getInputStream(), Constants.TELNET_ENCODING));
        out = new PrintWriter(new OutputStreamWriter(kvSocket.getOutputStream(), Constants.TELNET_ENCODING));
        toUpdateMetadata = false;
    }

    /**
     * method is used to read the parse the message with adress and port from KVStore
     *
     * @param msg string adress
     * @return splitted to port and host
     */

    private String[] parseAddressPort(String msg) {
        String[] toParse = msg.trim().split(" ");
        return toParse;
    }

    /**
     * sends a hand-off command to the kvserver
     *
     * @param joiner Metadata of server to which data is to be transfered
     * @return boolean whether hand off was successful or not
     */

    public String handoffSuccess(Metadata joiner) {
        //TODO: Check if handOff was success
        String answer = "";
        out.write("handoff\r\n");
        out.flush();
        List<Metadata> temp = new ArrayList<>();
        temp.add(joiner);
        Metadata.sendMetadata(temp, this.out);
        try {
            answer = in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return answer;
    }

    /**
     * Killing the thread
     */

    public void terminate() {
        System.out.println("killing thread at position " + ownMetadata.getRingPosition());
        running.set(false);
    }

    public void run() {
        running.set(true);
        try {
            String[] ap = this.parseAddressPort(in.readLine());
            this.setOwnMetadata(new Metadata(ap[0], Integer.parseInt(ap[1])));
            administrator.bootstrap(ownMetadata);
            Metadata.sendMetadata(administrator.getMetadata(), out);
            System.out.println("after line 125:" + in.readLine());
            System.out.println("Metadata is set. " + this.ownMetadata.getServerHash() + " " + this.ownMetadata.getRingPosition() + " " + this.ownMetadata.getRange());
            LOGGER.info("A wild KVServer appears!");
            boolean pingPending = false;
            long start = 0;
            long stop = 0;
            outer:
            while (running.get()) {
                try {
                    Thread.sleep(1000);
                    //if(!pingPending&&(System.currentTimeMillis() -start)>=10000) {
                    out.write("Ping..." + "\r\n");
                    out.flush();
                    start = System.currentTimeMillis();
                    pingPending = true;
                    // }
                    String firstLine;
                         synchronized (in) {
                        if (running.get()) {
                            firstLine = in.readLine();
                            System.out.println(firstLine);
                        } else {
                            System.out.println("Socket "+ap[1]+" closed");
                            kvSocket.close();
                            break outer;

                        }
                   }

                    //prints the ping delay when ping is answered
                    if (firstLine.equals("ping-ping")) {
                        stop = System.currentTimeMillis();
                        System.out.println("KV@port " + this.ownMetadata.getPort()+ " Ping: "+ (stop-start)+"ms");

                    } else if (firstLine.equals("Request metadata")) {
                        synchronized (out) {
                            administrator.broadcast();
                        }
                    }

                    else if (firstLine.equals("shutdown_request")) {
                        System.out.println("got a shutdown request!" + ap[1]);
                        administrator.disconnect(ownMetadata, true);

                    }

                    else if (!firstLine.equals("ping-ping") && !firstLine.equals("Updated") && !firstLine.equals("handoff_error") && !firstLine.equals("handoff_success")) {
                        try {
                            System.out.println("Disconnect because of ping");
                            administrator.disconnect(ownMetadata, false);

                            running.set(false);
                            break outer;
                        } catch (IndexOutOfBoundsException e) {
                            System.out.println("No server is connected.");

                        }
                        break outer;
                    }
                    if (this.toHandOff) {
                        synchronized (out) {
                            this.handoffSuccess(this.joiner);
                            this.setToHandOff(false);
                            this.setJoiner(null);
                        }
                    }
                    if (this.toUpdateMetadata) {
                        synchronized (out) {
                            Metadata.sendMetadata(administrator.getMetadata(), out);
                            this.setToUpdateMetadata(false);
                        }
                    }
                    //if there has been no ping answer since 10 seconds, server is considered lost
                    if(pingPending && System.currentTimeMillis()-start>10000){
                        System.out.println("Disconnecting because of ping");
                        administrator.disconnect(ownMetadata, false);
                        kvSocket.close();
                    }


                } catch (SocketException s) {
                    System.out.println("Exception caught and disconnect");
                    administrator.disconnect(ownMetadata, false);

                    kvSocket.close();


                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }
}





