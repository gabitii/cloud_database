package de.tum.i13.server.kv;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Metadata;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.TimeUnit;


import static de.tum.i13.shared.Config.LOGGER;

/**
 * This class interacts with clients and parses its requests
 */
public class ConnectionHandleThread extends Thread {
    private CommandProcessor cp;
    protected Socket clientSocket;
    private int port;
    private BufferedReader in;
    //private PrintWriter out;
    private List<Metadata> metadata;
    private KVStore kvStore;
    private MessageSender sender;
    private PubSubBroker broker;
    private String notify;
    private boolean running;

    public MessageSender getSender() {
        return sender;
    }

    public int getPort() {
        return port;
    }

    /**
     * The constructor
     * @param commandProcessor command processor attribute which processes the client-requests
     * @param clientSocket socket-objects with client
     * @param kvStore kv-store which is related to the kv-server
     * @param broker broker which processes publications and subscriptions
     */
    public ConnectionHandleThread(CommandProcessor commandProcessor, Socket clientSocket, KVStore kvStore, PubSubBroker broker) {
        this.cp = commandProcessor;
        this.clientSocket = clientSocket;
        this.kvStore = kvStore;
        try {
            this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
            this.sender = new MessageSender(clientSocket.getOutputStream());
            //this.out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.broker = broker;
        this.port = clientSocket.getPort();
        this.running = true;
        this.notify = "";
        //  System.out.println(String.valueOf(clientSocket.getRemoteSocketAddress())+" "+String.valueOf(clientSocket.getInetAddress())+" "+clientSocket.getLocalSocketAddress());

    }

    /**
     * This method disconnects this server from client
     * @throws IOException for input-output exceptions
     */
    private void disconnect() throws IOException{
        this.in.close();
        this.sender.disconnect();
        this.clientSocket.close();
    }

    /**
     * This method is used to kill the thread
     */
    public void kill(){
        running = false;
        try {
            this.disconnect();
        } catch (IOException e) {
            
        }
    }

    /**
     * Overriding run-method
     */
    @Override
    public void run() {
        LOGGER.info("A wild client appears!");
        try {
            sender.send(cp.connectionAccepted((InetSocketAddress) clientSocket.getLocalSocketAddress(), (InetSocketAddress) clientSocket.getRemoteSocketAddress()));
            String firstLine = "";
            while (running) {
                if (notify.length() > 0) {
                    sender.send(notify);
                    notify = "";
                }
                if (!clientSocket.isClosed()&&clientSocket.getInputStream().available() > 0) {
                    firstLine = in.readLine();
                    String res = cp.process(firstLine);
                    if (res.startsWith("subscribe")) {
                        String[] temp = KVCommandProcessor.parseSubscription(res);
                        if (temp != null) {
                            if(this.kvStore.getOwnMetadata().isResponsible(temp[2])
                                    || (kvStore.getOwnMetadata().getCoordinatorOne()!=null && Metadata.getByHash(kvStore.getMetadata(),kvStore.getOwnMetadata().getCoordinatorOne()).isResponsible(temp[2]))
                                    || (kvStore.getOwnMetadata().getCoordinatorTwo()!= null && Metadata.getByHash(kvStore.getMetadata(),kvStore.getOwnMetadata().getCoordinatorTwo()).isResponsible(temp[2]))){
                                sender.send("subscribe_success " + temp[1] + " " + temp[2]);
                                broker.addSubscription(temp[2],this, temp[1], Integer.parseInt(temp[3]), clientSocket.getInetAddress());

                            }else {
                                sender.send("server_not_responsible");
                                  }
                        } else {
                            sender.send("subscribe_error");
                        }
                    }
                    else if (res.startsWith("unsubscribe")){
                        String[] temp = KVCommandProcessor.parseUnsubscribe(res);
                        if (this.kvStore.getOwnMetadata().isResponsible(temp[2])
                                || (kvStore.getOwnMetadata().getCoordinatorOne()!=null && Metadata.getByHash(kvStore.getMetadata(),kvStore.getOwnMetadata().getCoordinatorOne()).isResponsible(temp[2]))
                                || (kvStore.getOwnMetadata().getCoordinatorTwo()!= null && Metadata.getByHash(kvStore.getMetadata(),kvStore.getOwnMetadata().getCoordinatorTwo()).isResponsible(temp[2]))) {

                            sender.send("unsubscribe_success "  + temp[2]);
                            broker.deleteSubscription(temp[1],temp[2]);

                        }else {
                            sender.send("server_not_responsible");
                        }
                    }

                    else if (res.startsWith("publish")) {
                        Publication publication = KVCommandProcessor.parsePublication(res, System.currentTimeMillis());
                        if (publication != null) {
                            if (this.kvStore.getOwnMetadata().isResponsible(publication.getKeyValue().getKey())) {
                                Metadata backupOne = Metadata.getByHash(kvStore.getMetadata(),kvStore.getOwnMetadata().getBackupOne());
                                Metadata backupTwo = Metadata.getByHash(kvStore.getMetadata(),kvStore.getOwnMetadata().getBackupTwo());
                                if (backupOne!=null&&backupTwo!=null) {
                                this.sendPubCopy(backupOne.getAddress(), backupOne.getPort(), res);
                                this.sendPubCopy(backupTwo.getAddress(), backupTwo.getPort(), res); }
                                broker.addPublication(publication);
                                sender.send("publication_success " + publication.getKeyValue().getKey() + " " + publication.getKeyValue().getValue());

                            } else{
                                sender.send("server_not_responsible");
                            }
                        } else {
                            sender.send("publication_error " + publication.getKeyValue().getKey() + " " + publication.getKeyValue().getValue());
                        }
                    }

                    else if (res.startsWith("copy_publish")) {
                        Publication publication = KVCommandProcessor.parsePublication(res, System.currentTimeMillis());
                        if (publication != null) {
                            broker.addPublication(publication);
                            System.out.println("COPY PUB");
                        }
                    }

                    else if (res.equals("keyrange")) {
                        LOGGER.info("keyrange requested");
                        kvStore.setRequestMD(true);
                        TimeUnit.SECONDS.sleep(1);
                        sender.sendKeyrange(kvStore.getMetadata());
                        //Metadata.keyrange(kvStore.getMetadata(),out);
                    } else if (res.equals("keyrange_read")) {
                        LOGGER.info("keyrange_read requested");
                        kvStore.setRequestMD(true);
                        TimeUnit.SECONDS.sleep(1);
                        sender.sendKeyrangeRead(kvStore.getMetadata());
                        //Metadata.keyrange_read(kvStore.getMetadata(),out);
                    } else {
                        sender.send(res);
                    }
                }
            }
        } catch (InterruptedException interruptedException) {
            interruptedException.printStackTrace();
        } catch (IOException ioException) {
            System.out.println("Socket is closed. No connection is possible anymore.");
        }



    }

    public void sendPubCopy(String address,int port, String msg) throws IOException {
        Socket socket = new Socket(address, port);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
        in.readLine();
        out.write("copy_"+msg+"\r\n");
        out.flush();
        out.close();
        in.close();
        socket.close();
    }

    public void setNotify(String notify) {
        System.out.println("Notification Gang");
        this.notify = notify;

/**
 * Method to trigger the sending of metadata to a node
 *
 * @param metadata metadata that needs to be sent
 */
    /*public void keyrange(List<Metadata> metadata){
        Metadata.sendMetadata(metadata,out);
    }*/
    }
}

