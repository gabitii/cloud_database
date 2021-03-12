package de.tum.i13.server.kv;

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
 * This class provides establishes connection with ECSServer
 * and provides a functionality of interacting with it
 */
public class ECSHandler extends Thread{
    private Socket ecsServer;
    private KVStore kvStore;
    private BufferedReader in ;
    private PrintWriter out ;
    private ECSMessageHandler messageHandler;
    private Boolean toShutDown;

    public Boolean getToShutDown() {
        return toShutDown;
    }

    public void setToShutDown(Boolean toShutDown) {
        this.toShutDown = toShutDown;
    }

    //address and port of the ECS
    //related KVstore for this server node

    /**
     * Constructor for ECSHandler
     * @param address adrress of related KVServer
     * @param port port of related KVServer
     * @param kvStore KVStore which belongs to related KVServer
     * @throws IOException for InputOutputException
     */
    public ECSHandler(InetAddress address, int port, KVStore kvStore) throws IOException {
        /*this.port = port;
        this.address = address;
         */
        this.ecsServer = new Socket(address,port);
        in = new BufferedReader(new InputStreamReader(ecsServer.getInputStream(), Constants.TELNET_ENCODING));
        out = new PrintWriter(new OutputStreamWriter(ecsServer.getOutputStream(), Constants.TELNET_ENCODING));
        this.kvStore=kvStore;
        this.messageHandler = new ECSMessageHandler(this.kvStore, in);
        toShutDown = false;
    }

    /**
     * this method sends shut down request to ecsserver
     */
    public void onShutdown() {
        out.write("shutdown_request" + "\r\n");
        out.flush();
        System.out.println("shutdown sent");
    }


    @Override
    public void run() {
        try {
            out.write(kvStore.getAddress() + " " +kvStore.getPort()+"\r\n");
            out.flush();
            String firstLine;
            String res = "";
            while ((firstLine = in.readLine()) != null || this.toShutDown == true) {
                if(this.toShutDown == true){
                    System.out.println("sending shutdown request");
                    res = "shutdown_request\r\n";
                    out.write(res);
                    out.flush();
                    toShutDown= false;

                }
                if(kvStore.requestMD()){
                   this.kvStore.setBalancing(true);
                    LOGGER.info("Requesting metadata");
                    res= "Request metadata";

                    out.write(res + "\r\n");
                    out.flush();
                    synchronized(kvStore) {
                        List<Metadata> newMeta = Metadata.receiveMetadata(in);
                        kvStore.setMetadata(newMeta);
                    }
                    LOGGER.info("Received metadata");
                    kvStore.setRequestMD(false);
                    this.kvStore.setBalancing(false);
                    continue;
                }
                if(firstLine.equals("Ping...")){
                    res="ping-ping";
                }
                else if(firstLine.equals("METADATA_TRANSMISSION_START")){
                    String oldBackUpOne = null;
                    String oldBackUpTwo = null;
                    if(this.kvStore.getMetadata()!=null) {
                        oldBackUpOne = this.kvStore.getOwnMetadata().getBackupOne();
                        oldBackUpTwo = this.kvStore.getOwnMetadata().getBackupTwo();
                    }
                    this.kvStore.setBalancing(true);
                    synchronized(kvStore) {
                        List<Metadata> newMeta = Metadata.receiveMetadata(in);
                        this.kvStore.setMetadata(newMeta);
                        res = "Updated";
                        LOGGER.info("updated metadata");
                        this.kvStore.setBalancing(false);
                    }
                    if(this.kvStore.getMetadata().size()>=3) {
                        if (oldBackUpOne == null || oldBackUpTwo == null) {
                            KVHandOff handOff = new KVHandOff(this.kvStore);
                            handOff.backUp();
                        } else if (!oldBackUpOne.equals(this.kvStore.getOwnMetadata().getBackupOne())
                                || !oldBackUpTwo.equals(this.kvStore.getOwnMetadata().getBackupTwo())) {
                            KVHandOff handOff = new KVHandOff(this.kvStore);
                            String answer = handOff.backUp();
                        }
                    }
                }else{
                    res = messageHandler.process(firstLine);
                }
                out.write(res + "\r\n");
                out.flush();

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        System.out.println("Thread is killed! "+kvStore.getPort());

    }
}
