package de.tum.i13.server.kv;

import de.tum.i13.shared.Metadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.Buffer;
import java.util.LinkedList;
import java.util.List;

/**
 * This class processes messages from ECSServer
 */
public class ECSMessageHandler {
    private KVStore kvStore;
    private BufferedReader in;
    private KVHandOff kvHandOff;

    /**
     * Constructor for ECSMessageHandler
     * @param kvStore kvStore which belongs to current kvStore
     * @param in BufferedReader for reading next inputs
     */
    public ECSMessageHandler(KVStore kvStore, BufferedReader in){
        this.kvStore=kvStore;
        this.in = in;

    }

    //TODO: Implement according to the ECSServer replies
    //TODO: Lock KVServer while processing reply from ECSServer

    /**
     * This metod gets a parsed message and makes an answer due to request
     * @param msg parsed message
     * @return answer
     * @throws IOException for InputOutput Exception
     */
    public String process(String msg) throws IOException {
        //TODO: send data to another kvServer
        String[] term;
        String res = "";
        term = msg.trim().split("\\s+");

        switch(term[0].toLowerCase()) {
            case "stop": this.kvStore.setStopped(true);
            res = "Updated";
            break;
            case "start": this.kvStore.setStopped(false);
            res = "Updated";
            break;
            // handoff metadata old metadata joiner
            case "handoff"://"handoff + START + ... + STOP"
                in.readLine();
                List<Metadata> temp = Metadata.receiveMetadata(in);
                try {
                    this.kvHandOff = new KVHandOff(kvStore,temp.get(0));
                    res = kvHandOff.handOff();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (IndexOutOfBoundsException e){
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
        }









        return res;
    }
}
