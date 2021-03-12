package de.tum.i13.server.kv;

import de.tum.i13.server.echo.EchoLogic;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Metadata;

import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class KVCommandProcessor implements CommandProcessor {
    /**
     * @return KVStore where key value pairs are persisted
     */
    public KVStore getKvStore() {
        return kvStore;
    }
    /**
     * Method to create a KVStore where key value pairs are persisted
     */
    public void setKvStore(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    private KVStore kvStore;
    public KVCommandProcessor(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    /**
     *
     * @param command that a server becomes from a client to be parsed
     * This method invokes a Remote Call Procedure to a particular kvStore to resolve the client's request
     * Three possibilities are : get, put and delete
     * False request are also possible and will be treated accordingly
     * @return answer that will be send back to a client
     */

    @Override
    public String process(String command) {

        String answer = "";
        KVMessage kvm = null;
        KVMessage.StatusType temp = null;
        String[] term;
        term = command.trim().split("\\s+");
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        try {
            switch (term[0].toLowerCase()) {
                //one KV server send data to another KV server
                case "handoff":
                    try {
                        kvm = this.kvStore.put(term[1], term[2]);
                        answer = kvm.getStatus().toString().toLowerCase();
                        System.out.println("handoff " + term[1]);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case "put":
                    if (this.kvStore.isBalancing()) {
                        return "server_write_lock" ;
                    }
                    else if (this.kvStore.isStopped()){
                        return "server_stopped" ;
                    }
                    if(!this.kvStore.getOwnMetadata().isResponsible(term[1])){
                        return "server_not_responsible" ;
                    }

                    String value = "";
                    try {
                        if (term.length == 2) {
                            kvm = this.kvStore.put(term[1], "");
                            if(kvm.getStatus()!= KVMessage.StatusType.DELETE_ERROR
                                    && kvStore.getOwnMetadata().getBackupOne()!=null && kvStore.getOwnMetadata().getBackupTwo()!=null){
                                Metadata backUp1 = Metadata.getByHash(this.kvStore.getMetadata(), this.kvStore.getOwnMetadata().getBackupOne());
                                Metadata backUp2 = Metadata.getByHash(this.kvStore.getMetadata(), this.kvStore.getOwnMetadata().getBackupTwo());
                                KVHandOff handOff1 = new KVHandOff(backUp1);
                                KVHandOff handOff2 = new KVHandOff(backUp2);
                                String answer1 = handOff1.update(term[1], null);
                                String answer2 = handOff2.update(term[1], null);
                            }

                        } else {
                            for (int i = 2; i < term.length; i++) {
                                value += term[i] + " ";
                            }
                            value = value.trim();
                            kvm = this.kvStore.put(term[1], value);
                            if(kvm.getStatus()!= KVMessage.StatusType.PUT_ERROR
                                    && kvStore.getOwnMetadata().getBackupOne()!=null && kvStore.getOwnMetadata().getBackupTwo()!=null) {
                                Metadata backUp1 = Metadata.getByHash(this.kvStore.getMetadata(), this.kvStore.getOwnMetadata().getBackupOne());
                                Metadata backUp2 = Metadata.getByHash(this.kvStore.getMetadata(), this.kvStore.getOwnMetadata().getBackupTwo());
                                KVHandOff handOff1 = new KVHandOff(backUp1);
                                KVHandOff handOff2 = new KVHandOff(backUp2);
                                String answer1 = handOff1.update(term[1], value);
                                String answer2 = handOff2.update(term[1], value);
                            }
                        }
                        temp = kvm.getStatus();
                        switch (temp) {
                            case PUT_ERROR:
                                answer = kvm.getStatus().toString().toLowerCase() + " " + kvm.getKey() + " " + kvm.getValue();
                                break;
                            default:
                                answer = kvm.getStatus().toString().toLowerCase() + " " + kvm.getKey();
                                break;

                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case "get":
                    if (this.kvStore.isStopped()){
                        return "server_stopped";
                    }

                    if(this.kvStore.getOwnMetadata().isResponsible(term[1])
                            || (kvStore.getOwnMetadata().getCoordinatorOne()!=null && Metadata.getByHash(kvStore.getMetadata(),kvStore.getOwnMetadata().getCoordinatorOne()).isResponsible(term[1]))
                            || (kvStore.getOwnMetadata().getCoordinatorTwo()!= null && Metadata.getByHash(kvStore.getMetadata(),kvStore.getOwnMetadata().getCoordinatorTwo()).isResponsible(term[1])) )
                    {
                        try {
                            kvm = this.kvStore.get(term[1]);
                            if (kvm.getStatus().equals(KVMessage.StatusType.GET_ERROR)) {
                                answer = kvm.getStatus().toString().toLowerCase() + " " + kvm.getKey();
                            } else {
                                answer = kvm.getStatus().toString().toLowerCase() + " " + kvm.getKey() + " " + kvm.getValue();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                    else {
                        return "server_not_responsible";
                    }


                case "delete":
                    if(!this.kvStore.getOwnMetadata().isResponsible(term[1])){
                        return "server_not_responsible";
                    }
                    if (this.kvStore.isBalancing()) {
                        return "server_write_lock";
                    }
                    else if (this.kvStore.isStopped()){
                        return "server_stopped";
                    }
                    if (term.length == 2) {
                        try {
                            kvm = this.kvStore.put(term[1], "");
                            answer = kvm.getStatus().toString().toLowerCase() + " " + kvm.getKey();
                            if(kvm.getStatus()!= KVMessage.StatusType.DELETE_ERROR
                                    && kvStore.getOwnMetadata().getBackupOne()!=null && kvStore.getOwnMetadata().getBackupTwo()!=null){
                                Metadata backUp1 = Metadata.getByHash(this.kvStore.getMetadata(), this.kvStore.getOwnMetadata().getBackupOne());
                                Metadata backUp2 = Metadata.getByHash(this.kvStore.getMetadata(), this.kvStore.getOwnMetadata().getBackupOne());
                                KVHandOff handOff1 = new KVHandOff(backUp1);
                                KVHandOff handOff2 = new KVHandOff(backUp2);
                                String answer1 = handOff1.update(term[1], null);
                                String answer2 = handOff2.update(term[1], null);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        answer = "error";
                    }
                    break;
                //TODO: Create a new command to update data in backup
                case "put_rep":
                    String value2 = "";
                    for (int i = 2; i < term.length; i++) {
                        value2 += term[i] + " ";
                    }
                    value2 = value2.trim();
                    try {
                        kvm = this.kvStore.put(term[1], value2);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if(kvm.getStatus()== KVMessage.StatusType.PUT_ERROR) {
                        answer = "error";
                    }
                    else {
                        answer = "put_rep_success";
                    }
                    break;
                case "delete_rep":
                    try {
                        kvm = this.kvStore.put(term[1],"");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if(kvm.getStatus()== KVMessage.StatusType.DELETE_ERROR){
                        answer = "error";
                    }
                    else {
                        answer = "delete_rep_success";
                    }
                    break;
                case "backup":
                    ArrayList<KeyValue> data = FileHandler.parseData(term[1]);
                    ArrayList<KVMessage> messages = new ArrayList<>();
                    try {
                        for (KeyValue kv : data) {
                            messages.add(kvStore.put(kv.key, kv.value));
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    for (KVMessage message : messages) {
                        if(message.getStatus().equals(KVMessage.StatusType.PUT_ERROR)){
                            return "backup_error";
                        }
                    }
                    return "backup_success" ;
                case "keyrange":
                    return "keyrange";
                case "keyrange_read":
                    return "keyrange_read";
                case "stop":
                    this.kvStore.setStopped(true);
                    answer = "stopped";
                    break;
                case "unlock":
                    this.kvStore.setStopped(false);
                    answer = "unlocked";
                    break;
                case "subscribe":
                    if(term.length!=4){
                        answer= "error";
                    }
                    else {
                        return command;
                    }
                    //TODO: term[1] = sid; term[2] = key; term[3] = port;
                    //TODO: else: "error"
                    //TODO: pass to ConnectionHandleThread
                    break;
                case "publish":
                    //TODO: return = key and value
                    if(term.length!=3){
                        answer= "error";
                    }
                    else {
                        return command;
                    }
                    break;
                case "copy_publish":
                    return command;
                case "unsubscribe":
                    if(term.length!=3){
                        answer="error";
                    }
                    else {
                        return command;
                    }
                    break;
                default:
                    answer = "error";
                    break;

            }
        } finally {
            lock.unlock();
        }
        return answer;
    }


    /**
     * @return Welcome message to a client after accepted connection
     */
    @Override
    public String connectionAccepted (InetSocketAddress address, InetSocketAddress remoteAddress){
        String originalInput = "Connection to server " + address + " established.";
        return originalInput;
    }

    @Override
    public void connectionClosed (InetAddress address){

    }

    public static String[] parseSubscription(String msg){
        String[] term = msg.split("\\s+");
        if(term.length!=4){
            return null;
        }

        return term;
    }

    public static Publication parsePublication(String msg, long timestamp){
        String[] term = msg.split("\\s+");
        if(term.length!=3){
            return null;
        }
        KeyValue kv = new KeyValue(term[1],term[2]);
        Publication result = new Publication(timestamp,kv);
        return result;

    }
    public static Publication parsePublicationCopy(String msg){
        String[] term = msg.split("\\s+");
        KeyValue kv = new KeyValue(term[1],term[2]);
        Publication result = new Publication(Long.valueOf(term[4]),kv);
        return result;

    }


    public static String[] parseUnsubscribe(String msg){
        String[] term = msg.split("\\s+");
        if(term.length!=3){
            return null;
        }

        return term;
    }




}


