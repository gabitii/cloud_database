package de.tum.i13.server.kv;

import de.tum.i13.shared.Metadata;
import java.io.*;
import java.net.*;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * This class connects to other KVServer in case HandOff is needed
 * and sends data to it
 */
public class KVHandOff {
    private Socket clientSocket;
    private KVStore kvStore;
    private Metadata joiner;
    private Metadata backUp;
    /**
     * Constructor for KVHandOff calss
     * @param kvStore kvStore, from which data is to be sent
     * @param joiner Metadata to which data is to be sent
     * @throws IOException for InputOutputException
     */
    public KVHandOff(KVStore kvStore, Metadata joiner) throws IOException {
        this.joiner = joiner;
        this.kvStore = kvStore;
        clientSocket = new Socket(joiner.getAddress(), joiner.getPort());
    }

    public KVHandOff(Metadata backUp) throws IOException {
        this.backUp=backUp;
        clientSocket = new Socket(backUp.getAddress(), backUp.getPort());
    }
    public KVHandOff(KVStore kvStore){
        this.kvStore = kvStore;
    }

    //handoff address port (server B) ... plus updated Metadata

    /**
     * Method provides main functionality of this class, namely handOff process
     * @return returns an answer with the status of HandOff-process
     * @throws IOException for InputOutputException while reading/writing messages from/to another KVSever
     * @throws InterruptedException in case of Exception while waiting for an answer
     */
    public String handOff() throws IOException, InterruptedException {
        //TODO: stop joiner
        boolean success = false;
        String answer = "";
        this.kvStore.setBalancing(true);
        this.kvStore.putAll();
        PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        in.readLine();
        List<KeyValue> temp = this.kvStore.getFileHandler().deserialize();
        List<KeyValue> toDelete = new LinkedList<KeyValue>();

        out.write("stop" + "\r\n");
        out.flush();
        for (int i = 0; i<10; i++) {
            String line = in.readLine();
            if (line.equals("stopped")) {
                success = true;
                break;
            }
            TimeUnit.SECONDS.sleep(1);
        }
        if (success!=true) {
            return "handoff_error";
        }
        success = false;

        for (KeyValue kv: temp) {
            if (joiner.isResponsible(kv.getKey())) {
              out.write("handoff " + kv.getKey() + " " + kv.getValue() + "\r\n");
              out.flush();
              inner: for (int i = 0; i < 15; i++) {
                  String line2= in.readLine();
                  if(line2.equals("put_success")) {
                      success = true;
                      break inner;
                  }
                  TimeUnit.SECONDS.sleep(1);
              }
              if(!success){
                  return "handoff_error";
              }
              success = false;
              toDelete.add(kv);
            }
        }
        out.write("unlock" + "\r\n");
        out.flush();
        for (int i = 0; i<10; i++) {
            String line3 = in.readLine();
            if (line3.equals("unlocked")){
                success = true;
                break;
            }
            TimeUnit.SECONDS.sleep(1);
        }
        if (!success) {
            return "handoff_error";
        }

        if (!toDelete.isEmpty()) {
            for (KeyValue kv : toDelete) {
                if(!this.kvStore.getOwnMetadata().isResponsible(kv.getKey())
                        && (kvStore.getOwnMetadata().getCoordinatorOne()!=null && !Metadata.getByHash(kvStore.getMetadata(),kvStore.getOwnMetadata().getCoordinatorOne()).isResponsible(kv.getKey()))
                        && (kvStore.getOwnMetadata().getCoordinatorTwo()!= null && !Metadata.getByHash(kvStore.getMetadata(),kvStore.getOwnMetadata().getCoordinatorTwo()).isResponsible(kv.getKey())) )
                {
                    this.kvStore.getFileHandler().deleteFromDisk(kv.getKey());
                }
            }
        }
        this.kvStore.setBalancing(false);
        this.clientSocket.close();
        answer = "handoff_success";
        return answer;
    }

    /**
     * Method provides a functionality to make a backUp process, when kv server sends the data to its backups
     * @return String if backup was success or not
     * @throws IOException for input-outpot exception
     * @throws InterruptedException for interrupted exception when sleeping
     */
    public String backUp() throws IOException, InterruptedException {
        Metadata backUp1 = Metadata.getByHash(kvStore.getMetadata(), kvStore.getOwnMetadata().getBackupOne());
        Metadata backUp2 = Metadata.getByHash(kvStore.getMetadata(), kvStore.getOwnMetadata().getBackupTwo());
        Socket socketBackUp1 = new Socket(backUp1.getAddress(),backUp1.getPort());
        Socket socketBackUp2 = new Socket(backUp2.getAddress(),backUp2.getPort());

        boolean success1 = false;
        boolean success2 = false;
        this.kvStore.setBalancing(true);
        this.kvStore.putAll();
        PrintWriter out1 = new PrintWriter(new OutputStreamWriter(socketBackUp1.getOutputStream()));
        BufferedReader in1 = new BufferedReader(new InputStreamReader(socketBackUp1.getInputStream()));
        PrintWriter out2 = new PrintWriter(new OutputStreamWriter(socketBackUp2.getOutputStream()));
        BufferedReader in2 = new BufferedReader(new InputStreamReader(socketBackUp2.getInputStream()));
        in1.readLine();
        in2.readLine();
        List<KeyValue> temp = this.kvStore.getFileHandler().deserialize();

        for (int i = 0; i < temp.size(); i++) {
            if(!this.kvStore.getOwnMetadata().isResponsible(temp.get(i).getKey())){
                temp.remove(i);
                i--;
            }
        }

        if(temp.size()>=1) {
            out1.write("stop" + "\r\n");
            out1.flush();
            out2.write("stop" + "\r\n");
            out2.flush();
            for (int i = 0; i < 10; i++) {
                String line1 = "";
                String line2 = "";
                if (!success1) {
                    line1 = in1.readLine();
                }
                if (!success2) {
                    line2 = in2.readLine();
                }
                if (!success1 && line1.equals("stopped")) {
                    success1 = true;
                }
                if (!success2 && line2.equals("stopped")) {
                    success2 = true;
                }
                if (success1 && success2) {
                    break;
                }
                TimeUnit.SECONDS.sleep(1);
            }
            if (!success1 || !success2) {
                return "receive_error";
            }
            success1 = false;
            success2 = false;


            String compressedData = FileHandler.compressData(temp);
            out1.write("backup " + compressedData + "\r\n");
            out1.flush();
            out2.write("backup " + compressedData + "\r\n");
            out2.flush();
            inner:
            for (int i = 0; i < 10; i++) {
                String line1 = "";
                String line2 = "";
                if (!success1) {
                    line1 = in1.readLine();
                }
                if (!success2) {
                    line2 = in2.readLine();
                }
                if (!success1 && line1.equals("backup_success")) {
                    success1 = true;
                }
                if (!success2 && line2.equals("backup_success")) {
                    success2 = true;
                }
                TimeUnit.SECONDS.sleep(1);
                if (success1 && success2) {
                    break inner;
                }
            }
            if (!success1 && !success2) {
                return "backup_error";
            }
            success1 = false;


            out1.write("unlock" + "\r\n");
            out1.flush();
            out2.write("unlock" + "\r\n");
            out2.flush();
            for (int i = 0; i < 10; i++) {
                String line1 = "";
                String line2 = "";
                if (!success1) {
                    line1 = in1.readLine();
                }
                if (!success2) {
                    line2 = in2.readLine();
                }
                if (!success1 && line1.equals("unlocked")) {
                    success1 = true;
                }
                if (!success2 && line2.equals("unlocked")) {
                    success2 = true;
                }
                if (success1 && success2) {
                    break;
                }
                TimeUnit.SECONDS.sleep(1);
            }
            if (!success1 || !success2) {
                return "backup_error";
            }
        }
        this.kvStore.setBalancing(false);
        socketBackUp1.close();
        socketBackUp2.close();
        return "backup_success";
    }

    /**
     * Method to trigger an update of replicas of a storage node
     * @param key key to update
     * @param value value to update
     * @return success message
     * @throws IOException
     */

    public String update(String key, String value) throws IOException {
        boolean success = false;
        String answer = "";
        PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        in.readLine();
        if (value.equals(null)) {
            out.write("delete_rep " + key + " " + value + "\r\n");
            out.flush();
        }
        else {
            out.write("put_rep " + key + " " + value + "\r\n");
            out.flush();
        }
        String line = in.readLine();
        if(line.equals("put_rep_success") ||  line.equals("delete_rep_success")){
            return "update_success";
        }
        else{
            return "update_error";
        }
    }



}
