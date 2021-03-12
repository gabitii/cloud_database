package de.tum.i13.server.kv;

import de.tum.i13.shared.Metadata;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * KVStoreFIFO is implementation of KVStore which
 * manages elements in cache with the FIFO-rules
 * (First in - first Out)
 */

public class KVStoreFIFO implements KVStore {
    private boolean isBalancing;
    private Queue<KeyValue> queue;
    private int cacheSize;
    private FileHandler fh;
    private String address;
    private int port;
    private boolean isStopped;
    private List<Metadata> metadata;
    private boolean requestMD;

    @Override
    public boolean requestMD() {
        return false;
    }

    @Override
    public void setRequestMD(boolean requestMD) {
        this.requestMD = requestMD;
    }

    /**
     * Constructor for KVStoreFIFO sets the following attributes:
     * @param cacheSize the maximum amount of elements, that can be stored in cache
     * @param path for the persistent Disk, where elements will be stored
     * @param address is the address of server, which kv server belongs to
     * @param port is the port of server, which kv server belongs to
     */
    public KVStoreFIFO(int cacheSize, Path path, String address, int port) {
        queue = new LinkedList<KeyValue>();
        this.cacheSize = cacheSize;
        this.fh = new FileHandler(path, port, false, 0);
        this.address = address;
        this.port = port;
        isStopped = false;
        isBalancing = false;
    }


    @Override
    public List<Metadata> getMetadata() {
        return metadata;
    }

    public void setMetadata(List<Metadata> metadata) {
        this.metadata = metadata;
    }

    public FileHandler getFileHandler() {
        return this.fh;
    }

    @Override
    public void setBalancing(boolean value) {
        this.isBalancing = value;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public boolean isStopped() {
        return isStopped;
    }

    public void setStopped(boolean stopped) {
        isStopped = stopped;
    }

    public boolean isBalancing() {
        return isBalancing;
    }

    /**
     * This metod searchs for own Metadata in globalMetadata
     * @return own Metadata
     */
    public Metadata getOwnMetadata(){

        for(Metadata md: this.metadata){
            if(md.getPort()==this.port && md.getAddress().equals(this.address)){
                return md;
            }
        }
        return null;
    }

    /**
     * get-method for queue of elements
     * @return a queue with elements
     */
    public Queue<KeyValue> getQueue() {
        return queue;
    }

    /**
     * set-method for queue of elements
     * @param queue - the parameter which should be set
     */
    public void setQueue(Queue<KeyValue> queue) {
        this.queue = queue;
    }


    /**
     * this method adds an element into the cash.
     * If the cash is full, it will store the next "out"-value
     * in the disc and add the new value in cache.
     * @param kv the value which should be added to the cache
     */
    public void append(KeyValue kv) {
        if (queue.size() == this.cacheSize) {
            this.fh.writeToDisc(queue.remove());
        }
        if(this.inCache(kv.key)){
            queue.remove(kv);
            queue.add(kv);
        }
        else{
            queue.add(kv);
        }
    }

    /**
     * This methods returns a boolean, whether tuple is in cache or not
     * @param key - searched key
     * @return true/false for is/is not in cache
     */
    private boolean inCache(String key){
        for(KeyValue kv: this.queue){
            if (kv.key.equals(key)){
                return true;
            }
        }
        return false;
    }


    /**
     * This is a method which puts the value to data storage,
     * it also handles cases,  it decides, whether we delete element,
     * add a new element or update the value of an existing element, and
     * sets value of KVMessage to DELETE_SUCCESS/ERROR, PUT_UPDATE or
     * PUT_SUCCESS/ERROR relatively
     * @param key   the key that identifies the given value.
     * @param value the value that is indexed by the given key.
     * @return KVMessage with key,value and status
     * @throws Exception to avoid unexpected Exceptions
     */
    @Override
    public KVMessage put(String key, String value){
        KVMessageImpl msg = new KVMessageImpl(key);
        for (KeyValue kv : this.queue) {
            if (kv.key.equals(key)) {
                if (value.equals("")) {
                    if(fh.readFromDisk(key)!=null){
                        fh.deleteFromDisk(key);
                    }

                    this.queue.remove(kv);
                    msg.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
                    return msg;
                }

                if(fh.readFromDisk(key)!=null){
                    fh.writeToDisc(new KeyValue(key,value));
                }
                kv.value = value;
                msg.setStatus(KVMessage.StatusType.PUT_UPDATE);
                msg.setValue(value);
                return msg;
            }
        }
        if(fh.readFromDisk(key)!=null){
            if (fh.readFromDisk(key).key.equals(key)) {
                if (value.equals("")) {
                    fh.deleteFromDisk(key);
                    msg.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
                    return msg;
                }
                fh.writeToDisc(new KeyValue(key,value));
                msg.setStatus(KVMessage.StatusType.PUT_UPDATE);
                msg.setValue(value);
                return msg;
            }
        }

        else if(!this.inCache(key) && !value.equals("")) {
            this.append(new KeyValue(key, value));
            msg.setStatus(KVMessage.StatusType.PUT_SUCCESS);
            msg.setValue(value);
            return msg;
        }
        if (value.equals("")){
            msg.setStatus(KVMessage.StatusType.DELETE_ERROR);
            return msg;
        }
            msg.setStatus(KVMessage.StatusType.PUT_ERROR);

        return msg;
    }

    /**
     * This is a method which tries to get a value of
     * the given key, in case the given key is not in
     * the store, it will set a status of a KVMessageImpl
     * to DELETE_ERROR
     * @param key the key that identifies the value.
     * @return KVMessage with the given key, found value and the Status
     * @throws Exception
     */
    @Override
    public KVMessage get(String key) throws Exception {
        KVMessageImpl msg = new KVMessageImpl(key);
        for (KeyValue kv : this.queue) {
            if (kv.key.equals(key)) {
                this.append(kv);

                msg.setValue(kv.value);
                msg.setStatus(KVMessage.StatusType.GET_SUCCESS);
                return msg;
            }
        }
        if(fh.readFromDisk(key)!=null){
            this.append(fh.readFromDisk(key));
            msg.setValue(fh.readFromDisk(key).value);
            msg.setStatus(KVMessage.StatusType.GET_SUCCESS);
            return msg;
        }
        msg.setValue("");
        msg.setStatus(KVMessage.StatusType.GET_ERROR);
        return msg;
    }
    /**
     * Method to maintain persistence. This methods writes all existing cache entries to disk.
     */
    public void putAll() {
        fh.writeAll(queue);
        queue.clear();
    }

/*
    public static void main(String[] args) {
        KVStoreFIFO test = new KVStoreFIFO(4,Path.of("src/"));
        try {
            test.put("apple123@#","orange -@+$");
            test.put("1", "lo l");
            test.put("2", "ke k");
            test.putAll();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

 */


}
