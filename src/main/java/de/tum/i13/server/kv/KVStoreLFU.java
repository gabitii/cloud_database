package de.tum.i13.server.kv;

import de.tum.i13.shared.Metadata;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * KVStoreLFU is implementation of KVStore which
 * manages elements in cache with the LFU-rules
 * (Last recently viewed)
 */
public class KVStoreLFU implements KVStore {

    private boolean isBalancing;
    private String address;
    private int port;
    private List<KeyValueLFU> cache;
    private int cacheSize;
    private FileHandler fh;
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
    @Override
    public List<Metadata> getMetadata() {
        return metadata;
    }

    public void setMetadata(List<Metadata> metadata) {
        this.metadata = metadata;
    }

    @Override
    public boolean isStopped() {
        return isStopped;
    }

    public void setStopped(boolean stopped) {
        isStopped = stopped;
    }

    public FileHandler getFileHandler() {
        return this.fh;
    }

    public boolean isBalancing() {
        return isBalancing;
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
     * @return a cache of elements
     */
    public List<KeyValueLFU> getCache() {
        return cache;
    }
    /**
     * set-method for queue of elements
     * @param cache - the parameter which should be set
     */
    public void setCache(List<KeyValueLFU> cache) {
        this.cache = cache;
    }

    /**
     * Constructor for KVStoreLFU sets the following attributes:
     * @param cacheSize the maximum amount of elements, that can be stored in cache
     * @param path for the persistent Disk, where elements will be stored
     * @param address is the address of server, which kv server belongs to
     * @param port is the port of server, which kv server belongs to
     */
    public KVStoreLFU(int cacheSize, Path path, String address, int port) {
        cache = new ArrayList<KeyValueLFU>();
        this.cacheSize = cacheSize;
        this.fh = new FileHandler(path, port, false, 0);
        this.address = address;
        this.port = port;
        isStopped = false;
        isBalancing = false;
    }

    /**
     * this method adds an element into the cash.
     * If the cash is full, it will store the next "out"-value
     * in the disc and add the new value in cache.
     * @param kv the value which should be added to the cache
     */
    public void append(KeyValueLFU kv) {
        if (cache.size() == this.cacheSize) {
            KeyValueLFU lfu = new KeyValueLFU("proxy", "proxy");
            lfu.proxyUseCount();
            for(KeyValueLFU k: this.cache){
                if(k.getKey().equals(kv.key)){
                    k.value = kv.value;
                    return;
                }
                if(k.getUseCount() <= lfu.getUseCount()){
                    lfu = k;
                }
            }
            fh.writeToDisc(new KeyValue(lfu.key,lfu.value));
            this.cache.remove(lfu);
        }
        cache.add(kv);

    }

    /**
     * This methods returns a boolean, whether tuple is in cache or not
     * @param key - searched key
     * @return true/false for is/is not in cache
     */

    private boolean inCache(String key){
        for(KeyValue kv: this.cache){
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
    public KVMessage put(String key, String value) throws Exception {
        KVMessageImpl msg = new KVMessageImpl(key);
        for (KeyValueLFU kv : this.cache) {
            if (kv.key.equals(key)) {
                if (value.equals("")) {
                    if(fh.readFromDisk(key)!=null){
                        fh.deleteFromDisk(key);
                    }
                    this.cache.remove(kv);
                    msg.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
                    return msg;
                }

                if(fh.readFromDisk(key)!=null){
                    fh.writeToDisc(new KeyValue(key,value));
                }
                kv.value = value;
                msg.setStatus(KVMessage.StatusType.PUT_UPDATE);
                msg.setValue(value);
                kv.increaseUseCount();
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
        //TODO: BUG BEI PUT KEY
        else if(!this.inCache(key) && !value.equals("")) {
            this.append(new KeyValueLFU(key, value));
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
        for (KeyValueLFU kv : this.cache) {
            if (kv.key.equals(key)) {
                if(!this.inCache(key)){
                    this.append(kv);
                }
                msg.setValue(kv.value);
                msg.setStatus(KVMessage.StatusType.GET_SUCCESS);
                kv.increaseUseCount();
                return msg;
            }
        }
        if(fh.readFromDisk(key)!=null){
            KeyValue kv = fh.readFromDisk(key);
            KeyValueLFU kvLfu = new KeyValueLFU(kv.key, kv.value);
            if(!this.inCache(key)){
                this.append(kvLfu);
            }
            msg.setValue(kvLfu.value);
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
        for(KeyValue kv: this.cache){
            fh.writeToDisc(kv);
        }
        this.cache.clear();
    }



   /*
    public static void main(String[] args) {
        KVStoreLFU test = new KVStoreLFU(4, Path.of("src/testLFU.txt"));
        KVMessage kvm = null;
        //initial fill
        for (int i = 0; i < 4; i++){
            String key = "key" + i;
            String value = "value"+i;
            try {
                System.out.println("Add " + i);
                kvm = test.put(key, value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //lfu = 4
        for (int i = 0; i < 3; i++){
            String key = "key" + i;
            String value = "value"+i;
            try {
                System.out.println("Add "+ i);
                kvm = test.put(key, value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //knock out 4
        try {
            System.out.println("Add 5");
            test.put("key5","value5");
            test.put("key5","value6");
            test.get("key3");
            test.put("key3", "uhmama");
            test.put("key3","");
            test.get("key5");

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Cache: ");
        for(KeyValueLFU kv : test.cache){
            System.out.println(kv.key + " "+kv.value + " " + kv.getUseCount());
        }


    }


*/


}
