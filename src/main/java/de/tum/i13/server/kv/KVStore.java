package de.tum.i13.server.kv;


import de.tum.i13.shared.Metadata;

import java.util.List;

public interface KVStore {
    public void setRequestMD(boolean value);

    public boolean requestMD();

    public void setBalancing(boolean value);

    public void setStopped(boolean value);

    public String getAddress();

    public int getPort();

    public List<Metadata> getMetadata();

    public Metadata getOwnMetadata();

    public void setMetadata(List<Metadata> metadata);
    //for initialization
    public boolean isStopped();

    // for write lock
    public boolean isBalancing();
    /**
     * Inserts a key-value pair into the KVServer.
     *
     * @param key   the key that identifies the given value.
     * @param value the value that is indexed by the given key.
     * @return a message that confirms the insertion of the tuple or an error.
     * @throws Exception if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    public KVMessage put(String key, String value) throws Exception;


    /**
     * Retrieves the value for a given key from the KVServer.
     *
     * @param key the key that identifies the value.
     * @return the value, which is indexed by the given key.
     * @throws Exception if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    public KVMessage get(String key) throws Exception;

    /**
     * Method to maintain persistence. This methods writes all existing cache entries to disk.
     */
    public void putAll();

    public FileHandler getFileHandler();

}
