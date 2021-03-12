package de.tum.i13.server.kv;

import java.util.LinkedList;
import java.util.List;

/**
 * lass to represent a subscription object
 */
public class Subscription {

    //value always empty!
    private KeyValue kv;
    private long lastSent;
    final Subscriber master;
    private String sid;

    public Subscription(String sid, String key, Subscriber master) {
        this.sid = sid;
        this.kv = new KeyValue(key,"");
        this.lastSent = 0;
        this.master = master;
    }

    public String getSid() {
        return sid;
    }

    public Subscription(KeyValue kv, Subscriber master) {
        this.kv = kv;
        this.lastSent = 0;
        this.master = master;
    }

    public KeyValue getKeyValue() {
        return kv;
    }

    public long getLastSent() {
        return lastSent;
    }

    public void setLastSent(long lastSent) {
        this.lastSent = lastSent;
    }

    public Subscriber getMaster() {
        return master;
    }
}