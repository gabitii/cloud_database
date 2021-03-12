package de.tum.i13.server.kv;

/**
 * class represents one publication with according attributes
 */
public class Publication {
    private final long timestamp;
    private final KeyValue keyValue;


    public Publication(long timestamp, KeyValue keyValue) {
        this.timestamp = timestamp;
        this.keyValue = keyValue;
    }


    public long getTimestamp() {
        return timestamp;
    }

    public KeyValue getKeyValue() {
        return keyValue;
    }



}
