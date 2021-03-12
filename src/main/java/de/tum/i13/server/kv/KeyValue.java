package de.tum.i13.server.kv;


/**
 * Class to represent a key value object of a storage system
 */
public class KeyValue {
    public String key;


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String value;



    public KeyValue(String key, String value){
        this.key = key;
        this.value = value;
    }

public String toString(){
        return key + " " +value;
}
}
