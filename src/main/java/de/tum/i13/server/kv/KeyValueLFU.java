package de.tum.i13.server.kv;

public class KeyValueLFU extends KeyValue {

    public int getUseCount() {
        return useCount;
    }

    public void proxyUseCount(){
        this.useCount = 1000000000;
    }

    public void increaseUseCount() {
        this.useCount++;
    }
    public void halfCount(){
        this.useCount = (this.useCount/2)+1;
    }
    private int useCount;

    public KeyValueLFU(String key, String value){
        super(key,value);
        this.useCount = 0;

    }
}
