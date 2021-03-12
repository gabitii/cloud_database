package de.tum.i13.server.kv;

/**
 *
 * The implementation of KVMessage interface
 *
 */
public class KVMessageImpl implements KVMessage {
    private String key;
    private String value;
    private StatusType statusType;


    public KVMessageImpl(String key) {
        this.key = key;
    }

    /**
     * @return key associated to a message
     */
    @Override
    public String getKey() {
        return this.key;
    }
    /**
     * @return value associated to a message
     */
    @Override
    public String getValue() {
        return this.value;
    }
    /**
     * @return status associated to a message
     */
    @Override
    public StatusType getStatus() {
        return statusType;
    }

    /**
     *
     * @param statusType to set a status of a message
     */
    public void setStatus(StatusType statusType) {
        this.statusType = statusType;
    }

    /**
     * @param value to set a value for a message
     */
    public void setValue(String value) {
        this.value = value;
    }
}
