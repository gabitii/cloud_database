package de.tum.i13.server.kv;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

/**
 *This class provides one Subscriber object, which connects to the listener server and keeps all subscriptions of that listener
 */
public class Subscriber {
    private int subscriberPort;
    private final ConnectionHandleThread thread;
    private final String sid;
    private InetAddress host;
    private List<Subscription> subscriptionList;
    private Socket socket;
    private PrintWriter out;

    public Subscriber(int subscriberPort, InetAddress host, String sid, String key, ConnectionHandleThread thread) throws IOException {
        this.subscriberPort = subscriberPort;
        this.sid = sid;
        this.host = host;
        this.thread = thread;
        this.subscriptionList = new LinkedList<Subscription>();
        this.subscriptionList.add(new Subscription(sid,key, this));
        socket = new Socket(host,subscriberPort);
        out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    public InetAddress getHost() {
        return host;
    }

    public int getSubscriberPort() {
        return subscriberPort;
    }


    public void addSubscription(String sid,String key) {
        this.subscriptionList.add(new Subscription(sid,key, this));
    }

    public List<Subscription> getSubscriptionList() {
        return subscriptionList;
    }

    public String getSid() {
        return sid;
    }

    /**
     * Sends a message to listener
     * @param s - message
     */
    public void send(String s){
        out.write(s + "\r\n");
        out.flush();
    }

    /**
     * Sends a notify to listener
     * @param s - message
     */
    public void sendNotify(String s) throws IOException {
        out.write(s+"\r\n");
        out.flush();
    }

    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append("ID: " + sid);
        for (Subscription sub : subscriptionList) {
            s.append("KV: " + sub.getKeyValue().getKey() + sub.getKeyValue().getValue());
        }
        return s.toString();
    }

    /**
     * disconnects the subscriber from listener
     * @throws IOException
     */
    public void disconnect() throws IOException {
        this.out.close();
        this.socket.close();
    }
}

