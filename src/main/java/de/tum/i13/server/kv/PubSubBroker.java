package de.tum.i13.server.kv;

import de.tum.i13.shared.Metadata;

import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;
import static de.tum.i13.shared.Config.LOGGER;


/**
 * This class manages all publications and subscriptions and sends all notifies
 */
public class PubSubBroker extends Thread {
    private List<Publication> pubList;
    private List<Subscriber> subList;
    private boolean running;
    private long retentionTime;
    private KVStore kvStore;


    public PubSubBroker(long retentionTime, KVStore kvStore) {
        running = true;
        pubList = new LinkedList<Publication>();
        subList = new LinkedList<Subscriber>();
        this.retentionTime = retentionTime;
        this.kvStore = kvStore;
    }

    public List<Subscriber> getSubList() {
        return subList;
    }

    /**
     * public access to add a new Subscription
     *
     * @param
     */
    public void addSubscription(String key, ConnectionHandleThread thread, String sId, int port, InetAddress host) {
        //System.out.println("Adding Subscription: " + sId);
        LOGGER.info("Adding new Subscription " + sId + " " + key + " " + port + host);
        for (Subscriber s : subList) {
            if (s.getSubscriberPort()==port) {
                s.addSubscription(sId,key);
                return;
            }

        }

        try {
            this.subList.add(new Subscriber(port,host, sId, key, thread));
        } catch (IOException e) {
            e.printStackTrace();
        }
      //  System.out.println("Added new Subscriber: ");
        for (Subscriber s : subList) {
            System.out.println(s.toString());
        }
    }

    /**
     * Prepends a new Publication
     *
     * @param newPub: new Publication to be added
     */
    public void addPublication(Publication newPub) {
        LOGGER.info("Adding new publication " + newPub.getKeyValue().toString());
        this.pubList.add(newPub);
       // System.out.println("Added Pub: " + newPub.getKeyValue().getKey());
    }

    /**
     * method deletes subscription
     * @param sid sId of subscription
     * @param key key of subscription
     * @return a boolean if subscription was deleted
     */
    public boolean deleteSubscription(String sid,String key){
        for (Subscriber subscriber : subList) {
            for (int i = 0; i < subscriber.getSubscriptionList().size(); i++) {
                if(subscriber.getSubscriptionList().get(i).getSid().equals(sid) && subscriber.getSubscriptionList().get(i).getKeyValue().getKey().equals(key)){
                    LOGGER.info("Deleting subscription with sId :" + subscriber.getSubscriptionList().get(i).getSid());
                    subscriber.getSubscriptionList().remove(i--);

                    return true;
                }
            }
        }
        return false;
    }

    public void disconnectAllSubscribers() throws IOException{
        for (Subscriber subscriber : this.getSubList()) {
            LOGGER.info("Disconnection all subscribers");
            subscriber.disconnect();
        }
    }


    /**
     * will stop the Thread
     */
    public void kill() {
        try {
            this.disconnectAllSubscribers();
        } catch (IOException e) {
            e.printStackTrace();
        }
        running = false;
    }

    /**
     * checks all Publications for retentionTime (outer loop)
     * then compares the keys of relevant Publications for existing Subscriptions
     *
     * @return: A list of due Subscriptions
     */
    private List<Subscription> checkPubSub() {
        this.checkResponsibilities();
        List<Subscription> dueSubscriptions = new LinkedList<>();
        for (Subscriber subscriber : subList) {
            for (Subscription s : subscriber.getSubscriptionList()) {
                for (int i = 0; i<pubList.size(); i++) {
                    System.out.println(System.currentTimeMillis() - pubList.get(i).getTimestamp());
                    if ((System.currentTimeMillis() -  pubList.get(i).getTimestamp()) <= this.retentionTime) {
                       // System.out.println("Timing is correct");
                        if (pubList.get(i).getKeyValue().getKey().equals(s.getKeyValue().getKey())) {
                           // System.out.println("keys are correct");
                            //s.getKeyValue().setValue(p.getKeyValue().getValue());
                            LOGGER.info("Sending notification " + pubList.get(i).getKeyValue().toString());
                            Subscription temp = new Subscription( pubList.get(i).getKeyValue(), subscriber);
                            temp.setLastSent(s.getLastSent());
                            dueSubscriptions.add(temp);

                        }
                    } else {
                        pubList.remove(i--);
                    }
                }
            }


        }

        return dueSubscriptions;
    }

    /**
     * This method provides a subscription, which is equals to the searched subscription, if it is in the list
     * @param s searched subscription
     * @param subscriber subscriber, which should store it
     * @return subscription from subscriber's list
     */
    private Subscription getReference(Subscription s, Subscriber subscriber) {
        for (Subscription sub: subscriber.getSubscriptionList()) {
            if(sub.getKeyValue().getKey().equals(s.getKeyValue().getKey()))
                return sub;
        }
        return null;
    }

    /**
     * This method checks if the server is still responsible for all subscriptions, which it stores,
     * if the server and all its coordinators are not responsible for server anymore, the subscription is deleted
     */
    private void checkResponsibilities(){
        boolean changed = false;
        for(Subscriber s: this.subList){
            for(int i = 0;i<s.getSubscriptionList().size();i++){//TODO: add coordinators for checking responsibilities
                String key =s.getSubscriptionList().get(i).getKeyValue().getKey();
                if(!this.kvStore.getOwnMetadata().isResponsible(key)
                    && (this.kvStore.getOwnMetadata().getCoordinatorOne()!=null && !Metadata.getByHash(kvStore.getMetadata(),kvStore.getOwnMetadata().getCoordinatorOne()).isResponsible(key))
                    && (this.kvStore.getOwnMetadata().getCoordinatorTwo()!=null && !Metadata.getByHash(kvStore.getMetadata(),kvStore.getOwnMetadata().getCoordinatorTwo()).isResponsible(key))){
                    s.send("The server is not responsible for the key : " + s.getSubscriptionList().get(i).getKeyValue().getKey());
                   // System.out.println("Removing the key :" + s.getSubscriptionList().get(i).getKeyValue().getKey());
                    s.getSubscriptionList().remove(i--);
                    changed=true;
                }
            }
        }
        if (changed) {
            System.out.println("checking responsibilities ended");
        }
    }

    @Override
    public void run() {

        while (running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            List<Subscription> dueSubs = checkPubSub();

            for (Subscription s : dueSubs) {
                KeyValue temp = s.getKeyValue();
                //System.out.println("Last sent "+s.getKeyValue().getValue()+" "+s.getLastSent());
                if(s.getLastSent() == 0||(System.currentTimeMillis() - s.getLastSent()) > retentionTime) {
                    try {
                        //System.out.println("Notifying. Key:" + temp.getKey() + " Value: " +temp.getValue());
                        s.getMaster().sendNotify("notify " + temp.getKey() + " " + temp.getValue());
                        //s.getMaster().send("notify " + temp.getKey() + " " + temp.getValue());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    this.getReference(s,s.getMaster()).setLastSent(System.currentTimeMillis());
                    //s.setLastSent(System.currentTimeMillis());
                }

            }

        }
    }


}


