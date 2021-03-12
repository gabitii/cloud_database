package de.tum.i13.server.ecs;


import de.tum.i13.client.Client;
import de.tum.i13.shared.Metadata;

import java.io.IOException;

import java.net.ServerSocket;
import java.net.Socket;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * This class works as administrator of all existing KV Nodes
 */

public class ECSAdministrator {
    private List<Metadata> metadata;
    private List<ConnectionHandleThread> threads;
    private ServerSocket serverSocket;
    private final static Logger LOGGER = Logger.getLogger(Client.class.getName());

    /**
     * Constructor of ECS administrator
     *
     * @param serverSocket socket of an ECS server
     */

    public ECSAdministrator(ServerSocket serverSocket) {
        this.serverSocket = serverSocket;
        this.threads = new ArrayList<>();
        this.metadata = new ArrayList<>();
    }

    public ECSAdministrator() {
        this.serverSocket = null;
        this.threads = new ArrayList<>();
        this.metadata = new ArrayList<>();
    }

    public List<Metadata> getMetadata() {
        return this.metadata;
    }

    public void bootstrap(Metadata joiner) {
        rebalance(joiner);
    }

    /**
     * Method to adjust positions of KV servers in a custom list
     */

    public void consolidate() {
        LOGGER.info("Consolidating Metadata List");
        List<ConnectionHandleThread> threadsTemp = new ArrayList<>();
        for (Metadata md : this.metadata) {
            md.setRingPosition(this.metadata.indexOf(md));
            LOGGER.info(md.getAddress() + ":" + md.getPort() + " Ringposition: " + this.metadata.indexOf(md));
        }


    }

    /**
     * Method to rebalance storage nodes on the hash ring
     *
     * @param joiner metadata of a joiner server
     */

    private void rebalance(Metadata joiner) {
        //first server is responsible for the whole ring
        if (metadata.size() == 0) {
            LOGGER.info("First Server added to family.");
            joiner.setLonely(true);
            joiner.calculateRange(joiner.getServerHash());
            joiner.setRingPosition(0);
            metadata.add(joiner);
            return;
        } else {
            //no one is lonely anymore
            if (this.metadata.size() >= 1) {
                for (Metadata md : this.metadata) {
                    md.setLonely(false);
                }
            }

            int position = 0;
            //try finding index of joiner so that Sx<joiner<Sy
            for (Metadata md : metadata) {
                if (Metadata.isBigger(joiner.getServerHash(), md.getServerHash()) && !md.equals(joiner)) {
                    position = metadata.indexOf(md) + 1;
                }
            }
            System.out.println("Detected joiner at pos: " + position);
            metadata.add(position, joiner);
            ConnectionHandleThread join = threads.get(threads.size() - 1);
            threads.remove(join);
            threads.add(position, join);


            Metadata predecessor = null;
            if (position == 0) {
                predecessor = metadata.get(metadata.size() - 1);
            } else {
                predecessor = metadata.get(position - 1);
            }
            System.out.println("predecessor is " + predecessor.getServerHash());
            joiner.calculateRange(predecessor.getServerHash());
            consolidate();

            this.doHandoff(predecessor, joiner);
            Metadata antecessor = null;
            int indexAntecessor = joiner.getRingPosition() + 1;
            if (!(joiner.getRingPosition() == this.metadata.size() - 1)) {
                antecessor = metadata.get(joiner.getRingPosition() + 1);
            } else {
                antecessor = metadata.get(0);
            }
            System.out.println("antecessor is " + antecessor.getServerHash());
            antecessor.calculateRange(joiner.getServerHash());

            if (this.metadata.size() >= 3) {
                for (Metadata md : this.metadata) {

                    if (md.getRingPosition() == 0) {//If the server is first in the ring
                        md.setCoordinatorOne(this.metadata.get(metadata.size() - 1).getServerHash());
                        md.setCoordinatorTwo(this.metadata.get(metadata.size() - 2).getServerHash());
                        md.setBackupOne(this.metadata.get(md.getRingPosition() + 1).getServerHash());
                        md.setBackupTwo(this.metadata.get(md.getRingPosition() + 2).getServerHash());
                    } else if (md.getRingPosition() + 1 == this.metadata.size()) {//If the server is the last in the ring
                        md.setCoordinatorOne(this.metadata.get(md.getRingPosition() - 1).getServerHash());
                        md.setCoordinatorTwo(this.metadata.get(md.getRingPosition() - 2).getServerHash());
                        md.setBackupOne(this.metadata.get(0).getServerHash());
                        md.setBackupTwo(this.metadata.get(1).getServerHash());
                    } else if (md.getRingPosition() == 1) {// If the server is second in the ring
                        if (metadata.size() == 3) {
                            md.setCoordinatorOne(this.metadata.get(0).getServerHash());
                            md.setCoordinatorTwo(this.metadata.get(metadata.size() - 1).getServerHash());
                            md.setBackupOne(this.metadata.get(md.getRingPosition() + 1).getServerHash());
                            md.setBackupTwo(this.metadata.get(0).getServerHash());
                        } else {
                            md.setCoordinatorOne(this.metadata.get(0).getServerHash());
                            md.setCoordinatorTwo(this.metadata.get(metadata.size() - 1).getServerHash());
                            md.setBackupOne(this.metadata.get(md.getRingPosition() + 1).getServerHash());
                            md.setBackupTwo(this.metadata.get(md.getRingPosition() + 2).getServerHash());
                        }
                    } else if (md.getRingPosition() + 2 == this.metadata.size()) {
                        if (metadata.size() == 3) {
                            md.setCoordinatorOne(this.metadata.get(md.getRingPosition() - 1).getServerHash());
                            md.setCoordinatorTwo(this.metadata.get(this.metadata.size() - 1).getServerHash());
                            md.setBackupOne(this.metadata.get(this.metadata.size() - 1).getServerHash());
                            md.setBackupTwo(this.metadata.get(0).getServerHash());
                        } else {
                            md.setCoordinatorOne(this.metadata.get(md.getRingPosition() - 1).getServerHash());
                            md.setCoordinatorTwo(this.metadata.get(md.getRingPosition() - 2).getServerHash());
                            md.setBackupOne(this.metadata.get(this.metadata.size() - 1).getServerHash());
                            md.setBackupTwo(this.metadata.get(0).getServerHash());
                        }
                    } else {
                        md.setCoordinatorOne(this.metadata.get(md.getRingPosition() - 1).getServerHash());
                        md.setCoordinatorTwo(this.metadata.get(md.getRingPosition() - 2).getServerHash());
                        md.setBackupOne(this.metadata.get(md.getRingPosition() + 1).getServerHash());
                        md.setBackupTwo(this.metadata.get(md.getRingPosition() + 2).getServerHash());
                    }


                }



            }


            broadcast();


        }
    }

    /**
     * Method to send new metadata to all nodes
     */
    public void broadcast() {
        System.out.println("Broadcasting metadata");
        LOGGER.info("Broadcasting metadata.");
        for (ConnectionHandleThread t : threads) {
            t.setToUpdateMetadata(true);
        }
    }

    /**
     * Data handoff between two nodes (for both joiner and leaver)
     *
     * @param predecessor old node that sends the data
     * @param joiner      new node that joins the ring
     */
    private void doHandoff(Metadata predecessor, Metadata joiner) {
        LOGGER.info("Starting handoff between: " + predecessor.getAddress() + ":" + predecessor.getPort() + " and " + joiner.getAddress() + ":" + joiner.getPort());
        int indexOld = predecessor.getRingPosition();
        threads.get(indexOld).setToHandOff(true);
        threads.get(indexOld).setJoiner(joiner);
    }

    /**
     * Method to disconnect a node
     *
     * @param ownMetadata own metadata of a node
     * @param graceful    whether the shutdown was graceful or not
     * @throws InterruptedException
     */

    public synchronized void disconnect(Metadata ownMetadata, boolean graceful) {


            LOGGER.info("Disconnecting " + ownMetadata.getAddress() + ":" + ownMetadata.getPort() + " graceful: " + graceful);
            //get ringPosition of disconnected server
            int ringPosition = ownMetadata.getRingPosition();


            //delete from metadata List
            for (Metadata md : metadata) {
                if (Metadata.equals(md, ownMetadata)) {
                    metadata.remove(md);
                    break;
                }
            }
            if(ringPosition==metadata.size()){
                ringPosition= metadata.size()-1;
            }/*
            if (graceful) {
                doHandoff(ownMetadata, metadata.get(ringPosition));
            }*/


            //delete from threads List

            ConnectionHandleThread leaver = null;
            for (ConnectionHandleThread ct : threads) {
                if (Metadata.equals(ct.getOwnMetadata(), ownMetadata)) {
                    leaver = ct;
                    threads.remove(ct);
                    break;
                }
            }
            consolidate();
            //last element is responsible for whole ring
            if(!metadata.isEmpty()) {
                if (metadata.size() == 1) {
                    metadata.get(0).calculateRange(metadata.get(0).getServerHash());
                    metadata.get(0).setRingPosition(0);
                    //edge case 2 servers point at each other
                } else if (metadata.size() == 2) {
                    metadata.get(0).calculateRange(metadata.get(1).getServerHash());
                    metadata.get(1).calculateRange(metadata.get(0).getServerHash());
                    //if last element was deleted, first is responsible
                } else if (ringPosition == metadata.size()) {
                    metadata.get(0).setRange(ownMetadata.getRange());
                    //else transfer own range to new element at old place
                } else {
                    //update metadata
                    metadata.get(ringPosition).setRange(ownMetadata.getRange());
                }
            }


            if (metadata.size() < 3) {
                for (Metadata md : metadata) {
                    md.setBackupOne(null);
                    md.setBackupTwo(null);
                    md.setCoordinatorOne(null);
                    md.setCoordinatorTwo(null);
                }
            } else {
                for (Metadata md : this.metadata) {
                    if (md.getRingPosition() == 0) {//If the server is first in the ring
                        md.setCoordinatorOne(this.metadata.get(metadata.size() - 1).getServerHash());
                        md.setCoordinatorTwo(this.metadata.get(metadata.size() - 2).getServerHash());
                        md.setBackupOne(this.metadata.get(md.getRingPosition() + 1).getServerHash());
                        md.setBackupTwo(this.metadata.get(md.getRingPosition() + 2).getServerHash());
                    } else if (md.getRingPosition() + 1 == this.metadata.size()) {//If the server is the last in the ring
                        md.setCoordinatorOne(this.metadata.get(md.getRingPosition() - 1).getServerHash());
                        md.setCoordinatorTwo(this.metadata.get(md.getRingPosition() - 2).getServerHash());
                        md.setBackupOne(this.metadata.get(0).getServerHash());
                        md.setBackupTwo(this.metadata.get(1).getServerHash());
                    } else if (md.getRingPosition() == 1) {// If the server is second in the ring
                        if (metadata.size() == 3) {
                            md.setCoordinatorOne(this.metadata.get(0).getServerHash());
                            md.setCoordinatorTwo(this.metadata.get(metadata.size() - 1).getServerHash());
                            md.setBackupOne(this.metadata.get(md.getRingPosition() + 1).getServerHash());
                            md.setBackupTwo(this.metadata.get(0).getServerHash());
                        } else {
                            md.setCoordinatorOne(this.metadata.get(0).getServerHash());
                            md.setCoordinatorTwo(this.metadata.get(metadata.size() - 1).getServerHash());
                            md.setBackupOne(this.metadata.get(md.getRingPosition() + 1).getServerHash());
                            md.setBackupTwo(this.metadata.get(md.getRingPosition() + 2).getServerHash());
                        }
                    } else if (md.getRingPosition() + 2 == this.metadata.size()) {
                        if (metadata.size() == 3) {
                            md.setCoordinatorOne(this.metadata.get(md.getRingPosition() - 1).getServerHash());
                            md.setCoordinatorTwo(this.metadata.get(this.metadata.size() - 1).getServerHash());
                            md.setBackupOne(this.metadata.get(this.metadata.size() - 1).getServerHash());
                            md.setBackupTwo(this.metadata.get(0).getServerHash());
                        } else {
                            md.setCoordinatorOne(this.metadata.get(md.getRingPosition() - 1).getServerHash());
                            md.setCoordinatorTwo(this.metadata.get(md.getRingPosition() - 2).getServerHash());
                            md.setBackupOne(this.metadata.get(this.metadata.size() - 1).getServerHash());
                            md.setBackupTwo(this.metadata.get(0).getServerHash());
                        }
                    } else {
                        md.setCoordinatorOne(this.metadata.get(md.getRingPosition() - 1).getServerHash());
                        md.setCoordinatorTwo(this.metadata.get(md.getRingPosition() - 2).getServerHash());
                        md.setBackupOne(this.metadata.get(md.getRingPosition() + 1).getServerHash());
                        md.setBackupTwo(this.metadata.get(md.getRingPosition() + 2).getServerHash());
                    }

                }

            }
            //send metadata to all servers
            broadcast();
        leaver.getOut().write("shutdown_safe\r\n");
        leaver.getOut().flush();
        leaver.terminate();
        }


    /**
     * Method to administrate new joining threads
     */

    public void handleServers() {

        List<ConnectionHandleThread> threads = new ArrayList<>();
        while (true) {
            Socket KVSocket = null;
            try {
                KVSocket = serverSocket.accept();
                ConnectionHandleThread connectionHandleThread = null;
                connectionHandleThread = new ConnectionHandleThread(KVSocket);
                connectionHandleThread.setAdministrator(this);
                connectionHandleThread.start();
                this.threads.add(connectionHandleThread);

            } catch (SocketException se) {

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /*public static void main(String[] args) {


        ECSAdministrator ecs = new ECSAdministrator();

        Metadata one = new Metadata("127.0.0.1", 0001);
        Metadata two = new Metadata("127.0.0.1", 0002);
        Metadata three = new Metadata("127.0.0.1", 0003);
        Metadata four = new Metadata("127.0.0.1", 0004);

        ecs.bootstrap(one);
        ecs.bootstrap(two);
        ecs.bootstrap(three);
        ecs.bootstrap(four);
        ecs.disconnect(one, false);
        ecs.disconnect(two, false);
        ecs.disconnect(three, false);
    }*/


}
