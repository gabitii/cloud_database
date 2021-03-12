package de.tum.i13.client;

import de.tum.i13.shared.Metadata;
import org.w3c.dom.ls.LSOutput;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.SocketException;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Thread created to read messages from server parallel to other Client processes
 */
public class ServerThread extends Thread {
    private Socket clientSocket;
    private BufferedReader in;
    private String key;
    private String request;
    private boolean isStopped;
    private boolean isWaiting = false;
    private Client client;
    private boolean socketChanged = false;
    private boolean toRetry = false;
    private ConnectionHandler connectionHandler;
    private final static Logger LOGGER = Logger.getLogger(Client.class.getName());



    public ServerThread() throws IOException {
    }

    public void setSocketChanged(boolean isChanged) {
        this.socketChanged = isChanged;
    }

    public void setToRetry(boolean toRetry) {
        this.toRetry = toRetry;
    }

    public void setRequest(String request) {
        this.request = request;
    }


    public void setClient(Client client) {
        this.client = client;
    }

    public void setWaiting(boolean isWaiting) {
        this.isWaiting = isWaiting;
    }

    public void setConnectionHandler(ConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }
    public void setStopped(boolean isStopped) {
        this.isStopped = isStopped;
    }

    public void addSocket(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    public void run() {
        socketChanged = true;

        while (true) {


            if (isStopped) {
                break;
            }

            if (socketChanged) {
                try {
                    this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                socketChanged = false;
            }

            try {
                String message = null;

                try {
                    if (!isStopped) {
                    message = in.readLine(); }
                } catch (IOException e) {

                }
                if (!message.equals("")) {

                    if (message.startsWith("keyrange_success")) {
                        if (this.toRetry) {
                            String keyrange = "";
                            keyrange = message;
                            List<Metadata> newMeta = Metadata.deserializeKeyrange(keyrange);
                            for (Metadata md: newMeta) {
                                System.out.println(md.getServerHash() + " "+md.getRange());
                            }
                            Metadata m = new Metadata();
                            for(Metadata md : newMeta){
                                if(md.isResponsible(key)){
                                    m = md;
                                    break;
                                }
                            }
                            System.out.println("EchoClient> Please connect to responsible server: "+m.getAddress()+":"+m.getPort()+".");
                            this.setToRetry(false);
                        }
                        else {
                            System.out.println("EchoClient> " + message);
                        }
                        TimeUnit.SECONDS.sleep(2);
                    }
                   else if (message.startsWith("get_success")) {

                        String[] msg = message
                                .trim()
                                .split(" ");
                        byte[] decoded = Base64.getDecoder().decode(msg[2]);
                        String value = "";
                        for (byte b : decoded) {
                            char c = (char) b;
                            value += c;
                        }
                        msg[2] = value;
                        String result = "";
                        for (String str : msg) {
                            result += str + " ";
                        }
                        message = result;
                        System.out.println("EchoClient> " + message);
                    }
                    else if (message.equals("server_not_responsible")) {
                        System.out.println("EchoClient> " + message);
                        System.out.println("EchoClient> requesting keyrange...");
                        this.setToRetry(true);
                        try {
                            connectionHandler.send("keyrange");
                        } catch (IOException e) {
                            System.out.println("IOException while trying to send message");
                        } catch (NullPointerException e) {
                            System.out.println("NullPointerException while trying to send message");
                        }
                        TimeUnit.SECONDS.sleep(2);
                    }
                    else {
                    System.out.println("EchoClient> " + message); }

                    /*  else if (message.equals("server_not_responsible")) {
                        System.out.println("EchoClient> " + message);
                        System.out.println("EchoClient> requesting keyrange...");
                        LOGGER.info("Requesting the key range of the current connected server");
                        try {
                            connectionHandler.send("keyrange");
                            this.setToRetry(true);
                        } catch (IOException e) {
                            System.out.println("IOException while trying to send message");
                        } catch (NullPointerException e) {
                            System.out.println("NullPointerException while trying to send message");
                        }
                        try {
                            TimeUnit.SECONDS.sleep(2);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } else if (message.startsWith("keyrange_success")) {
                        if (this.toRetry) {
                            String keyrange = "";
                            try {
                                System.out.println("EchoClient> " + message);
                                List<Metadata> newMeta = Metadata.deserializeKeyrange(message);
                                for (Metadata md : newMeta) {
                                    System.out.println(md.getServerHash() + " " + md.getRange());
                                }

                                Metadata m = new Metadata();
                                for (Metadata md : newMeta) {
                                    if (md.isResponsible(this.request)) {
                                        m = md;
                                        break;
                                    }
                                }
                                System.out.println("EchoClient> Connecting to responsible server:"+m.getAddress()+":"+m.getPort()+"...");
                                connectionHandler.disconnect();
                                client.setPort(m.getPort());
                                client.setHost(m.getAddress());
                                connectionHandler = new ConnectionHandler(client.getHost(), client.getPort());
                                this.addSocket(connectionHandler.getSocket());
                                this.setSocketChanged(true);
                                LOGGER.info("Requested the value of " + client.applicationHandler.getKey());
                                LOGGER.info("Putting a new data with key " + client.applicationHandler.getKey() + " and value " + client.applicationHandler.getValue());


                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }


                    }  */
                }


            } catch (Exception e) {

            }
        }

    }
    }
