package de.tum.i13.client;

import de.tum.i13.server.ecs.ConnectionHandleThread;
import de.tum.i13.shared.Metadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static de.tum.i13.client.Command.EMPTY;
import static de.tum.i13.client.Command.QUIT;
import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * EchoClient in Java.
 * Should connect to "clouddatabases.msrg.in.tum.de 5551"
 * version = Milestone
 *
 * @author Gabit Saygutdinov, Aleksandr Minakov, Florian Messmer
 * @version 1.0
 */
public class Client {
    private final ApplicationHandler applicationHandler;
    private ConnectionHandler connectionHandler;
    private final static Logger LOGGER = Logger.getLogger(Client.class.getName());
    private String host;

    public Boolean getConnected() {
        return isConnected;
    }

    public void setConnected(Boolean connected) {
        isConnected = connected;
    }

    private Boolean isConnected;

    private List<Metadata> metadata;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    private int port;


    public Client() {
        this.applicationHandler = new ApplicationHandler();
        this.host = "";
        this.port = 0;
        this.isConnected = false;
        this.metadata = new ArrayList<>();
    }
/*
    Wird wahrsch nicht gebraucht
    public Metadata parseMetadata(String line) {
        String[] split = line.split("%");
        Metadata meta = new Metadata();
        meta.setAddress(split[0]);
        meta.setPort(Integer.parseInt(split[1]));
        meta.setServerHash(split[2]);
        meta.setRange(split[3]);
        return meta;
    }

 */

    /**
     * Main program.
     *
     * @param args command line parameter
     */
    public static void main(String[] args) throws IOException {
        setupLogging(Paths.get("test.log"));
        BufferedReader in;
        ServerThread serverThread = null;
        String rawInput;
        Client client = new Client();
        in = new BufferedReader(new InputStreamReader(System.in));
        outer:
        while (true) {


            System.out.print("EchoClient> ");
            LOGGER.info("Reading new user input");
            //reading input
            rawInput = in.readLine();


            try {
                LOGGER.info("Parsing user input");
                String argument = "";
                try {
                    argument = client.applicationHandler.parse(rawInput);
                } catch (NumberFormatException n) {
                    System.out.println("Could not parse your Entry for port. Please check if number format.");
                }

                do {
                    LOGGER.info("Inferring application logic from input command");
                    switch (client.applicationHandler.getCommand()) {
                        case QUIT:
                            System.out.println("Quitting the application...");
                            LOGGER.info("Checking for active connection");
                            if (client.getConnected()) {
                                client.connectionHandler.disconnect();
                            }
                            LOGGER.info("Quitting application...");
                            System.exit(0);
                            break outer;
                        case HELP:
                            System.out.println("EchoClient> This client serves as a storage client and offers the following Commands:");
                            System.out.println("EchoClient> connect <address> <port>: Connects the client to the given address and port.");
                            System.out.println("EchoClient> disconnect: Tries to disconnect from the connected server");
                            System.out.println("EchoClient> put <key> <value>: Stores the given key value pair on the remote server.");
                            System.out.println("EchoClient> put <key>: Deletes the key value pair.");
                            System.out.println("EchoClient> get <key>: Retrieves the value for the given key from the remote server.");
                            System.out.println("EchoClient> logLevel: Sets the logger to the specified log level.");
                            System.out.println("EchoClient> help: Displays the help text");
                            System.out.println("EchoClient> quit: Terminates the active connection and exits the program.");
                            System.out.println("EchoClient> keyrange: requests the current keyrange.");
                            System.out.println("EchoClient> keyrange_read: requests the current readable keyranges");
                            System.out.println("EchoClient> subscribe <sid> <key> <port>: Subscribe a client with a unique sid to a key with a given port");
                            System.out.println("EchoClient> publish <key> <value>: Publish a value to a given key");
                            System.out.println("EchoClient> unsubscribe <sid> <key>: Unsubscribe to the topic key with a unique subscriber ID sid");
                            client.applicationHandler.setCommand(EMPTY);
                            continue outer;
                        case CONNECT:
                            if (client.applicationHandler.getPort() != -1 && rawInput.trim().split("\\s+").length == 3) {
                                if (!client.isConnected) {
                                    client.setHost(client.applicationHandler.getHost());
                                    client.setPort(client.applicationHandler.getPort());
                                    LOGGER.info("EchoClient> Setting up new connection via handler.");
                                    try {
                                        client.connectionHandler = new ConnectionHandler(client.getHost(), client.getPort());
                                        client.applicationHandler.setIsConnected(true);
                                        client.setConnected(true);
                                      /*  serverThread = new ServerThread();
                                        serverThread.addSocket(client.connectionHandler.getSocket());
                                        serverThread.setConnectionHandler(client.connectionHandler);
                                        serverThread.start(); */
                                    } catch (UnknownHostException e) {
                                        System.out.println("Unknown Host. Could not connect");
                                    } catch (IOException u) {
                                        System.out.println("IOException at setting up connection");
                                    }

                                    TimeUnit.SECONDS.sleep(1);
                                    LOGGER.info("Reading welcome message from remote.");
                                    String s = "";
                                    try {
                                        s = client.connectionHandler.receive();
                                    } catch (IOException i) {
                                        System.out.println("IOException while reading message from remote.");
                                    }
                                    System.out.println(s);
                                } else {
                                    System.out.println("Already connected to " + client.getHost() + client.getPort());
                                }
                            } else {
                                System.out.println("Invalid number of arguments!");
                            }
                            continue outer;
                        case LOGLEVEL:

                            String logLevel = argument.toLowerCase();
                            switch (logLevel) {
                                case "all":
                                    LOGGER.setLevel(Level.ALL);
                                    System.out.println("EchoClient> logLevel set to ALL");

                                    continue outer;
                                case "config":
                                    LOGGER.setLevel(Level.CONFIG);
                                    System.out.println("EchoClient> logLevel set to CONFIG");
                                    continue outer;
                                case "fine":
                                    LOGGER.setLevel(Level.FINE);
                                    System.out.println("EchoClient> logLevel set to FINE");

                                    continue outer;
                                case "finest":
                                    LOGGER.setLevel(Level.FINEST);
                                    System.out.println("EchoClient> logLevel set to FINEST");

                                    continue outer;
                                case "info":
                                    LOGGER.setLevel(Level.INFO);
                                    System.out.println("EchoClient> logLevel set to INFO");

                                    continue outer;
                                case "off":
                                    LOGGER.setLevel(Level.OFF);
                                    System.out.println("EchoClient> logLevel set to OFF");

                                    continue outer;
                                case "severe":
                                    LOGGER.setLevel(Level.SEVERE);
                                    System.out.println("EchoClient> logLevel set to SEVERE");

                                    continue outer;
                                case "warning":
                                    LOGGER.setLevel(Level.WARNING);
                                    System.out.println("EchoClient> logLevel set to WARNING");
                                    continue outer;
                                default:
                                    System.out.println("EchoClient> Illegal Argument for LogLevel. Please enter another command");
                                    continue outer;
                            }
                        case DISCONNECT:
                            if (!client.isConnected) {
                                System.out.println("EchoClient> Client is already disconnected.");
                            } else {
                                String hostDisc = client.getHost();
                                int portDisc = client.getPort();
                             //   serverThread.setStopped(true);
                                LOGGER.info("Disconnecting from remote.");
                                try {
                                    client.connectionHandler.disconnect();
                                } catch (IOException | NullPointerException i) {
                                    System.out.println("Error while disconnecting.");
                                }
                                System.out.println("EchoClient> Disconnected from host " + hostDisc + " on port " + portDisc);
                                client.setConnected(false);
                                client.applicationHandler.setIsConnected(false);

                            }
                            continue outer;
                        case GET:
                            if (!client.isConnected) {
                                System.out.println("EchoClient> Please connect to a server first.");
                            } else {
                                if (rawInput.equals("METADATA_TRANSMISSION_START")) {
                                    List<Metadata> newMeta = Metadata.receiveMetadata(in);
                                    client.metadata = newMeta;
                                } else {
                                    LOGGER.info("Requested the value of " + client.applicationHandler.getKey());
                                    try {
                                        client.connectionHandler.setRequestKey(client.applicationHandler.getKey());
                                        client.connectionHandler.send("get " + client.applicationHandler.getKey());
                                    } catch (IOException i) {
                                        System.out.println("IOException while trying to send message");
                                    } catch (NullPointerException n) {
                                        System.out.println("NullPointerException while trying to send message");
                                    }
                                    TimeUnit.SECONDS.sleep(1);


                                    String answer = "";
                                    try {
                                        answer = client.connectionHandler.receiveGet();
                                    } catch (IOException e) {
                                        System.out.println("IOException while trying to receive message.");
                                    }
                                    System.out.println("EchoClient> " + answer);

                                    if(answer.equals("server_not_responsible\r\n")){

                                        System.out.println("EchoClient> requesting keyrange...");
                                        LOGGER.info("Requesting the key range of the current connected server");
                                        try {
                                            client.connectionHandler.send("keyrange");
                                        } catch (IOException e) {
                                            System.out.println("IOException while trying to send message");
                                        } catch (NullPointerException e) {
                                            System.out.println("NullPointerException while trying to send message");
                                        }
                                        TimeUnit.SECONDS.sleep(2);
                                        String keyrange = "";
                                        try {
                                            keyrange = client.connectionHandler.receive();
                                            System.out.println("EchoClient> " + keyrange);
                                            List<Metadata> newMeta = Metadata.deserializeKeyrange(keyrange);
                                            client.metadata = newMeta;
                                            for (Metadata md: client.metadata) {
                                                System.out.println(md.getServerHash() + " "+md.getRange());
                                            }
                                        } catch (IOException e) {
                                            System.out.println("IOException while trying to receive message.");
                                        }
                                        Metadata m = new Metadata();
                                        for(Metadata md : client.metadata){
                                            if(md.isResponsible(client.applicationHandler.getKey())){
                                                m = md;
                                                break;
                                            }
                                        }
                                        System.out.println("EchoClient> Please connect to responsible server:"+m.getAddress()+":"+m.getPort()+"...");

                                    }

                                }
                            }
                            continue outer;
                        case PUT:
                            if (!client.isConnected) {
                                System.out.println("EchoClient> Please connect to a server first.");
                            } else {
                                if (rawInput.equals("METADATA_TRANSMISSION_START")) {
                                    List<Metadata> newMeta = Metadata.receiveMetadata(in);
                                    client.metadata = newMeta;
                                } else {
                                    LOGGER.info("Putting a new data with key " + client.applicationHandler.getKey() + " and value " + client.applicationHandler.getValue());
                                    try {
                                        client.connectionHandler.setRequestKey(client.applicationHandler.getKey());
                                        client.connectionHandler.sendKey("put " + client.applicationHandler.getKey() + " " + client.applicationHandler.getValue());
                                    } catch (IOException i) {
                                        System.out.println("IOException while trying to send message");
                                    } catch (NullPointerException n) {
                                        System.out.println("NullPointerException while trying to send message");
                                    }
                                    TimeUnit.SECONDS.sleep(1);

                                    String answer = "";
                                    try {
                                        answer = client.connectionHandler.receive();
                                    } catch (IOException e) {
                                        System.out.println("IOException while trying to receive message.");
                                    }
                                    System.out.println("EchoClient> " + answer);
                                    if(answer.equals("server_not_responsible")){
                                        System.out.println("EchoClient> requesting keyrange...");
                                        LOGGER.info("Requesting the key range of the current connected server");
                                        try {
                                            client.connectionHandler.send("keyrange");
                                        } catch (IOException e) {
                                            System.out.println("IOException while trying to send message");
                                        } catch (NullPointerException e) {
                                            System.out.println("NullPointerException while trying to send message");
                                        }
                                        TimeUnit.SECONDS.sleep(2);
                                        String keyrange = "";
                                        try {
                                            keyrange = client.connectionHandler.receive();
                                            System.out.println("EchoClient> " + keyrange);
                                            List<Metadata> newMeta = Metadata.deserializeKeyrange(keyrange);
                                            client.metadata = newMeta;
                                            for (Metadata md: client.metadata) {
                                                System.out.println(md.getServerHash() + " "+md.getRange());
                                            }
                                        } catch (IOException e) {
                                            System.out.println("IOException while trying to receive message.");
                                        }
                                        Metadata m = new Metadata();
                                        String key = Metadata.hashIt(client.applicationHandler.getKey());
                                        System.out.println(key);
                                        for(Metadata md : client.metadata){
                                            if(md.isResponsible(key)){
                                                m = md;
                                                break;
                                            }
                                        }
                                        System.out.println("EchoClient> Connecting to responsible server:"+m.getAddress()+":"+m.getPort()+"...");
                                        client.connectionHandler.disconnect();
                                        client.setPort(m.getPort());
                                        client.setHost(m.getAddress());
                                        client.connectionHandler = new ConnectionHandler(client.getHost(), client.getPort());
                                        LOGGER.info("Requested the value of " + client.applicationHandler.getKey());
                                        LOGGER.info("Putting a new data with key " + client.applicationHandler.getKey() + " and value " + client.applicationHandler.getValue());
                                        try {
                                            client.connectionHandler.sendKey("put " + client.applicationHandler.getKey() + " " + client.applicationHandler.getValue());
                                        } catch (IOException i) {
                                            System.out.println("IOException while trying to send message");
                                        } catch (NullPointerException n) {
                                            System.out.println("NullPointerException while trying to send message");
                                        }
                                        TimeUnit.SECONDS.sleep(1);
                                        String answer_two = "";
                                        try {
                                            answer_two = client.connectionHandler.receive();
                                        } catch (IOException e) {
                                            System.out.println("IOException while trying to receive message.");
                                        }
                                        System.out.println("EchoClient> " + answer_two);



                                    }


                                }


                            }
                            continue outer;


                        case KEYRANGE:
                            if (!client.isConnected) {
                                System.out.println("EchoClient> Please connect to a server first.");
                            } else {
                                LOGGER.info("Requesting the key range of the current connected server");
                                try {
                                    client.connectionHandler.send("keyrange");
                                } catch (IOException e) {
                                    System.out.println("IOException while trying to send message");
                                } catch (NullPointerException e) {
                                    System.out.println("NullPointerException while trying to send message");
                                }
                                TimeUnit.SECONDS.sleep(2);

                                String answer = "";
                                try {
                                    answer = client.connectionHandler.receive();
                                    System.out.println("EchoClient> " + answer);
                                    List<Metadata> newMeta = Metadata.deserializeKeyrange(answer);
                                    client.metadata = newMeta;
                                } catch (IOException e) {
                                    System.out.println("IOException while trying to receive message.");
                                }

                            }
                            continue outer;
                        case KEYRANGE_READ:
                            if (!client.isConnected) {
                                System.out.println("EchoClient> Please connect to a server first.");
                            } else {
                                LOGGER.info("Requesting the keyrange_read of the current connected server");
                                try {
                                    client.connectionHandler.send("keyrange_read");
                                } catch (IOException e) {
                                    System.out.println("IOException while trying to send message");
                                } catch (NullPointerException e) {
                                    System.out.println("NullPointerException while trying to send message");
                                }
                                TimeUnit.SECONDS.sleep(2);

                                TimeUnit.SECONDS.sleep(2);
                                String answer = "";
                                try {
                                    answer = client.connectionHandler.receive();
                                    System.out.println("EchoClient> " + answer);
                                } catch (IOException e) {
                                    System.out.println("IOException while trying to receive message.");
                                }
                            }
                            continue outer;

                        case EMPTY:
                            continue outer;

                        case SUBSCRIBE:
                            if (!client.isConnected) {
                            System.out.println("EchoClient> Please connect to a server first.");
                        } else {
                                try {
                                    client.connectionHandler.setRequestKey(client.applicationHandler.getKey());
                                    //System.out.println("subscribe "+client.applicationHandler.getsId()+" "+client.applicationHandler.getKey()+" "+ client.applicationHandler.getPort());
                                    client.connectionHandler.send("subscribe "+client.applicationHandler.getsId()+" "+client.applicationHandler.getKey()+" "+ client.applicationHandler.getPort());
                                } catch (IOException e) {
                                    System.out.println("IOException while trying to send message");
                                } catch (NullPointerException e) {
                                    System.out.println("NullPointerException while trying to send message");
                                }
                                TimeUnit.SECONDS.sleep(1);

                                String answer = "";
                                try {
                                    answer = client.connectionHandler.receive();
                                } catch (IOException e) {
                                    System.out.println("IOException while trying to receive message.");
                                }
                                System.out.println("EchoClient> " + answer);

                                if(answer.equals("server_not_responsible")){

                                    System.out.println("EchoClient> requesting keyrange...");
                                    LOGGER.info("Requesting the key range of the current connected server");
                                    try {
                                        client.connectionHandler.send("keyrange");
                                    } catch (IOException e) {
                                        System.out.println("IOException while trying to send message");
                                    } catch (NullPointerException e) {
                                        System.out.println("NullPointerException while trying to send message");
                                    }
                                    TimeUnit.SECONDS.sleep(2);
                                    String keyrange = "";
                                    try {
                                        keyrange = client.connectionHandler.receive();
                                        System.out.println("EchoClient> " + keyrange);
                                        List<Metadata> newMeta = Metadata.deserializeKeyrange(keyrange);
                                        client.metadata = newMeta;
                                        for (Metadata md: client.metadata) {
                                            System.out.println(md.getServerHash() + " "+md.getRange());
                                        }
                                    } catch (IOException e) {
                                        System.out.println("IOException while trying to receive message.");
                                    }
                                    Metadata m = new Metadata();
                                    for(Metadata md : client.metadata){
                                        if(md.isResponsible(client.applicationHandler.getKey())){
                                            m = md;
                                            break;
                                        }
                                    }
                                    System.out.println("EchoClient> Please connect to responsible server:"+m.getAddress()+":"+m.getPort()+"...");

                                }

                            }
                            continue outer;

                        case UNSUBSCRIBE:
                            if (!client.isConnected) {
                                System.out.println("EchoClient> Please connect to a server first.");
                            } else {
                                try {
                                    client.connectionHandler.setRequestKey(client.applicationHandler.getKey());
                                    client.connectionHandler.send("unsubscribe "+client.applicationHandler.getsId()+" "+client.applicationHandler.getKey());
                                } catch (IOException e) {
                                    System.out.println("IOException while trying to send message");
                                } catch (NullPointerException e) {
                                    System.out.println("NullPointerException while trying to send message");
                                }
                                TimeUnit.SECONDS.sleep(1);
                                String answer = "";
                                try {
                                    answer = client.connectionHandler.receive();
                                } catch (IOException e) {
                                    System.out.println("IOException while trying to receive message.");
                                }
                                System.out.println("EchoClient> " + answer);

                                if(answer.equals("server_not_responsible")){

                                    System.out.println("EchoClient> requesting keyrange...");
                                    LOGGER.info("Requesting the key range of the current connected server");
                                    try {
                                        client.connectionHandler.send("keyrange");
                                    } catch (IOException e) {
                                        System.out.println("IOException while trying to send message");
                                    } catch (NullPointerException e) {
                                        System.out.println("NullPointerException while trying to send message");
                                    }
                                    TimeUnit.SECONDS.sleep(2);
                                    String keyrange = "";
                                    try {
                                        keyrange = client.connectionHandler.receive();
                                        System.out.println("EchoClient> " + keyrange);
                                        List<Metadata> newMeta = Metadata.deserializeKeyrange(keyrange);
                                        client.metadata = newMeta;
                                        for (Metadata md: client.metadata) {
                                            System.out.println(md.getServerHash() + " "+md.getRange());
                                        }
                                    } catch (IOException e) {
                                        System.out.println("IOException while trying to receive message.");
                                    }
                                    Metadata m = new Metadata();
                                    for(Metadata md : client.metadata){
                                        if(md.isResponsible(client.applicationHandler.getKey())){
                                            m = md;
                                            break;
                                        }
                                    }
                                    System.out.println("EchoClient> Please connect to responsible server:"+m.getAddress()+":"+m.getPort()+"...");
                                }
                            }
                            continue outer;

                        case PUBLISH:
                            if (!client.isConnected) {
                                System.out.println("EchoClient> Please connect to a server first.");
                            } else {
                                try {
                                    client.connectionHandler.setRequestKey(client.applicationHandler.getKey());
                                    //System.out.println("publish "+client.applicationHandler.getKey()+" "+client.applicationHandler.getValue());
                                    client.connectionHandler.send("publish "+client.applicationHandler.getKey()+" "+client.applicationHandler.getValue());
                                } catch (IOException e) {
                                    System.out.println("IOException while trying to send message");
                                } catch (NullPointerException e) {
                                    System.out.println("NullPointerException while trying to send message");
                                }
                                TimeUnit.SECONDS.sleep(1);

                                String answer = "";
                                try {
                                    answer = client.connectionHandler.receive();
                                } catch (IOException e) {
                                    System.out.println("IOException while trying to receive message.");
                                }
                                System.out.println("EchoClient> " + answer);

                                if(answer.equals("server_not_responsible")){

                                    System.out.println("EchoClient> requesting keyrange...");
                                    LOGGER.info("Requesting the key range of the current connected server");
                                    try {
                                        client.connectionHandler.send("keyrange");
                                    } catch (IOException e) {
                                        System.out.println("IOException while trying to send message");
                                    } catch (NullPointerException e) {
                                        System.out.println("NullPointerException while trying to send message");
                                    }
                                    TimeUnit.SECONDS.sleep(2);
                                    String keyrange = "";
                                    try {
                                        keyrange = client.connectionHandler.receive();
                                        System.out.println("EchoClient> " + keyrange);
                                        List<Metadata> newMeta = Metadata.deserializeKeyrange(keyrange);
                                        client.metadata = newMeta;
                                        for (Metadata md: client.metadata) {
                                            System.out.println(md.getServerHash() + " "+md.getRange());
                                        }
                                    } catch (IOException e) {
                                        System.out.println("IOException while trying to receive message.");
                                    }
                                    Metadata m = new Metadata();
                                    for(Metadata md : client.metadata){
                                        if(md.isResponsible(client.applicationHandler.getKey())){
                                            m = md;
                                            break;
                                        }
                                    }
                                    System.out.println("EchoClient> Please connect to responsible server:"+m.getAddress()+":"+m.getPort()+"..."); }
                            }
                            continue outer;

                        default:
                            throw new IllegalStateException("EchoClient> Unexpected value: " + client.applicationHandler.getCommand());
                    }

                } while (client.applicationHandler.getCommand() != QUIT);

            } catch (Exception e) {
                LOGGER.throwing(Client.class.getName(), "main", e);
            }
        }
    }
}
