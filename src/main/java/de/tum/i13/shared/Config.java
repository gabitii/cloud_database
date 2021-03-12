package de.tum.i13.shared;

//import jdk.jfr.internal.LogLevel;
import picocli.CommandLine;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.*;

public class Config {
    public static Logger LOGGER = Logger.getLogger(Config.class.getName());

    //TODO: -r Befehl
    @CommandLine.Option(names = "-p", description = "sets the port of the server", defaultValue = "5153")
    public int port;

    @CommandLine.Option(names = "-a", description = "which address the server should listen to", defaultValue = "127.0.0.1")
    public String listenaddr;

    @CommandLine.Option(names = "-b", description = "bootstrap broker where clients and other brokers connect first to retrieve configuration, port and ip, e.g., 192.168.1.1:5153", defaultValue = "clouddatabases.i13.in.tum.de:5153")
    public InetSocketAddress bootstrap;

    @CommandLine.Option(names = "-d", description = "Directory for files", defaultValue = "data")
    public Path dataDir;

    @CommandLine.Option(names = "-l", description = "Logfile", defaultValue = "echo.log")
    public Path logfile;

    @CommandLine.Option(names = "-ll", description = "Loglevel", defaultValue = "ALL")
    public String logLevel;

    @CommandLine.Option(names = "-c", description = "cache size", defaultValue = "2")
    public int cache;

    @CommandLine.Option(names = "-s", description = "Cache displacement strategy, FIFO, LRU, LFU ", defaultValue = "FIFO")
    public String strategy;

    @CommandLine.Option(names = "-h", description = "Displays help", usageHelp = true)
    public boolean usagehelp;

    @CommandLine.Option(names = "-r", description = "retention time", defaultValue = "10")
    public long retentionTime;


    public static Config parseCommandlineArgs(String[] args) {
        LogSetup.setupLogging(Paths.get("test.log"));
        Config cfg = new Config();
        CommandLine.ParseResult parseResult = new CommandLine(cfg).registerConverter(InetSocketAddress.class, new InetSocketAddressTypeConverter()).parseArgs(args);
        if(!Files.exists(cfg.dataDir)) {
            try {
                Files.createDirectory(cfg.dataDir);
            } catch (IOException e) {
                System.out.println("Could not create directory");
                e.printStackTrace();
                System.exit(-1);
            }
        }

        if(!parseResult.errors().isEmpty()) {
            for(Exception ex : parseResult.errors()) {
                ex.printStackTrace();
            }

            CommandLine.usage(new Config(), System.out);
            System.exit(-1);
        }
        cfg.setLogging(cfg.logLevel);

        return cfg;
    }

    private void setLogging(String level) {
        String logLevel = level.toLowerCase();
        switch (logLevel) {
            case "all":
                this.LOGGER.setLevel(Level.ALL);
                break;
            case "config":
                this.LOGGER.setLevel(Level.CONFIG);
                break;
            case "fine":
                this.LOGGER.setLevel(Level.FINE);
                break;
            case "finest":
                this.LOGGER.setLevel(Level.FINEST);
                break;
            case "info":
                this.LOGGER.setLevel(Level.INFO);
                break;
            case "off":
                this.LOGGER.setLevel(Level.OFF);
                break;
            case "severe":
                this.LOGGER.setLevel(Level.SEVERE);
                break;
            case "warning":
                this.LOGGER.setLevel(Level.WARNING);
                break;
            default:
                this.LOGGER.setLevel(Level.ALL);
                break;
        }
    }

    @Override
    public String toString() {
        return "Config{" +
                "port=" + port +
                ", listenaddr='" + listenaddr + '\'' +
                ", bootstrap=" + bootstrap +
                ", dataDir=" + dataDir +
                ", logfile=" + logfile +
                ", Loglevel=" + LOGGER.getLevel()+
                ", cache=" + cache+
                ", Strategy=" + strategy+
                ", usagehelp=" + usagehelp +
                '}';
    }

    /*
    public static void main(String[] args) {
        String msg = "-p 5153 -a 127.0.01 -ll SEVERE -c 100 -h false";

        Config config = Config.parseCommandlineArgs(msg.split(" "));
        System.out.println(config.toString());
    }

     */
}

