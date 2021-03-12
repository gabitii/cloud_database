package de.tum.i13.server.echo;

import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

/*
EchoLogic is responsible for processing commands sent by client, parsing them and eventually preparing an answer to the client
 */
public class EchoLogic implements CommandProcessor {
    public static Logger logger = Logger.getLogger(EchoLogic.class.getName());
    private KVStore kv;

    //TODO: Constructor public EchoLogic(kvStore kv)
    public EchoLogic() {
    }

    public String process(String command) {
        String answer = "";
        logger.info("received request: " + command.trim());
        KVCommandProcessor kvp = new KVCommandProcessor(this.kv);

        //TODO: put, get, errors
        //TODO: put processes here

        return null;



        //return answer; //=results at server side depending on what happened above;
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        logger.info("new connection: " + remoteAddress.toString());

        return "Connection to MSRG Echo server established: " + address.toString() + "\r\n";
    }

    @Override
    public void connectionClosed(InetAddress remoteAddress) {
        logger.info("connection closed: " + remoteAddress.toString());
    }
}
