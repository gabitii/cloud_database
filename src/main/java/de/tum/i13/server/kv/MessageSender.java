package de.tum.i13.server.kv;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Metadata;

import java.io.*;
import java.util.List;

/**
 * This class sends messages to the clients via outputStream of Socket
 */
public class MessageSender {
    private PrintWriter out;


    /**
     * The constructor for MessageSender which gets outputStream and creates own PrintWriter
     * @param outputStream is used to create PrintWriter
     * @throws IOException for input/output exception
     */
    public MessageSender(OutputStream outputStream) throws IOException {
        this.out = new PrintWriter(new OutputStreamWriter(outputStream, Constants.TELNET_ENCODING));
    }

    /**
     * This method sends message to the client
     * @param msg message that is to send
     */
    public void send(String msg){
        out.write(msg+"\r\n");
        out.flush();
    }

    /**
     * This method closes the Printwriter
     */
    public void disconnect(){
        this.out.close();
    }

    /**
     * Sends the answer for keyrange request
     * @param metadata to determine keyrange
     */
    public void sendKeyrange(List<Metadata> metadata){
        send(Metadata.keyrange(metadata));
    }


    /**
     * Sends the answer for keyrange_read request
     * @param metadata to determine keyrange
     */
    public void sendKeyrangeRead(List<Metadata> metadata){
        send(Metadata.keyrange_read(metadata,out));
    }

}
