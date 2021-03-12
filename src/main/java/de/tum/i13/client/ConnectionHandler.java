package de.tum.i13.client;

import de.tum.i13.shared.Metadata;

import java.io.*;
import java.lang.reflect.Member;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Base64;
import java.util.List;


/**
 * Connection Handler is created to establish  the connection
 * between client and server. Also it is responsible for
 * receiving, unmarshalling and decrypting as well as
 * for sending, marshalling and crypting messages
 */
public class ConnectionHandler {
    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private String requestKey;
    private boolean isConnected;

    /**
     * The constructor gets parameters host and port and uses them to connect to the server
     * It also sets up streams needed for sending and receiving messages
     * @param host parameter host identifies host to which the client has to be connected
     * @param port identifies port
     * @throws IOException for input/output error
     * @throws UnknownHostException for incompatible host name
     */
    public ConnectionHandler(String host, int port) throws IOException, UnknownHostException{
        socket = new Socket(host, port);
        in = socket.getInputStream();
        out = socket.getOutputStream();
        isConnected = true;
    }

    public Socket getSocket(){
        return this.socket;
    }

    public String getRequestKey() {
        return requestKey;
    }

    public void setRequestKey(String requestKey) {
        this.requestKey = requestKey;
    }

    /**
     * Disconnects the client from the server
     * @throws IOException for input/output error
     */
    public void disconnect() throws IOException{
        if (isConnected) {
            out.close();
            in.close();
            socket.close();
            isConnected = false;
        }
    }

    /**
     * Reads all bytes coming from server,saving them to the byte array,
     * and  then unmarshalls it to string
     * @return message as a string
     * @throws IOException for input/output error
     */
    public synchronized String receive() throws IOException {
        String s = this.unmarshall(in, this.in.available());
        return s.toString().replace("\r\n", "");
    }

    public synchronized List<Metadata> receiveMetadata() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(this.in));
        reader.readLine();
        List<Metadata> msg = Metadata.receiveMetadata(reader);
        return msg;
    }

    /**
     * Method receives a key with encrypted value and decrypts it
     * @return decrypted String
     * @throws IOException for input/output error
     */
    public synchronized String receiveGet() throws IOException{
        String s = this.unmarshall(this.in, in.available());
        if(s.startsWith("get_error")){
            return s;
        }
        if(s.startsWith("server_not_responsible")){
            return s;
        }
        String [] msg =s.trim().split(" ");
        byte[] decoded = Base64.getDecoder().decode(msg[2]);
        String value = "";
        for(byte b: decoded){
            char c = (char)b;
            value+=c;
        }
        msg[2] = value;
        String result = "";
        for(String str: msg){
            result+=str + " ";
        }

        return result.trim();
    }

    /**
     * Marshalls Sends the message the server
     * @param msg message that has to be sent to the server
     * @throws IOException for input/output error
     */
    public void send(String msg) throws IOException {
        String s = msg + "\r\n";
        out.write(marshall(s));
        out.flush();
    }

    /**
     * Marshalls an Object of type String to the byte array
     * @param s String that has to be marshalled
     * @return byte array from String
     */
    private byte[] marshall(String s) {
        //byte[] encoded = Base64.getEncoder().encode(s.getBytes());
        return s.getBytes();
    }

    /**
     * Method unmarshalls a byte array to the Object of type String
     * @param in = Input stream
     * @param size = number of bytes
     * @return unmarshalled String
     * @throws IOException for input/output error
     */
    private String unmarshall(InputStream in, int size) throws IOException {
        byte[] bytes = new byte[size];
        for(int i = 0; i <bytes.length; i++) {
            bytes[i] = (byte) this.in.read();
        }
        //byte[] decoded = Base64.getDecoder().decode(bytes);
        StringBuilder s = new StringBuilder();
        for (byte b : bytes) {
            char c = (char) b;
            s.append(c);
        }
        return s.toString();
    }

    /**
     * method sends the key after put-command, encrypting the value
     * @param msg = the message, which contains the value
     * @throws IOException for input/output error
     */
    public void sendKey(String msg) throws IOException {
        String[] s = msg.trim().split(" ");
        if(s.length<3){
            this.send(msg);
            return;
        }
        s[2] = Base64.getEncoder().encodeToString(s[2].getBytes());
        String result= "";
        for (String str : s){
            result+=str+" ";
        }
        result=result.trim();
        result+="\r\n";
        out.write(result.getBytes());
        out.flush();


    }

}