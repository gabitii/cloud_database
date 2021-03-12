package de.tum.i13.client;





/**
 * Application Handler class is created to read user's input from CLI
 * and to detect a right command and parse the input to the connection handler
 * to send it to a server.
 */

public class ApplicationHandler {


    private Command command;
    private String host;
    private int port;
    private boolean isConnected;
    private String key;
    private String value;
    private String sId;

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    /**
     * Constructor to create an Application Handler object to parse user's input
     */
    public ApplicationHandler() {
        isConnected = false;
        host = "";
        port = 0;
    }

    /**
     * Sets a command enum to detect a correct request
     * @param command enum
     */
    public void setCommand(Command command) {
        this.command = command;
    }

    /**
     * @return host name
     */
    public String getHost() {
        return host;
    }

    /**
     * @return port name as Integer
     */
    public int getPort() {
        return port;
    }

    public Command getCommand() {
        return command;
    }

    /**
     * Method to detect avoid double connection to NOT change host and port in this object
     *
     * @param in boolean variable to set a connection to true or false
     */
    public void setIsConnected(boolean in) {
        isConnected = in;
    }


    public String getsId() {
        return sId;
    }

    public void setsId(String sId) {
        this.sId = sId;
    }

    /**
     * Method to parse user's input and detect a command to set an enum for a Client object to do the right step.
     * The output is also saved as a String which the Client object then receives.
     *
     * @param input as user's input from command line
     * @return parsed input to send to the server (e.g. message) or post/host
     * @throws NumberFormatException for incompatible port description
     *
     * For enums description
     * @see Command class
     */
    public String parse(String input) throws NumberFormatException {

        String output = null;
        String[] cleanInput;
        cleanInput = input.trim().split("\\s+");
        cleanInput[0]=cleanInput[0].toLowerCase();

        if (cleanInput[0].equals("connect")) {
            command = Command.CONNECT;
            if (!isConnected) {
                try {
                    host = cleanInput[1];
                    port = Integer.parseInt(cleanInput[2]);
                } catch (IndexOutOfBoundsException e){
                    port = -1;
                }
            }
            output = input.replaceFirst("connect", " ").trim();

        } else if (cleanInput[0].equals("disconnect")) {
            command = Command.DISCONNECT;
        }
         else if (cleanInput[0].equals("loglevel")) {
            command = Command.LOGLEVEL;
            output = cleanInput[1];

        } else if (cleanInput[0].equals("help")) {
            command = Command.HELP;
        } else if (cleanInput[0].equals("quit")) {
            command = Command.QUIT;
        } else if(cleanInput[0].equals("put")){
            if(cleanInput.length==2){
                this.key= cleanInput[1];
                this.value= "";
                command= Command.PUT;
            }
            else {
                this.key = cleanInput[1];
                this.value = "";
                for (int i=2; i<cleanInput.length; i++) {
                this.value+=cleanInput[i] + " ";
                }
                this.value = this.value.trim();
                command = Command.PUT;
            }
        } else if(cleanInput[0].equals("get")){
            if (cleanInput.length!=2) {
                command = Command.HELP;
                return null;
            }
            this.key=cleanInput[1];
            command=Command.GET;

        } else if(cleanInput[0].equals("keyrange")){
            command=Command.KEYRANGE;
        } else if(cleanInput[0].equals("keyrange_read")){
            command=Command.KEYRANGE_READ;
        }
         else if(cleanInput[0].equals("publish")) {
                if (cleanInput.length<3) {
                    command = Command.HELP;
                    return null;
                }
                command=Command.PUBLISH;
                this.key = cleanInput[1];
                this.value = "";
                for (int i=2; i<cleanInput.length; i++) {
                    this.value+=cleanInput[i] + " ";
                }
                this.value = this.value.trim();
            }
         else if(cleanInput[0].equals("unsubscribe")) {
             if (cleanInput.length!=3) {
                 command = Command.HELP;
                 return null;
             }
                command = Command.UNSUBSCRIBE;
                this.sId=cleanInput[1];
                this.key=cleanInput[2];
        }
         else if(cleanInput[0].equals("subscribe")) {
             if (cleanInput.length!=4) {
                 command = Command.HELP;
                 return null;
             }
                 command = Command.SUBSCRIBE;
                 this.sId=cleanInput[1];
                 this.key = cleanInput[2];
                 this.port = Integer.parseInt(cleanInput[3]);
        }

        else {
            command = Command.HELP;
        }
        return output;
    }




}
