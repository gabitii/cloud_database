package de.tum.i13.server.kv;

import java.io.*;

import java.nio.file.Path;

import java.util.*;

import static de.tum.i13.shared.Config.LOGGER;

public class FileHandler {
    File file;
    private boolean isReplica;
    private int numRep;


    /**
     * Sets up an instance of FileHandler
     * @param path a relative path to the directory in which the persistent storage is created in
     * @param isReplica whether it is a replica or not
     * @param numRep whether it is a first or a second replica (write 0 if not a replica)
     * @param port for naming convetions
     */
    public FileHandler(Path path, int port,  boolean isReplica, int numRep) {
        if (isReplica) {
            this.file = new File(String.valueOf(path) + "/" + port + "_replica_" + numRep);
        } else {
            this.file = new File(String.valueOf(path) + "/" + port + "_ownData");
        }

        if (file.exists()) {
            LOGGER.info("Persistent storage found. ");
        } else {
            try {
                this.file.createNewFile();
                BufferedWriter writer = new BufferedWriter(new FileWriter(this.file));
                writer.write("0");
                LOGGER.info("Persistent storage created");

            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    /**
     * Accessory function to clean a String read from the storage
     * @param line the String to be parsed
     * @return new Key Value object with key and value
     */
    public KeyValue cleanString(String line) {
        String[] split = line.split("@@@");
        String[] clean = new String[2];
        clean[0] = "";
        clean[1] = "";
        int c = 0;
        for(String s : split){
            clean[c] = s;
            c++;
        }
        return new KeyValue(clean[0], clean[1]);
    }

    /**
     * Function to retrieve all elements from storage and create objects from them
     * @return A list of all persistently stored Key-Value-Pairs
     */
    public List<KeyValue> deserialize() {
        List<KeyValue> keyValues = new ArrayList<KeyValue>();
        try {
            FileInputStream fis = new FileInputStream(this.file);
            Scanner sc = new Scanner(fis);
            while (sc.hasNextLine()) {

                KeyValue kv = cleanString(sc.nextLine());
                keyValues.add(kv);
            }
            sc.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return keyValues;
    }

    /**
     * returns 0 if false, returns int index if true
     * @param keyValues List of Key-Value-Objects to be searched
     * @param kv single Key-Value-Pair
     * @return 0 if kv is not in keyValues; index of element if kv is in keyValues
     */
    public int hasKey(List<KeyValue> keyValues, KeyValue kv) {
        int element = 0;
        for (KeyValue k : keyValues) {
            element++;
            if (k.key.equals(kv.key)) {
                return element;
            }
        }
        return 0;
    }

    /**
     * Retrieves a single key value pair from storage
     * @param key the key of the key value pair to be retrieved
     * @return null if element not available, the key-value-object if available
     */
    public KeyValue readFromDisk(String key) {
        KeyValue result = null;
        try {
            List<KeyValue> keyValues = deserialize();
            if(keyValues.size()>0) {
                for (KeyValue kv : keyValues) {
                    if (kv.key.equals(key)) {
                        result = kv;
                    }
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Writes a key-value-pair to the persistent storage
     * @param kv the key-value-pair to be stored
     */
    public void writeToDisc(KeyValue kv) {
        //deserialize elements
        List<KeyValue> keyValues = deserialize();

        //empty file
        try {
           FileWriter writer = new FileWriter(this.file);
            writer.write("");
        } catch (IOException e) {
            e.printStackTrace();
        }

        //search for key
        int index = hasKey(keyValues, kv);
        if (index > 0) {
            //update
            KeyValue existing = keyValues.get(index - 1);
            existing.value = kv.value;
        } else {
            //add to List
            keyValues.add(kv);

        }
        //serialize elements
        try {
            serialize(keyValues);
        } catch (IOException i) {
            System.out.println("Could not serialize objects");
        }

    }

    /**
     * writes a collection of key-values to storage
     * @param queue collection of Key-value-objects
     */
    public void writeAll(Queue<KeyValue> queue){
        List<KeyValue> keyValues = deserialize();
        try {
            FileWriter writer = new FileWriter(this.file);
            writer.write("");
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(KeyValue kv : queue){
            if(hasKey(keyValues, kv) == 0){
                keyValues.add(kv);
            }
        }
        try {
            serialize(keyValues);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Stores key-Value-Objects to file
     * @param keyValues list of key-values to be written
     * @throws IOException if writing fails
     */
    public void serialize(List<KeyValue> keyValues) throws IOException {
        FileOutputStream fos = new FileOutputStream(this.file);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos));

        for (KeyValue kv : keyValues) {
            writer.write(kv.key + "@@@" + kv.value);
            writer.newLine();
        }
        writer.close();
    }

    /**
     * takes a single entry and deletes it
     * @param key of the key-value pair to be deleted
     */
    public void deleteFromDisk(String key) {
        List<KeyValue> keyValues = deserialize();
        for (KeyValue kv1: keyValues) {
            if (kv1.getKey().equals(key)) {
                keyValues.remove(keyValues.indexOf(kv1));
                break;
            }
        }
        try {
            serialize(keyValues);
        } catch (IOException i) {
            System.out.println("Could not serialize objects");
        }

    }

    /**
     * This method makes one String out of list of keyvalues
     * @param list with keyvalues
     * @return String
     */
    public static String compressData(List<KeyValue> list){
        StringBuilder sb = new StringBuilder();
        for(KeyValue kv:list){
            sb.append(kv.getKey() + "@@@" + kv.getValue() + "@@@@");
        }
        return sb.toString();
    }

    /**
     * This method makeslist of keyvalues out of one String
     * @param compressedData string which should be parsed
     * @return ArrayList list of KeyValues
     */
    public static ArrayList<KeyValue> parseData(String compressedData){
        ArrayList result = new ArrayList();
        String[] arr = compressedData.split("@@@@");
        for (String s : arr) {
            String[] kv = s.split("@@@");
            KeyValue keyValue = new KeyValue(kv[0],kv[1]);
            result.add(keyValue);
        }
        return result;
    }
    

}
