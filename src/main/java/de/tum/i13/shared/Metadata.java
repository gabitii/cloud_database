package de.tum.i13.shared;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the Metadata of ONE KV Server
 */
public class Metadata {
    private String address;
    private int port;
    private String serverHash;
    private int ringPosition;
    private boolean isLonely;
    private String backupOne;
    private String backupTwo;
    private String coordinatorOne;
    private String coordinatorTwo;

    public String getCoordinatorOne() {
        return coordinatorOne;
    }

    public void setCoordinatorOne(String coordinatorOne) {
        this.coordinatorOne = coordinatorOne;
    }

    public String getCoordinatorTwo() {
        return coordinatorTwo;
    }

    public void setCoordinatorTwo(String coordinatorTwo) {
        this.coordinatorTwo = coordinatorTwo;
    }

    public String getBackupOne() {
        return backupOne;
    }

    public void setBackupOne(String backupOne) {
        this.backupOne = backupOne;
    }

    public String getBackupTwo() {
        return backupTwo;
    }

    public void setBackupTwo(String backupTwo) {
        this.backupTwo = backupTwo;
    }

    public boolean isLonely() {
        return isLonely;
    }

    public void setLonely(boolean lonely) {
        isLonely = lonely;
    }

    public int getRingPosition() {
        return ringPosition;
    }

    public void setRingPosition(int ringPosition) {
        this.ringPosition = ringPosition;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getServerHash() {
        return serverHash;
    }

    public void setServerHash(String serverHash) {
        this.serverHash = serverHash;
    }

    public String getRange() {
        return range;
    }

    public void setRange(String range) {
        this.range = range;
    }

    private String range;

    public static String hashIt(String s) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        md.update(s.getBytes());
        byte[] bytes = md.digest();
        StringBuffer hashtext = new StringBuffer();
        for (int i = 0; i < bytes.length; i++) {
            hashtext.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
        }
        return hashtext.toString();
    }

    public Metadata() {
    }

    public Metadata(String address, int port) {
        this.address = address;
        this.port = port;
        String s = address + ":" + port;
        this.serverHash = hashIt(s);
        this.range = "";
        this.ringPosition = -1;

    }



    public boolean isResponsible(String key) {
        Boolean isResponsible = false;
        //Hashing
        String hashKey = hashIt(key);
        if (this.isLonely()) {
            return true;
        } else if (hashKey.equals(serverHash)) {
            return true;
        } else if (isBigger(serverHash, range)) {
            if (isBigger(hashKey, range) && isBigger(serverHash, hashKey)) {
                return true;
            } else {
                return false;
            }
        } else {
            if (isBigger(hashKey, serverHash) && isBigger(range, hashKey)) {
                return false;
            } else {
                return true;
            }
        }
    }

    /**
     * returns true if first byte[] represents larger number, false if equal or smaller
     *
     * @param one
     * @param two
     * @return
     */
    public static boolean isBigger(String one, String two) {

        for (int i = 0; i < one.length(); i += 2) {
            String o = one.substring(i, i + 2);
            String t = two.substring(i, i + 2);
            if (Long.parseLong(o, 16) > Long.parseLong(t, 16)) {
                return true;
            } else if (Long.parseLong(o, 16) < Long.parseLong(t, 16)) {
                return false;
            }
        }
        return false;
    }


    public void calculateRange(String hash) {
        StringBuilder sb = new StringBuilder();
        sb.append(hash.toLowerCase());
        int i = 32;
        while (i > 0) {
            if (sb.toString().equals("00000000000000000000000000000000")) {
                this.range = "00000000000000000000000000000010";
                return;
            } else if (sb.toString().equals("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")) {
                this.range = "00000000000000000000000000000000";
                return;
            }

            if (!(sb.substring(i - 2, i).equals("ff"))) {
                StringBuilder temp = new StringBuilder();
                temp.append(sb.substring(0, i - 2));
                long l = Long.parseLong(sb.substring(i - 2, i), 16);
                l++;
                if (l < 16) {
                    temp.append("0");
                }
                temp.append(Long.toHexString(l));
                if (i < 31) {
                    temp.append(sb.substring(i, sb.length()));
                }
                sb = temp;
                if (l == 16) {
                    i -= 2;
                } else {
                    break;
                }

            } else {
                while (i > 0 && sb.substring(i - 2, i).equals("ff")) {
                    StringBuilder temp = new StringBuilder();
                    temp.append(sb.substring(0, i - 2));
                    temp.append("00");
                    if (i < 31) {
                        temp.append(sb.substring(i, sb.length()));
                    }
                    sb = temp;
                    i -= 2;
                }
                StringBuilder temp = new StringBuilder();
                temp.append(sb.substring(0, i - 2));
                long l = Long.parseLong(sb.substring(i - 2, i), 16);
                l++;
                if (l < 16) {
                    temp.append("0");
                }
                temp.append(Long.toHexString(l));
                if (i < 31) {
                    temp.append(sb.substring(i, sb.length()));
                }
                sb = temp;
                break;
            }
        }
        this.setRange(sb.toString());
    }


    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public static Metadata parseMetadata(String line) {
        String[] split = line.split("@@@");
        Metadata meta = new Metadata();
        meta.setAddress(split[0].toString());
        meta.setPort(Integer.valueOf(split[1].toString()));
        meta.setServerHash(split[2].toString());
        meta.setRange(split[3].toString());
        meta.setLonely(Boolean.parseBoolean(split[4].toString()));
        if(!split[5].equals("null")){
            meta.setBackupOne(split[5]);
        }
        else {
            meta.setBackupOne(null);
        }
        if(!split[6].equals("null")){
            meta.setBackupTwo(split[6]);
        }
        else{
            meta.setBackupTwo(null);
        }
        if(!split[7].equals("null")){
            meta.setCoordinatorOne(split[7]);
        }
        else {
            meta.setCoordinatorOne(null);
        }
        if(!split[8].equals("null")){
            meta.setCoordinatorTwo(split[8]);
        }
        else {
            meta.setCoordinatorTwo(null);
        }
        return meta;
    }

    public static List<Metadata> receiveMetadata(BufferedReader in) throws IOException {
        String nextLine;
        List<Metadata> md = new ArrayList<Metadata>();
        while (true) {
            nextLine = in.readLine();
            if(!nextLine.equals("METADATA_TRANSMISSION_END") && !nextLine.equals("METADATA_TRANSMISSION_START")){
                String [] metadata = nextLine.split("@@@@");
                for (int i = 0; i < metadata.length; i++) {
                    md.add(parseMetadata(metadata[i]));
                }
            }
            if (nextLine.equals("METADATA_TRANSMISSION_END")) {
                break;
            }
        }
        return md;
    }
    public static List<Metadata> deserializeKeyrange(String keyrange) {
        List<Metadata> md = new ArrayList<Metadata>();
        String[] ranges = keyrange.split(";");
       ranges[0] =  ranges[0].replace("keyrange_success ","");
       ranges[0] = ranges[0].replace("keyrange_read_success ","");
        for (String r : ranges){
            String[] elements = r.split(",");
            Metadata m = new Metadata();
            m.setRange(elements[0]);
            m.setServerHash(elements[1]);
            String[] address = elements[2].split(":");
            m.setAddress(address[0]);
            m.setPort(Integer.parseInt(address[1]));
            md.add(m);
        }
        return md;
    }

    public static String[] toStringArray(Metadata md) {
        String[] represent = new String[4];
        represent[0] = md.getAddress();
        represent[1] = String.valueOf(md.getPort());
        represent[2] = md.getRange();
        represent[3] = md.getServerHash();
        return represent;
    }

    public static void sendMetadata(List<Metadata> metadata, PrintWriter out) {
        out.write("METADATA_TRANSMISSION_START" + "\r\n");
        out.flush();
        StringBuilder sb = new StringBuilder();
        for (Metadata m : metadata) {
            sb.append(m.getAddress() + "@@@" + m.getPort() + "@@@" + m.getServerHash() + "@@@" + m.getRange()
                      + "@@@" + m.isLonely()  + "@@@" + m.getBackupOne() + "@@@" + m.getBackupTwo() + "@@@"
                      + m.getCoordinatorOne() + "@@@" + m.getCoordinatorTwo() + "@@@@");
        }
        out.write(sb.toString()+"\r\n");
        out.write("METADATA_TRANSMISSION_END" + "\r\n");
        out.flush();
    }

    public static String keyrange(List<Metadata> metadata) {

        StringBuilder sb = new StringBuilder();
        sb.append("keyrange_success ");
        for (Metadata m : metadata) {
            sb.append(m.getRange() + "," + m.getServerHash() + "," + m.getAddress() + ":" + m.getPort() + ";");
        }
        return sb.toString();
    }

    public static String keyrange_read(List<Metadata> metadata, PrintWriter out){
        if(metadata.size()<3) {
            StringBuilder sb = new StringBuilder();
            sb.append("keyrange_read_success ");
            for (Metadata m : metadata) {
                sb.append(m.getRange() + "," + m.getServerHash() + "," + m.getAddress() + ":" + m.getPort() + ";");
            }
            return sb.toString();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("keyrange_read_success ");
        for (int i = 0; i<metadata.size();i++) {
            Metadata coordinator = metadata.get(i);
            Metadata repOne = Metadata.getByHash(metadata, coordinator.backupOne);
            Metadata repTwo = Metadata.getByHash(metadata, coordinator.backupTwo);
            sb.append(coordinator.getRange() + "," + coordinator.getServerHash() + "," + coordinator.getAddress() + ":" + coordinator.getPort() + ";");
            sb.append(coordinator.getRange() + "," + coordinator.getServerHash() + "," + repOne.getAddress() + ":" + repOne.getPort() + ";");
            sb.append(coordinator.getRange() + "," + coordinator.getServerHash() + "," + repTwo.getAddress() + ":" + repTwo.getPort() + ";");
        }
        return sb.toString();
    }

    public static boolean equals(Metadata mone, Metadata mtwo) {
        String one = mone.getServerHash();
        String two = mtwo.getServerHash();
        for (int i = 0; i < one.length(); i += 2) {
            String o = one.substring(i, i + 2);
            String t = two.substring(i, i + 2);
            if (Long.parseLong(o, 16) != Long.parseLong(t, 16)) {
                return false;
            }
        }
        return true;
    }

    public static Metadata getByHash(List<Metadata> metadata, String hash){
        for (Metadata md: metadata){
            if(md.getServerHash().equals(hash)){
                return md;
            }
        }
        return null;
    }



    /*
    public static void main(String[] args) {
        Metadata metadata = new Metadata();
        metadata.setServerHash("ceff029fefd729308fcfb38e41e4cc0d");
        metadata.setRange("6e03cd40b4d47a62919294d23ae3f2c9");
        System.out.println(metadata.isResponsible("FoBqbTPRyH"));
    }*/
}


