package de.tum.i13;

import de.tum.i13.server.ecs.ECSServer;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.shared.Metadata;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdvancedTesting {
    public String doRequest(Socket s, String req) throws IOException, InterruptedException {
        PrintWriter output = new PrintWriter(s.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
        long before = System.currentTimeMillis();
        output.write(req + "\r\n");
        output.flush();
        TimeUnit.SECONDS.sleep(2);
        String res = input.readLine();
        long after = System.currentTimeMillis();
        System.out.println("Time of execution: " + (after - before) + " Milliseconds.");
        return res;
    }

    public void doKeyrange(Socket s) throws IOException, InterruptedException {
        PrintWriter output = new PrintWriter(s.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
        long before = System.currentTimeMillis();
        output.write("keyrange" + "\r\n");
        output.flush();
        TimeUnit.SECONDS.sleep(2);
        String in;
        //while ((in = input.readLine())!=null) {
            System.out.println(input.readLine());
      //  }
        long after = System.currentTimeMillis();
        System.out.println("Time of execution: " + (after - before) + " Milliseconds.");
    }
    public void doKeyrangeRead(Socket s) throws IOException, InterruptedException {
        PrintWriter output = new PrintWriter(s.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
        long before = System.currentTimeMillis();
        output.write("keyrange_read" + "\r\n");
        output.flush();
        TimeUnit.SECONDS.sleep(2);
        String in;
        //while ((in = input.readLine())!=null) {
        List<Metadata> metadata = Metadata.deserializeKeyrange(input.readLine());
        Object[][] table = new String[metadata.size()][];
        int e = 0;
        for (Metadata md : metadata){
            table[e] = Metadata.toStringArray(md);
            e++;
        }
        System.out.format("%-10s%-10s%-34s%-34s\n", "address", "port", "to", "from");
        for (int i = 0; i < table.length; i++) {
            Object[] row = table[i];
            System.out.format("%-10s%-10s%-34s%-34s\n", row[0], row[1], row[2], row[3]);
        }
        //  }
        long after = System.currentTimeMillis();
        System.out.println("Time of execution: " + (after - before) + " Milliseconds.");
    }


    public String readFirstLine(Socket s) throws IOException {
        String res = "";
        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
        res = input.readLine();
        return res;
    }

    public String doRequest(String req, int port) throws IOException, InterruptedException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", port));
        String res = doRequest(s, req);

        return res;
    }

    public void createServer(int port) {
        Thread server = new Thread() {
            @Override
            public void run() {
                try {
                    KVServer.main(new String[]{"-p", String.valueOf(port),"-b" + "127.0.0.1:1234"});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        server.start();
    }

    @Test
    public void test() throws InterruptedException, IOException {
        //Create ECS
        Thread ecs = new Thread() {
            @Override
            public void run() {
                try {
                    ECSServer.main(new String[]{"-p1234"});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        ecs.start();
        Thread.sleep(1000);

        //Create 5 KV Servers
        int i = 0;
        while (i != 5) {
            createServer(5090 + i);
            Thread.sleep(5000);
            i++;

        }
        //Create 10 clients
        int z = 0;
        int y = 0;

        while (z != 10) {
            if (y==4) {
                y=0;
            }
            Socket s = new Socket();
            s.connect(new InetSocketAddress("127.0.0.1", 5090 + y));
            System.out.println(readFirstLine(s));
            String temp = doRequest(s, "put " + z+y + " test");
            System.out.println(temp);
            assertTrue(temp.equals("put_success "+z+y)||temp.equals("server_not_responsible"));
            s.close();
            z++;
            y++;
        }
        Socket temp = new Socket();
        temp.connect(new InetSocketAddress("127.0.0.1", 5090));
        System.out.println(readFirstLine(temp));
        doKeyrangeRead(temp);




    }

}
