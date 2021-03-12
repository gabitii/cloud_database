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

/*
For more information please read testing report
 */


public class PerformanceTestingThree {


    public String doRequest(Socket s, String req) throws IOException, InterruptedException {
        PrintWriter output = new PrintWriter(s.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
        long before = System.currentTimeMillis();
        output.write(req + "\r\n");
        output.flush();
        String res = input.readLine();
        long after = System.currentTimeMillis();
        System.out.println("Time of execution: " + (after - before) + " Milliseconds.");
        return res;
    }

    public String doRequestLoad(Socket s, int x, int client, List<Metadata> metadataList) throws IOException, InterruptedException {
        s.connect(new InetSocketAddress("127.0.0.1", metadataList.get(0).getPort()));
        PrintWriter output = new PrintWriter(s.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
        input.readLine();
        long before = System.currentTimeMillis();
        for (int i = 0; i <= x; i++) {
            String msg = "put key" + (i+1) * client + " value" + (i+1) * client;
            System.out.println("putting key:" + i+1*client);
            output.write(msg + "\r\n");
            output.flush();
            String res1 = input.readLine();
            String res2 = input.readLine();
            while (!res1.contains("put_success")&& !res1.contains("put_update")) {
                System.out.println("Entering loop");
                Metadata m = new Metadata();
                for (Metadata md : metadataList) {
                    if (md.isResponsible("key" + (i+1) * client)) {
                        m = md;
                        break;
                    }
                }
                s.close();
                s = new Socket();
                s.connect(new InetSocketAddress("127.0.0.1", m.getPort()));
                System.out.println("Reconnect to: " + m.getPort());
                output = new PrintWriter(s.getOutputStream());
                input = new BufferedReader(new InputStreamReader(s.getInputStream()));
                input.readLine();
                output.write(msg + "\r\n");
                output.flush();
                res1 = input.readLine();
                System.out.println("Exiting loop if res1: " + res1);
                res2 = input.readLine();

            }
        }
        long after = System.currentTimeMillis();
        String time = Long.toString((after - before));
        return "Load time: " + time + "ms";
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


    @Test
    public void testOneServer() throws IOException, InterruptedException {
//Start ECS
        Thread ecs = new Thread() {
            @Override
            public void run() {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        ECSServer.main(new String[]{"-p" + 1234});
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        ecs.start();

        Thread.sleep(1000);
//Start server 1
        Thread server1 = new Thread() {
            @Override
            public void run() {

                try {
                    KVServer.main(new String[]{"-p" + 5090, "-b" + "127.0.0.1:1234"});
                    Thread.sleep(1000);

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                }
            }
        };
        server1.start();
        Thread.sleep(2000);
/*
        Thread server2 = new Thread() {
            @Override
            public void run() {

                try {
                    KVServer.main(new String[]{"-p" + 5091, "-b" + "127.0.0.1:1234"});
                    Thread.sleep(1000);

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                }
            }
        };
        server2.start();
        Thread.sleep(1000);

        Thread server3 = new Thread() {
            @Override
            public void run() {

                try {
                    KVServer.main(new String[]{"-p" + 5092, "-b" + "127.0.0.1:1234"});
                    Thread.sleep(1000);

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                }
            }
        };
        server3.start();
        Thread.sleep(5000);

 */


        //create 2 sockets
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", 5090));
        PrintWriter output = new PrintWriter(s.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
        System.out.println("Expect welcome 1: " + input.readLine());
        Thread.sleep(2000);

        //get Metadata
        System.out.println("Debug1");
        output.write("keyrange" + "\r\n");
        output.flush();
        System.out.println("Debug2");
        Thread.sleep(2000);
        String keyrange = input.readLine();
        System.out.println(keyrange);
        System.out.println("Debug3");
        List<Metadata> metadata = Metadata.deserializeKeyrange(keyrange);
        s.close();

        Thread socket1 = new Thread(){
            @Override
            public void run() {
                try {
                    Socket s = new Socket();
                    String resp1 = doRequestLoad(s, 25, 1, metadata);
                    System.out.println(resp1);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        Thread socket2 = new Thread(){
            @Override
            public void run() {
                try {
                    Socket s = new Socket();
                    String resp1 = doRequestLoad(s, 25, 2, metadata);
                    System.out.println(resp1);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        //let them make 100 put requests each
        socket1.start();
        socket2.start();
       TimeUnit.SECONDS.sleep(120);


    }
    /*
    @Test
    public void testTwoServers() throws IOException, InterruptedException {

        Thread ecs = new Thread() {
            @Override
            public void run() {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        ECSServer.main(new String[]{"-p" + 1234}); }
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        ecs.start();

        Thread.sleep(1000);

        Thread server1 = new Thread() {
            @Override
            public void run() {

                try {
                    KVServer.main(new String[]{"-p" + 5090, "-b" + "127.0.0.1:1234"});
                    Thread.sleep(1000);

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                }
            }
        };
        server1.start();
        Thread.sleep(2000);


        Socket s10 = new Socket();
        s10.connect(new InetSocketAddress("127.0.0.1", 5090));
        System.out.println(readFirstLine(s10));
        System.out.println("Putting original value at server 1");
        String resp1 = doRequest(s10, "put 1 value");
        assertEquals("put_success 1", resp1);
        Thread.sleep(1000);



        Thread server2 = new Thread() {
            @Override
            public void run() {
                try {
                    KVServer.main(new String[]{"-p" + 5091, "-b" + "127.0.0.1:1234"});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        server2.start();
        server2.setPriority(7);
        Thread.sleep(6000);

        Thread server3 = new Thread() {
            @Override
            public void run() {
                try {
                    KVServer.main(new String[]{"-p" + 5092, "-b" + "127.0.0.1:1234"});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        server3.start();
        server3.setPriority(8);
        Thread.sleep(6000);


        String resp12 = doRequest(s10, "get 1");
        System.out.println("Reading original value from server 1");
        assertEquals("get_success 1 value", resp12);


        Socket s12 = new Socket();
        s12.connect(new InetSocketAddress("127.0.0.1", 5091));
        System.out.println("Reading original value from server 2");
        System.out.println(readFirstLine(s12));
        String resp13 = doRequest(s12, "get 1");
        assertEquals("get_success 1 value", resp13);

        Socket s13 = new Socket();
        s13.connect(new InetSocketAddress("127.0.0.1", 5092));
        System.out.println(readFirstLine(s13));
        System.out.println("Reading original value from server 3");
        String resp14 = doRequest(s13, "get 1");
        assertEquals("get_success 1 value", resp14);


        System.out.println("Updating original value at server 2");
        System.out.println(doRequest(s12, "put 1 value_updated"));
        Thread.sleep(5000);

        //assertEquals("put_update 1", resp15);

        System.out.println("Reading updated value from server 1");
        String resp16 = doRequest(s10, "get 1");
        assertEquals("get_success 1 value_updated", resp16);

        doKeyrange(s12);










    }

     */
}
