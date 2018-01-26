package com.metrics;


import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class Ingester {


    private static ServerSocket server;
    private static ExecutorService executor = Executors.newFixedThreadPool(10);

    // arg_1 -> Host
    // arg_2 -> Port
    // arg_3 -> propertyFilePath
    public static void main(String args[]) throws Exception {
        //create the socket server object
        if (args.length < 2) {
            System.out.println("Please give all args");
            System.out.println("arg_1 -> Host, arg_2 -> Port, arg_3 -> propertyFilePath");
            return;
        }
        server = new ServerSocket(Integer.parseInt(args[1]));
        MetricsGenerator.init(args[2]);
        while (true) {
            System.out.println("Waiting for client request");
            Socket socket = server.accept();
            Future future = executor.submit(new ThreadIngester(socket));
            if (future.isDone() || future.isCancelled()) {
                break;
            }
        }
        System.out.println("Shutting down Socket server!!");
        server.close();
    }

}