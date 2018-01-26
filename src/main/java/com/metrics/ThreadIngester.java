package com.metrics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class ThreadIngester implements Runnable {

    private Socket socket;

    public ThreadIngester(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        System.out.println("Started new ingester thread");
        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String userInput;
            while ((userInput = in.readLine()) != null) {
                MetricsGenerator.ingestStats(userInput);
                if (userInput.equalsIgnoreCase("exit")) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    System.out.println("Closing BufferedReader");
                    in.close();
                    System.out.println("closed BufferedReader");
                }
                if (socket != null) {
                    System.out.println("Closing socket");
                    socket.close();
                    System.out.println("Closed socket");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("closed socket");
    }
}
