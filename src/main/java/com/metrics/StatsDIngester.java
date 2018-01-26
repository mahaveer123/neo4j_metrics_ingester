package com.metrics;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Random;

public class StatsDIngester {

    private static Random RNG = new Random();
    private static StatsDIngester instance;
    private static Object syncRoot = new Object();

    private InetSocketAddress address;
    private DatagramChannel channel;
    private boolean enabled = true;

    public static StatsDIngester getInstance(String host, int port) {
        if (instance == null) {
            synchronized (syncRoot) {
                if (instance == null) {
                    instance = new StatsDIngester(host, port);
                }
            }
        }
        return instance;
    }

    private StatsDIngester(String host, int port) {
        try {
            InetSocketAddress addr = new InetSocketAddress(host, port);
            address = addr;
            channel = DatagramChannel.open();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean send(double sampleRate, String... stats) {

        boolean retval = false; // didn't send anything
        if (sampleRate < 1.0) {
            for (String stat : stats) {
                if (RNG.nextDouble() <= sampleRate) {
                    stat = metricFormat("%s|@%f", stat, sampleRate);
                    if (doSend(stat)) {
                        retval = true;
                    }
                }
            }
        } else {
            for (String stat : stats) {
                if (doSend(stat)) {
                    retval = true;
                }
            }
        }

        return retval;
    }

    private boolean doSend(final String stat) {

        if (enabled && address != null && channel != null && stat != null) {
            try {
                System.out.println("Sending stats : " + stat);
                final byte[] data = stat.getBytes("utf-8");
                final ByteBuffer buff = ByteBuffer.wrap(data);
                final int nbSentBytes = channel.send(buff, address);

                if (data.length == nbSentBytes) {
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                return false;
            }
        }
        return false;
    }

    private String metricFormat(String format, String key, Object value) {
        if (key != null) {
            return String.format(format, key, value);
        } else {
            return null;
        }
    }
}
