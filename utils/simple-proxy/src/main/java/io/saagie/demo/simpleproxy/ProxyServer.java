package io.saagie.demo.simpleproxy;

import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.ArrayList;

public class ProxyServer {

    public static void main(String[] args) throws Exception {

        // Usage : java ProxyServer <http proxy port> <tcp remote host:port> ...
        int httpPort = Integer.parseInt(args[0]);
        List<Integer> tcpPorts = new ArrayList<>();
        List<String> tcpHosts = new ArrayList<>();

        for (int i = 1; i < args.length; i++) {
          String[] remote = args[i].split(":");
          tcpPorts.add(Integer.parseInt(remote[1]));
          tcpHosts.add(remote[0]);
        }

        System.out.println("Start HTTP Proxy...");
        HttpProxyServer httpProxy =
                DefaultHttpProxyServer.bootstrap()
                        //.withPort(httpPort)
                        .withAddress(new InetSocketAddress(httpPort))
                        .start();

        for (int i = 0; i < tcpHosts.size(); i++) {
          int tcpPort = tcpPorts.get(i);
          String tcpRemoteHost = tcpHosts.get(i);
          System.out.println("Start TCP Proxy for " + tcpRemoteHost + ":" + tcpPort + "...");
          TcpProxyServer tcpProxy =
                TcpProxyServer.bootstrap()
                        .withLocalPort(tcpPort)
                        .withRemotePort(tcpPort)
                        .withRemoteHost(tcpRemoteHost)
                        .start();
        }

        System.out.println("Waiting for connections...");
    }
}
