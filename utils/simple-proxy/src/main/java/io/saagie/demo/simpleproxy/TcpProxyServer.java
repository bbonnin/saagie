package io.saagie.demo.simpleproxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class TcpProxyServer implements Runnable {

    private int localPort = 3090;
    private int remotePort = -1;
    private String remoteHost = "localhost";
    private ServerSocket serverSocket;

    public static TcpProxyServer bootstrap() {
        return new TcpProxyServer();
    }

    private TcpProxyServer() {

    }

    public TcpProxyServer withLocalPort(int localPort) {
        this.localPort = localPort;
        return this;
    }

    public TcpProxyServer withRemotePort(int remotePort) {
        this.remotePort = remotePort;
        return this;
    }

    public TcpProxyServer withRemoteHost(String remoteHost) {
        this.remoteHost = remoteHost;
        return this;
    }

    public TcpProxyServer start() throws IOException {
        if (remotePort == -1) {
            throw new RuntimeException("Remote port not set");
        }

        serverSocket = new ServerSocket(localPort);

        new Thread(this).start();

        return this;
    }

    public void run() {

        //
        // Source: http://www.java2s.com/Code/Java/Network-Protocol/Asimpleproxyserver.htm
        //

        final byte[] request = new byte[1024];
        byte[] reply = new byte[4096];

        while (true) {
            Socket client = null, server = null;
            try {
                // Wait for a connection on the local port
                client = serverSocket.accept();
                System.out.println("==> NEW CONNECTION <==");

                final InputStream streamFromClient = client.getInputStream();
                final OutputStream streamToClient = client.getOutputStream();

                try {
                    server = new Socket(remoteHost, remotePort);
                }
                catch (IOException e) {
                    System.out.println("ERR: cannot connect to " + remoteHost + ":" + remotePort);
                    PrintWriter out = new PrintWriter(streamToClient);
                    out.print("TCP Proxy server cannot connect to " + remoteHost + ":" + remotePort + ":\n" + e + "\n");
                    out.flush();
                    client.close();
                    continue;
                }

                // Get server streams.
                final InputStream streamFromServer = server.getInputStream();
                final OutputStream streamToServer = server.getOutputStream();

                // a thread to read the client's requests and pass them
                // to the server. A separate thread for asynchronous.
                Thread t = new Thread() {
                    public void run() {
                        int bytesRead;
                        try {
                            while ((bytesRead = streamFromClient.read(request)) != -1) {
                                streamToServer.write(request, 0, bytesRead);
                                streamToServer.flush();
                            }
                            System.out.println("INFO: read from client, bytesRead=" + bytesRead);
                        } catch (IOException e) {
                          System.out.println("ERR: read from client " + e.getMessage());
                        }

                        // the client closed the connection to us, so close our
                        // connection to the server.
                        try {
                            streamToServer.close();
                        } catch (IOException e) {
                        }
                    }
                };

                // Start the client-to-server request thread running
                t.start();

                // Read the server's responses
                // and pass them back to the client.
                int bytesRead;
                try {
                    while ((bytesRead = streamFromServer.read(reply)) != -1) {
                        streamToClient.write(reply, 0, bytesRead);
                        streamToClient.flush();
                    }
                    System.out.println("INFO: read from server, bytesRead=" + bytesRead);
                }
                catch (IOException e) {
                  System.out.println("ERR: read from server " + e.getMessage());
                }

                // The server closed its connection to us, so we close our
                // connection to our client.
                streamToClient.close();
            }
            catch (IOException e) {
                System.err.println(e);
            }
            finally {
                try {
                    if (server != null)
                        server.close();
                    if (client != null)
                        client.close();
                }
                catch (IOException e) {
                }
            }
        }
    }
}
