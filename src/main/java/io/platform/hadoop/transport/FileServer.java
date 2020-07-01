
package io.platform.hadoop.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class FileServer {
    private Integer port;
    private String FILE_DIR = "";

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public void startServer() {
        Selector selector = null;
        ServerSocketChannel serverSocketChannel = null;
        try {
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().setReuseAddress(true);
            serverSocketChannel.socket().bind(new InetSocketAddress(this.port));
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            while (selector.select() > 0) {
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey next = iterator.next();
                    iterator.remove();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void receiveFile(final ServerSocketChannel serverSocketChannel) {
        try (SocketChannel socketChannel = serverSocketChannel.accept()) {

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void writeFile(SocketChannel socketChannel) {

    }

    private String readFileNameFromSocketChannel(SocketChannel socketChannel) throws IOException {
        ByteBuffer allocate = ByteBuffer.allocate(1024);
        socketChannel.read(allocate);
        String fileName = new String(allocate.array());
        allocate.clear();
        return fileName;
    }
}
