package com.thorinhood;

import com.thorinhood.db.H2DB;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class Server {

    private final int port;
    private final ServerInitializer serverInitializer;

    public Server(int port, String basePath, H2DB h2Db) {
        this.port = port;
        serverInitializer = new ServerInitializer(basePath, h2Db);
    }

    public void run() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(serverInitializer);

            Channel channel = bootstrap.bind(port).sync().channel();
            channel.closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}