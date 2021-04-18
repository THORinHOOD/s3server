package com.thorinhood;

import com.thorinhood.drivers.user.UserDriver;
import com.thorinhood.drivers.main.S3Driver;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class Server {

    private final int port;
    private final ServerInitializer serverInitializer;

    public Server(int port, S3Driver s3Driver, UserDriver userDriver) {
        this.port = port;
        serverInitializer = new ServerInitializer(s3Driver, userDriver);
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
