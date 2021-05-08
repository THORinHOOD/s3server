package com.thorinhood;

import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.drivers.user.UserDriver;
import com.thorinhood.utils.RequestUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class Server {

    private final int port;
    private final ServerInitializer serverInitializer;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture future;

    public Server(int port, S3Driver s3Driver, RequestUtil requestUtil) {
        this.port = port;
        serverInitializer = new ServerInitializer(s3Driver, requestUtil);
    }

    public void run() throws Exception {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(serverInitializer);

            future = bootstrap.bind(port).sync();
            Channel channel = future.channel();
            channel.closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public void stop() {
        try {
            bossGroup.shutdownGracefully().sync();
            workerGroup.shutdownGracefully().sync();
            future.channel().closeFuture().sync();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
