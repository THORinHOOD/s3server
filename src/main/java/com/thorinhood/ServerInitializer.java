package com.thorinhood;

import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.drivers.user.UserDriver;
import com.thorinhood.handlers.ServerHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class ServerInitializer extends ChannelInitializer<SocketChannel> {

    private final ServerHandler serverHandler;

    public ServerInitializer(S3Driver s3Driver, UserDriver userDriver) {
        serverHandler = new ServerHandler(s3Driver, userDriver);
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast(new HttpRequestDecoder());
        pipeline.addLast(new HttpResponseEncoder());
        pipeline.addLast(new HttpObjectAggregator(Integer.MAX_VALUE));
        pipeline.addLast(serverHandler);
    }

}
