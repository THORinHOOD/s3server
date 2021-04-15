package com.thorinhood;

import com.thorinhood.drivers.config.ConfigDriver;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.handlers.ServerHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class ServerInitializer extends ChannelInitializer<SocketChannel> {

    private final ServerHandler serverHandler;

    public ServerInitializer(String basePath, S3Driver s3Driver, ConfigDriver configDriver) {
        serverHandler = new ServerHandler(basePath, s3Driver, configDriver);
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast(new HttpRequestDecoder());
        pipeline.addLast(new HttpResponseEncoder());
        pipeline.addLast(new HttpObjectAggregator(Integer.MAX_VALUE)); //TODO ограничение на размер
//        pipeline.addLast(new ChunkedWriteHandler());
        pipeline.addLast(serverHandler);
    }

}
