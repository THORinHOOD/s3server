package com.thorinhood;

import com.thorinhood.db.H2DB;
import com.thorinhood.handlers.ServerHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

public class ServerInitializer extends ChannelInitializer<SocketChannel> {

    private final ServerHandler serverHandler;

    public ServerInitializer(String basePath, H2DB h2Db) {
        serverHandler = new ServerHandler(basePath, h2Db);
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast(new HttpRequestDecoder());
        pipeline.addLast(new HttpResponseEncoder());
        pipeline.addLast(new HttpObjectAggregator(665536)); //TODO ограничение на размер
        pipeline.addLast(new ChunkedWriteHandler());
        pipeline.addLast(serverHandler);
    }

}
