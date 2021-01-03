package handlers;

import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import processors.GetObjectProcessor;
import processors.Processor;
import processors.PutObjectProcessor;

import java.util.ArrayList;
import java.util.List;

@ChannelHandler.Sharable
public class ServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final List<Processor> processors;
    private HttpPostRequestDecoder httpDecoder;

    public ServerHandler(String baseBucketPath) {
        processors = new ArrayList<>();
//        processors.add(new MenuProcessor());
        processors.add(new GetObjectProcessor(baseBucketPath));
        processors.add(new PutObjectProcessor(baseBucketPath, httpDecoder));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        try {
            for (Processor processor : processors) {
                if (processor.isThisProcessor(request)) {
                    processor.process(context, request);
                    return;
                }
            }
            System.out.println("Not found any processor for request");
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.channel().close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (httpDecoder != null) {
            httpDecoder.cleanFiles();
        }
    }
}
