package handlers;

import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import processors.*;

import java.util.ArrayList;
import java.util.List;

@ChannelHandler.Sharable
public class ServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final List<Processor> processors;
    private HttpPostRequestDecoder httpDecoder;

    public ServerHandler(String basePath) {
        processors = new ArrayList<>();
        processors.add(new GetObjectProcessor(basePath));
        processors.add(new CreateBucketProcessor(basePath));
        processors.add(new PutObjectProcessor(basePath, httpDecoder));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        try {
            for (Processor processor : processors) {
                ProcessorPreArguments preArguments = processor.isThisProcessor(request);
                if (preArguments.isThisProcessor()) {
                    processor.process(context, request, preArguments.getArguments());
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
