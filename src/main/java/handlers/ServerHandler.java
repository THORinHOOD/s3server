package handlers;

import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import processors.GetObjectProcessor;
import processors.Processor;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@ChannelHandler.Sharable
public class ServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final Set<Processor> processors;

    public ServerHandler(String baseBucketPath) {
        processors = new HashSet<>();
        processors.add(new GetObjectProcessor(baseBucketPath));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        try {
            Set<Processor> collected = processors.stream()
                    .filter(processor -> processor.isThisProcessor(request))
                    .collect(Collectors.toSet());
            if (collected.size() > 1) {
                System.out.println("Found more than 1 processor for request");
            } else if (collected.size() == 0) {
                System.out.println("Not found any processor for request");
            } else {
                collected.iterator().next().process(context, request);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

}
