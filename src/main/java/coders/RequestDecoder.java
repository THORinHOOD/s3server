package coders;

import data.RequestData;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RequestDecoder extends ReplayingDecoder<RequestData> {

    private final Charset charset = StandardCharsets.UTF_8;

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf in, List<Object> out)
            throws Exception {
        RequestData data = new RequestData();
        data.setIntValue(in.readInt());
        int strLen = in.readInt();
        data.setStrValue(in.readCharSequence(strLen, charset).toString());
        out.add(data);
    }
}
