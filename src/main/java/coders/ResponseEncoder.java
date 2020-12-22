package coders;

import data.ResponseData;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class ResponseEncoder extends MessageToByteEncoder<ResponseData> {
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, ResponseData msg, ByteBuf out)
            throws Exception {
        out.writeInt(msg.getIntValue());
    }
}
