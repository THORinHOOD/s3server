package com.thorinhood.chunks;

import com.thorinhood.utils.ParsedRequest;
import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.Credential;
import com.thorinhood.utils.SignUtil;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class ChunkReader {

    public static byte[] readChunks(FullHttpRequest request, ParsedRequest parsedRequest, String secretKey) 
            throws S3Exception {
        String prevSignature = parsedRequest.getSignature();
        byte[] result = new byte[parsedRequest.getDecodedContentLength()];
        int index = 0;

        try (ByteArrayInputStream is = new ByteArrayInputStream(parsedRequest.getBytes())) {
            ChunkInfo chunkInfo = new ChunkInfo(1, null);
            boolean first = true;
            while (chunkInfo.getChunkSize() != 0) {
                chunkInfo = ChunkReader.readInfoChunkLine(is, !first);
                if (chunkInfo.getChunkSize() != 0) {
                    byte[] chunk = ChunkReader.readChunk(chunkInfo.getChunkSize(), is);

                    checkChunk(
                        chunkInfo.getSignature(),
                        prevSignature,
                        chunk,
                        request,
                        parsedRequest.getCredential(),
                        secretKey
                    );

                    if (chunkInfo.getChunkSize() >= 0) {
                        System.arraycopy(chunk, 0, result, index, chunkInfo.getChunkSize());
                    }
                    index += chunkInfo.getChunkSize();
                }
                first = false;
                prevSignature = chunkInfo.getSignature();
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception.getMessage())
                    .setMessage(exception.getMessage())
                    .setResource("1")
                    .setRequestId("1");
        }
        return result;
    }

    private static void checkChunk(String chunkSignature, String prevSignature, byte[] chunkData,
                                   FullHttpRequest request, Credential credential, String secretKey) throws S3Exception {
        String calculatedChunkSignature = SignUtil.calcPayloadSignature(request, credential, prevSignature, chunkData,
                secretKey);
        if (!calculatedChunkSignature.equals(chunkSignature)) {
            throw S3Exception.build("Chunk signature is incorrect")
                    .setStatus(HttpResponseStatus.BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.SIGNATURE_DOES_NOT_MATCH)
                    .setMessage("Chunk signature is incorrect")
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    private static ChunkInfo readInfoChunkLine(ByteArrayInputStream bytes, boolean skip) {
        StringBuilder info = new StringBuilder();
        if (skip) {
            bytes.skip(2);
        }
        char b;
        char prev = (char) bytes.read();
        info.append(prev);
        int chunkSize = 0;
        while ((b = (char)bytes.read()) != '\n' && (prev != '\r')) {
            if (b == ';') {
                chunkSize = Integer.parseInt(info.toString(), 16);
                info.setLength(0);
            } else if (b == '=') {
                info.setLength(0);
            } else if (b != '\r') {
                info.append(b);
            }
            prev = b;
        }
        return new ChunkInfo(chunkSize, info.toString());
    }

    private static byte[] readChunk(int chunkSize, ByteArrayInputStream bytes) {
        byte[] result = new byte[chunkSize];
        bytes.read(result, 0, chunkSize);
        return result;
    }

}
