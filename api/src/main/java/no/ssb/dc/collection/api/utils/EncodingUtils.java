package no.ssb.dc.collection.api.utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class EncodingUtils {

    public static String encodeArray(List<String> list, ByteBuffer encodingBuffer) {
        final Base64.Encoder base64encoder = Base64.getEncoder();
        try {
            for (String segment : list) {
                byte[] bytes = segment.getBytes(StandardCharsets.UTF_8);
                encodingBuffer.putInt(bytes.length);
                encodingBuffer.put(bytes);
            }
            encodingBuffer.flip();

            byte[] array = new byte[encodingBuffer.remaining()];
            encodingBuffer.get(array);
            return base64encoder.encodeToString(array);
        } finally {
            encodingBuffer.clear();
        }
    }

    public static List<String> decodeArray(String base64, ByteBuffer encodingBuffer) {
        final Base64.Decoder base64decoder = Base64.getDecoder();
        List<String> list = new ArrayList<>();
        byte[] encodedBytes = base64decoder.decode(base64);
        try {
            encodingBuffer.put(encodedBytes);
            encodingBuffer.flip();
            for (; ; ) {
                int offset = encodingBuffer.getInt();
                if (offset == 0) break;
                byte[] segment = new byte[offset];
                encodingBuffer.get(segment);
                list.add(new String(segment));
                if (!encodingBuffer.hasRemaining()) {
                    break;
                }
            }
        } finally {
            encodingBuffer.clear();
        }
        return list;
    }
}
