package no.ssb.dc.collection.api.source;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DetectDataTypeTest {

    static final Logger LOG = LoggerFactory.getLogger(DetectDataTypeTest.class);

    static final String string = "hello world";
    static final String integerString = "1";
    static final String longString = String.valueOf(Integer.MAX_VALUE + 1L);
    static final String long2String = String.valueOf(Integer.MIN_VALUE - 1L);
    static final String floatString = "-10,1";
    static final String float2String = "10.1";
    static final String float3String = String.valueOf(Float.MIN_VALUE);
    static final String float4String = String.valueOf(Float.MAX_VALUE);
    static final String doubleString = "0.000000000000000000000000000000000000000000001";
    static final String double2String = String.valueOf(Integer.MAX_VALUE).concat(".000000000000000000000000000000000000000000001");

    @Test
    void detectedDataType() {
        assertEquals(DataType.STRING, DetectDataType.detect(string));
        assertEquals(DataType.BOOLEAN, DetectDataType.detect("true"));
        assertEquals(DataType.BOOLEAN, DetectDataType.detect("false"));
        assertEquals(DataType.INTEGER, DetectDataType.detect(integerString));
        assertEquals(DataType.LONG, DetectDataType.detect(longString));
        assertEquals(DataType.LONG, DetectDataType.detect(long2String));
        assertEquals(DataType.FLOAT, DetectDataType.detect(floatString));
        assertEquals(DataType.FLOAT, DetectDataType.detect(float2String));
        assertEquals(DataType.FLOAT, DetectDataType.detect(float3String));
        assertEquals(DataType.FLOAT, DetectDataType.detect(float4String));
        assertEquals(DataType.DOUBLE, DetectDataType.detect(doubleString));
        assertEquals(DataType.DOUBLE, DetectDataType.detect(double2String));
    }

    @Test
    void name() {
        DataType[] dataTypeImpressions = new DataType[DataType.values().length];
//        dataTypeImpressions[0]
    }
}
