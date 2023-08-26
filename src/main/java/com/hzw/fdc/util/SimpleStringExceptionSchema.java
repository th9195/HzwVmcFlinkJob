package com.hzw.fdc.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author gdj
 * @create 2020-06-15-15:43
 * 改造了原来的SimpleStringSchema不能处理kafka非String类型的数据过来报NullPointerException
 */
@PublicEvolving
public class SimpleStringExceptionSchema implements DeserializationSchema<String>, SerializationSchema<String> {

    private static final long serialVersionUID = 1L;

    /**
     * The charset to use to convert between strings and bytes.
     * The field is transient because we serialize a different delegate object instead
     */
    private transient Charset charset;

    /**
     * Creates a new SimpleStringSchema that uses "UTF-8" as the encoding.
     */
    public SimpleStringExceptionSchema() {
        this(StandardCharsets.UTF_8);
    }

    /**
     * Creates a new SimpleStringSchema that uses the given charset to convert between strings and bytes.
     *
     * @param charset The charset to use to convert between strings and bytes.
     */
    public SimpleStringExceptionSchema(Charset charset) {
        this.charset = checkNotNull(charset);
    }

    /**
     * Gets the charset used by this schema for serialization.
     *
     * @return The charset used by this schema for serialization.
     */
    public Charset getCharset() {
        return charset;
    }

    // ------------------------------------------------------------------------
    //  Kafka Serialization
    // ------------------------------------------------------------------------

    @Override
    public String deserialize(byte[] message) {
        try {
            return new String(message, charset);


        } catch (Exception e) {
            e.printStackTrace();
            return "message not String";
        }
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(String element) {
        return element.getBytes(charset);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    // ------------------------------------------------------------------------
    //  Java Serialization
    // ------------------------------------------------------------------------

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(charset.name());
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String charsetName = in.readUTF();
        this.charset = Charset.forName(charsetName);
    }
}
