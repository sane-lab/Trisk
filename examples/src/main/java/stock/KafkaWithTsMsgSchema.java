package stock;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

public class KafkaWithTsMsgSchema implements KafkaDeserializationSchema<Tuple3<String, String, Long>>, SerializationSchema<Tuple2<String, String>> {
    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Last_Upd_Time = 2;
    private static final int Order_Price = 3;
    private static final int Order_Exec_Vol = 4;
    private static final int Order_Vol = 5;
    private static final int Sec_Code = 6;
    private static final int Trade_Dir = 7;


    private static final long serialVersionUID = 1L;
    private transient Charset charset;

    public KafkaWithTsMsgSchema() {
        this(Charset.forName("UTF-8"));
    }

    public KafkaWithTsMsgSchema(Charset charset) {
        this.charset = Preconditions.checkNotNull(charset);
    }

    public Charset getCharset() {
        return this.charset;
    }

    // split stock string with delimeter
//    public Tuple2<String, String> deserialize(byte[] message) {
//        String msg = new String(message, charset);
//        String[] stockArr = msg.split("\\|");
//
//        if(stockArr.length > 2){
//            return new Tuple2<String, String>(stockArr[Sec_Code], msg);
//        }else{
//            System.out.println("Fail due to invalid msg format.. ["+msg+"]");
//            return new Tuple2<String, String>(msg, "0");
//        }
//    }

    @Override
    public boolean isEndOfStream(Tuple3<String, String, Long> stringLongTuple2) {
        return false;
    }

    @Override
    public Tuple3<String, String, Long> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
        Long ts = consumerRecord.timestamp();
        String msg = new String(consumerRecord.value(), charset);
        List<String> stockArr = Arrays.asList(msg.split("\\|"));

        if(stockArr.size() > 2){
            return new Tuple3<>(stockArr.get(Sec_Code), msg, ts);
        } else {
            System.out.println("Fail due to invalid msg format.. ["+msg+"]");
            return new Tuple3<>(msg, "0", 0L);
        }
    }

    public byte[] serialize(Tuple2<String, String> element) {
        return String.valueOf(element.f1).getBytes(this.charset);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(this.charset.name());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String charsetName = in.readUTF();
        this.charset = Charset.forName(charsetName);
    }

    @Override
    public TypeInformation<Tuple3<String, String, Long>> getProducedType() {
        return new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);
    }}