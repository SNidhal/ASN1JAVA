import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.ASN1Primitive;
import org.bouncycastle.asn1.ASN1Sequence;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.List;

public class SparkApp {

    public static void main(String[] args) {

        SparkConf conf1 = new SparkConf().setAppName("ASN1-Spark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf1);
        Configuration conf = new Configuration(sc.hadoopConfiguration());
        System.out.println   ("tessssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssst");
        JavaPairRDD<LongWritable ,Text > rdd = sc.newAPIHadoopFile("hdfs://hadoop1.example.com:8020/user/admin/test.ber", RawFileAsBinaryInputFormat.class, LongWritable.class, Text.class, conf);
        JavaRDD rdd3 =rdd.map(new Function<Tuple2<LongWritable, Text>, Object>() {
            public Object call(Tuple2<LongWritable, Text> longWritableTextTuple2) throws Exception {
                return longWritableTextTuple2._2().toString();
            }
        });

        JavaRDD rdd2 = rdd3.repartition(5);
        /*List rd =rdd2.map(new Function<Tuple2<LongWritable, Text>, Object>() {
            public Object call(Tuple2<LongWritable, Text> longWritableTextTuple2) throws Exception {
                return longWritableTextTuple2._2().toString();
            }
        }).collect();*/

        List<CallDetailRecord> rd =rdd2.map(x->{
            InputStream is= new ByteArrayInputStream(((String)x).getBytes());
            ASN1InputStream asnin = new ASN1InputStream(is);
            ASN1Primitive obj = null;
            CallDetailRecord thisCdr = null;
            while ((obj = asnin.readObject()) != null) {

                 thisCdr= new CallDetailRecord((ASN1Sequence) obj);

                System.out.println("CallDetailRecord "+thisCdr.getRecordNumber()+" Calling "+thisCdr.getCallingNumber()
                        +" Called "+thisCdr.getCalledNumber()+ " Start Date-Time "+thisCdr.getStartDate()+"-"
                        +thisCdr.getStartTime()+" duration "+thisCdr.getDuration()
                );

            }

            asnin.close();
            CallDetailRecord2 cdr2 = new CallDetailRecord2(thisCdr.recordNumber,thisCdr.callingNumber, thisCdr.calledNumber, thisCdr.startDate,thisCdr.startTime, thisCdr.duration);
            return cdr2;
        }).collect();


 for(Object o : rd){
     System.out.println("main");
     System.out.println(o);
 }
        System.out.println   (" end size : "+rd.size());

    }


}
