package hello;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class Spark
{
    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf().setAppName("HBaseRead").setMaster("local[2]");
        sparkConf.registerKryoClasses(new Class[] { ImmutableBytesWritable.class });

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Configuration conf = HBaseConfiguration.create();
        String tableName = "medical_records";

        conf.set("hbase.zookeeper.quorum", "master.cloudera");
        conf.set(TableInputFormat.INPUT_TABLE, tableName);

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<ImmutableBytesWritable, Integer> updatedRDD = hBaseRDD.mapToPair(TABLE_MAPPER);

        JavaPairRDD<ImmutableBytesWritable, Integer> reducedRDD = updatedRDD.reduceByKey(COUNT_REDUCER);

        reducedRDD.foreach(FOREACH);

        sc.stop();
    }

    private static final PairFunction<Tuple2<ImmutableBytesWritable, Result>, ImmutableBytesWritable, Integer> TABLE_MAPPER =
            new PairFunction<Tuple2<ImmutableBytesWritable, Result>, ImmutableBytesWritable, Integer>()
            {
                @Override
                public Tuple2<ImmutableBytesWritable, Integer> call(Tuple2<ImmutableBytesWritable, Result> in)
                {
                    ImmutableBytesWritable key = in._1;
                    Result values = in._2;

                    byte[] patientKey = ArrayUtils.subarray(key.get(), 0, 16);

                    Cell descriptionCell = values.getColumnLatestCell("medical_records".getBytes(), "description".getBytes());
                    byte[] descriptionBytes = CellUtil.cloneValue(descriptionCell);
                    String description = new String(descriptionBytes);

                    Cell typeCell = values.getColumnLatestCell("medical_records".getBytes(), "type".getBytes());
                    byte[] type = CellUtil.cloneValue(typeCell);
                    byte[] key2 = ArrayUtils.addAll(patientKey, type);
                    return new Tuple2(new ImmutableBytesWritable(key2), description.length() < 10 ? 1 : 0);
                }
            };

    private static final Function2<Integer, Integer, Integer> COUNT_REDUCER =
            new Function2<Integer, Integer, Integer>()
            {
                @Override
                public Integer call(Integer a, Integer b) throws Exception
                {
                    return a + b;
                }
            };

    private static final VoidFunction<Tuple2<ImmutableBytesWritable, Integer>> FOREACH =
            new VoidFunction<Tuple2<ImmutableBytesWritable, Integer>>()
            {
                @Override
                public void call(Tuple2<ImmutableBytesWritable, Integer> in) throws Exception
                {
                    byte[] key = in._1.get();
                    Text patientId = new Text();
                    patientId.set(key, 0, 16);
                    Text type = new Text();
                    type.set(key, 16, key.length-16);
                    System.out.println("hello.Patient id: " + patientId
                            + " has" + in._2.toString() + " medical record size is less than 10 and has a type of: " + type);
                }
            };
}
