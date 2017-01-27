package com.medicalrecords.database.job;
import java.io.IOException;
import java.util.UUID;

import com.lits.kundera.test.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
/**
 * Created by oleg on 12/30/16.
 */
public class MCMapper extends TableMapper<Text, IntWritable>
{

    private final IntWritable ONE = new IntWritable(1);

    private Text text = new Text();

    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

        ImmutableBytesWritable patientKey = new ImmutableBytesWritable(row.get(), 0, 2*Bytes.SIZEOF_BYTE);
        UUID patientId = Util.fromBytes(patientKey.get());

        byte[] type = value.getValue(Bytes.toBytes("medical_records"), Bytes.toBytes("type"));
        String val = patientId + " " + " - " + new String(type);
        text.set(val);
        context.write(text, ONE);
    }
}