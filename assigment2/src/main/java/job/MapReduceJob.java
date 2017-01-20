package com.medicalrecords.database.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by oleg on 12/30/16.
 */

public class MapReduceJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "counter_job");
        job.setJarByClass(MapReduceJob.class);
        job.setMapperClass(MCMapper.class);
        job.setReducerClass(MCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setNumReduceTasks(1);

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.addFamily("medical_records".getBytes());

        TableMapReduceUtil.initTableMapperJob(
                "medical_records",
                scan,
                MCMapper.class,
                Text.class,
                IntWritable.class,
                job
        );

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}