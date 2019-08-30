/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pers.yzq.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * @Author: YZQ
 * @date 2019/5/7
 */
public class HBaseBulkLoad implements Serializable {


    public byte[][] getSplits(List<String> ss){
        TreeSet set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        ss.forEach(e -> set.add(Bytes.toBytes(e)));
        Iterator<byte[]> iterator = set.iterator();
        byte[][] splits = new byte[ss.size()][];
        for(int i =0; i<ss.size() && iterator.hasNext();){
            splits[i++] = iterator.next();
        }
        return splits;
    }

    public void bulkLoad(String tableName, String columnFamily, String column, String hadoop_file, String hfile) throws IOException {
        Configuration hc = HBaseConfiguration.create();
        hc.set("hbase.mapred.outputtable", tableName);
        hc.setLong("hbase.hregion.max.filesize", HConstants.DEFAULT_MAX_FILE_SIZE);
        hc.set("hbase.mapreduce.hfileoutputformat.table.name", tableName);
        hc.setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 1024 * 1024 * 1024);
        Connection con = ConnectionFactory.createConnection(hc);
        Admin admin = con.getAdmin();
        //TO-DO
        SparkConf conf = new SparkConf().setAppName("YZQ-HBASE-BULKLOAD-"+System.currentTimeMillis());
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = jsc.textFile(hadoop_file).persist(StorageLevel.MEMORY_AND_DISK_SER());
        JavaPairRDD<ImmutableBytesWritable, KeyValue> rdd_ = hbaseRDDv2(rdd, columnFamily, column, 20);
        Table table = con.getTable(TableName.valueOf(tableName));
        TableDescriptor td = table.getDescriptor();
        Job job = Job.getInstance(hc);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        HFileOutputFormat2.configureIncrementalLoadMap(job, td);
        rdd_.saveAsNewAPIHadoopFile(
                hfile,
                ImmutableBytesWritable.class,
                KeyValue.class,
                HFileOutputFormat2.class,
                hc);
        LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(hc);
        RegionLocator locator = con.getRegionLocator(TableName.valueOf(tableName));
        bulkLoader.doBulkLoad(
                new Path(hfile),
                admin,
                table,
                locator
        );
    }

    public JavaPairRDD<ImmutableBytesWritable, KeyValue> hbaseRDDv2(JavaRDD<String> rdd, String columnFamily, String column, int totalTimeStamp){
        String [] rkPrefix = {"t","a", "b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
        return rdd.mapToPair((PairFunction<String, String, Tuple2<Integer, Long>>) s -> {
            String[] vt = s.split("-");
            Long timestamp = Long.valueOf(vt[1]);
            Long p = timestamp % rkPrefix.length;
            String rowKey = rkPrefix[p.intValue()] + String.format("%0" + (totalTimeStamp) + "d", timestamp);
            // (rowKey, (data, timestamp))
            return new Tuple2<>(rowKey, new Tuple2<>(Integer.valueOf(vt[0]), timestamp));
        }).sortByKey().
                mapToPair((PairFunction<Tuple2<String, Tuple2<Integer, Long>>, ImmutableBytesWritable, KeyValue>) t -> {
                    ImmutableBytesWritable writable = new ImmutableBytesWritable(Bytes.toBytes(t._1()));
                    KeyValue keyValue = new KeyValue(
                            Bytes.toBytes(t._1()),
                            Bytes.toBytes(columnFamily),
                            Bytes.toBytes(column),
                            t._2()._2(),
                            Bytes.toBytes(t._2()._1()));
                    return new Tuple2<>(writable, keyValue);
                });
    }
}
