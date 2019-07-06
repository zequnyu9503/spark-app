package pers.yzq.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

    public HBaseBulkLoad() {
        System.out.println("BulkLoad");
    }

    public Boolean cTable(Admin admin, String tableName, List<String> families, byte[][] splits){
        try {
            TableName tn = TableName.valueOf(tableName);
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);
            HashSet cfdbs = new HashSet<ColumnFamilyDescriptor>(families.size());
            Iterator<String> familyIterator = families.iterator();
            while (familyIterator.hasNext()) {
                String family = familyIterator.next();
                ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(family))
                        .setBlockCacheEnabled(true)
                        .setBloomFilterType(BloomType.NONE)
                        .setDataBlockEncoding(DataBlockEncoding.NONE);
                cfdbs.add(cfdb.build());
            }
            admin.createTable(tdb.setColumnFamilies(cfdbs).build(), splits);
            return true;
        } catch (IOException e){
            e.printStackTrace();
            return false;
        }
    }

    public Boolean ddTable(Admin admin, String tableName){
        try{
            TableName tn = TableName.valueOf(tableName);
            if(!admin.isTableDisabled(tn)) {
                admin.disableTable(tn);
            }
            admin.deleteTable(tn);
            return true;
        }catch (IOException e){
            e.printStackTrace();
            return false;
        }
    }

    public byte[][] getSplits(long range, int num){
        long split = range / num;
        long [] keys = new long[num - 1];
        for(int i = 0;i<keys.length; ++i) {
            keys[i] = split * (i+1);
        }
        TreeSet set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for(int i = 0; i<keys.length;){
            set.add(Bytes.toBytes(keys[i++]));
        }
        Iterator<byte[]> iterator = set.iterator();
        byte[][] splits = new byte[keys.length][];
        for(int i =0; i<keys.length && iterator.hasNext();){
            splits[i++] = iterator.next();
        }
        return splits;
    }

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
        final int hRegionSize = 128 * 1024 * 1024;

        Configuration hc = HBaseConfiguration.create();
        hc.set("hbase.mapred.outputtable", tableName);
        hc.setLong("hbase.hregion.max.filesize", hRegionSize );
        hc.set("hbase.mapreduce.hfileoutputformat.table.name", tableName);
        hc.setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 1024 * 1024 * 1024);
        Connection con = ConnectionFactory.createConnection(hc);
        Admin admin = con.getAdmin();

        ddTable(admin, tableName);
        cTable(admin, tableName, new ArrayList<String>(){{add(columnFamily);}},
                getSplits(new ArrayList<String>(){{
                    add("b0000000000");
                    add("c0000000000");
                    add("d0000000000");
                    add("e0000000000");
                    add("f0000000000");
                    add("g0000000000");
                    add("h0000000000");
                    add("i0000000000");
                    add("j0000000000");
                }}));
        SparkConf conf = new SparkConf().setAppName("YZQ-HBASE-BULKLOAD-"+System.currentTimeMillis());
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = jsc.textFile(hadoop_file).persist(StorageLevel.MEMORY_AND_DISK_SER());
        JavaPairRDD<ImmutableBytesWritable, KeyValue> rdd_ = hbaseRDDv2(rdd, columnFamily, column, 10);
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
        String [] rkPrefix = {"j","a", "b","c","d","e","f","g","h","i"};
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
