package com.artists.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: ArtistS
 * @Site: https://github.com/ArtistSu
 * @Create: 2023/2/5 20:10
 * @Descriiption KEYIN: map阶段输入的key类型: LongWritable (wordcount的输入key是偏移量)
 * VALUEIN: map阶段输入value类型: Text
 * KEYOUT: map阶段输出key类型: Text
 * VALUEOUT: map阶段输出的value类型: IntWritable
 **/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // 为什么放在这? 因为map()是输入文件有多少行调用多少次, word则是一行内拆分多少个
    // 就调用多少次, 所有为了避免内存浪费就放在这里.
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 1. 获取一行
        String line = value.toString();

        // 2.切割
        String[] words = line.split(" ");

        // 3.循环写出
        for (String word :
                words) {
            // 封装outK
            outK.set(word);

            // 写出
            context.write(outK, outV);
        }
    }
}
