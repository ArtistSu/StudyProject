package com.artists.mapreduce.wordcount;

import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: ArtistS
 * @Site: https://github.com/ArtistSu
 * @Create: 2023/2/5 20:11
 * @Descriiption KEYIN: map阶段输入的key类型: Text (wordcount的输入key是偏移量)
 * VALUEIN: map阶段输入value类型: IntWritable
 * KEYOUT: map阶段输出key类型: Text
 * VALUEOUT: map阶段输出的value类型: IntWritable
 **/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        outV.set(sum);

        context.write(key, outV);
    }
}
