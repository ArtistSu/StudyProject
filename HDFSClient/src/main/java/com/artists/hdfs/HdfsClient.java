package com.artists.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 客户端代码常用套路
 * 1. 获取一个客户端对象
 * 2. 执行相关的操作命令
 * 3. 关闭资源
 * HDFS zookeeper
 */
public class HdfsClient {
    @Test
    public void testmkdir() throws URISyntaxException, IOException {
        // 连接的集群的nn地址
        URI uri = new URI("hdfs://34.132.121.211:8020");
        // 创建一个配置文件
        Configuration configuration = new Configuration();

        // 1.获取客户端对象
        FileSystem fs = FileSystem.get(uri, configuration);

        // 2.创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan"));

        // 3.关闭资源
        fs.close();
    }
}
