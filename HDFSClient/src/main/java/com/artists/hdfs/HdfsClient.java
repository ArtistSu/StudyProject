package com.artists.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
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
    private FileSystem fs;
    @Before
    public void init() throws IOException, InterruptedException, URISyntaxException {
        // 连接的集群的nn地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 创建一个配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");

        // 用户
        String user = "sylgg0918";

        // 1.获取客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }
    @After
    public void close() throws IOException{
        fs.close();
    }
    @Test
    public void testmkdir() throws URISyntaxException, IOException, InterruptedException {
        // 2.创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
    }

    // 上传
    @Test
    public void testPut() throws IOException {
        /**
         * 参数解读
         * delSrc: 上传后是否删除本地源文件
         * overwrite: 路径存在是否覆盖
         * srcs: 源数据路径(本地路径)
         * dst: 目的地路径
         */
        fs.copyFromLocalFile(false, true, new Path("C:\\Users\\ArtistS\\GitRepository\\StudyProject\\HDFSClient\\src\\main\\resources\\sunwukong.txt"),new Path("hdfs://hadoop102/xiyou/huaguoshan"));}
}