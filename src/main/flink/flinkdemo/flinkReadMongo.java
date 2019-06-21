package flinkdemo;

import com.alibaba.fastjson.JSON;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Author wenBin
 * Date 2019/6/12 14:52
 * Version 1.0
 */
public class flinkReadMongo {

    private static final Logger LOG = LoggerFactory.getLogger(flinkReadMongo.class);

    public static final String MONGO_URI = "mongodb://ip:port/db.collection";

    public static void main(String[] args) throws Exception {
        //获取条件参数
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String webSource = parameterTool.get("webSource", "baidu");
        int year = parameterTool.getInt("year", 2016);
        String condition = String.format("{'source':'%s','year':{'$regex':'^%d'}}", webSource, year);
        //创建运行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //将mongo数据转化为Hadoop数据格式
        HadoopInputFormat<BSONWritable, BSONWritable> hdIf =
                new HadoopInputFormat<BSONWritable, BSONWritable>(new MongoInputFormat(), BSONWritable.class, BSONWritable.class, new JobConf());
        hdIf.getJobConf().set("mongo.input.split.create_input_splits", "false");
        hdIf.getJobConf().set("mongo.input.uri", MONGO_URI);
        hdIf.getJobConf().set("mongo.input.query", condition);

//        long count = env.createInput(hdIf)
//                .map((MapFunction<Tuple2<BSONWritable, BSONWritable>, String>) value -> {
//                    BSONWritable v = value.getField(1);
//                    return JSON.parseObject(v.getDoc().toString()).toJSONString();
//                })
//                .count();
//        LOG.info("总共读取到{}条MongoDB数据", count);
    }

}
