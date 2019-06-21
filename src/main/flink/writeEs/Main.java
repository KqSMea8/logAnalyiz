//package writeEs;
//
//import com.sun.javafx.font.Metrics;
//import com.zhisheng.common.model.MetricEvent;
//import com.zhisheng.common.utils.ExecutionEnvUtil;
//import com.zhisheng.common.utils.GsonUtil;
//import com.zhisheng.common.utils.KafkaConfigUtil;
//import com.zhisheng.connectors.es6.utils.ElasticSearchSinkUtil;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.http.HttpHost;
//import org.elasticsearch.client.Requests;
//import org.elasticsearch.common.xcontent.XContentType;
//
//import java.util.List;
//
//import static com.zhisheng.common.constant.PropertiesConstants.*;
//
///**
// * Author wenBin
// * Date 2019/6/12 13:33
// * Version 1.0
// */
//public class Main {
//    public static void main(String[] args) throws Exception {
//        //获取所有参数
//        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
//        //准备好环境
//        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
//        //从kafka读取数据
//        DataStreamSource<Metrics> data = KafkaConfigUtil.buildSource(env);
//
//        //从配置文件中读取 es 的地址
//        List<HttpHost> esAddresses = ElasticSearchSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
//        //从配置文件中读取 bulk flush size，代表一次批处理的数量，这个可是性能调优参数，特别提醒
//        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
//        //从配置文件中读取并行 sink 数，这个也是性能调优参数，特别提醒，这样才能够更快的消费，防止 kafka 数据堆积
//        int sinkParallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 5);
//
//        //自己再自带的 es sink 上一层封装了下
//        ElasticSearchSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, data,
//                (Metrics metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
//                    requestIndexer.add(Requests.indexRequest()
//                            .index(ZHISHENG + "_" + metric.getName())  //es 索引名
//                            .type(ZHISHENG) //es type
//                            .source(GsonUtil.toJSONBytes(metric), XContentType.JSON));
//                });
//        env.execute("flink learning connectors es6");
//    }
//}
