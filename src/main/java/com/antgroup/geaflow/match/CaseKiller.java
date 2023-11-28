package com.antgroup.geaflow.match;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.internal.CollectionSource;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.cluster.local.client.LocalEnvironment;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.Encoders;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.encoder.impl.*;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.metaserver.client.DefaultClientOption;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueLabelVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueTimeVertex;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.state.StoreType;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc.BackendType;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.channel.ChannelType;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.*;
import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_OFFSET_BACKEND_TYPE;
import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

public class CaseKiller {
    private static final Logger LOGGER = LoggerFactory.getLogger(CaseKiller.class);
    static Integer eValueZero1 = Integer.MAX_VALUE - 4;
    static Integer eValueZero3 = Integer.MAX_VALUE - 3;
    static Integer eValueZero8 = Integer.MAX_VALUE - 2;
    static Integer eValueZero9 = Integer.MAX_VALUE - 1;
    static Integer personStart = 0;
    static Integer accountStart = 1000 * 1000;
    static Integer loanStart = 2000 * 1000;
    static Map<Integer, Long> idMap = new ConcurrentHashMap<>(500000);
    static Map<Long, Integer> idRvtMap1 = new ConcurrentHashMap<>(8 * 10000);
    static Map<Long, Integer> idRvtMap2 = new ConcurrentHashMap<>(25 * 10000);
    static Map<Long, Integer> idRvtMap3 = new ConcurrentHashMap<>(15 * 10000);
    static long start = System.currentTimeMillis();
    public static String RESULT_FILE_PATH = "";
    public static String REF_FILE_PATH = "";
    public static final Integer readParallel = 1;
    public static final Integer itrParallel = 8;
    public static Double curStage = -1D;
    public static long curTime = System.currentTimeMillis();
    static ExecutorService es = Executors.newCachedThreadPool();
    static ConcurrentLinkedQueue<IVertex<Integer, String>> vertexList = new ConcurrentLinkedQueue<>();
    static ConcurrentLinkedQueue<IEdge<Integer, Integer>> edgeList = new ConcurrentLinkedQueue<>();
    static CountDownLatch readRead = new CountDownLatch(1);

    public static void main(String[] args) {
        try {
            if (args.length > 0) REF_FILE_PATH = args[0];
            if (args.length > 1) RESULT_FILE_PATH = args[1];
            // 预读取
            readAllData();
            // 预创建文件
            BufferedWriter[] writers = new BufferedWriter[4];
            for (int i = 0; i < 4; i++) {
                int finalI = i;
                es.execute(() -> {
                    try {
                        BufferedWriter dataWriter = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get(RESULT_FILE_PATH + "/result" + (finalI + 1) + ".csv"))), 1024 * 1024 * 8);
                        writers[finalI] = dataWriter;
                    } catch (IOException e) {
                        LOGGER.error("xx", e);
                    }
                });
            }
            Map<String, String> config = new HashMap<>();
            config.put(RUN_LOCAL_MODE.getKey(), Boolean.TRUE.toString());
            config.put(SYSTEM_STATE_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
            config.put(SYSTEM_OFFSET_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
            Environment environment = new LocalEnvironment();
            environment.getEnvironmentContext().withConfig(config);
            Configuration conf = environment.getEnvironmentContext().getConfig();
            conf.put(ENABLE_DETAIL_METRIC, "false");
            conf.put(SHUFFLE_FLUSH_BUFFER_TIMEOUT_MS, "2000");
            conf.put(NETTY_CONNECT_INITIAL_BACKOFF_MS, "200");
            conf.put(DRIVER_JVM_OPTION, "-Xmx4096m,-Xms4096m,-Xmn1025m,-Xss1024k,-XX:MaxDirectMemorySize=2048m");
            conf.put(CLIENT_JVM_OPTIONS, "-Xmx2048m,-Xms2048m,-Xmn512m,-Xss512k,-XX:MaxDirectMemorySize=1024m");
            conf.put(DRIVER_MEMORY_MB, "8192");
            conf.put(CLIENT_MEMORY_MB, "4096");
            conf.put(RUN_LOCAL_MODE, "true");
            conf.put("brpc.connection.keep.alive.time.sec", "1000");
            conf.put("brpc.io.thread.num", "8");
            setDefaultOption();
            // MemSink可能会被创建出多个实例，因此只能使用Static静态变量存储结果，进而只能每一个结果集创建一个MemSink类
            MemSinkOne<IVertex<Integer, String>> sinkOne = new MemSinkOne<>();
            MemSinkTwo<IVertex<Integer, String>> sinkTwo = new MemSinkTwo<>();
            MemSinkThree<IVertex<Integer, String>> sinkThree = new MemSinkThree<>();
            MemSinkFour<IVertex<Integer, String>> sinkFour = new MemSinkFour<>();
            conf.put(SCHEDULE_PERIOD, "1");
            IPipelineResult result = submitOne(environment, sinkOne, sinkTwo, sinkThree, sinkFour);
            if (!result.isSuccess()) {
                throw new GeaflowRuntimeException("execute pipeline failed");
            }
            result.get();
            if (10 > curStage) {
                LOGGER.warn("startStage10 dur:" + (System.currentTimeMillis() - curTime));
                curTime = System.currentTimeMillis();
                curStage = 10D;
            }
            // 并发写
            CountDownLatch writeLatch = new CountDownLatch(4);
            es.execute(() -> {
                try {
                    writeFile(writers[0], RESULT_FILE_PATH + "/result1.csv", sinkOne.arrayList, 2);
                } catch (IOException e) {
                    LOGGER.error("writeFailed:", e);
                } finally {
                    writeLatch.countDown();
                }
            });
            es.execute(() -> {
                try {
                    writeFile(writers[1], RESULT_FILE_PATH + "/result2.csv", sinkTwo.arrayList, 0);
                } catch (IOException e) {
                    LOGGER.error("writeFailed:", e);
                } finally {
                    writeLatch.countDown();
                }
            });
            es.execute(() -> {
                try {
                    writeFile(writers[2], RESULT_FILE_PATH + "/result3.csv", sinkThree.arrayList, 2);
                } catch (IOException e) {
                    LOGGER.error("writeFailed:", e);
                } finally {
                    writeLatch.countDown();
                }
            });
            es.execute(() -> {
                try {
                    writeFile(writers[3], RESULT_FILE_PATH + "/result4.csv", sinkFour.arrayList, 2);
                } catch (IOException e) {
                    LOGGER.error("writeFailed:", e);
                } finally {
                    writeLatch.countDown();
                }
            });
            writeLatch.await();
            if (12 > curStage) {
                LOGGER.warn("startStage12 dur:" + (System.currentTimeMillis() - curTime));
                curTime = System.currentTimeMillis();
                curStage = 12D;
            }
            ProcessBuilder processBuilder = new ProcessBuilder();
            System.out.println("costMs" + (System.currentTimeMillis() - start));
            // 切腹自尽！快速结束进程
            processBuilder.command("kill", "-9", ProcessUtil.getProcessId() + "");
            processBuilder.start();
            Thread.sleep(100);
            System.exit(0);
        } catch (Exception e) {
            LOGGER.error("ExitException:", e);
        }
    }

    public static IPipelineResult submitOne(Environment environment, SinkFunction<IVertex<Integer, String>> sinkOne, SinkFunction<IVertex<Integer, String>> sinkTwo, SinkFunction<IVertex<Integer, String>> sinkThree, SinkFunction<IVertex<Integer, String>> sinkFour) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();
            if (0 > curStage) {
                LOGGER.warn("startStage0 dur:" + (System.currentTimeMillis() - curTime));
                curTime = System.currentTimeMillis();
                curStage = 0D;
            }
            try {
                readRead.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            PWindowSource<IVertex<Integer, String>> personVts = pipelineTaskCxt.buildSource(new CollectionSource<>(vertexList), AllWindow.getInstance()).withParallelism(readParallel);
            PWindowSource<IEdge<Integer, Integer>> prEdges = pipelineTaskCxt.buildSource(new CollectionSource<>(edgeList), AllWindow.getInstance()).withParallelism(readParallel);
            if (1 > curStage) {
                LOGGER.warn("startStage1 dur:" + (System.currentTimeMillis() - curTime));
                curTime = System.currentTimeMillis();
                curStage = 1D;
            }
            int iterationParallelism = itrParallel;
            GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView(GraphViewBuilder.DEFAULT_GRAPH).withShardNum(iterationParallelism).withBackend(BackendType.Memory).build();
            PGraphWindow<Integer, String, Integer> graphWindow = pipelineTaskCxt.buildWindowStreamGraph(personVts, prEdges, graphViewDesc);
            if (2 > curStage) {
                LOGGER.warn("startStage2 dur:" + (System.currentTimeMillis() - curTime));
                curTime = System.currentTimeMillis();
                curStage = 1D;
            }
            graphWindow.compute(new PRAlgorithms(5)).compute(iterationParallelism).getVertices().sink(e -> {
                try {
                    if (StringUtils.isNotBlank(e.getValue())) {
                        String[] xx = e.getValue().split("&");
                        // case1
                        if (e.getId() < accountStart) {
                            // person 节点数据解析，case1在前,
                            if (xx.length > 0 && !xx[0].contains("-") && StringUtils.isNotBlank(xx[0])) {
                                sinkOne.write(new PVertex(e.getId(), xx[0]));
                            }
                            // case4，编码格式 “loanIDA-loanAmountA|loanIDB-loanAmountB|loanIDC-loanAmountC”
                            // case4 由于无法在3 4迭代内确认第五轮迭代还会不会收到三级担保子节点的消息，所以不能提前计算
                            // 但是第五轮迭代只有有三级担保子节点的person会收到消息，在第五轮直接计算赋值也不能覆盖所有有担保子节点的person
                            // 所以只能每个person在迭代过程中，将收到的所有1-3级子节点的loan，都存在顶点属性中，在最终的sink计算时遍历加和，输出
                            if (e.getValue().contains("-")) {
                                double d = Arrays.stream(xx[xx.length - 1].split("\\|")).distinct().filter(StringUtils::isNotBlank).mapToDouble(x -> Double.parseDouble(x.split("-")[1])).sum();
                                sinkFour.write(new PVertex(e.getId(), BigDecimal.valueOf(d / (10000 * 10000)).setScale(2, RoundingMode.HALF_UP).toPlainString()));
                            }
                        } else if (e.getId() < loanStart) {
                            if (xx.length > 0 && !xx[0].contains("-")) {
                                // case3
                                if (xx[0].length() > 0) {
                                    sinkThree.write(new PVertex(e.getId(), xx[0]));
                                }
                                // case2
                                if (xx.length > 1 && !xx[1].contains("-") && !xx[1].equals("0")) {
                                    sinkTwo.write(new PVertex(e.getId(), xx[1]));
                                }
                            }
                        }
                    }
                } catch (Exception ex) {
                    LOGGER.error("parseResultError ", ex);
                }
            }).withParallelism(1);
        });
        return pipeline.execute();
    }

    // 自定义跨节点消息编码类，加速序列化效率
    public static class MEncoder extends AbstractEncoder<MValue> {
        @Override
        public void encode(MValue data, OutputStream outputStream) throws IOException {
            if (data == null) {
                Encoders.INTEGER.encode(NULL, outputStream);
                return;
            }
            ByteEncoder.INSTANCE.encode(data.typ, outputStream);
            IntegerEncoder.INSTANCE.encode(data.src + 1, outputStream);
            StringEncoder.INSTANCE.encode(data.valMapStr, outputStream);
            if (data.valMap != null && data.valMap.size() > 0) {
                IntegerEncoder.INSTANCE.encode(data.valMap.size() + 1, outputStream);
                for (Map.Entry<Integer, Double> et : data.valMap.entrySet()) {
                    IntegerEncoder.INSTANCE.encode(et.getKey(), outputStream);
                    DoubleEncoder.INSTANCE.encode(et.getValue(), outputStream);
                }
            } else {
                IntegerEncoder.INSTANCE.encode(1, outputStream);
            }
        }

        @Override
        public MValue decode(InputStream inputStream) throws IOException {
            MValue mValue = new MValue();
            mValue.typ = Encoders.BYTE.decode(inputStream);
            mValue.src = Encoders.INTEGER.decode(inputStream) - 1;
            mValue.valMapStr = Encoders.STRING.decode(inputStream);
            int size = Encoders.INTEGER.decode(inputStream) - 1;
            if (size > 0) {
                Map<Integer, Double> vmap = new HashMap<>(size);
                for (int i = 0; i < size; i++) {
                    vmap.put(Encoders.INTEGER.decode(inputStream), Encoders.DOUBLE.decode(inputStream));
                }
                mValue.valMap = vmap;
            }
            return mValue;
        }
    }

    public static class PRAlgorithms extends VertexCentricCompute<Integer, String, Integer, MValue> {

        public PRAlgorithms(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Integer, String, Integer, MValue> getComputeFunction() {
            return new PRVertexCentricComputeFunction();
        }

        @Override
        public IEncoder<MValue> getMessageEncoder() {
            return new MEncoder();
        }


        @Override
        public VertexCentricCombineFunction<MValue> getCombineFunction() {
            return null;
        }

        public class PRVertexCentricComputeFunction implements VertexCentricComputeFunction<Integer, String, Integer, MValue> {
            public VertexCentricComputeFuncContext<Integer, String, Integer, MValue> context;

            @Override
            public void init(VertexCentricComputeFuncContext<Integer, String, Integer, MValue> context) {
                this.context = context;
            }

            @Override
            public void finish() {
            }
            @Override
            public void compute(Integer vertexId, Iterator<MValue> messageIterator) {
                if (context != null) {
                    IVertex<Integer, String> vertex = context.vertex().get();
                    try {
                        switch ((int) context.getIterationId()) {
                            case 1:
                                // 第一次迭代处理
                                if (vertex.getId() > loanStart) {
                                    String val = vertex.getId() + "-" + vertex.getValue();
                                    Double dd = Double.parseDouble(vertex.getValue());
                                    List<IEdge<Integer, Integer>> edges = context.edges().getEdges();
                                    Set<Integer> sended1 = new HashSet<>();
                                    for (IEdge<Integer, Integer> edge : edges) {
                                        // Case1 loan节点发送消息到所有depositAccount
                                        if (edge.getDirect() == EdgeDirection.OUT && edge.getValue().equals(eValueZero3)) {
                                            if (!sended1.contains(edge.getTargetId())) {
                                                sended1.add(edge.getTargetId());
                                                context.sendMessage(edge.getTargetId(), MValue.depositLoanMap(vertex.getId(), dd));
                                            }
                                        }
                                        // Case4 loan节点发送消息到所有applyAccount
                                        if (edge.getDirect() == EdgeDirection.IN && edge.getValue().equals(eValueZero8)) {
                                            context.sendMessage(edge.getTargetId(), MValue.applyLoanMap(val));
                                        }
                                    }
                                }
                                // Case2 account节点将自己收到的transferAccount及次数传递给下一跳转账客户
                                // 这样所有src节点都拿到了，dst->other这个edge2的数量
                                // Case3 计算inEdge7 outEdge2 比值
                                else if (vertex.getId() > accountStart) {
                                    List<IEdge<Integer, Integer>> edges = context.edges().getEdges();
                                    Double in = 0D;
                                    Double out = 0D;
                                    for (IEdge<Integer, Integer> edge : edges) {
                                        if (edge.getDirect() == EdgeDirection.OUT && edge.getValue() < eValueZero1) {
                                            out += ((double) edge.getValue()) / 100;
                                        }
                                    }
                                    Map<Integer, Double> transferCnt = new HashMap<>();
                                    for (IEdge<Integer, Integer> edge : edges) {
                                        if (edge.getDirect() == EdgeDirection.IN && edge.getValue() < eValueZero1) {
                                            in += ((double) edge.getValue()) / 100;
                                            transferCnt.put(edge.getTargetId(), transferCnt.getOrDefault(edge.getTargetId(), 0D) + 1);
                                        }
                                    }
                                    if (in > 0 && out > 0) {
                                        context.setNewVertexValue(BigDecimal.valueOf(in / out).setScale(2, RoundingMode.HALF_UP).toPlainString());
                                    }
                                    if (transferCnt.size() > 0) {
                                        Set<Integer> sended = new HashSet<>();
                                        for (IEdge<Integer, Integer> edge : edges) {
                                            if (edge.getDirect() == EdgeDirection.OUT && edge.getValue() < eValueZero1 && !sended.contains(edge.getTargetId())) {
                                                sended.add(edge.getTargetId());
                                                context.sendMessage(edge.getTargetId(), MValue.transferCntMap(vertex.getId(), transferCnt));
                                            }
                                        }
                                    }
                                }
                                break;
                            case 2:
                                // 第二次迭代处理
                                // Case1 account节点处理loan消息，并把loanMap传递给所有transferAccount
                                // Case2 account节点处理自己收到的transferCntMap，即dst->other数量
                                // 然后开始计算src->dst->other->src 环路数量
                                if (vertex.getId() > accountStart && vertex.getId() < loanStart) {
                                    if (messageIterator.hasNext()) {
                                        List<IEdge<Integer, Integer>> edges = context.edges().getEdges();
                                        Map<Integer, Double> depositLoanMap = null;
                                        Map<Integer, Map<Integer, Double>> dstToOther = null;
                                        while (messageIterator.hasNext()) {
                                            MValue mValue = messageIterator.next();
                                            if (mValue.typ == 1) {
                                                if (depositLoanMap == null) {
                                                    depositLoanMap = mValue.valMap;
                                                } else {
                                                    depositLoanMap.putAll(mValue.valMap);
                                                }
                                            }
                                            if (mValue.typ == 3) {
                                                if (dstToOther == null) {
                                                    dstToOther = new HashMap<>(2);
                                                    dstToOther.put(mValue.src, mValue.valMap);
                                                } else {
                                                    dstToOther.put(mValue.src, mValue.valMap);
                                                }
                                            }
                                        }
                                        Map<Integer, Double> srcToDst = new HashMap<>();
                                        Map<Integer, Double> otherToSrc = new HashMap<>();
                                        for (IEdge<Integer, Integer> edge : edges) {
                                            if (edge.getDirect() == EdgeDirection.OUT && edge.getValue() < eValueZero1) {
                                                if (!srcToDst.containsKey(edge.getTargetId())) {
                                                    if (depositLoanMap != null && depositLoanMap.size() > 0) {
                                                        context.sendMessage(edge.getTargetId(), MValue.depositLoanMap(depositLoanMap));
                                                    }
                                                    srcToDst.put(edge.getTargetId(), 1D);
                                                } else {
                                                    srcToDst.put(edge.getTargetId(), srcToDst.get(edge.getTargetId()) + 1);
                                                }
                                            }
                                            if (edge.getDirect() == EdgeDirection.IN && edge.getValue() < eValueZero1) {
                                                otherToSrc.put(edge.getTargetId(), otherToSrc.getOrDefault(edge.getTargetId(), 0D) + 1);
                                            }
                                        }
                                        int cnt = 0;
                                        if (dstToOther != null) {
                                            for (Integer other : dstToOther.keySet()) {
                                                Map<Integer, Double> dstToOtherEdge = dstToOther.get(other);
                                                for (Integer dst : dstToOtherEdge.keySet()) {
                                                    if (srcToDst.containsKey(dst)) {
                                                        cnt += srcToDst.get(dst) * dstToOtherEdge.get(dst) * otherToSrc.get(other);
                                                    }
                                                }
                                            }
                                        }
                                        context.setNewVertexValue(vertex.getValue() + "&" + cnt);
                                    }
                                }
                                // Case4 person节点处理loan消息，往“担保父节点”发送自身申请的贷款金额Map
                                // 此时person节点拿到了自己申请的所有贷款金额
                                else if (vertex.getId() < accountStart) {
                                    if (messageIterator.hasNext()) {
                                        List<IEdge<Integer, Integer>> inEdges = context.edges().getInEdges();
                                        StringBuilder applyLoanMap = new StringBuilder();
                                        while (messageIterator.hasNext()) {
                                            MValue mValue = messageIterator.next();
                                            if (mValue.typ == 2 && StringUtils.isNotBlank(mValue.valMapStr)) {
                                                applyLoanMap.append("|").append(mValue.valMapStr);
                                            }
                                        }
                                        for (IEdge<Integer, Integer> edge : inEdges) {
                                            if (Objects.equals(edge.getValue(), eValueZero9)) {
                                                if (StringUtils.isNotBlank(applyLoanMap.toString())) {
                                                    context.sendMessage(edge.getTargetId(), MValue.applyLoanMap(applyLoanMap.toString()));
                                                }
                                            }
                                        }
                                    }
                                }
                                break;
                            case 3:
                                // 第三次迭代处理
                                // Case1: account节点处理loanMap消息，并把loanMap传递给ownPerson
                                if (vertex.getId() > accountStart && vertex.getId() < loanStart) {
                                    List<IEdge<Integer, Integer>> inEdges = context.edges().getInEdges();
                                    Map<Integer, Double> longDoubleMap = null;
                                    while (messageIterator.hasNext()) {
                                        MValue mValue = messageIterator.next();
                                        if (longDoubleMap == null) {
                                            longDoubleMap = mValue.valMap;
                                        } else {
                                            longDoubleMap.putAll(mValue.valMap);
                                        }
                                    }
                                    for (IEdge<Integer, Integer> edge : inEdges) {
                                        if (edge.getValue().equals(eValueZero1)) {
                                            context.sendMessage(edge.getTargetId(), MValue.depositLoanMap(longDoubleMap));
                                        }
                                    }
                                }
                                // Case4 person节点处理loan消息，往“担保父节点”发送自身申请的贷款金额Map
                                // 此时person节点可以拿到所有1级子节点的申请贷款金额
                                else if (vertex.getId() < accountStart) {
                                    if (messageIterator.hasNext()) {
                                        List<IEdge<Integer, Integer>> inEdges = context.edges().getInEdges();
                                        StringBuilder applyLoanMap = new StringBuilder();
                                        while (messageIterator.hasNext()) {
                                            MValue mValue = messageIterator.next();
                                            if (mValue.typ == 2) {
                                                applyLoanMap.append("|").append(mValue.valMapStr);
                                            }
                                        }
                                        for (IEdge<Integer, Integer> edge : inEdges) {
                                            if (edge.getValue().equals(eValueZero9)) {
                                                if (StringUtils.isNotBlank(applyLoanMap.toString())) {
                                                    context.sendMessage(edge.getTargetId(), MValue.applyLoanMap(applyLoanMap.toString()));
                                                }
                                            }
                                        }
                                        context.setNewVertexValue(applyLoanMap.toString());
                                    }
                                }
                                break;
                            case 4:
                                // 第四次迭代处理
                                // Case1: person节点处理loanMap消息，计算贷款总额
                                // Case4  person节点处理担保子节点贷款金额Map，汇总后再向担保父节点传递贷款金额Map
                                // 此时person节点可以拿到所有2级子节点的申请贷款金额
                                if (vertex.getId() < accountStart) {
                                    Map<Integer, Double> longDoubleMap = null;
                                    StringBuilder curStr = new StringBuilder();
                                    if (vertex.getValue().contains("-")) {
                                        curStr.append(vertex.getValue());
                                    }
                                    while (messageIterator.hasNext()) {
                                        MValue mValue = messageIterator.next();
                                        if (mValue.typ == 1 && mValue.valMap != null && mValue.valMap.size() > 0) {
                                            if (longDoubleMap == null) {
                                                longDoubleMap = mValue.valMap;
                                            } else {
                                                longDoubleMap.putAll(mValue.valMap);
                                            }
                                        }
                                        if (mValue.typ == 2 && StringUtils.isNotBlank(mValue.valMapStr)) {
                                            curStr.append("|").append(mValue.valMapStr);
                                        }
                                    }
                                    String cur = curStr.toString();
                                    double v = 0;
                                    if (longDoubleMap != null && longDoubleMap.size() > 0) {
                                        for (Double d : longDoubleMap.values()) {
                                            v += d;
                                        }
                                    }
                                    if (v > 0) {
                                        if (StringUtils.isNotBlank(cur)) {
                                            context.setNewVertexValue(BigDecimal.valueOf(v / (10000 * 10000)).setScale(2, RoundingMode.HALF_UP).toPlainString() + "&" + cur);
                                        } else {
                                            context.setNewVertexValue(BigDecimal.valueOf(v / (10000 * 10000)).setScale(2, RoundingMode.HALF_UP).toPlainString());
                                        }
                                    } else {
                                        if (StringUtils.isNotBlank(cur)) {
                                            context.setNewVertexValue("&" + cur);
                                        } else {
                                            context.setNewVertexValue("&");
                                        }
                                    }
                                    if (cur.length() > 0) {
                                        List<IEdge<Integer, Integer>> inEdges = context.edges().getInEdges();
                                        for (IEdge<Integer, Integer> edge : inEdges) {
                                            if (edge.getValue().equals(eValueZero9)) {
                                                context.sendMessage(edge.getTargetId(), MValue.applyLoanMap(cur));
                                            }
                                        }
                                    }
                                }
                                break;
                            case 5:
                                // Case4  person节点处理担保子节点贷款金额Map，汇总计算结果
                                // 此时person节点拿到了所有1级、2级、3级子节点的申请贷款金额
                                if (vertex.getId() < accountStart) {
                                    String[] xx = vertex.getValue().split("&");
                                    StringBuilder stb = new StringBuilder();
                                    boolean haveMap = vertex.getValue().contains("-");
                                    if (haveMap) {
                                        stb.append(xx[xx.length - 1]);
                                    }
                                    while (messageIterator.hasNext()) {
                                        MValue mValue = messageIterator.next();
                                        if (mValue.typ == 2 && StringUtils.isNotBlank(mValue.valMapStr)) {
                                            stb.append("|").append(mValue.valMapStr);
                                        }
                                    }
                                    if (haveMap) {
                                        xx[xx.length - 1] = stb.toString();
                                        if (xx.length == 1) {
                                            context.setNewVertexValue("&" + xx[0]);
                                        } else {
                                            context.setNewVertexValue(StringUtils.join(xx, "&"));
                                        }
                                    } else {
                                        context.setNewVertexValue(vertex.getValue() + "&" + stb);
                                    }

                                }
                                break;
                            default:

                        }
                    } catch (Exception e) {
                        LOGGER.error("executeError", e);
                    }
                }
            }
        }
    }

    public static void readAllData() throws InterruptedException {
        es.submit(() -> {
            try {
                LOGGER.info("StartReadFile");
                String person = REF_FILE_PATH + "/Person.csv";
                String account = REF_FILE_PATH + "/Account.csv";
                String loan = REF_FILE_PATH + "/Loan.csv";
                File personFile = new File(person);
                File accountFile = new File(account);
                File loanFile = new File(loan);
                CountDownLatch latch = new CountDownLatch(3);
                // 优化策略：
                // 1.使用文件“行号”代替原有ID，加速id索引效率，不同类型节点再加上不同的初始值，进而可以使用“号段”区分类型
                // 2.只有Loan节点需要读取loanAmount这一属性值，其他节点不需要读取属性
                // 3.只有accountTransferAccount边需要读取属性转账金额，其他边不需要读取属性
                // 4.使用多线程并发读取
                es.execute(() -> {
                    try {
                        AtomicInteger num = new AtomicInteger(personStart);
                        BufferedReader personReader = new BufferedReader(new InputStreamReader(Files.newInputStream(personFile.toPath())));
                        vertexList.addAll(personReader.lines().filter(e -> !e.startsWith("personId")).map(e -> {
                            String[] fields = e.split("\\|");
                            int id = num.addAndGet(1);
                            Long idx = Long.parseLong(fields[0]);
                            idMap.put(id, idx);
                            idRvtMap1.put(idx, id);
                            return new PVertex(id, "");
                        }).collect(Collectors.toList()));
                    } catch (Exception e) {
                        LOGGER.error("readError", e);
                    } finally {
                        latch.countDown();
                    }
                });
                es.execute(() -> {
                    try {
                        AtomicInteger num = new AtomicInteger(accountStart);
                        BufferedReader accountReader = new BufferedReader(new InputStreamReader(Files.newInputStream(accountFile.toPath())));
                        vertexList.addAll(accountReader.lines().parallel().filter(e -> e.charAt(0) < 90).map(e -> {
                            String[] fields = e.split("\\|");
                            int id = num.addAndGet(1);
                            Long idx = Long.parseLong(fields[0]);
                            idMap.put(id, idx);
                            idRvtMap2.put(idx, id);
                            return new PVertex(id, "");
                        }).collect(Collectors.toList()));
                    } catch (Exception e) {
                        LOGGER.error("readError", e);
                    } finally {
                        latch.countDown();
                    }
                });
                es.execute(() -> {
                    try {
                        AtomicInteger num = new AtomicInteger(loanStart);
                        BufferedReader loanReader = new BufferedReader(new InputStreamReader(Files.newInputStream(loanFile.toPath())));
                        vertexList.addAll(loanReader.lines().filter(e -> !e.startsWith("loanId")).map(e -> {
                            String[] fields = e.split("\\|");
                            int id = num.addAndGet(1);
                            Long idx = Long.parseLong(fields[0]);
                            idMap.put(id, idx);
                            idRvtMap3.put(idx, id);
                            return new PVertex(id, fields[1]);
                        }).collect(Collectors.toList()));
                    } catch (Exception e) {
                        LOGGER.error("readError", e);
                    } finally {
                        latch.countDown();
                    }
                });
                latch.await();
                CountDownLatch latch2 = new CountDownLatch(5);
                String personOwn = REF_FILE_PATH + "/PersonOwnAccount.csv";
                String accountTransfer = REF_FILE_PATH + "/AccountTransferAccount.csv";
                String loanDeposit = REF_FILE_PATH + "/LoanDepositAccount.csv";
                String personApply = REF_FILE_PATH + "/PersonApplyLoan.csv";
                String personGuarantee = REF_FILE_PATH + "/PersonGuaranteePerson.csv";
                File personOwnFile = new File(personOwn);
                File accountTransferFile = new File(accountTransfer);
                File loanDepositFile = new File(loanDeposit);
                File personApplyFile = new File(personApply);
                File personGuaranteeFile = new File(personGuarantee);
                es.execute(() -> {
                    try {
                        BufferedReader personReader = new BufferedReader(new InputStreamReader(Files.newInputStream(personOwnFile.toPath())));
                        edgeList.addAll(personReader.lines().filter(e -> !e.startsWith("personId")).map(e -> {
                            String[] fields = e.split("\\|");
                            return new PEdge(idRvtMap2.get(Long.parseLong(fields[1])), idRvtMap1.get(Long.parseLong(fields[0])), eValueZero1, false);
                        }).collect(Collectors.toList()));
                    } catch (Exception e) {
                        LOGGER.error("readError", e);
                    } finally {
                        latch2.countDown();
                    }
                });
                BigDecimal oneH = new BigDecimal(100);
                es.execute(() -> {
                    try {
                        long st = System.currentTimeMillis();
                        BufferedReader accountReader = new BufferedReader(new InputStreamReader(Files.newInputStream(accountTransferFile.toPath())));
                        edgeList.addAll(accountReader.lines().parallel().flatMap(e -> {
                            if (e.charAt(0) > 90) {
                                return Stream.of();
                            }
                            String[] fields = e.split("\\|");
                            Integer val = new BigDecimal(fields[2]).multiply(oneH).setScale(0, RoundingMode.HALF_UP).intValue();
                            IEdge<Integer, Integer> e1 = new PEdge(idRvtMap2.get(Long.parseLong(fields[0])), idRvtMap2.get(Long.parseLong(fields[1])), val, true);
                            IEdge<Integer, Integer> e2 = new PEdge(idRvtMap2.get(Long.parseLong(fields[1])), idRvtMap2.get(Long.parseLong(fields[0])), val, false);
                            return Stream.of(e1, e2);
                        }).collect(Collectors.toList()));
                        LOGGER.info("readAccount {}", System.currentTimeMillis() - st);
                    } catch (Exception e) {
                        LOGGER.error("readError", e);
                    } finally {
                        latch2.countDown();
                    }
                });
                es.execute(() -> {
                    try {
                        BufferedReader loanReader = new BufferedReader(new InputStreamReader(Files.newInputStream(loanDepositFile.toPath())));
                        edgeList.addAll(loanReader.lines().filter(e -> !e.startsWith("loanId")).flatMap(e -> {
                            String[] fields = e.split("\\|");
                            IEdge<Integer, Integer> e1 = new PEdge(idRvtMap3.get(Long.parseLong(fields[0])), idRvtMap2.get(Long.parseLong(fields[1])), eValueZero3, true);
//                    IEdge<Integer, EValue> e2 = new ValueEdge<>(idRvtMap3.get(2).get(Long.parseLong(fields[1])), IntegerMap.get(3).get(Long.parseLong(fields[0])), new EValue((byte) 6, 0D), EdgeDirection.IN);
                            return Stream.of(e1);
                        }).collect(Collectors.toList()));
                    } catch (Exception e) {
                        LOGGER.error("readError", e);
                    } finally {
                        latch2.countDown();
                    }
                });
                es.execute(() -> {
                    try {
                        BufferedReader personApplyReader = new BufferedReader(new InputStreamReader(Files.newInputStream(personApplyFile.toPath())));
                        edgeList.addAll(personApplyReader.lines().filter(e -> !e.startsWith("personId")).flatMap(e -> {
                            String[] fields = e.split("\\|");
//                return new ValueEdge<>(IntegerMap.get(1).get(Long.parseLong(fields[0])), IntegerMap.get(3).get(Long.parseLong(fields[1])), new EValue((byte) 4, 0D));
                            IEdge<Integer, Integer> e2 = new PEdge(idRvtMap3.get(Long.parseLong(fields[1])), idRvtMap1.get(Long.parseLong(fields[0])), eValueZero8, false);
                            return Stream.of(e2);
                        }).collect(Collectors.toList()));
                    } catch (Exception e) {
                        LOGGER.error("readError", e);
                    } finally {
                        latch2.countDown();
                    }
                });
                es.execute(() -> {
                    try {
                        BufferedReader personGuaranteeReader = new BufferedReader(new InputStreamReader(Files.newInputStream(personGuaranteeFile.toPath())));
                        edgeList.addAll(personGuaranteeReader.lines().filter(e -> !e.startsWith("fromId")).flatMap(e -> {
                            String[] fields = e.split("\\|");
                            IEdge<Integer, Integer> e1 = new PEdge(idRvtMap1.get(Long.parseLong(fields[1])), idRvtMap1.get(Long.parseLong(fields[0])), eValueZero9, false);
//                IEdge<Integer, EValue> e2 = new ValueEdge<>(IntegerMap.get(1).get(Long.parseLong(fields[0])), IntegerMap.get(1).get(Long.parseLong(fields[1])), new EValue((byte) 5, 0D));
                            return Stream.of(e1);
                        }).collect(Collectors.toList()));
                    } catch (Exception e) {
                        LOGGER.error("readError", e);
                    } finally {
                        latch2.countDown();
                    }
                });
                latch2.await();
                readRead.countDown();
            } catch (InterruptedException e) {
                LOGGER.error("xxxx", e);
            }
        });
    }

    public static void writeFile(BufferedWriter dataWriter, String path, ConcurrentLinkedQueue<IVertex<Integer, String>> outData, int scale) throws IOException {
        StringBuilder out = new StringBuilder();
        out.append("id|value\n");
        if (path.contains("result3")) {
            outData.stream().sorted(Comparator.comparingLong(e -> idMap.get(e.getId()))).parallel().forEachOrdered(e -> {
                out.append(idMap.get(e.getId())).append('|').append(e.getValue()).append('\n');
            });
        } else {
            outData.stream().sorted(Comparator.comparingLong(e -> idMap.get(e.getId()))).parallel().forEachOrdered(e -> {
                out.append(idMap.get(e.getId())).append('|').append(e.getValue()).append('\n');
            });
        }
        dataWriter.write(out.substring(0, out.length() - 1));
        dataWriter.flush();

    }

    public static class MemSinkOne<OUT> extends RichFunction implements SinkFunction<OUT> {
        static ConcurrentLinkedQueue arrayList = new ConcurrentLinkedQueue<>();

        public MemSinkOne() {
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
        }

        @Override
        public void close() {
        }

        @Override
        public void write(OUT out) {
            if (out != null) {
                arrayList.add(out);
            }
        }
    }

    public static class MemSinkTwo<OUT> extends RichFunction implements SinkFunction<OUT> {
        static ConcurrentLinkedQueue arrayList = new ConcurrentLinkedQueue<>();

        public MemSinkTwo() {
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
        }

        @Override
        public void close() {
        }

        @Override
        public void write(OUT out) {
            if (out != null) {
                arrayList.add(out);
            }
        }
    }

    public static class MemSinkThree<OUT> extends RichFunction implements SinkFunction<OUT> {
        static ConcurrentLinkedQueue arrayList = new ConcurrentLinkedQueue<>();

        public MemSinkThree() {
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
        }

        @Override
        public void close() {
        }

        @Override
        public void write(OUT out) {
            if (out != null) {
                arrayList.add(out);
            }
        }
    }

    public static class MemSinkFour<OUT> extends RichFunction implements SinkFunction<OUT> {
        static ConcurrentLinkedQueue arrayList = new ConcurrentLinkedQueue<>();

        public MemSinkFour() {
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
        }

        @Override
        public void close() {
        }

        @Override
        public void write(OUT out) {
            if (out != null) {
                arrayList.add(out);
            }
        }
    }

    public static void setDefaultOption() throws NoSuchFieldException, IllegalAccessException {
        RpcClientOptions clientOption = new RpcClientOptions();
        clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        int timeout = 1000000;
        clientOption.setWriteTimeoutMillis(timeout);
        clientOption.setReadTimeoutMillis(timeout);
        clientOption.setConnectTimeoutMillis(2 * timeout);
        clientOption.setMaxTotalConnections(2);
        clientOption.setMinIdleConnections(2);
        clientOption.setWorkThreadNum(2);
        clientOption.setIoThreadNum(1);
        clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
        clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
        clientOption.setChannelType(ChannelType.POOLED_CONNECTION);
        DefaultClientOption p = new DefaultClientOption();
        Field option = DefaultClientOption.class.getDeclaredField("clientOption");
        option.setAccessible(true);
        option.set(p, clientOption);

    }

    public static class MValue implements KryoSerializable {
        public byte typ;
        public int src;
        public String valMapStr = "";
        public Map<Integer, Double> valMap;

        public MValue() {

        }

        public MValue(byte typ) {
            this.typ = typ;
        }

        static MValue depositLoanMap(Integer fromLoan, Double loanAmount) {
            MValue mValue = new MValue((byte) 1);
            mValue.valMap = new HashMap<>(1);
            mValue.valMap.put(fromLoan, loanAmount);
            return mValue;
        }

        static MValue applyLoanMap(String str) {
            MValue mValue = new MValue((byte) 2);
            mValue.valMapStr = str;
            return mValue;
        }

        static MValue depositLoanMap(Map<Integer, Double> loanMap) {
            MValue mValue = new MValue((byte) 1);
            mValue.valMap = loanMap;
            return mValue;
        }

        static MValue transferCntMap(int src, Map<Integer, Double> cntMap) {
            MValue mValue = new MValue((byte) 3);
            mValue.src = src;
            mValue.valMap = cntMap;
            return mValue;
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeByte(typ);
            output.writeInt(src, true);
            output.writeString(valMapStr);
            if (valMap != null && valMap.size() > 0) {
                output.writeInt(valMap.size());
                for (Map.Entry<Integer, Double> et : valMap.entrySet()) {
                    output.writeInt(et.getKey(), true);
                    output.writeDouble(et.getValue(), 0.01, true);
                }
            } else {
                output.writeInt(0);
            }

        }

        @Override
        public void read(Kryo kryo, Input input) {
            this.typ = input.readByte();
            this.src = input.readInt(true);
            this.valMapStr = input.readString();
            int size = input.readInt();
            if (typ == 1 && size == 0) {
                System.out.println("xx");
            }
            if (size > 0) {
                Map<Integer, Double> vmap = new HashMap<>(size);
                for (int i = 0; i < size; i++) {
                    vmap.put(input.readInt(), input.readDouble(0.01, true));
                }
                this.valMap = vmap;
            }
        }

    }

    public static class PEdge implements IEdge<Integer, Integer>, KryoSerializable {
        public int srcId;
        public int targetId;
        public int value;
        public boolean isOut = true;

        public PEdge() {

        }

        public PEdge(int srcId, int targetId, int value, boolean isOut) {
            this.srcId = srcId;
            this.targetId = targetId;
            this.value = value;
            this.isOut = isOut;
        }

        @Override
        public Integer getSrcId() {
            return srcId;
        }

        @Override
        public void setSrcId(Integer srcId) {
            this.srcId = srcId;
        }

        @Override
        public Integer getTargetId() {
            return targetId;
        }

        @Override
        public void setTargetId(Integer targetId) {
            this.targetId = targetId;
        }

        @Override
        public EdgeDirection getDirect() {
            return isOut ? EdgeDirection.OUT : EdgeDirection.IN;
        }

        @Override
        public void setDirect(EdgeDirection direction) {
            this.isOut = direction == EdgeDirection.OUT;
        }

        @Override
        public Integer getValue() {
            return value;
        }

        @Override
        public IEdge<Integer, Integer> withValue(Integer value) {
            this.value = value;
            return this;
        }

        @Override
        public IEdge<Integer, Integer> reverse() {
            return new PEdge(this.srcId, this.targetId, value, !this.isOut);
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeInt(this.srcId, true);
            output.writeInt(this.targetId, true);
            output.writeInt(this.value, true);
            output.writeBoolean(isOut);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            this.srcId = input.readInt(true);
            this.targetId = input.readInt(true);
            this.value = input.readInt(true);
            this.isOut = input.readBoolean();

        }
    }

    public static class PVertex implements IVertex<Integer, String>, KryoSerializable {
        int id;
        String val;

        public PVertex(int idx, String value) {
            this.id = idx;
            this.val = value;
        }

        @Override
        public Integer getId() {
            return id;
        }

        @Override
        public void setId(Integer id) {
            this.id = id;
        }

        @Override
        public String getValue() {
            return val;
        }

        @Override
        public IVertex<Integer, String> withValue(String value) {
            this.val = value;
            return this;
        }

        @Override
        public IVertex<Integer, String> withLabel(String label) {
            return new ValueLabelVertex<>(this.id, this.val, label);
        }

        @Override
        public IVertex<Integer, String> withTime(long time) {
            return new ValueTimeVertex<>(this.id, this.val, time);
        }

        @Override
        public int compareTo(Object o) {
            PVertex p = (PVertex) o;
            return Integer.compare(this.id, p.id);
        }

        @Override
        public boolean equals(Object o) {
            PVertex p = (PVertex) o;
            return this.id == p.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public void write(Kryo kryo, Output output) {
            // serialize id, label and value
            output.writeInt(this.id, true);
            output.writeString(this.val);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            // deserialize id, label and value
            this.id = input.readInt(true);
            this.val = input.readString();
        }
    }

}


