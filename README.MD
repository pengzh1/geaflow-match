# 优化方案总结

赛题链接 [基于TuGraph Analytics的⾼性能图模式匹配算法设计](https://www.datafountain.cn/competitions/975)

[我的github主页](https://github.com/pengzh1)

## 最终性能

首先列举一下最终各阶段耗时(基于ldbc finbench sf1数据集)，再依次介绍各个阶段做了哪些优化

- 环境1:MacBookPro M2 CPU 16G内存: 总耗时 5.5S-6.5S之间
- 环境2:Linux机器 E5-2680v4 16核 32G内存: 总耗时 9S-11S之间

分阶段统计：(每阶段耗时只统计距离上一阶段结束之后的时间)

| 阶段                                | 耗时     | 
|-----------------------------------|--------|
| 总耗时                               | 6055ms |
| 1.集群启动，PipelineTask任务开始执行         | 830ms  |
| 2.读文件完成,vertexSource和edgeSource就绪 | 30ms   |
| 3.构图完成，开启迭代计算                     | 2950ms |
| 4.第一轮计算完成                         | 950ms  |
| 5.第二轮计算完成                         | 470ms  |
| 6.第三轮计算完成                         | 135ms  |
| 7.第四轮计算完成                         | 95ms   |
| 8.第五轮计算完成，并开始执行Sink函数             | 130ms  |
| 9.Sink执行完成，任务执行结束返回结果             | 210ms  |
| 9.进行节点排序，文件写入                     | 230ms  |
| 10.写入完成,进程退出                      | 5ms    |

## 主要优化手段
前提: 未使用任何图API之外的跨节点数据关联、点边数据关联、边合并等非标优化手段

0. 参数配置，工作线程数经过实验使用8为最佳；GC策略不使用G1而使用ParallelGc，并调大新生代及Eden区空间，进而提升内存分配吞吐量
1. 预读取文件，预创建文件，并且多线程并发读取/写入，优化了阶段2/9的耗时，详见readAllData()/writeFiles()及其调用处
2. 参考关系数据库自增ID这一设计，使用文件“行号”这一隐藏属性代替原有Long
   ID，加速id索引效率，不同类型节点再加上不同的初始值，进而可以使用“号段”区分类型。这样我们通过int类型就可以表示数十亿个不重复的节点，(
   此优化在大数据集上也可以使用,在结果收集时做行号->原有ID的转换并不困难，或者也可将原有ID作为顶点的label或属性带入计算)。此方法最终很大程度上优化了构图时间和计算迭代时间
3. 自定义实现了高效Kryo序列化器的顶点/边/消息结构体，极大的提升了图序列化效率，在构图/计算阶段带来了20%左右的提升；详见
   PVertex/PEdge/MValue类定义
4. 使用了经过缜密设计的图扩散算法，基于一张图，在五轮迭代内完成了全部节点四个case结果的计算,最终计算阶段耗时不到2秒，已经比直接遍历计算内存集合性能还要高，证明了图计算的高效，
   此策略在大数据集上也会有很好的应用。算法部分较为复杂，详见代码注释
5. 剩余细节优化详见CaseKiller代码注释
