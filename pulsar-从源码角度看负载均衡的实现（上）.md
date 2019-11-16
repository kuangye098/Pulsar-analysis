# 从源码角度看 Pulsar 的负载均衡实现（上）

## 前言

Pulsar 起始就定位为 Cloud Native 的消息系统，，采用的是计算与存储分离的分布式架构，作为新一代消息系统，跟前辈 kafka 相比，最大的特性之一就是有真正的、完善的负载均衡机制，本文通过源码来解读，Pulsar 的负载均衡设计。

## 回顾 kafka 的负载均衡设计

### 1. Producer 端负载均衡

当通过向 topic 发送消息时，kafka 主要是按照如下方式进行消息发送：

* 如果消息没有设置 Key，则通过轮询方式均匀发送到各主分区。
* 如果消息有设置 Key，则对键进行散列化后，再与分区的数量取模运算。
得到分区编号，然后通过编号均匀发送到各主分区。
* 用户可以自定义路由方式来进行负载均衡。

负载均衡触发条件如下：

1. broker 个数减少会触发（最终还是影响到分区分布）。
2. topic 的分区数变更会触发。

### 2. Broker 端负载均衡

事实上，broker 端所管理的 topic 分区数是创建时就已经确定的、均匀的分布在 broker 集群主机上（即属于静态的负载均衡），系统并不会自动变更，也就是 topic 的分区数、分布情况（当然，如果 broker 宕机，系统会把宕机的 broker 上的副本转移到其他 broker 上，但这仅仅属于故障转移、多副本管理范畴）是固定不变的。一般如下场景需要变更分区，以达到负载均衡的目的。以下操作均需要运维人员手动执行 kafka-reassign-partitions.sh 命令进行负载均衡操作。

1.  对 topic 的所有分区数据进行整体迁移。假如集群有 N 个 broker，后来新扩容 M（N < M）个 broker。由于新扩容的broker磁盘都是空的，原有的 broker 磁盘占用都很满。那么可以利用上述方法，将存储在原有N个broker的某些 topic 整体搬迁到新扩容的 M 个 broker，进而实现 kafka 集群的整体数据均衡。 
2. 某些broker磁盘占用很满，某些磁盘占用又很少。可以利用kafka-reassign-partitions.sh迁移某些topic的分区数据到磁盘占用少的broker，实现数据均衡。
3. kafka 集群扩容。需要把原来 broker 的 topic 数据整体迁移到新的 broker，合理利用新扩容的 broker，实现负载均衡。 

从以上可以看出，kafka 在 broker 端无论是负载均衡还是扩容，都是需要人工干预，而且还可能出错，这无疑增大的运维成本，需要专人维护。

### 3. Consumer 端负载均衡

对于 consumer group：

1. 允许 consumer group（包含多个 consumer，如一个集群同时消费）对一个 topic 进行消费，不同的 consumer group 之间独立消费。
2. 为了对减小一个 consumer group 中不同 consumer 之间的分布式协调开销，指定分区为最小的并行消费单位，即一个 group 内的 consumer 只能消费不同的分区。

其中，consumer 与 分区具体关系如下：

1. 如果 consumer 比分区多，是浪费，因为 kafka 的设计是在一个分区上是不允许并发的，所以 consumer 数不要大于分区数。
2. 如果 consumer 比分区少，一个 consumer 会对应于多个分区，这里主要合理分配 consumer 数和分区数，否则会导致分区中的数据被拉取的不均匀。
3. 如果 consumer 从多个分区读到数据，不保证数据间的顺序性，kafka 只保证在一个分区上数据是有序的，但多个分区，读的顺序会有不同。

负载均衡触发条件如下：

1. consumer group 中 consumer 增加或减少。
2. broker 个数减少。
3. topic 的分区数变更。

综合来看，只要 broker 或者 topic 的变动，都可能导致数据的重分配，此时意味着服务将暂停一段时间。另外，负载均衡不仅仅意味着数据分布是否均衡，更重要的是主机所负载的网络流量是否也有考虑。很显然，kafka 并没有考虑这个问题（所有用户也接受部分服务短时间内不可用来扩容）。

## Pulsar 负载均衡设计

pulsar 作为消息系统中的后起之秀，在 producer 端上负载均衡设计思想是大体相同的，但是在 broker 端自适应负载均衡以及consumer 端的负载均衡却是它极大的亮点之一，本文先探讨 broker 端，那么下面来谈谈它怎么设计。

### 1. 命名空间（Namespace）的引入

pulsar 有几个资源概念，它首先引入 `ServiceUnitId` 概念，可以认为是资源调度基础单元，如下：

```java
public interface ServiceUnitId {
 //返回资源全限定名
 String toString();

 //返回此资源属于哪个命名空间
 NamespaceName getNamespaceObject();

 // 检查 topic 全限定名是否在包括在 ServiceUnitId 对象内
 boolean includes(TopicName topicName);
}
```

再看看它的3个继承类：

1. `TopicName`  简介

最小资源单元，属于系统内部概念，包含 `namespaceName` 属性, 意味着此 `topic` 归在此 `namespaceName` 上。对于外部，只需要设置 topic 名即可，对应 `TopicName` 内部的 `localName` 属性。那么，这里给出 `TopicName` 实现 `ServiceUnitId` 三个接口的实现方法，如下：

```java
@Override
public String toString() {
    return completeTopicName;
}

@Override
public NamespaceName getNamespaceObject() {
    return namespaceName;
}

@Override
public boolean includes(TopicName otherTopicName) {
    return this.equals(otherTopicName);
}
```

这里，关于全限定名（completeTopicName）的定义，这里给出源码片段：

```java
//pulsar v2.x 版本的全限定名，它与v1.x不同之处在于，剥离了 cluster 字段。
if (isV2()) {
    //例子：persistent://prop/cluster/ns/my-topic
    this.completeTopicName = String.format("%s://%s/%s/%s",domain, tenant, namespacePortion, localName);
} else {
    //例子：persistent://prop/cluster/ns/my-topic
    this.completeTopicName = String.format("%s://%s/%s/%s/%s",domain, tenant, cluster, namespacePortion, localName);
}
```

从以上可以看出，topic 是分配到`namespaceName` 内，它归属于`namespaceName` 。

2. `NamespaceName` 简介

资源分配单元，属于系统内部概念，它设计可以包含多个 topic。对于外部，只需要设置 Namespace 名即可，对应 `Namespace`内部的 `localName` 属性。那么，这里给出 `NamespaceName` 实现 `ServiceUnitId` 三个接口的实现方法，如下：

```java
@Override
public String toString() {
    return namespace;
}

@Override
public NamespaceName getNamespaceObject() {
    return this;
}

@Override
public boolean includes(TopicName topicName) {
    return this.equals(topicName.getNamespaceObject());
}
```

这里，关于全限定名（NamespaceName）的定义，这里给出源码片段：

```java
//这里，跟 topic 全限定名一样，pulsar v2.x 版本的全限定名，它与v1.x不同之处在于，剥离了 cluster 字段。
//这里是v2.x版本构造函数
public static NamespaceName get(String tenant, String namespace) {
    validateNamespaceName(tenant, namespace);
    return get(tenant + '/' + namespace);
}
//这里是v1.x版本构造函数
public static NamespaceName get(String tenant, String cluster, String namespace) {
    validateNamespaceName(tenant, cluster, namespace);
    return get(tenant + '/' + cluster + '/' + namespace);
}

```

3. `NamespaceBundle` 简介

资源分配分片，它是 `NamespaceName` 的分片，创建时可以根据策略来指定分片个数，如果没有指定，则用系统默认值4，来均匀分配 `NamespaceBundle` 个数。

```java
//NamespaceBundle的实现
@Override
public NamespaceName getNamespaceObject() {
    return this.nsname;
}

@Override
public String toString() {
    return getKey(this.nsname, this.keyRange);
}

@Override
public boolean includes(TopicName topicName) {
    if (!this.nsname.equals(topicName.getNamespaceObject())) {
        return false;
    }
    return this.keyRange.contains(factory.getLongHashCode(topicName.toString()));
}

```

关于分配 `NamespaceBundle`具体算法如下：

```java
//此方法处于 NamespacesBase 类中，创建 NamespaceName 时调用，是 NamespaceName 中 Policies 属性 BundlesData 初始化时赋值
public static BundlesData getBundles(int numBundles) {
    if (numBundles <= 0 || numBundles > MAX_BUNDLES) {
        throw new RestException(Status.BAD_REQUEST, "Invalid number of bundles. Number of numbles has to be    in the range of (0, 2^32].");
    }
    Long maxVal = ((long) 1) << 32;
    Long segSize = maxVal / numBundles;
    List<String> partitions = Lists.newArrayList();
    partitions.add(String.format("0x%08x", 0l));
    Long curPartition = segSize;
    for (int i = 0; i < numBundles; i++) {
        if (i != numBundles - 1) {
            partitions.add(String.format("0x%08x", curPartition));
        } else {
            partitions.add(String.format("0x%08x", maxVal - 1));
        }
        curPartition += segSize;
    }
    return new BundlesData(partitions);
}
//此方法处于 NamespaceBundleFactory 类中，典型的一致性Hash算法构建一个环 FIRST_BOUNDARY --> LAST_BOUNDARY
public NamespaceBundles getBundles(NamespaceName nsname, BundlesData bundleData, long version) {
    long[] partitions;
    if (bundleData == null) {
        partitions = new long[] { Long.decode(FIRST_BOUNDARY), Long.decode(LAST_BOUNDARY) };
    } else {
        partitions = new long[bundleData.boundaries.size()];
        for (int i = 0; i < bundleData.boundaries.size(); i++) {
            partitions[i] = Long.decode(bundleData.boundaries.get(i));
        }
    }
    return new NamespaceBundles(nsname, partitions, this, version);
}
```

至此，**一个完整的一致性Hash算法所形成的环完成了**。那么，读者肯定会问，形成环了，那 topic 怎么确定在哪个环分区（以下简称分区）中呢？而各个分区是怎么归属到哪个 broker 上的？解决了这两个问题，就从架构上解决了流量集中的问题，为负载均衡打下基础，更重要的是解决了 broker 水平扩容缩容的问题。

### 1. 创建并初始化负载均衡器

在 PulsarService 类 start() 方法中，系统调用  `LoadManager` 接口类方法 `create` ，如下：

```java
this.loadManager.set(LoadManager.create(this));
```

从 `create` 方法可以看出，pulsar 有两个负载均衡接口类，一个最初版本为  `LoadManager`,一个为了更直观的方法名称而创建的 `ModularLoadManager`，这两个接口类，
`ModularLoadManager` 通过包装成 `ModularLoadManagerWrapper`，以便统一加载内置或自定义负载均衡器，如下：

```java
static LoadManager create(final PulsarService pulsar) {
 try {
        final ServiceConfiguration conf = pulsar.getConfiguration();
        final Class<?> loadManagerClass = Class.forName(conf.getLoadManagerClassName());
        final Object loadManagerInstance = loadManagerClass.newInstance();
        if (loadManagerInstance instanceof LoadManager) {
            final LoadManager casted = (LoadManager) loadManagerInstance;
            casted.initialize(pulsar);
            return casted;
        } else if (loadManagerInstance instanceof ModularLoadManager) {
            final LoadManager casted = new ModularLoadManagerWrapper((ModularLoadManager) loadManagerInstance);
            casted.initialize(pulsar);
            return casted;
        }
    } catch (Exception e) {
        log.warn("Error when trying to create load manager: {}");
    }
    // 如果前面加载负载均衡器失败，则默认使用系统内置简单的负载均衡器。
    return new SimpleLoadManagerImpl(pulsar);
}
```

从以上可以看出，当系统启动时，通过反射自动加载 broker.conf 配置文件中的负载均衡器实现类，如果加载失败，则加载系统内置简单的负载均衡器 `SimpleLoadManagerImpl`。目前，系统默认的负载均衡实现类为 `org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl`，本文重点通过此类来讲讲 pulsar 负载均衡设计。

### 2. 启动负载均衡服务

#### 2.1 启动选举服务

首先，负载均衡服务中，其最关键的两个任务并不是负载均衡器来调度运行的，而是通过 zookeeper 选举服务，在 broker 间选举确定领导者（leader）后，再通过领导者的回调方法，来进行启动的，详细如下：

```java
// PulsarService 类启动领导者选举服务，一旦被选为领导者，则会启动两个任务
private void startLeaderElectionService() {
    this.leaderElectionService = new LeaderElectionService(this, new LeaderListener() {
        @Override
        public synchronized void brokerIsTheLeaderNow() {
            //如果是启用负载均衡器的，则定期执行 loadSheddingTask 和 loadResourceQuotaTask。
            if (getConfiguration().isLoadBalancerEnabled()) {
                long loadSheddingInterval = TimeUnit.MINUTES
                  .toMillis(getConfiguration().getLoadBalancerSheddingIntervalMinutes());
                long resourceQuotaUpdateInterval = TimeUnit.MINUTES .toMillis(getConfiguration().getLoadBalancerResourceQuotaUpdateIntervalMinutes());
                loadSheddingTask = loadManagerExecutor.scheduleAtFixedRate(new LoadSheddingTask(loadManager), loadSheddingInterval, loadSheddingInterval, TimeUnit.MILLISECONDS);
                loadResourceQuotaTask = loadManagerExecutor.scheduleAtFixedRate(
                new LoadResourceQuotaUpdaterTask(loadManager), resourceQuotaUpdateInterval, resourceQuotaUpdateInterval, TimeUnit.MILLISECONDS);
            }
        }

        @Override
        public synchronized void brokerIsAFollowerNow() {
            if (loadSheddingTask != null) {
                loadSheddingTask.cancel(false);
                loadSheddingTask = null;
            }
            if (loadResourceQuotaTask != null) {
                loadResourceQuotaTask.cancel(false);
                loadResourceQuotaTask = null;
            }
        }
    });
    leaderElectionService.start();
}
// LoadSheddingTask 核心方法
public void run() {
    try {
        loadManager.get().doLoadShedding();
    } catch (Exception e) {
        LOG.warn("Error during the load shedding", e);
    }
}
// LoadResourceQuotaUpdaterTask 核心方法
public void run() {
    try {
        this.loadManager.get().writeResourceQuotasToZooKeeper();
    } catch (Exception e) {
        LOG.warn("Error write resource quota", e);
    }
}
```

从以上可以看出，broker 领导者选举服务注册了一个监听类，有2个方法，一是成为领导者（leader）时需回调方法 `brokerIsTheLeaderNow`，二成为跟随者（follower）时需回调方法 `brokerIsAFollowerNow`。其中
`brokerIsTheLeaderNow` 方法，主要动作是启动两个定时任务，一为 `LoadSheddingTask` , 二为 `LoadResourceQuotaUpdaterTask`，然而，这2个任务实际上分别调用负载均衡器的
`doLoadShedding` 和 `writeResourceQuotasToZooKeeper`方法。`brokerIsAFollowerNow`方法，则是取消这2个任务的执行。关于 `doLoadShedding` 、`writeResourceQuotasToZooKeeper` 这两个方法的实现，接下来的负载均衡器实现中会一一讲述。

#### 2.2 启动负载均衡器，并开启定时负载数据上报任务

任一 broker 都会启动负载均衡器，并且定期执行负载数据上报给 zookeeper 任务（LoadReportUpdaterTask），如下：

```java
private void startLoadManagementService() throws PulsarServerException {
    LOG.info("Starting load management service ...");
    this.loadManager.get().start();

    if (config.isLoadBalancerEnabled()) {
        LOG.info("Starting load balancer");
        if (this.loadReportTask == null) {
            long loadReportMinInterval = LoadManagerShared.LOAD_REPORT_UPDATE_MIMIMUM_INTERVAL;
            this.loadReportTask = this.loadManagerExecutor.scheduleAtFixedRate(
                new LoadReportUpdaterTask(loadManager), loadReportMinInterval, loadReportMinInterval, TimeUnit.MILLISECONDS);
        }
    }
}
// LoadReportUpdaterTask 核心方法
public void run() {
    try {
        loadManager.get().writeLoadReportOnZookeeper();
    } catch (Exception e) {
        LOG.warn("Unable to write load report on Zookeeper - [{}]", e);
    }
}
```

跟`2.1`小结类似，LoadReportUpdaterTask 实际上也是直接调用负载均衡器的方法实现：writeLoadReportOnZookeeper。很显然，系统把负载均衡所有的关联方法抽象一个接口，实现一个负载均衡器，这种设计使职责非常清晰，也符合面向接口编程，用户可以根据自己的需求来进行负载均衡器定制，比如支持跨机房、跨区域资源调度等，以上方法将在负载均衡器的实现章节详细讲述。



### 3. 负载均衡器的实现

下面，来了解下负载均衡器`LoadManger` 接口主要方法（删除了一些无关紧要的方法），如下：

```java
public interface LoadManager {
    // 返回由某些特定于实现的算法或标准决定的最小加载资源单元
    Optional<ResourceUnit> getLeastLoaded(ServiceUnitId su) throws Exception;
    // 生成负载报告
    LoadManagerReport generateLoadReport() throws Exception;
    // 发布负载报告到ZK
    void writeLoadReportOnZookeeper() throws Exception;
    // 发布资源配额信息到ZK
    void writeResourceQuotasToZooKeeper() throws Exception;
    // 生成负载指标列表
    List<Metrics> getLoadBalancingMetrics();
    // 执行负载均衡操作（当一 broker 负载过高，强行将部分负载分配到负载较低的 broker 上）
    void doLoadShedding();
    // 执行命名空间集合分割操作
    void doNamespaceBundleSplit() throws Exception;
    // 从loadbalancer列表中删除当前 broker ，因此，其他 broker 无法将任何请求重定向到此 broker
    // ，并且此 broker 不会接受新的连接请求。
    public void disableBroker() throws Exception;
    // 返回集群中存活的 broker
    Set<String> getAvailableBrokers() throws Exception;
```

从章节1可知，目前 pulsar 系统目前使用的负载均衡类使用的是 `ModularLoadManagerImpl` ，但是它并不是继承以上所说接口，而是 `ModularLoadManager` 接口，然后通过包装类
来兼容 `LoadManager`,故大同小异。

经过铺垫，通过解析 `ModularLoadManagerImpl` 构建初始化来实现负载均衡的剖析。

#### 3.1 `ModularLoadManagerImpl` 关键属性

```java
// 存活的 broker zookeeper 缓存，当 broker 加入或退出，会立即通过zk更新本地缓存
private ZooKeeperChildrenCache availableActiveBrokers;
// 本地 broker 数据 zookeeper 缓存，它存储在 zookeeper 的 /loadbalance/brokers 上
private ZooKeeperDataCache<LocalBrokerData> brokerDataCache;
// 这里用于计算系统（主机）资源使用情况，其中资源包括宽带扇入扇出，内存，CPU
private BrokerHostUsage brokerHostUsage;
// 标识 broker 上存储了哪些 NamespaceBundle，是一个映射关系，其存储顺序为 broker -> namespace -> （namespace）bundleRange
private final Map<String, Map<String, Set<String>>> brokerToNamespaceToBundleRange;
// bundle 分割策略，其决定哪些 NamespaceBundle 将被分割
private BundleSplitStrategy bundleSplitStrategy;
// 用于初始化 NamespaceBundle 状态历史数据，当系统产生第一个采样数据时，将被覆盖
private final NamespaceBundleStats defaultStats;
// 此 broker 过滤器 用于 selectBrokerForAssignment() 方法
private final List<BrokerFilter> filterPipeline;
// 最近一次 broker 更新的负载数据
private LocalBrokerData lastData;
// 决定 broker 卸载 namespaceBundle 策略
private final List<LoadSheddingStrategy> loadSheddingPipeline;
// 当前 broker 运行时负载数据
private LocalBrokerData localData;
// 此核心数据结构，由各个存活的 broker 负载数据组成
private final LoadData loadData;
// 预分配 namespaceBundle 到 broker
private final Map<String, String> preallocatedBundleToBroker;
// 用于确定应放置新 topic 的位置的策略
private ModularLoadManagerStrategy placementStrategy;
// 简单的资源分配策略，即用于确定对于特定 namespace 那些 broker 是可用的策略。
private SimpleResourceAllocationPolicies policies;
// 检查给定 broker 是否可加载可持久化的或非持久化的 topic
private final BrokerTopicLoadingPredicate brokerTopicLoadingPredicate;
// broker 故障域
private Map<String, String> brokerToFailureDomainMap;
```

以上介绍了，`ModularLoadManagerImpl` 的关键属性，读者肯定还有疑问，因为负载均衡是需要依赖数据决策的，这里实际上用了哪些指标数据并没有体现出来，只是统一封装在几个类中，那么现在列一下它们的关键属性，这样，在接下来的数据分析中不至于云里雾里。

* SystemResourceUsage

SystemResourceUsage 由 `BrokerHostUsage` 组件的方法计算给出，代表一个主机各资源使用情况，其结构如下：

```java
// 扇入流量指标
public ResourceUsage bandwidthIn;
// 扇出流量指标
public ResourceUsage bandwidthOut;
// CPU使用指标
public ResourceUsage cpu;
// Java （堆内）内存使用指标
public ResourceUsage memory;
// Java （堆外）直接内存使用指标
public ResourceUsage directMemory;
```

其中，`ResourceUsage`的如下：

```java
//（当前）使用值
public double usage;
//限额值
public double limit;
```

到此，主机核心资源使用情况就描述出来了。

* NamespaceBundleStats 

NamespaceBundleStats 由 `updateBundleData` 方法维护，代表一个`NamespaceBundle`下各资源使用情况，其结构如下：

```java
// 消息入站速率
public double msgRateIn;
// 消息入站吞吐量
public double msgThroughputIn;
// 消息出站速率
public double msgRateOut;
// 消息入站吞吐量
public double msgThroughputOut;
// 消费者数
public int consumerCount;
// 生产者数
public int producerCount;
// topic 数
public long topics;
// 缓存大小
public long cacheSize;
```

* LocalBrokerData 

LocalBrokerData 由每个 broker 主机维护的负载数据，它会定时上报给 `zookeeper` 上,其结构如下：

```java
// ServiceLookupData 结构，其为静态数据
private final String webServiceUrl;
private final String webServiceUrlTls;
private final String pulsarServiceUrl;
private final String pulsarServiceUrlTls;
private boolean persistentTopicsEnabled=true;
private boolean nonPersistentTopicsEnabled=true;

// 此可以参考 SystemResourceUsage 类.
private ResourceUsage cpu;
private ResourceUsage memory;
private ResourceUsage directMemory;
private ResourceUsage bandwidthIn;
private ResourceUsage bandwidthOut;

// 以下是当前 broker 中 所有的 NamespaceBundle 对应总数据
private double msgThroughputIn;
private double msgThroughputOut;
private double msgRateIn;
private double msgRateOut;
private int numTopics;
private int numConsumers;
private int numProducers;
// bundle 总数
private int numBundles;
// 最新更新时间
private long lastUpdate;
// 每个 namespaceBundle 的状态数据
private Map<String, NamespaceBundleStats> lastStats;
// 当前 broker 包含的所有 namespaceBundle
private Set<String> bundles;
// 最新新增的 namespaceBundle
private Set<String> lastBundleGains;
// 最新丢失的 namespaceBundle
private Set<String> lastBundleLosses;
// broker 版本信息
private String brokerVersionString;
```

从以上可以看出，`LocalBrokerData` 包含了一个 broker 所有类型的负载数据。

* LoadData 

LoadData 是通过 updateAll 方法进行更新的，其中有三个进行调用：1.负载均衡器启动 2. broker 加入或退出 3.broker 上报负载数据，其结构如下：

```java
// broker name ---> brokerData 的映射
private final Map<String, BrokerData> brokerData;
// bundle names ----> bundle 时间敏感的聚合数据的映射
private final Map<String, BundleData> bundleData;
//最近卸载的 namespace ---> namespaceBundle 映射
private final Map<String, Long> recentlyUnloadedBundles;
```

`LoadData` 实际上包含了每个 `broker` 、 `namespaceBundle` 的负载数据，还有最近被卸载的 `namespaceBundle` 数据集。
其中，`BrokerData` 的结构如下：

```java
private LocalBrokerData localData;
private TimeAverageBrokerData timeAverageData;
private Map<String, BundleData> preallocatedBundleData;
```

`BrokerData` 实际上包含一个 broker 负载数据，一个时间聚合负载数据，一个预先分配的 namespaceBundle 数据。
其中， `BundleData` 的结构如下：

```java
private TimeAverageMessageData shortTermData;
private TimeAverageMessageData longTermData;
private int topics;
```

`BundleData` 实际上包含了 一个短期采样数据，一个长期采样数据，topic 数。
其中，`TimeAverageMessageData` 的结果如下：

```java
//最大采样数
private int maxSamples;
//当前数据采样数，最大数等于最大采样数
private int numSamples;
//每秒消息入站字节数
private double msgThroughputIn;
//每秒消息出站字节数
private double msgThroughputOut;
//平均每秒消息入站数
private double msgRateIn;
//平均每秒消息出站数
private double msgRateOut;
```
`TimeAverageMessageData` 实际上包含了采样数据选项：消息出入站字节数，消息出入站数。

那么，从以上可以看出，`LoadData` 实际上是包含所有的 broker 、 namespaceBundle 的性能采样数据。由此可以猜测，负载均衡器就是通过它通过设定要的资源阈值来执行负载均衡的。

#### 3.2 `ModularLoadManagerImpl` 构造

```java
public ModularLoadManagerImpl() {
    brokerCandidateCache = new HashSet<>();
    brokerToNamespaceToBundleRange = new HashMap<>();
    defaultStats = new NamespaceBundleStats();
    filterPipeline = new ArrayList<>();
    loadData = new LoadData();
    loadSheddingPipeline = new ArrayList<>();
    // 负载超载时，执行动作类
    loadSheddingPipeline.add(new OverloadShedder());
    preallocatedBundleToBroker = new ConcurrentHashMap<>();
    // 采用单线程进行数据上报
    scheduler = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-modular-load-manager"));
    this.brokerToFailureDomainMap = Maps.newHashMap();

    this.brokerTopicLoadingPredicate = new BrokerTopicLoadingPredicate() {
        @Override
        public boolean isEnablePersistentTopics(String brokerUrl) {
            final BrokerData brokerData = loadData.getBrokerData().get(brokerUrl.replace("http://", ""));
            return brokerData != null && brokerData.getLocalData() != null
                    && brokerData.getLocalData().isPersistentTopicsEnabled();
        }

        @Override
        public boolean isEnableNonPersistentTopics(String brokerUrl) {
            final BrokerData brokerData = loadData.getBrokerData().get(brokerUrl.replace("http://", ""));
            return brokerData != null && brokerData.getLocalData() != null
                    && brokerData.getLocalData().isNonPersistentTopicsEnabled();
        }
    };
}
```
构造方法比较简单，只是简单进行一些数据、服务的创建与初始化（限于边幅，这里不重要初始化全省略，下同）。

#### 3.3 `ModularLoadManagerImpl` 初始化

```java
public void initialize(final PulsarService pulsar) {
    this.pulsar = pulsar;
    //1. 监听 zookeeper 上 /loadbalance/brokers 的路径，并注册事件回调
    // 主要用于监听 broker 增加或减少事件，以便执行如下操作
    availableActiveBrokers = new ZooKeeperChildrenCache(pulsar.getLocalZkCache(),
            LoadManager.LOADBALANCE_BROKERS_ROOT);
    availableActiveBrokers.registerListener(new ZooKeeperCacheListener<Set<String>>() {
        @Override
        public void onUpdate(String path, Set<String> data, Stat stat) {
            if (log.isDebugEnabled()) {
                log.debug("Update Received for path {}", path);
            }
            // 1. 回收已经停机或宕机的 broker 的任何预分配资源
            reapDeadBrokerPreallocations(data);
            // 2. 提交一个任务重新计算负载（updateAll方法下节将详细分析）
            scheduler.submit(ModularLoadManagerImpl.this::updateAll);
        }
    });
    
    //2. 监控各 broker 的负载数据变动，如果有 broker 上报（更新）负载报告，则执行回调 onUpdate 函数
    brokerDataCache = new ZooKeeperDataCache<LocalBrokerData>(pulsar.getLocalZkCache()) {
        @Override
        public LocalBrokerData deserialize(String key, byte[] content) throws Exception {
            return ObjectMapperFactory.getThreadLocal().readValue(content, LocalBrokerData.class);
        }
    };
    brokerDataCache.registerListener(this);
    //3. 判断是 *nix系还是win系操作系统，根据不同的操作系统环境来计算主机资源数据服务，服务定时来更新主机资源负载数据，以便使用时直接读取数据，下文将会提到使用场景
    if (SystemUtils.IS_OS_LINUX) {
        brokerHostUsage = new LinuxBrokerHostUsageImpl(pulsar);
    } else {
        brokerHostUsage = new GenericBrokerHostUsageImpl(pulsar);
    }
    //4. 创建 bundle 分割任务
    bundleSplitStrategy = new BundleSplitterTask(pulsar);
    //5. 初始化负载数据
    lastData = new LocalBrokerData(pulsar.getWebServiceAddress(), pulsar.getWebServiceAddressTls(),
            pulsar.getBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls());
    localData = new LocalBrokerData(pulsar.getWebServiceAddress(), pulsar.getWebServiceAddressTls(),
            pulsar.getBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls());
    localData.setBrokerVersionString(pulsar.getBrokerVersion());
    //6. 创建唯一的 LeastLongTermMessageRate 实现
    placementStrategy = ModularLoadManagerStrategy.create(conf);
    //7. 创建简单资源分配策略服务
    policies = new SimpleResourceAllocationPolicies(pulsar);
    // 新增 broker 版本过滤器
    filterPipeline.add(new BrokerVersionFilter());
    //8. 刷新各个 broker 分配到不同的故障域 
    refreshBrokerToFailureDomainMap();
    //9. 注册故障域中主机变更事件，一旦变更，则执行 refreshBrokerToFailureDomainMap 方法
    pulsar.getConfigurationCache().failureDomainListCache()
            .registerListener((path, data, stat) -> scheduler.execute(() -> refreshBrokerToFailureDomainMap()));
    pulsar.getConfigurationCache().failureDomainCache()
            .registerListener((path, data, stat) -> scheduler.execute(() -> refreshBrokerToFailureDomainMap()));
}

// brokerDataCache 注册回调方法,同样是提交一个计算负载任务，实际上，还是调用 updateAll 方法
public void onUpdate(final String path, final LocalBrokerData data, final Stat stat) {
    scheduler.submit(this::updateAll);
}

// 如果启用了故障域，则根据配置把 broker 分配到不同的故障域中
private void refreshBrokerToFailureDomainMap() {
    if (!pulsar.getConfiguration().isFailureDomainsEnabled()) {
        return;
    }
    final String clusterDomainRootPath = pulsar.getConfigurationCache().CLUSTER_FAILURE_DOMAIN_ROOT;
    try {
        synchronized (brokerToFailureDomainMap) {
            Map<String, String> tempBrokerToFailureDomainMap = Maps.newHashMap();
            for (String domainName : pulsar.getConfigurationCache().failureDomainListCache().get()) {
                try {
                    Optional<FailureDomain> domain = pulsar.getConfigurationCache().failureDomainCache()
                            .get(clusterDomainRootPath + "/" + domainName);
                    if (domain.isPresent()) {
                        for (String broker : domain.get().brokers) {
                            tempBrokerToFailureDomainMap.put(broker, domainName);
                        }
                    }
                } catch (Exception e) {
                    log.warn("Failed to get domain {}", domainName, e);
                }
            }
            this.brokerToFailureDomainMap = tempBrokerToFailureDomainMap;
        }
        log.info("Cluster domain refreshed {}", brokerToFailureDomainMap);
    } catch (Exception e) {
        log.warn("Failed to get domain-list for cluster {}", e.getMessage());
    }
}
//当 broker 存活数变更时，将会回调此方法，回收预分配资源
private void reapDeadBrokerPreallocations(Set<String> aliveBrokers) {
    for ( String broker : loadData.getBrokerData().keySet() ) {
        //表明 broker 已停机或宕机（当然也可能是broker与zookeeper通讯中断）
        if ( !aliveBrokers.contains(broker)) {
            if ( log.isDebugEnabled() ) {
                log.debug("Broker {} appears to have stopped; now reclaiming any preallocations", broker);
            }
            final Iterator<Map.Entry<String, String>> iterator = preallocatedBundleToBroker.entrySet().iterator();
            while ( iterator.hasNext() ) {
                Map.Entry<String, String> entry = iterator.next();
                final String preallocatedBundle = entry.getKey();
                final String preallocatedBroker = entry.getValue();
                if ( broker.equals(preallocatedBroker) ) {
                    if ( log.isDebugEnabled() ) {
                        log.debug("Removing old preallocation on dead broker {} for bundle {}",preallocatedBroker, preallocatedBundle);
                    }
                    //移除在已经宕机 broker 上老预分配资源
                    iterator.remove();
                }
            }
        }
    }
}
```

#### 3.4 `ModularLoadManagerImpl` 启动

```java
public void start() throws PulsarServerException {
    try {
        // 尝试在 zookeeper 注册 /loadbalance/brokers 路径
        createZPathIfNotExists(zkClient, LoadManager.LOADBALANCE_BROKERS_ROOT);

        String lookupServiceAddress = pulsar.getAdvertisedAddress() + ":" + conf.getWebServicePort().get();
        brokerZnodePath = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + lookupServiceAddress;
        final String timeAverageZPath = TIME_AVERAGE_BROKER_ZPATH + "/" + lookupServiceAddress;
        //更新当前 broker 负载数据到 localData 数据结构
        updateLocalBrokerData();
        try {
            //尝试创建在 zookeeper 上注册 brokerZnodePath 路径
            ZkUtils.createFullPathOptimistic(zkClient, brokerZnodePath, localData.getJsonBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            //如果此节点已存在（即当前节点已经注册了），那么检查当前系统是否与 zookeeper 正有2个会话
            long ownerZkSessionId = getBrokerZnodeOwner();
            if (ownerZkSessionId != 0 && ownerZkSessionId != zkClient.getSessionId()) {
                log.error("Broker znode - [{}] is own by different zookeeper-ssession {} ", brokerZnodePath, ownerZkSessionId);
                //如果确实有2个不同的会话，抛异常
                throw new PulsarServerException(
                    "Broker-znode owned by different zk-session " + ownerZkSessionId);
            }
            //当前节点可能已经被其他负载均衡器创建未清除数据，那么，清空此节点数据
            zkClient.setData(brokerZnodePath, localData.getJsonBytes(), -1);
        } catch (Exception e) {//创建节点失败，并抛异常
            log.error("Unable to create znode - [{}] for load balance on zookeeper ", brokerZnodePath, e);
            throw e;
        }
        //尝试创建在 zookeeper 上注册 timeAverageZPath 路径，并把数据清空
        createZPathIfNotExists(zkClient, timeAverageZPath);
        zkClient.setData(timeAverageZPath, (new TimeAverageBrokerData()).getJsonBytes(), -1);  
        //更新所有负载数据，并更新刷新时间
        updateAll();
        lastBundleDataUpdate = System.currentTimeMillis();
    } catch (Exception e) {
        //负载均衡器不能在 zookeeper 上创建 brokerZnodePath 路径，启动失败，抛异常
        log.error("Unable to create znode - [{}] for load balance on zookeeper ", brokerZnodePath, e);
        throw new PulsarServerException(e);
    }
}

//核心在于，更新当前 broker 所在主机以及 namespaceBundle 的负载数据
public LocalBrokerData updateLocalBrokerData() {
    try {
        final SystemResourceUsage systemResourceUsage = LoadManagerShared.getSystemResourceUsage(brokerHostUsage);
        //更新主机与 namespaceBundles数据，具体不再展开
        localData.update(systemResourceUsage, getBundleStats());
    } catch (Exception e) {
        log.warn("Error when attempting to update local broker data: {}", e);
    }
    return localData;
}
//这里主要是获取主机资源数据，并通过JVM的Runtime中的方法获取内存相关信息
public static SystemResourceUsage getSystemResourceUsage(final BrokerHostUsage brokerHostUsage) throws IOException {
    //这里是直接读取了定时更新的 broker 主机状态数据，上文已有描述，这里不再赘述
    SystemResourceUsage systemResourceUsage = brokerHostUsage.getBrokerHostUsage();
    // 这里覆盖了主机收集的内存使用以及内存限制，用JVM自己的内存信息
    long maxHeapMemoryInBytes = Runtime.getRuntime().maxMemory();
    long memoryUsageInBytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    systemResourceUsage.memory.usage = (double) memoryUsageInBytes / MIBI;
    systemResourceUsage.memory.limit = (double) maxHeapMemoryInBytes / MIBI;
    systemResourceUsage.directMemory.usage = (double) (getJvmDirectMemoryUsed() / MIBI);
    systemResourceUsage.directMemory.limit = (double) (PlatformDependent.maxDirectMemory() / MIBI);
    return systemResourceUsage;
}

//更新 broker 和 NamespaceBundle 负载数据
public void updateAll() {
    if (log.isDebugEnabled()) {
        log.debug("Updating broker and bundle data for loadreport");
    }
    updateAllBrokerData();
    updateBundleData();
    // broker 已经有最新的负载数据了，检查是否有 namespaceBundle 需要拆分
    checkNamespaceBundleSplit();
}

//作为领导者（leader） broker，需要通过从 zookeeper 上查询所有的 broker 负载数据，而这负载数据是每个 broker 通过 updateLocalBrokerData 方法定时上报给 zookeeper 的，然后更新到 brokerDataMap 中
private void updateAllBrokerData() {
    //从 zookeeper 上获取当前所有存活的 broker 
    final Set<String> activeBrokers = getAvailableBrokers();
    final Map<String, BrokerData> brokerDataMap = loadData.getBrokerData();
    for (String broker : activeBrokers) {
        try {
            String key = String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, broker);    
            //从 zookeeper 缓存中读取（每个） broker 上负载数据（LocalBrokerData），如果没有 key 节点，则抛 NoNodeException 异常
            final LocalBrokerData localData = brokerDataCache.get(key)
                .orElseThrow(KeeperException.NoNodeException::new);
             //是否包含此 broker 节点负载数据
            if (brokerDataMap.containsKey(broker)) {
                // 如果包含，则替换最新的负载数据
            } else {
                // 否则，用当前数据创建新的 brokerData
                brokerDataMap.put(broker, new BrokerData(localData));
            }
        } catch (NoNodeException ne) {
            // 仅当在使用最新数据刷新 availableBrokerCache 之前更新 update-brokerData 并且 broker 的delete-znode监视事件未更新 availableBrokerCache 时，才会发生（简单的说，当前已经获取到availableBrokerCache数据，但是此时刚好有个 broker 挂掉，这时候，availableBrokerCache数据已经过期，然后在从 brokerDataCache 中获取缓存数据，可能就出现此种情况）
            brokerDataMap.remove(broker);
            log.warn("[{}] broker load-report znode not present", broker, ne);
        } catch (Exception e) {
            log.warn("Error reading broker data from cache for broker - [{}], [{}]", broker, e.getMessage());
        }
    }
    // 移除那些已经停止或宕机的 broker 负载数据
    for (final String broker : brokerDataMap.keySet()) {
        if (!activeBrokers.contains(broker)) {
            brokerDataMap.remove(broker);
        }
    }
}

//接上，这里是更新 NamespaceBundle 负载数据
private void updateBundleData() {
    final Map<String, BundleData> bundleData = loadData.getBundleData();
    // 迭代 broker 负载数据
    for (Map.Entry<String, BrokerData> brokerEntry : loadData.getBrokerData().entrySet()){
        final String broker = brokerEntry.getKey();
        final BrokerData brokerData = brokerEntry.getValue();
        //从最新更新的 broker 负载中读取 NamespaceBundle 负载数据
        final Map<String, NamespaceBundleStats> statsMap = 
            brokerData.getLocalData().getLastStats();
             
        // 迭代当前最新可用的 bundle 状态数据更新到 bundleData 中（注：这里bundle指的是NamespaceBundle的全限定名，下同）
        for (Map.Entry<String, NamespaceBundleStats> entry : statsMap.entrySet()) {
            final String bundle = entry.getKey();
            final NamespaceBundleStats stats = entry.getValue();
            if (bundleData.containsKey(bundle)) {
                // 如果原 bundleData 已有此 bundle，则直接更新新的采样数据
                bundleData.get(bundle).update(stats);
            } else {
                //否则，尝试从 zookeeper 中读取此 bundle 的采样数据，如果没有找到，这返回默认采样数据，并且用最新的采样数据更新。
                BundleData currentBundleData = getBundleDataOrDefault(bundle);
                currentBundleData.update(stats);
                bundleData.put(bundle, currentBundleData);
            }
        }

        // 从 预分配的 bundleData 中移除所有已加载的 bundle
        final Map<String, BundleData> preallocatedBundleData = brokerData.getPreallocatedBundleData();
        synchronized (preallocatedBundleData) {
            for (String preallocatedBundleName : brokerData.getPreallocatedBundleData().keySet()) {
              // 如果 broker 所在的 bundle 已包含 preallocatedBundleName   
              if(brokerData.getLocalData().getBundles().contains(preallocatedBundleName)) {                   //迭代预分配的 BundleData，用于以此移除相应的数据
                    final Iterator<Map.Entry<String, BundleData>> preallocatedIterator = preallocatedBundleData.entrySet().iterator();
                    while (preallocatedIterator.hasNext()) {
                        final String bundle = preallocatedIterator.next().getKey();
                        //bundleData 已有 bundle信息，则把预分配的Bundle数据全移除
                        if (bundleData.containsKey(bundle)) {
                            preallocatedIterator.remove();
                            preallocatedBundleToBroker.remove(bundle);
                        }
                    }
                }
                //如果已分配了 broker 的 bundle 死亡并重新出现，也需要这样做。
                if ( preallocatedBundleToBroker.containsKey(preallocatedBundleName) ) {
                    preallocatedBundleToBroker.remove(preallocatedBundleName);
                }
            }
        }
        // 使用最新数据，更新当前 broker 的聚合时间平均数据。
        brokerData.getTimeAverageData().reset(statsMap.keySet(), bundleData, defaultStats);
        final Map<String, Set<String>> namespaceToBundleRange = brokerToNamespaceToBundleRange
            .computeIfAbsent(broker, k -> new HashMap<>());
        //清空 namespaceToBundleRange 中原有数据，用最新的statMap以及preallocatedBundleData更新namespaceToBundleRange，其数据为 NamespaceName --> BundleRange
        synchronized (namespaceToBundleRange) {
            namespaceToBundleRange.clear();
            LoadManagerShared.fillNamespaceToBundlesMap(statsMap.keySet(), namespaceToBundleRange);
            LoadManagerShared.fillNamespaceToBundlesMap(preallocatedBundleData.keySet(), namespaceToBundleRange);
        }
    }
}

//好了，重头戏来了，checkNamespaceBundleSplit 方法，这里就是检测 NamespaceBundle是否要拆分，下面看看怎么具体运行
public void checkNamespaceBundleSplit() {
    /*首先作预执行检测，只要满足以下情况：
    1. 没有启用自动拆分 bundle 2. pulsar 的领导者选举服务为null 3. 当前 broker 不是领导者 
    则不自动进行 NamespaceBundle 拆分（当然，这里要说的是，系统默认是自动拆分，且有领导者选举服务，只是 当 broker 不是领导者时，此方法调用无用，只有领导者才会执行以下代码）
    */
    if (!conf.isLoadBalancerAutoBundleSplitEnabled() || pulsar.getLeaderElectionService()         == null || !pulsar.getLeaderElectionService().isLeader()) {
        return;
    }
    //负载均衡自动卸载分割后（新增的）bundle开关是否打开（下面详细讲）
    final boolean unloadSplitBundles =       pulsar.getConfiguration().isLoadBalancerAutoUnloadSplitBundlesEnabled();
    //bundle 分割策略
    synchronized (bundleSplitStrategy) {
        //核心方法，寻找哪些 bundle 需要被拆分
        final Set<String> bundlesToBeSplit = bundleSplitStrategy.findBundlesToSplit(loadData, pulsar);
        NamespaceBundleFactory namespaceBundleFactory = pulsar.getNamespaceService().getNamespaceBundleFactory();
        for (String bundleName : bundlesToBeSplit) {
            try {
                final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundleName);
                final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundleName);
                //判定是否还能被拆分，如果不能则进行下一个，否则执行拆分
                if (!namespaceBundleFactory
                    .canSplitBundle(namespaceBundleFactory.getBundle(namespaceName, bundleRange))) {
                    continue;
                }
                log.info("Load-manager splitting bundle {} and unloading {}", bundleName, unloadSplitBundles);
                // 调用拆分bundle方法
                pulsar.getAdminClient().namespaces().splitNamespaceBundle(namespaceName, bundleRange, unloadSplitBundles);
                // 移除对应的bundle，确保不会再被选中
                loadData.getBundleData().remove(bundleName);
                localData.getLastStats().remove(bundleName);
                // 使 namespaceName 的 bundle 缓存失效（即清理缓存）
                this.pulsar.getNamespaceService().getNamespaceBundleFactory()
                    .invalidateBundleCache(NamespaceName.get(namespaceName));
                //从 zookeeper 对应的节点上删除 bundleName
                deleteBundleDataFromZookeeper(bundleName);
                log.info("Successfully split namespace bundle {}", bundleName);
            } catch (Exception e) {
                log.error("Failed to split namespace bundle {}", bundleName, e);
            }
        }
    }

}

//基于特定阈值来决定哪些bundle需要被拆分，返回需要拆分的 bundle 集
public Set<String> findBundlesToSplit(final LoadData loadData, final PulsarService pulsar) {
    bundleCache.clear();
    final ServiceConfiguration conf = pulsar.getConfiguration();
    //任意一broker上最大bundle数
    int maxBundleCount = conf.getLoadBalancerNamespaceMaximumBundles();
    //任意bundle上最大topic数
    long maxBundleTopics = conf.getLoadBalancerNamespaceBundleMaxTopics();
    //任意bundle上最大会话数
    long maxBundleSessions = conf.getLoadBalancerNamespaceBundleMaxSessions();
    //任意bundle上最大消息速率
    long maxBundleMsgRate = conf.getLoadBalancerNamespaceBundleMaxMsgRate();
    //任意bundle上最大带宽（包括扇入扇出）
    long maxBundleBandwidth = conf.getLoadBalancerNamespaceBundleMaxBandwidthMbytes() * LoadManagerShared.MIBI;
    //先按照每个主机来进行迭代
    loadData.getBrokerData().forEach((broker, brokerData) -> {
        LocalBrokerData = brokerData.getLocalData();
        //一个主机下，所有的 NamespaceBundle 负载数据迭代 
        for (final Map.Entry<String, NamespaceBundleStats> entry : localData.getLastStats().entrySet()) {
            //这里是一个 bundle ，并且对应的状态
            final String bundle = entry.getKey();
            final NamespaceBundleStats stats = entry.getValue();
            double totalMessageRate = 0;
            double totalMessageThroughput = 0;
            // 读取 bundleData 数据，测试是否包含 bundle，如果有，则获取长周期数据，否则忽略
            if (loadData.getBundleData().containsKey(bundle)) {
                final TimeAverageMessageData longTermData = loadData.getBundleData().get(bundle).getLongTermData();
                totalMessageRate = longTermData.totalMsgRate();
                totalMessageThroughput = longTermData.totalMsgThroughput();
            }
            // 比较是否超过配置中定义的阈值，只要任意一个超过阈值，则将可能被拆分
            if (stats.topics > maxBundleTopics || stats.consumerCount + stats.producerCount > maxBundleSessions  || totalMessageRate > maxBundleMsgRate || totalMessageThroughput > maxBundleBandwidth) {
                final String namespace = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
                try {//读取当前 namespaceName 被分割数
                    final int bundleCount = pulsar.getNamespaceService()
                        .getBundleCount(NamespaceName.get(namespace));
                    // 如果分割数小于阈值，则可以尝试分割，否则给出警告信息
                    if (bundleCount < maxBundleCount) {
                        bundleCache.add(bundle);
                    } else {
                        log.warn(
                            "Could not split namespace bundle {} because namespace {} has too many bundles: {}", bundle, namespace, bundleCount);
                    }
                } catch (Exception e) {
                    log.warn("Error while getting bundle count for namespace {}", namespace, e);
                }
            }
        }
    });
    return bundleCache;
}
//从这里可以看出，只是发一个namespaceBundle拆分请求给 broker，其请求URL为:http(s)://brokerip:port/{tenant}/{namespace}/{bundle}/split
public void splitNamespaceBundle(String namespace, String bundle, boolean unloadSplitBundles)
    throws PulsarAdminException {
    try {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle, "split");
        request(path.queryParam("unload", Boolean.toString(unloadSplitBundles)))
            .put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
    } catch (Exception e) {
        throw getApiException(e);
    }
}
```
从以上可以看出，负载均衡器的职责非常清晰，其核心工作如下：

* 更新集群中所有 broker 负载数据
* 更新集群中所有 broker 上 `NamespaceBundle` 集合负载数据
* 基于以上负载数据与设定的阈值判断，是否有出现超载的 namespaceBundle ，如果有，则发送拆分 namespaceBundle 命令给 broker，执行拆分任务。

那么，现在就详细的看看拆分 namespaceBundle 过程，如下：

```java
//接受请求执行处理的实际位于:org.apache.pulsar.broker.admin.v2.Namespaces,其方法如下
@PUT
@Path("/{tenant}/{namespace}/{bundle}/split")
@ApiOperation(value = "Split a namespace bundle")
@ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
public void splitNamespaceBundle(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,@PathParam("bundle") String bundleRange,
@QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
@QueryParam("unload") @DefaultValue("false") boolean unload) {
    //验证NamespaceName
    validateNamespaceName(tenant, namespace);
    //这里才是实际拆分NamespaceBundle的方法
    internalSplitNamespaceBundle(bundleRange, authoritative, unload);
}

//实际处理NamespaceBundle拆分的方法
protected void internalSplitNamespaceBundle(String bundleRange, boolean authoritative, boolean unload) {
    log.info("[{}] Split namespace bundle {}/{}", clientAppId(), namespaceName, bundleRange);
    //验证是否有超级管理员访问权限
    validateSuperUserAccess();
    //获取NamespaceName的策略信息
    Policies policies = getNamespacePolicies(namespaceName);
    //判断是否全局NamespceName
    if (namespaceName.isGlobal()) {
        //检查此集群所有者是否在全局（global）namespace：如果集群所属它，则重定向
        validateGlobalNamespaceOwnership(namespaceName);
    } else {
        //检查此集群的所有者，如果存在，则重定向它所有者（v1.x版本有用，v2.x版本此检查已废弃）
        validateClusterOwnership(namespaceName.getCluster());
        //检查当前租户是否有访问Namesapce的集群权限
        validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
    }
    //验证当前节点是否有权限对 global zookeeper 中策略数据进行读写操作
    validatePoliciesReadOnlyAccess();
    //验证 bundleRange 是否存在，不存在则抛异常，验证是否属于当前 broker 节点，如果不是，重定向
    NamespaceBundle nsBundle = validateNamespaceBundleOwnership(namespaceName, policies.bundles, bundleRange,authoritative, true);

    try {
        //继续往下看
        pulsar().getNamespaceService().splitAndOwnBundle(nsBundle, unload).get();
        log.info("[{}] Successfully split namespace bundle {}", clientAppId(), nsBundle.toString());
    } catch (IllegalArgumentException e) {
        log.error("[{}] Failed to split namespace bundle {}/{} due to {}", clientAppId(), namespaceName, bundleRange, e.getMessage());
        throw new RestException(Status.PRECONDITION_FAILED, "Split bundle failed due to invalid request");
    } catch (Exception e) {
        log.error("[{}] Failed to split namespace bundle {}/{}", clientAppId(), namespaceName, bundleRange, e);
        throw new RestException(e);
    }
}
//分割属于自己的 namespaceBundle，包装方法
public CompletableFuture<Void> splitAndOwnBundle(NamespaceBundle bundle, boolean unload)
    throws Exception {

    final CompletableFuture<Void> unloadFuture = new CompletableFuture<>();
    //最大重试次数
    final AtomicInteger counter = new AtomicInteger(BUNDLE_SPLIT_RETRY_LIMIT);
    splitAndOwnBundleOnceAndRetry(bundle, unload, counter, unloadFuture);
    return unloadFuture;
}

void splitAndOwnBundleOnceAndRetry(NamespaceBundle bundle,
                                   boolean unload,
                                   AtomicInteger counter,
                                   CompletableFuture<Void> unloadFuture) {
    CompletableFuture<List<NamespaceBundle>> updateFuture = new CompletableFuture<>();
    //默认把当前的bundle分割成2个bundle（分割算法见下）
    final Pair<NamespaceBundles, List<NamespaceBundle>> splittedBundles = bundleFactory.splitBundles(bundle, 2);

    // Split and updateNamespaceBundles. Update may fail because of concurrent write to Zookeeper.
    // 这是拆分后的NamespaceBundles，因为并发写入 zookeeper，更新可能失败
    if (splittedBundles != null) {
        //对拆分后的进行非空校验
        checkNotNull(splittedBundles.getLeft());
        checkNotNull(splittedBundles.getRight());
        checkArgument(splittedBundles.getRight().size() == 2, "bundle has to be split in two bundles");
        NamespaceName nsname = bundle.getNamespaceObject();
        if (LOG.isDebugEnabled()) {
            LOG.debug("[{}] splitAndOwnBundleOnce: {}, counter: {},  2 bundles: {}, {}",
                      nsname.toString(), bundle.getBundleRange(), counter.get(),
                      splittedBundles != null ? splittedBundles.getRight().get(0).getBundleRange() : "null splittedBundles",
                      splittedBundles != null ? splittedBundles.getRight().get(1).getBundleRange() : "null splittedBundles");
        }
        try {
            // 试着把拆分后新产生的分区分配给当前 broker
            for (NamespaceBundle sBundle : splittedBundles.getRight()) {
                checkNotNull(ownershipCache.tryAcquiringOwnership(sBundle));
            }
            // 把拆分后的完整 NamespaceBundles 更新到 local zookeeper。
            updateNamespaceBundles(nsname, splittedBundles.getLeft(),
                                   (rc, path, zkCtx, stat) ->  {
                                       if (rc == Code.OK.intValue()) {
                                        // 如果更新成功，则把本地缓存更新
                       bundleFactory.invalidateBundleCache(bundle.getNamespaceObject());
                       updateFuture.complete(splittedBundles.getRight());
                                       } else if (rc == Code.BADVERSION.intValue()) {
                       KeeperException keeperException = KeeperException.create(KeeperException.Code.get(rc));
                                        //更新 NamespaceName 策略失败
           String msg = format("failed to update namespace policies [%s], NamespaceBundle: %s " + "due to %s, counter: %d",nsname.toString(), bundle.getBundleRange(), keeperException.getMessage(), counter.get());
                                           LOG.warn(msg);
                                           updateFuture.completeExceptionally(new ServerMetadataException(keeperException));
                                       } else {
                                           String msg = format("failed to update namespace policies [%s], NamespaceBundle: %s due to %s", nsname.toString(), bundle.getBundleRange(), KeeperException.create(KeeperException.Code.get(rc)).getMessage());
                                           LOG.warn(msg);
                                           updateFuture.completeExceptionally(new ServiceUnitNotReadyException(msg));
                                       }
                                   });
        } catch (Exception e) {
            String msg = format("failed to acquire ownership of split bundle for namespace [%s], %s",nsname.toString(), e.getMessage());
            LOG.warn(msg, e);
            updateFuture.completeExceptionally(new ServiceUnitNotReadyException(msg));
        }
    } else {
        String msg = format("bundle %s not found under namespace", bundle.toString());
        LOG.warn(msg);
        updateFuture.completeExceptionally(new ServiceUnitNotReadyException(msg));
    }
    //更新动作完成，执行回调
    updateFuture.whenCompleteAsync((r, t)-> {
        if (t != null) {
            if ((t instanceof ServerMetadataException) && (counter.decrementAndGet() >= 0)) {           //因版本导致异常和重试次数未达到7次，则继续尝试分割bundle任务
                pulsar.getOrderedExecutor().execute(() -> splitAndOwnBundleOnceAndRetry(bundle, unload, counter, unloadFuture));
            } else {
                // 达到重试次数，可能是其他原因导致的拆分bundle失败
                String msg2 = format(" %s not success update nsBundles, counter %d, reason %s", bundle.toString(), counter.get(), t.getMessage());
                LOG.warn(msg2);
                unloadFuture.completeExceptionally(new ServiceUnitNotReadyException(msg2));
            }
            return;
        }
        // 拆分bundle任务成功，执行如下操作
        try {
            //在内存中禁用老bundle
            getOwnershipCache().updateBundleState(bundle, false);
            //更新 bundle 中的 topic 
            pulsar.getBrokerService().refreshTopicToStatsMaps(bundle);
            //设置负载数据强制更新标记
            loadManager.get().setLoadReportForceUpdateFlag();
            //根据开关来决定是否卸载拆分后生成的 namespaceBundles（这样，就可以把拆分后多出来的 namespaceBundle 附加到其他的 broker 上）
            if (unload) {
                r.forEach(splitBundle -> {
                    try {
                        unloadNamespaceBundle(splitBundle);
                    } catch (Exception e) {
                        LOG.warn("Failed to unload split bundle {}", splitBundle, e);
                        throw new RuntimeException("Failed to unload split bundle " + splitBundle, e);
                    }
                });
            }
            //拆分完全成功
            unloadFuture.complete(null);
        } catch (Exception e) {
            String msg1 = format(
                "failed to disable bundle %s under namespace [%s] with error %s",
                bundle.getNamespaceObject().toString(), bundle.toString(), e.getMessage());
            LOG.warn(msg1, e);
            unloadFuture.completeExceptionally(new ServiceUnitNotReadyException(msg1));
        }
        return;
    }, pulsar.getOrderedExecutor());
}

//简单的说，就是把指定的NamespaceBundle分成numbundles份
public Pair<NamespaceBundles, List<NamespaceBundle>> splitBundles(NamespaceBundle targetBundle, int numBundles) {
    checkArgument(canSplitBundle(targetBundle), "%s bundle can't be split further", targetBundle);
    checkNotNull(targetBundle, "can't split null bundle");
    checkNotNull(targetBundle.getNamespaceObject(), "namespace must be present");
    NamespaceName nsname = targetBundle.getNamespaceObject();
    //获取此NamespaceName的所有NamespaceBundle集合
    NamespaceBundles sourceBundle = bundlesCache.synchronous().get(nsname);

    final int lastIndex = sourceBundle.partitions.length - 1;
    //分割后产生的分区数
    final long[] partitions = new long[sourceBundle.partitions.length + (numBundles - 1)];
    int pos = 0;
    int splitPartition = -1;
    final Range<Long> range = targetBundle.getKeyRange();
    for (int i = 0; i < lastIndex; i++) {
        //由待拆分的namespaceBundle从源sourceBundle找出指定范围
        if (sourceBundle.partitions[i] == range.lowerEndpoint()
            && (range.upperEndpoint() == sourceBundle.partitions[i + 1])) {
            splitPartition = i;
            Long maxVal = sourceBundle.partitions[i + 1];
            Long minVal = sourceBundle.partitions[i];
            //计算出新分区的间隔大小
            Long segSize = (maxVal - minVal) / numBundles;
            partitions[pos++] = minVal;
            Long curPartition = minVal + segSize;
            //把targetBundle分割成numBundles数（这里是核心）
            for (int j = 0; j < numBundles - 1; j++) {
                partitions[pos++] = curPartition;
                curPartition += segSize;
            }
        } else {
            //没匹配到的，按照原位置即可
            partitions[pos++] = sourceBundle.partitions[i];
        }
    }
    partitions[pos] = sourceBundle.partitions[lastIndex];
    if (splitPartition != -1) {
        // 创建一个新NamespaceBundles（相比源NamespaceBundles，只是partitions变化），版本保持不变
        NamespaceBundles splittedNsBundles = new NamespaceBundles(nsname, partitions, this, sourceBundle.getVersion());
        // 这是拆分时，多出来的分区
        List<NamespaceBundle> splittedBundles = splittedNsBundles.getBundles().subList(splitPartition,(splitPartition + numBundles));
        return new ImmutablePair<NamespaceBundles, List<NamespaceBundle>>(splittedNsBundles, splittedBundles);
    }
    return null;
}

// 此方法就是把oldBundle的中的topic根据拆分后的bundle重新分布一次，核心操作！
public void refreshTopicToStatsMaps(NamespaceBundle oldBundle) {
    checkNotNull(oldBundle);
    try {
        // 从oldBundle下获取所有的 topic 集合（注意，这里只重新分配 oldBundle 下的 topic）
        List<Topic> topics = getAllTopicsFromNamespaceBundle(oldBundle.getNamespaceObject().toString(),
                                                             oldBundle.toString());
        if (!isEmpty(topics)) {
            // 拆分后的oldBundle，此时已经更新到zookeeper上，本地缓存也更新到了，这时候，把 topic 集合重新分配到拆分后的bundle中，下面将解析重点方法 addTopicToStatsMaps
            topics.stream().forEach(t -> {
                addTopicToStatsMaps(TopicName.get(t.getName()), t);
            });
            // 这里从缓存中移除oldBundle
            synchronized (multiLayerTopicsMap) {
                multiLayerTopicsMap.get(oldBundle.getNamespaceObject().toString())
         .remove(oldBundle.toString());                                                                   pulsarStats.invalidBundleStats(oldBundle.toString());
            }
        }
    } catch (Exception e) {
        log.warn("Got exception while refreshing topicStats map", e);
    }
}

private void addTopicToStatsMaps(TopicName topicName, Topic topic) {
    try {
        //注意getBundle方法
        NamespaceBundle namespaceBundle = pulsar.getNamespaceService().getBundle(topicName);
        //把拆分后的 namespaceBundle 放入 multiLayerTopicsMap 中
        if (namespaceBundle != null) {
            synchronized (multiLayerTopicsMap) {
                String serviceUnit = namespaceBundle.toString();
                multiLayerTopicsMap //
                    .computeIfAbsent(topicName.getNamespace(), k -> new ConcurrentOpenHashMap<>()) //
                    .computeIfAbsent(serviceUnit, k -> new ConcurrentOpenHashMap<>()) //
                    .put(topicName.toString(), topic);
            }
        }
        //刷新 topic 的缓存
        invalidateOfflineTopicStatCache(topicName);
    } catch (Exception e) {
        log.warn("Got exception when retrieving bundle name during create persistent topic", e);
    }
}
//通过 topic 对应的 namespaceName ，从缓存中读取拆分后最新的 namespaceBundles， 再通过其 findBundle 方法，顺利的为 topicName 找到 namespaceBundle
public NamespaceBundle getBundle(TopicName topicName) throws Exception {
    return bundleFactory.getBundles(topicName.getNamespaceObject()).findBundle(topicName);
}
//这里真正意义上的分配了，为 topic 分配新的 namespaceBundle，这样就完成了 topic 重新分布任务
public NamespaceBundle findBundle(TopicName topicName) {
    checkArgument(this.nsname.equals(topicName.getNamespaceObject()));
    //根据 topicName 全限定名的 hashCode 来选择 namespaceBundle
    long hashCode = factory.getLongHashCode(topicName.toString());
    NamespaceBundle bundle = getBundle(hashCode);
    if (topicName.getDomain().equals(TopicDomain.non_persistent)) {
        bundle.setHasNonPersistentTopic(true);
    }
    return bundle;
}
//通过 TopicName 的全限定名的 HashCode 落在 namespaceBundle 分区 index 上，通过纠正 index，这样就返回了对应的 namespaceBundle
protected NamespaceBundle getBundle(long hash) {
    int idx = Arrays.binarySearch(partitions, hash);
    int lowerIdx = idx < 0 ? -(idx + 2) : idx;
    return bundles.get(lowerIdx);
}
```

到了这里，有的读者就会问了，namespaceBundle 虽然被拆分了，并且原 namespaceBundle 也卸载了，但是并没有分配到其他 broker 上，而是试着把新产生的分区也分配到当前的 broker，这不是做无用功吗？但是，还记得上文 splitAndOwnBundle 方法有个`unload`开关吗？这个开关决定是否把新分区从当前 broker 卸载下来。这样做的目的，个人觉得2个方面，1.拆分时尽量不影响新的生产者（因为一旦拆分成功，原namespaceBundle要剔除的，这样就留下空挡了）。2.兼顾自动和手动操作。那么，假设设置 `unload` 为 `true`呢，那么就引出了负载均衡器的另一个服务—— namespaceBundle 分配服务，也就是负载均衡器需要通过负载数据找出负载较低的 broker ，然后把 namespaceBundle 附加到此 broker 上，这样就完成了负载均衡的操作，那么谁来启动的呢？还记得上文提到的选举服务中提到的两个方法吗，没错，就是其中之一：

```java
public synchronized void doLoadShedding() {
    //是否启用自动namespaceBundle分配服务
    if (!LoadManagerShared.isLoadSheddingEnabled(pulsar)) {
        return;
    }
    //如果 broker 个数少于等于1，则直接返回
    if (getAvailableBrokers().size() <= 1) {
        log.info("Only 1 broker available: no load shedding will be performed");
        return;
    }
    // 移除近来已在卸载过处于 GracePeriod 时间的 namespaceBundle（简单的说是，上一次卸载还没超过GracePeriod 时间，保护期不再进行二次卸载的 NamespaceBundle） 
    final long timeout = System.currentTimeMillis()
        - TimeUnit.MINUTES.toMillis(conf.getLoadBalancerSheddingGracePeriodMinutes());
    final Map<String, Long> recentlyUnloadedBundles = loadData.getRecentlyUnloadedBundles();
    //过滤掉已卸载的 namespaceBundle，其在规定 GracePeriod 时间内，不再执行二次卸载
    recentlyUnloadedBundles.keySet().removeIf(e -> recentlyUnloadedBundles.get(e) < timeout);
    //减载策略
    for (LoadSheddingStrategy strategy : loadSheddingPipeline) {
        //根据减载策略，选出集群内，每个 broker 上负载大的 namespaceBundle 集，以便接下来进行卸载
        final Multimap<String, String> bundlesToUnload = strategy.findBundlesForUnloading(loadData, conf);

        bundlesToUnload.asMap().forEach((broker, bundles) -> {
            bundles.forEach(bundle -> {
                final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
                final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
                //对存活的集群内 broker 执行 namespace 策略，符合要求的 broker 放入brokerCandidateCache
                if (!shouldAntiAffinityNamespaceUnload(namespaceName, bundleRange, broker)) {
                    return;
                }

                log.info("[Overload shedder] Unloading bundle: {} from broker {}", bundle, broker);
                try {
                //发命令给 admin，请求把 NamespaceName 的 bundleRange 分区卸载 
                pulsar.getAdminClient().namespaces().unloadNamespaceBundle(namespaceName, bundleRange);       //记录卸载成功的 NamespaceBundle
                    loadData.getRecentlyUnloadedBundles().put(bundle, System.currentTimeMillis());
                } catch (PulsarServerException | PulsarAdminException e) {
                    log.warn("Error when trying to perform load shedding on {} for broker {}", bundle, broker, e);
                }
            });
        });
    }
}
```
从上文可以看出，这个方法最重要的是 LoadSheddingStrategy 类的 findBundlesForUnloading 以及 本类中的shouldAntiAffinityNamespaceUnload 方法。先分析 findBundlesForUnloading 方法，如下： 
```java
public Multimap<String, String> findBundlesForUnloading(final LoadData loadData, final ServiceConfiguration conf) {
    //清理上一次使用缓存
    selectedBundlesCache.clear();
    //资源超载比率阈值
    final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
    //获取最近未卸载的 namespaceBundles
    final Map<String, Long> recentlyUnloadedBundles = loadData.getRecentlyUnloadedBundles();

    // 检查当前集群里每一个 broker，并选出使用资源已超载的
    loadData.getBrokerData().forEach((broker, brokerData) -> {

        final LocalBrokerData localData = brokerData.getLocalData();
        //获取 broker 上最大资源使用率
        final double currentUsage = localData.getMaxResourceUsage();
        //没有超载，直接返回，继续下一个 broker 
        if (currentUsage < overloadThreshold) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Broker is not overloaded, ignoring at this point", broker);    }
            return;
        }
        //调整后的负载设置 = broker 当前负载比率 - 超载阈值比率 + 宽限步进比率（主要用于调整后的负载比不至于在超载阈值比率附近，这样很有可能频繁触发减载操作，通俗的将就是有个安全缓存区域）
        double percentOfTrafficToOffload = currentUsage - overloadThreshold + ADDITIONAL_THRESHOLD_PERCENT_MARGIN;
        // broker 当前消息吞吐量 = 消息扇入量 + 消息扇出量
        double brokerCurrentThroughput = localData.getMsgThroughputIn() + localData.getMsgThroughputOut();
        //最小的消息吞吐量超载量（简单的说，broker 超载时经过负载均衡后，目标要减载到这个值）
        double minimumThroughputToOffload = brokerCurrentThroughput * percentOfTrafficToOffload;
        log.info(
            "Attempting to shed load on {}, which has max resource usage above threshold {}% > {}% -- Offloading at least {} MByte/s of traffic",
            broker, currentUsage, overloadThreshold, minimumThroughputToOffload / 1024 / 1024);
        MutableDouble trafficMarkedToOffload = new MutableDouble(0);
        MutableBoolean atLeastOneBundleSelected = new MutableBoolean(false);
        //namespaceBundle 数量
        if (localData.getBundles().size() > 1) {
            // 按照吞吐量为 namespaceBundle 排序
            loadData.getBundleData().entrySet().stream().map((e) -> {
                // 这里返回一个以key为 namespaceBundle，以value为 namespaceBundle 短期采样吞吐量数据的 Map，以减少系统资源负载
                String bundle = e.getKey();
                BundleData bundleData = e.getValue();
                TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                double throughput = shortTermData.getMsgThroughputIn() + shortTermData.getMsgThroughputOut();
                return Pair.of(bundle, throughput);
            }).filter(e -> {
                // 仅考虑最近尚未卸载的 nameBundle 
                return !recentlyUnloadedBundles.containsKey(e.getLeft());
            }).sorted((e1, e2) -> {
                // 按照吞吐量降序排序（简单说就是，吞吐量大的排前面）
                return Double.compare(e2.getRight(), e1.getRight());
            }).forEach(e -> {
                //这里有二层意思，对于一 broker 来说，其所有的 namespaceBundle 上对应的吞吐量累加后小于 minimumThroughputToOffload 值的，都会被选进入 selectedBundlesCache 中，其二，第一个 namespaceBundle 都大于等于 minimumThroughputToOffload 值，就直接选入，也就是只要进入这层循环，都会选至少一个 namespaceBundle 出来（简单的说就是选负载之和超过minimumThroughputToOffload的 namespaceBundle集合）
                if (trafficMarkedToOffload.doubleValue() < minimumThroughputToOffload
                    || atLeastOneBundleSelected.isFalse()) {
                    selectedBundlesCache.put(broker, e.getLeft());
                    trafficMarkedToOffload.add(e.getRight());
                    atLeastOneBundleSelected.setTrue();
                }
            });
        } else if (localData.getBundles().size() == 1) {
            //broker 只有一个 namespaceBundle 情况，只打印警告信息
            log.warn(
                "HIGH USAGE WARNING : Sole namespace bundle {} is overloading broker {}. " + "No Load Shedding will be done on this broker",
                localData.getBundles().iterator().next(), broker);
        } else {
            //broker 还没有一个 namespaceBundle 情况，只打印警告信息
            log.warn("Broker {} is overloaded despite having no bundles", broker);
        }

    });

    return selectedBundlesCache;
}
```
上面方法相对简单，而  shouldAntiAffinityNamespaceUnload 方法却并不简单，如下：
```java
public boolean shouldAntiAffinityNamespaceUnload(String namespace, String bundle, String currentBroker) {
    try {
        //读取 namespace 的策略数据，如果没有策略信息或策略中没有设置反亲缘性组检查，就直接返回true
        Optional<Policies> nsPolicies = pulsar.getConfigurationCache().policiesCache()
            .get(path(POLICIES, namespace));
        if (!nsPolicies.isPresent() || StringUtils.isBlank(nsPolicies.get().antiAffinityGroup)) {
            return true;
        }
        //最关键的来了，broker 候选缓存
        synchronized (brokerCandidateCache) {
            //清空 broker 候选缓存
            brokerCandidateCache.clear();
            //获取资源单元（实际上是namespaceBundle）
            ServiceUnitId serviceUnit = pulsar.getNamespaceService().getNamespaceBundleFactory()
                .getBundle(namespace, bundle);
            //应用（执行）此 namespace 策略
            LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache,getAvailableBrokers(), brokerTopicLoadingPredicate);
            //反亲缘性卸载的namespace（通俗的讲，）
            return LoadManagerShared.shouldAntiAffinityNamespaceUnload(namespace, bundle, currentBroker, pulsar,brokerToNamespaceToBundleRange, brokerCandidateCache);
        }

    } catch (Exception e) {
        log.warn("Failed to check anti-affinity namespace ownership for {}/{}/{}, {}", namespace, bundle,
                 currentBroker, e.getMessage());

    }
    return true;
}
//应用此 namespace 策略
public static void applyNamespacePolicies(final ServiceUnitId serviceUnit,
                                          final SimpleResourceAllocationPolicies policies, final Set<String> brokerCandidateCache,final Set<String> availableBrokers, final BrokerTopicLoadingPredicate brokerTopicLoadingPredicate) {
    //负载主缓存
    Set<String> primariesCache = localPrimariesCache.get();
    primariesCache.clear();
    //负载从缓存
    Set<String> secondaryCache = localSecondaryCache.get();
    secondaryCache.clear();

    NamespaceName namespace = serviceUnit.getNamespaceObject();
    //是否有隔离策略
    boolean isIsolationPoliciesPresent = policies.areIsolationPoliciesPresent(namespace);
    //如果是 namespaceBundle，是否支持非持久化 topic
    boolean isNonPersistentTopic = (serviceUnit instanceof NamespaceBundle)
        ? ((NamespaceBundle) serviceUnit).hasNonPersistentTopic() : false;
    if (isIsolationPoliciesPresent) {
        log.debug("Isolation Policies Present for namespace - [{}]", namespace.toString());
    }
    //这里迭代集群中存活的 broker
    for (final String broker : availableBrokers) {
        final String brokerUrlString = String.format("http://%s", broker);
        URL brokerUrl;
        try {
            brokerUrl = new URL(brokerUrlString);
        } catch (MalformedURLException e) {
            log.error("Unable to parse brokerUrl from ResourceUnitId - [{}]", e);
            continue;
        }
        // 如果有隔离策略
        if (isIsolationPoliciesPresent) {
            //这里是通过正则表达式验证是否主 broker，如果是，则加入主 broker 缓存区
            if (policies.isPrimaryBroker(namespace, brokerUrl.getHost())) {
                primariesCache.add(broker);
                if (log.isDebugEnabled()) {
                    log.debug("Added Primary Broker - [{}] as possible Candidates for" + " namespace - [{}] with policies", brokerUrl.getHost(), namespace.toString());
                }
                //同上，只不过这是从 broker
            } else if (policies.isSecondaryBroker(namespace, brokerUrl.getHost())) {
                secondaryCache.add(broker);
                if (log.isDebugEnabled()) {
                    log.debug(
                        "Added Shared Broker - [{}] as possible "
                        + "Candidates for namespace - [{}] with policies",
                        brokerUrl.getHost(), namespace.toString());
                }
            } else {
                //既不是主 broker ，也不是从 broker，则跳过
                if (log.isDebugEnabled()) {
                    log.debug("Skipping Broker - [{}] not primary broker and not shared" + " for namespace - [{}] ",
                              brokerUrl.getHost(), namespace.toString());
                }
            }
        } else {//无隔离策略
            // 非持久性 topic 仅仅只能分配到启用非持久性的 topic 的 broker 上
            if (isNonPersistentTopic
                && !brokerTopicLoadingPredicate.isEnableNonPersistentTopics(brokerUrlString)) {
                if (log.isDebugEnabled()) {
                    log.debug("Filter broker- [{}] because it doesn't support non-persistent namespace - [{}]", brokerUrl.getHost(), namespace.toString());
                }
            // 持久性 topic 仅仅只能分配到启用持久化的 topic 的 broker 上
            } else if (!isNonPersistentTopic
                       && !brokerTopicLoadingPredicate.isEnablePersistentTopics(brokerUrlString)) {
                if (log.isDebugEnabled()) {
                    log.debug("Filter broker- [{}] because broker only supports non-persistent namespace - [{}]",
                              brokerUrl.getHost(), namespace.toString());
                }
            //通过策略中正则表达式判断是否共享 broker（即非主 broker）
            } else if (policies.isSharedBroker(brokerUrl.getHost())) {
                secondaryCache.add(broker);
                if (log.isDebugEnabled()) {
                    log.debug("Added Shared Broker - [{}] as possible Candidates for namespace - [{}]",
                              brokerUrl.getHost(), namespace.toString());
                }
            }
        }
    }
    //如果有隔离策略，则只把主 broker 放入 brokerCandidateCache
    if (isIsolationPoliciesPresent) {
        brokerCandidateCache.addAll(primariesCache);
        //到了这里，终于看到故障转移相关的策略配置了，如果对于当前 namespace 没有足够的主 broker 分配，根据故障转移策略是否增加一些从 broker 进去（上文提到的简单资源分配策这里终于派上用场了）
        if (policies.shouldFailoverToSecondaries(namespace, primariesCache.size())) {
            log.debug(
                "Not enough of primaries [{}] available for namespace - [{}], "
                + "adding shared [{}] as possible candidate owners",
                primariesCache.size(), namespace.toString(), secondaryCache.size());
            brokerCandidateCache.addAll(secondaryCache);
        }
    } else {
        //如果当前 namespace 没有设置隔离策略，则把从 broker 全部加入到 broker 候选名单
        log.debug(
            "Policies not present for namespace - [{}] so only "
            + "considering shared [{}] brokers for possible owner",
            namespace.toString(), secondaryCache.size());
        brokerCandidateCache.addAll(secondaryCache);
    }
}
//这里就是故障转移是否能使用从 broker 上去
public boolean shouldFailoverToSecondaries(NamespaceName namespace, int totalPrimaryCandidates) {
    Optional<NamespaceIsolationPolicy> nsPolicy = getNamespaceIsolationPolicy(namespace);
    return nsPolicy.isPresent() && nsPolicy.get().shouldFailover(totalPrimaryCandidates);
}

//获取 namespace 的隔离策略
private Optional<NamespaceIsolationPolicy> getNamespaceIsolationPolicy(NamespaceName namespace) {
    try {
        Optional<NamespaceIsolationPolicies> policies =getIsolationPolicies(pulsar.getConfiguration().getClusterName());
        if (!policies.isPresent()) {
            return Optional.empty();
        }
        return Optional.ofNullable(policies.get().getPolicyByNamespace(namespace));
    } catch (Exception e) {
        LOG.warn("Unable to get the namespaceIsolationPolicies [{}]", e);
        return Optional.empty();
    }
}
//通过集群名从 zookeeper 上获取 namespace 隔离策略
private Optional<NamespaceIsolationPolicies> getIsolationPolicies(String clusterName) {
    try {
        return namespaceIsolationPolicies
            .get(AdminResource.path("clusters", clusterName, NAMESPACE_ISOLATION_POLICIES));
    } catch (Exception e) {
        LOG.warn("GetIsolationPolicies: Unable to get the namespaceIsolationPolicies [{}]", e);
        return Optional.empty();
    }
}
//然后通过 namespace 的隔离策略中的自动故障转移策略来确定是否可以使用从 broker ，那么这个自动故障转移策略是怎么实现的呢？
public boolean shouldFailover(int totalPrimaryResourceUnits) {
    return this.auto_failover_policy.shouldFailoverToSecondary(totalPrimaryResourceUnits);
}
//原来，自动故障转移策略有个实现类，叫MinAvailablePolicy，即最小可用主broker数策略，从这里可以看出，小于预定义的最小可用限制，则在自动故障转移过程中，允许从 broker 加入到候选 broker 缓存中
@Override
public boolean shouldFailoverToSecondary(int totalPrimaryCandidates) {
    return totalPrimaryCandidates < this.min_limit;
}

//LoadManagerShared.applyNamespacePolicies 方法完成解析，其主要还是对选出来的 namespaceBundle，应用 namespace 策略，选出合适的 broker 放入候选 broker 集缓存（brokerCandidateCache）。好了，那么来解析 LoadManagerShared.shouldAntiAffinityNamespaceUnload 方法。

public static boolean shouldAntiAffinityNamespaceUnload(String namespace, String bundle, String currentBroker,final PulsarService pulsar, Map<String, Map<String, Set<String>>> brokerToNamespaceToBundleRange,Set<String> candidateBroekrs) throws Exception {
    //这里是计算出 broker 上 namespace 中策略的反亲和性组与特定的 namespace 中策略的反亲和性组相同数
    Map<String, Integer> brokerNamespaceCount = getAntiAffinityNamespaceOwnedBrokers(pulsar, namespace,                                                                 brokerToNamespaceToBundleRange).get(10, TimeUnit.SECONDS);
    //如果不为空
    if (brokerNamespaceCount != null && !brokerNamespaceCount.isEmpty()) {
        int leastNsCount = Integer.MAX_VALUE;
        int currentBrokerNsCount = 0;
        //迭代候选 broker 集
        for (String broker : candidateBroekrs) {
            //从存活的 broker 找出其 namespace 数
            int nsCount = brokerNamespaceCount.getOrDefault(broker, 0);
            //如果当前的 broker（这个为需要卸载 namespaceBundle 所在的 broker） 与 迭代中一 broker匹配，顺便就统计出当前 broker 上 namespace 数
            if (currentBroker.equals(broker)) {
                currentBrokerNsCount = nsCount;
            }
            //找出数量最少的 namespace 的 broker
            if (leastNsCount > nsCount) {
                leastNsCount = nsCount;
            }
        }
        // 如果 currentBrokerNsCount 大于候选 broker 集内其他 broker 上 namespace 数最少的总和 或者 总和等于0，则返回true（简单的说，就是统计候选 broker 集内每个 broker 上包含多少 namespace 数，选出最少数的 broker 与指定 currentBroker 上 namespace 数比较大小）
        if (leastNsCount == 0 || currentBrokerNsCount > leastNsCount) {
            return true;
        }
        // check if all the brokers having same number of ns-count then broker can't unload
        int leastNsOwnerBrokers = 0;
        // 候选 broker 集中有与 broker 最小 namespace 数相等的，累加 
        for (String broker : candidateBroekrs) {
            if (leastNsCount == brokerNamespaceCount.getOrDefault(broker, 0)) {
                leastNsOwnerBrokers++;
            }
        }
        // 注意，这里是唯一可能出现返回值为false，其意思非常明确:如果所有的候选 broker 具有相同数量的 namespace，那么就不能卸载（意味着卸载也没用，因为这个组里的broker状态都完全一样了，只能提醒扩容）。
        return candidateBroekrs.size() != leastNsOwnerBrokers;
    }
    return true;
}
// 统计指定 namespaceName 上反亲和性组与当前负载均衡器brokerToNamespaceToBundleRange中数据比对，找出 broker 与其 namespace 亲和性一致的映射
public static CompletableFuture<Map<String, Integer>> getAntiAffinityNamespaceOwnedBrokers(final PulsarService pulsar, String namespaceName,
    Map<String, Map<String, Set<String>>> brokerToNamespaceToBundleRange) {

    CompletableFuture<Map<String, Integer>> antiAffinityNsBrokersResult = new CompletableFuture<>();
    ZooKeeperDataCache<Policies> policiesCache = pulsar.getConfigurationCache().policiesCache();
    //从缓存中异步读取 namespaceName 的策略信息
    policiesCache.getAsync(path(POLICIES, namespaceName)).thenAccept(policies -> {
        if (!policies.isPresent() || StringUtils.isBlank(policies.get().antiAffinityGroup)) {
            antiAffinityNsBrokersResult.complete(null);
            return;
        }
        //反亲和性组
        final String antiAffinityGroup = policies.get().antiAffinityGroup;
        final Map<String, Integer> brokerToAntiAffinityNamespaceCount = new ConcurrentHashMap<>();
        final List<CompletableFuture<Void>> futures = Lists.newArrayList();
        //从 broker --> namespace --> namespaceBundles 统计反亲和性组的 broker 上 namespace 数，统计与参数 namespace 的反亲和性一致的 broker 上各个 namespaceBundle
        brokerToNamespaceToBundleRange.forEach((broker, nsToBundleRange) -> {
            nsToBundleRange.forEach((ns, bundleRange) -> {
                CompletableFuture<Void> future = new CompletableFuture<>();
                futures.add(future);
                policiesCache.getAsync(path(POLICIES, ns)).thenAccept(nsPolicies -> {
                    if (nsPolicies.isPresent() && antiAffinityGroup.equalsIgnoreCase(nsPolicies.get().antiAffinityGroup)) {
                        brokerToAntiAffinityNamespaceCount.compute(broker, (brokerName, count) -> count == null ? 1 : count + 1);
                    }
                    future.complete(null);
                }).exceptionally(ex -> {
                    future.complete(null);
                    return null;
                });
            });
        });
        FutureUtil.waitForAll(futures)
            .thenAccept(r -> antiAffinityNsBrokersResult.complete(brokerToAntiAffinityNamespaceCount));
    }).exceptionally(ex -> {
        // 这里是 namespace 没有创建 策略信息
        antiAffinityNsBrokersResult.complete(null);
        return null;
    });
    return antiAffinityNsBrokersResult;
}
```

好了，到这里，基本上 shouldAntiAffinityNamespaceUnload  方法已经解析完毕，这个方法所承担的功能其实就是执行 namespace 反亲缘性检查，当然，还有就是 broker 主从分组，，自动故障转移策略执行，持久化和非持久化 topic 支持的 broker 主机隔离，最终选出符合要求的候选 broke 集合（brokerCandidateCache）。

好了，总结下 doLoadShedding 方法的功能：先检查系统配置是否启用了自动减载（isLoadSheddingEnabled），负责找出集群内 broker 上 namespaceBundle 超载的分区，然后依次对 namespaceBundle 进行如下操作：执行 namespace 隔离策略（包括 broker 主从分组、反亲缘性检查）、故障转移策略检查，持久化和非持久化 topic 支持的 broker 主机隔离，期间，负载均衡器会记录符合要求的候选 broke 集合（brokerCandidateCache），然后对符合卸载要求的 namespaceBundle 进行卸载，并记录在 loadData 的最近卸载 bundles （loadData 的 recentlyUnloadedBundles）中。到了这里，读者可能会问，上面都没看到负载数据上报给 zookeeper 的操作还没看到？ 在2.2节，还有一个方法没解析到，那就是 writeLoadReportOnZookeeper 方法，那么请看：

```java
// 根据上面的得知，系统配置的负载均衡器实现实际上是 org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl,经过 ModularLoadManagerWrapper 包装后，实际上就是调用 ModularLoadManagerImpl 类的 writeBrokerDataOnZooKeeper 方法
public void writeLoadReportOnZookeeper() {
    loadManager.writeBrokerDataOnZooKeeper();
}
//也就是任一 broker 都会周期性调用此方法把 broker 负载信息上报到 zookeeper 上
public void writeBrokerDataOnZooKeeper() {
    try {
        //此方法已在负载均衡器启动时，详细分析过，不再赘述
        updateLocalBrokerData();
        //判断是否需要上报 zookeeper
        if (needBrokerDataUpdate()) {
            //更新上报最新时间
            localData.setLastUpdate(System.currentTimeMillis());
            //直接上报给 zookeeper
            zkClient.setData(brokerZnodePath, localData.getJsonBytes(), -1);
            //清理新增或丢失的 namespaceBundle 数据
            localData.getLastBundleGains().clear();
            localData.getLastBundleLosses().clear();
            // 把当前上报数据覆盖 lastData（具体前面已有介绍）
            lastData.update(localData);
        }
    } catch (Exception e) {
        log.warn("Error writing broker data on ZooKeeper: {}", e);
    }
}
//判断 broker 负载数据是否需要更新到 zookeeper
private boolean needBrokerDataUpdate() {
    //负载数据更新最大间隔
    final long updateMaxIntervalMillis = TimeUnit.MINUTES
        .toMillis(conf.getLoadBalancerReportUpdateMaxIntervalMinutes());
    //负载数据上一次上报 zookeeper 时间间隔
    long timeSinceLastReportWrittenToZooKeeper = System.currentTimeMillis() - localData.getLastUpdate();
    //如果 timeSinceLastReportWrittenToZooKeeper 大于 updateMaxIntervalMillis，则立即上报负载数据到 zookeeper ，返回 true
    if (timeSinceLastReportWrittenToZooKeeper > updateMaxIntervalMillis) {
        log.info("Writing local data to ZooKeeper because time since last update exceeded threshold of {} minutes",
                 conf.getLoadBalancerReportUpdateMaxIntervalMinutes());
        return true;
    }
    // 如果小于等于呢？则通过计算最小负载数据变更最大率（maxChange），通过系统配置的负载变更最大率阈值conf.getLoadBalancerReportUpdateThresholdPercentage()对比，如果 maxChange 大于阈值，则立即上报，否则等下一次上报（maxChange计算也较为简单:上一次上报的负载数据与当前本地负载数据每个指标计算出变动百分比，选出其中最大值）
    final double maxChange = Math
        .max(100.0 * (Math.abs(lastData.getMaxResourceUsage() - localData.getMaxResourceUsage())), Math.max(percentChange(lastData.getMsgRateIn() + lastData.getMsgRateOut(), localData.getMsgRateIn() + localData.getMsgRateOut()),
                      Math.max( percentChange(lastData.getMsgThroughputIn() + lastData.getMsgThroughputOut(),localData.getMsgThroughputIn() + localData.getMsgThroughputOut()), percentChange(lastData.getNumBundles(), localData.getNumBundles()))));
    if (maxChange > conf.getLoadBalancerReportUpdateThresholdPercentage()) {
        log.info("Writing local data to ZooKeeper because maximum change {}% exceeded threshold {}%; " +
                 "time since last report written is {} seconds", maxChange,
                 conf.getLoadBalancerReportUpdateThresholdPercentage(), timeSinceLastReportWrittenToZooKeeper/1000.0);
        return true;
    }
    return false;
}
//变动百分比
private double percentChange(final double oldValue, final double newValue) {
    if (oldValue == 0) {
        if (newValue == 0) {
            return 0;
        }
        return Double.POSITIVE_INFINITY;
    }
    return 100 * Math.abs((oldValue - newValue) / oldValue);
}
```

到了，读者忍不住爆发了，到底卸载的 namespaceBundle 的分配在什么地方完成的？请看如下方法：

```java
protected void validateTopicOwnership(TopicName topicName, boolean authoritative) {
    NamespaceService nsService = pulsar().getNamespaceService();

    try {
        // 通过 namespaceService 请求把 topic 所在的 namespace 分配给当前 broker 
        Optional<URL> webUrl = nsService.getWebServiceUrl(topicName, authoritative, isRequestHttps(), false);
        // 确保获取到 webUrl，否则抛异常
        if (webUrl == null || !webUrl.isPresent()) {
            log.info("Unable to get web service url");
            throw new RestException(Status.PRECONDITION_FAILED, "Failed to find ownership for topic:" + topicName);
        }
        //nameService 判断 topicName 是不是属于当前 broker，如果不属于，则重定向到相应的 broker 上
        if (!nsService.isServiceUnitOwned(topicName)) {
            boolean newAuthoritative = isLeaderBroker(pulsar());
            URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(webUrl.get().getHost())
                .port(webUrl.get().getPort()).replaceQueryParam("authoritative", newAuthoritative).build();
            // 抛异常，执行重定向
            log.debug("Redirecting the rest call to {}", redirect);
            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
        }
    } catch (IllegalArgumentException iae) {
        // namespace 格式非法
        log.debug(String.format("Failed to find owner for topic :%s", topicName), iae);
        throw new RestException(Status.PRECONDITION_FAILED, "Can't find owner for topic " + topicName);
    } catch (IllegalStateException ise) {
        log.debug(String.format("Failed to find owner for topic:%s", topicName), ise);
        throw new RestException(Status.PRECONDITION_FAILED, "Can't find owner for topic " + topicName);
    } catch (WebApplicationException wae) {
        throw wae;
    } catch (Exception oe) {
        log.debug(String.format("Failed to find owner for topic:%s", topicName), oe);
        throw new RestException(oe);
    }
}
//服务单位ID（上段代码请求的是 TopicName 实例）请求 broker 地址，这里注意readOnly 参数，这个参数不是 true，而是false，这很重要，从这个方法可以看出点题了文章开头的资源
public Optional<URL> getWebServiceUrl(ServiceUnitId suName, boolean authoritative, boolean isRequestHttps, boolean readOnly)
    throws Exception {
    if (suName instanceof TopicName) {
        TopicName name = (TopicName) suName;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting web service URL of topic: {} - auth: {}", name, authoritative);
        }
        //走这条分支，通过 topicName 定位 namespaceBundle
        return this.internalGetWebServiceUrl(getBundle(name), authoritative, isRequestHttps, readOnly).get();
    }

    if (suName instanceof NamespaceName) {
        return this.internalGetWebServiceUrl(getFullBundle((NamespaceName) suName), authoritative, isRequestHttps, readOnly).get();
    }

    if (suName instanceof NamespaceBundle) {
        return this.internalGetWebServiceUrl((NamespaceBundle) suName, authoritative, isRequestHttps, readOnly).get();
    }

    throw new IllegalArgumentException("Unrecognized class of NamespaceBundle: " + suName.getClass().getName());
}
//其核心为 findBundle 方法，前面的章节已经分析过了，这里不再赘述，简单说就是 topic 会落在 namespace 哪个 namespaceBundle
public NamespaceBundle getBundle(TopicName topicName) throws Exception {
    return bundleFactory.getBundles(topicName.getNamespaceObject()).findBundle(topicName);
}

private CompletableFuture<Optional<URL>> internalGetWebServiceUrl(NamespaceBundle bundle, boolean authoritative, boolean isRequestHttps, boolean readOnly) {
    //到达这里，终于知道这个方法的用意了，每次，就是通过 namespaceBundle 找 broker 位置，类似于服务发现的意思
    return findBrokerServiceUrl(bundle, authoritative, readOnly).thenApply(lookupResult -> {
        if (lookupResult.isPresent()) {
            try {
                LookupData lookupData = lookupResult.get().getLookupData();
                final String redirectUrl = isRequestHttps ? lookupData.getHttpUrlTls() : lookupData.getHttpUrl();
                return Optional.of(new URL(redirectUrl));
            } catch (Exception e) {
                //仅仅打印异常，返回空即可
                LOG.warn("internalGetWebServiceUrl [{}]", e.getMessage(), e);
            }
        }
        return Optional.empty();
    });
}
//激动人心的时刻，通过 namespaceBundle 寻找合适的 broker
private CompletableFuture<Optional<LookupResult>> findBrokerServiceUrl(NamespaceBundle bundle, boolean authoritative, boolean readOnly) {
    if (LOG.isDebugEnabled()) {
        LOG.debug("findBrokerServiceUrl: {} - read-only: {}", bundle, readOnly);
    }

    ConcurrentOpenHashMap<NamespaceBundle, CompletableFuture<Optional<LookupResult>>> targetMap;
    //是否需要认证，认证和非认证的分开
    if (authoritative) {
        targetMap = findingBundlesAuthoritative;
    } else {
        targetMap = findingBundlesNotAuthoritative;
    }

    return targetMap.computeIfAbsent(bundle, (k) -> {
        CompletableFuture<Optional<LookupResult>> future = new CompletableFuture<>();

        // 首先检查当前 broker 或其他 broker 已经所属了 namespaceBundle
        ownershipCache.getOwnerAsync(bundle).thenAccept(nsData -> {
            //表示没有查到 namespaceBundle 分配 broker 信息
            if (!nsData.isPresent()) {
                // 表示 namespaceBundle 无 broker 拥有（就是当前没分配给任一 broker）
                if (readOnly) {
                    //如果 readOnly 为 true，意味着不尝试把 namespaceBundle 分配 broker 上
                    future.complete(Optional.empty());
                } else {
                    // 现在，无 broker 拥有 namespaceBundle，这时候，尝试动态分配给候选 broker（这时候，上一章节更新的候选 broker 集终于要派上用场了）
                    pulsar.getExecutor().execute(() -> {
                        searchForCandidateBroker(bundle, future, authoritative);
                    });
                }
            } else if (nsData.get().isDisabled()) {
                // 表示 namespaceBundle 正在被卸载中，抛异常
                future.completeExceptionally(
                    new IllegalStateException(String.format("Namespace bundle %s is being unloaded", bundle)));
            } else {
                //这表明 namespaceBundle 已经被分配了，broker 信息为 nsData
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Namespace bundle {} already owned by {} ", bundle, nsData);
                }
                future.complete(Optional.of(new LookupResult(nsData.get())));
            }
        }).exceptionally(exception -> {
            LOG.warn("Failed to check owner for bundle {}: {}", bundle, exception.getMessage(), exception);
            future.completeExceptionally(exception);
            return null;
        });
        //如果执行完，则移除相应的
        future.whenComplete((r, t) -> pulsar.getExecutor().execute(
            () -> targetMap.remove(bundle)
        ));
        return future;
    });
}
//异步获取 NamespaceEphemeralData 的信息(主要用于定位 broker 的，并且标记为启用或禁用)
public CompletableFuture<Optional<NamespaceEphemeralData>> getOwnerAsync(NamespaceBundle suname) {
    String path = ServiceUnitZkUtils.path(suname);
    //先从当前 broker 所属的 namespaceBundles 尝试获取 suname 信息
    CompletableFuture<OwnedBundle> ownedBundleFuture = ownedBundlesCache.getIfPresent(path);
    if (ownedBundleFuture != null) {
        // suname 要么已经是当前broker所有，要么尝试分配给当前 broker
        return ownedBundleFuture.thenApply(serviceUnit -> {
            // 查看资源是否是存活的，返回响应的 broker 信息
            return Optional.of(serviceUnit.isActive() ? selfOwnerInfo : selfOwnerInfoDisabled);
        });
    }
    //如果suname 不属于当前 broker 的，则从只读缓存中读取 namespaceBundle 相关信息（即存在其他 broker 的 namespaceBundle 信息）
    return ownershipReadOnlyCache.getAsync(path);
}
//给被卸载的 namespaceBundle 选择候选的 broker（前面章节知道：候选 broker 集是经过负载管理器筛选过的）
private void searchForCandidateBroker(NamespaceBundle bundle,CompletableFuture<Optional<LookupResult>> lookupFuture, boolean authoritative) {
    String candidateBroker = null;
    try {
        // 先检查 namespaceBundle 所属的 namespace 是不是 Heartbeat 或 SLAMonitor namespace
        candidateBroker = checkHeartbeatNamespace(bundle);
        if (candidateBroker == null) {
            String broker = getSLAMonitorBrokerName(bundle);
            // 如果 broker 不为空，检查 broker 是不是存活的
            if (broker != null && isBrokerActive(broker)) {
                candidateBroker = broker;
            }
        }
        //还没找到候选 broker
        if (candidateBroker == null) {
            if (!this.loadManager.get().isCentralized() //暂时无用
                || pulsar.getLeaderElectionService().isLeader()//当前 broker 是否领导者
                // 如果领导者不是存活的，则回退到用当前 broker 负载均衡器拿到最小负载 broker（简单的说，如果领导者没选举出来或者宕机了，当前的 broker 也执行分配 namespaceBundle 任务）
                || !isBrokerActive(pulsar.getLeaderElectionService().getCurrentLeader().getServiceUrl())
               ) {
                // 通过 namespaceBundle 从负载均衡器拿到最小负载可用 broker
                Optional<String> availableBroker = getLeastLoadedFromLoadManager(bundle);
                if (!availableBroker.isPresent()) {
                    //可用为空，返回空
                    lookupFuture.complete(Optional.empty());
                    return;
                }
                candidateBroker = availableBroker.get();
            } else {
                //这里有2种情况：1. 当前 broker 不是领导者 2.当前 broker 是领导者且存活的
                if (authoritative) {
                    // 领导者 broker 已经分配 namespaceBundle 为当前 broker 所有
                    candidateBroker = pulsar.getWebServiceAddress();
                } else {
                    // 重定向领导者 broker 以进行分配
                    candidateBroker = pulsar.getLeaderElectionService().getCurrentLeader().getServiceUrl();
                }
            }
        }
    } catch (Exception e) {
        LOG.warn("Error when searching for candidate broker to acquire {}: {}", bundle, e.getMessage(), e);
        lookupFuture.completeExceptionally(e);
        return;
    }

    try {
        //预防性编码
        checkNotNull(candidateBroker);
        //发现选出的候选 broker 与当前 broker 一致
        if (pulsar.getWebServiceAddress().equals(candidateBroker)) {
            // 负载管理器决定让当前 broker 试着把 bundle 收归自己拥有
            ownershipCache.tryAcquiringOwnership(bundle).thenAccept(ownerInfo -> {
                // bundle 正在卸载，抛异常
                if (ownerInfo.isDisabled()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Namespace bundle {} is currently being unloaded", bundle);
                    }
                    lookupFuture.completeExceptionally(new IllegalStateException(
                        String.format("Namespace bundle %s is currently being unloaded", bundle)));
                } else {
                    // 已成功把 bundle 收归当前 broker 所有，那么现在开启一个任务进行预加载 topic 集（原先分配 namespaceBundle 上的 topic 集）
                    pulsar.loadNamespaceTopics(bundle);
                    //返回当前 broker 的地址信息
                    lookupFuture.complete(Optional.of(new LookupResult(ownerInfo)));
                }
            }).exceptionally(exception -> {
                LOG.warn("Failed to acquire ownership for namespace bundle {}: ", bundle, exception.getMessage(),
                         exception);
                lookupFuture.completeExceptionally(new PulsarServerException(
                    "Failed to acquire ownership for namespace bundle " + bundle, exception));
                return null;
            });

        } else {
            // 负载管理器决定把 bundle 重定向其他的 broker 上
            if (LOG.isDebugEnabled()) {
                LOG.debug("Redirecting to broker {} to acquire ownership of bundle {}", candidateBroker, bundle);
            }
            // 获取 candidateBroker 详细地址信息，返回
            createLookupResult(candidateBroker)
                .thenAccept(lookupResult -> lookupFuture.complete(Optional.of(lookupResult)))
                .exceptionally(ex -> {
                    lookupFuture.completeExceptionally(ex);
                    return null;
                });
        }
    } catch (Exception e) {
        LOG.warn("Error in trying to acquire namespace bundle ownership for {}: {}", bundle, e.getMessage(), e);
        lookupFuture.completeExceptionally(e);
    }
}
// 这里，就是调用负载均衡器的 getLeastLoaded 方法来获取负载最低的且可用的 broker
private Optional<String> getLeastLoadedFromLoadManager(ServiceUnitId serviceUnit) throws Exception {
    Optional<ResourceUnit> leastLoadedBroker = loadManager.get().getLeastLoaded(serviceUnit);
    //没有找到可用的 broker ，这返回空
    if (!leastLoadedBroker.isPresent()) {
        LOG.warn("No broker is available for {}", serviceUnit);
        return Optional.empty();
    }
    //找到可用的 broker ，返回其地址，这时候会重定向到这个 broker 上
    String lookupAddress = leastLoadedBroker.get().getResourceId();
    if (LOG.isDebugEnabled()) {
        LOG.debug("{} : redirecting to the least loaded broker, lookup address={}", pulsar.getWebServiceAddress(),
                  lookupAddress);
    }
    return Optional.of(lookupAddress);
}
//终于，负载均衡器的接口方法最后一个派上用场了
public Optional<ResourceUnit> getLeastLoaded(final ServiceUnitId serviceUnit) {
    //实际上，调用的是 ModularLoadManager 接口的 selectBrokerForAssignment 方法
    Optional<String> leastLoadedBroker = loadManager.selectBrokerForAssignment(serviceUnit);
    if (leastLoadedBroker.isPresent()) {
        return Optional.of(new SimpleResourceUnit(String.format("http://%s", leastLoadedBroker.get()),new PulsarResourceDescription()));
    } else {
        return Optional.empty();
    }
}
//此方法名还是非常点题，给定资源单元（这里是namespaceBundle），选择（状态最好） broker 进行分配
public Optional<String> selectBrokerForAssignment(final ServiceUnitId serviceUnit) {
    // 上文提到的 brokerCandidateCache 终于派上用场了，这里加锁
    synchronized (brokerCandidateCache) {
        final String bundle = serviceUnit.toString();
        //检查 bundle 是否已经预分配到 broker 上
        if (preallocatedBundleToBroker.containsKey(bundle)) {
            // 如果已预分配，则返回 broker 地址
            return Optional.of(preallocatedBundleToBroker.get(bundle));
        }
        //从负载数据中获取 bundleData
        final BundleData data = loadData.getBundleData().computeIfAbsent(bundle, key -> getBundleDataOrDefault(bundle));
        //清空broker候选缓存
        brokerCandidateCache.clear();
        //这里应用 namespace 策略，上文已经分析，不再赘述
        LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache, getAvailableBrokers(),brokerTopicLoadingPredicate);
        // 过滤掉 namespaceBundles 上拥有 topic 数超过阈值的 broker
        LoadManagerShared.filterBrokersWithLargeTopicCount(brokerCandidateCache, loadData, conf.getLoadBalancerBrokerMaxTopics());
        // 根据反亲和性组来把 namespace 分布到主机和 broker 集群上
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar, serviceUnit.toString(), brokerCandidateCache,brokerToNamespaceToBundleRange, brokerToFailureDomainMap);
        // 选出候选 broker 集中，包含 namespace 最少的 broker。
        LoadManagerShared.removeMostServicingBrokersForNamespace(serviceUnit.toString(), brokerCandidateCache,brokerToNamespaceToBundleRange);
        log.info("{} brokers being considered for assignment of {}", brokerCandidateCache.size(), bundle);

        // 用 broker 过滤器决定最后的 broker 候选集，官方只加了一个方法，就是 broker 版本过滤器（其作用就是 broker 版本，实现了滚动升级）
        try {
            for (BrokerFilter filter : filterPipeline) {
                filter.filter(brokerCandidateCache, data, loadData, conf);
            }
        } catch ( BrokerFilterException x ) {
            // 如果出现过滤异常，这里重新进行候选 broker ，意味着又回到了最初候选 broker 集
            LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache, getAvailableBrokers(),brokerTopicLoadingPredicate);
        }
        //如果此时broker 候选集为空，又得重新进行候选 broker ，意味着回到了最初候选 broker 集
        if ( brokerCandidateCache.isEmpty() ) {
            LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache, getAvailableBrokers(),brokerTopicLoadingPredicate);
        }

        // 如果可能，再通过一个打分策略选择 broker 
        Optional<String> broker = placementStrategy.selectBroker(brokerCandidateCache, data, loadData, conf);
        if (log.isDebugEnabled()) {
            log.debug("Selected broker {} from candidate brokers {}", broker, brokerCandidateCache);
        }

        if (!broker.isPresent()) {
            //此时通过打分选择 broker 如果为空，则直接返回
            return broker;
        }
        //从打分选出来的 broker 在进行有一次判断，是否超载
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        final double maxUsage = loadData.getBrokerData().get(broker.get()).getLocalData().getMaxResourceUsage();
        if (maxUsage > overloadThreshold) { 
            //所有已过滤的 broker 都已经过载，此时再进行一次选拔，从超载里面选出一个较好的 broker
            LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache, getAvailableBrokers(),brokerTopicLoadingPredicate);
            broker = placementStrategy.selectBroker(brokerCandidateCache, data, loadData, conf);
        }
        //把 bundle 新增到 预分配中
       loadData.getBrokerData().get(broker.get()).getPreallocatedBundleData().put(bundle, data);  //把 bundle 分配到指定 broker 上了
        preallocatedBundleToBroker.put(bundle, broker.get());
        final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
        final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
        //更新映射关系，返回经过层层选拔的 broker
        brokerToNamespaceToBundleRange.get(broker.get()).computeIfAbsent(namespaceName, k -> new HashSet<>()).add(bundleRange);
        return broker;
    }
}
//简单的说，就是从brokerCandidateCache中过滤掉 topic 拥有数（包括预分配的 topic 数）超过 loadBalancerBrokerMaxTopics 的 broker
public static void filterBrokersWithLargeTopicCount(Set<String> brokerCandidateCache, LoadData loadData,int loadBalancerBrokerMaxTopics) {
    Set<String> filteredBrokerCandidates = brokerCandidateCache.stream().filter((broker) -> {
        BrokerData brokerData = loadData.getBrokerData().get(broker);
        long totalTopics = brokerData != null && brokerData.getPreallocatedBundleData() != null? brokerData.getPreallocatedBundleData().values().stream()
            .mapToLong((preAllocatedBundle) -> preAllocatedBundle.getTopics()).sum()
            + brokerData.getLocalData().getNumTopics()
            : 0;
        return totalTopics <= loadBalancerBrokerMaxTopics;
    }).collect(Collectors.toSet());
    //把过滤后的 broker 放入 brokerCandidateCache
    if (!filteredBrokerCandidates.isEmpty()) {
        brokerCandidateCache.clear();
        brokerCandidateCache.addAll(filteredBrokerCandidates);
    }
}
//过滤反亲和性组
public static void filterAntiAffinityGroupOwnedBrokers(final PulsarService pulsar, final String assignedBundleName, final Set<String> candidates, final Map<String, Map<String, Set<String>>> brokerToNamespaceToBundleRange, Map<String, String> brokerToDomainMap) {
    if (candidates.isEmpty()) {
        return;
    }
    final String namespaceName = getNamespaceNameFromBundleName(assignedBundleName);
    try {
        //获取反亲和性的 broker 组，返回的是 broker 与 namespacebundle 数量，上文已解析过
        final Map<String, Integer> brokerToAntiAffinityNamespaceCount = getAntiAffinityNamespaceOwnedBrokers(pulsar,namespaceName, brokerToNamespaceToBundleRange).get(30, TimeUnit.SECONDS);
        if (brokerToAntiAffinityNamespaceCount == null) {
            // 如果没有 broker 属于反亲和性组，直接返回
            return;
        }
        //如果启用了故障域，则还需要进行故障域过滤
        if (pulsar.getConfiguration().isFailureDomainsEnabled()) {
        // 分属所有故障域的 broker，先计算故障域中 所有 broker 中包含的namespace 数，选出最小数N,然后移除候选 broker 集中所属 namespace 数的 broker。（简单的说，就是故障域中 broker 中namespace 数最小的留下，其他的全部过滤掉）
filterDomainsNotHavingLeastNumberAntiAffinityNamespaces(brokerToAntiAffinityNamespaceCount, candidates, brokerToDomainMap);
        }
        //这里的 candidates 可能经过2次过滤：1. 反亲和性组过滤 2. 故障域中选取其中 broker 集拥有 namespace 数最小的（参考上面方法）
        int leastNamaespaceCount = Integer.MAX_VALUE;
        for (final String broker : candidates) {
            if (brokerToAntiAffinityNamespaceCount.containsKey(broker)) {
                Integer namespaceCount = brokerToAntiAffinityNamespaceCount.get(broker);
                if (namespaceCount == null) {
                    // 如果 broker namespaceCount 为 null，则认为它没有分配 namespace
                    leastNamaespaceCount = 0;
                    break;
                }
                leastNamaespaceCount = Math.min(leastNamaespaceCount, namespaceCount);
            } else {
                //TODO broker 没有处于反亲和性组里面，可能是0 
                leastNamaespaceCount = 0;
                break;
            }
        }
        // 跟故障域中相同的算法，同样是找出 反亲和性组中 namespace 数最少的
        if (leastNamaespaceCount == 0) {
            //如果有等于0的情况，则直接移除那些大于0的即可
            candidates.removeIf(broker -> brokerToAntiAffinityNamespaceCount.containsKey(broker)
                                && brokerToAntiAffinityNamespaceCount.get(broker) > 0);
        } else {
            //如果大于0的情况，则移除那些不等于 namespace 数最少的
            final int finalLeastNamespaceCount = leastNamaespaceCount;
            candidates.removeIf(broker -> brokerToAntiAffinityNamespaceCount.get(broker) != finalLeastNamespaceCount);
        }
    } catch (Exception e) {
        log.error("Failed to filter anti-affinity group namespace {}", e.getMessage());
    }
}

//根据故障域分组，candidates 中找出故障域中 namespace 最少的 broker 保留，其他的 broker 全部移除
private static void filterDomainsNotHavingLeastNumberAntiAffinityNamespaces(
    Map<String, Integer> brokerToAntiAffinityNamespaceCount, Set<String> candidates,
    Map<String, String> brokerToDomainMap) {
    //故障域的分配，如果没有信息，这直接返回
    if (brokerToDomainMap == null || brokerToDomainMap.isEmpty()) {
        return;
    }

    final Map<String, Integer> domainNamespaceCount = Maps.newHashMap();
    int leastNamespaceCount = Integer.MAX_VALUE;
    //候选 broker 迭代
    candidates.forEach(broker -> {
        //获取 broker 所在的故障域，如果没有设置则默认
        final String domain = brokerToDomainMap.getOrDefault(broker, DEFAULT_DOMAIN);
        //读取 broker 所在的 namespace 数量
        final int count = brokerToAntiAffinityNamespaceCount.getOrDefault(broker, 0);
        //计算故障域内 namespace 数分布
        domainNamespaceCount.compute(domain, (domainName, nsCount) -> nsCount == null ? count : nsCount + count);
    });
    // 找故障域中 namespace 数量最少的
    for (Entry<String, Integer> domainNsCountEntry : domainNamespaceCount.entrySet()) {
        if (domainNsCountEntry.getValue() < leastNamespaceCount) {
            leastNamespaceCount = domainNsCountEntry.getValue();
        }
    }
    final int finalLeastNamespaceCount = leastNamespaceCount;
    // 候选 broker 集中仅仅保存故障域中 namespace 数等于最小的 namespace 数的 broker 集合（简单的说就是留下 namespace 数最少的 broker，移除其他的 broker）
    candidates.removeIf(broker -> {
        Integer nsCount = domainNamespaceCount.get(brokerToDomainMap.getOrDefault(broker, DEFAULT_DOMAIN));
        return nsCount != null && nsCount != finalLeastNamespaceCount;
    });
}

public static void removeMostServicingBrokersForNamespace(final String assignedBundleName,final Set<String> candidates, final Map<String, Map<String, Set<String>>> brokerToNamespaceToBundleRange) {
    //此时如果候选 broker 为空，直接返回
    if (candidates.isEmpty()) {
        return;
    }
    final String namespaceName = getNamespaceNameFromBundleName(assignedBundleName);
    int leastBundles = Integer.MAX_VALUE;
    //迭代候选 broker 集
    for (final String broker : candidates) {
        if (brokerToNamespaceToBundleRange.containsKey(broker)) {
            final Set<String> bundleRanges = brokerToNamespaceToBundleRange.get(broker).get(namespaceName);
            //意味着 assignedBundleName（此时还没分配，这番就是要找出 namespace 数最少的 broker） 所属的 namespace 中还没有分区信息（即一致性Hash还没分环）
            if (bundleRanges == null) {
                //意味着找到为0的，直接跳出
                leastBundles = 0;
                break;
            }
            leastBundles = Math.min(leastBundles, bundleRanges.size());
        } else {
            // 此时，候选的 broker 里面还是空的，什么 namespace 都没有分配
            leastBundles = 0;
            break;
        }
    }
    if (leastBundles == 0) {
        // 一般来说，如果 namespace 没有分区分配到 broker，这意味着 namespaceName 不存在，所以这里移除那些已有 namespace 的 broker 即可 （因为候选 broker 集里有还没有分配 namespace 的 broker）
        candidates.removeIf(broker -> brokerToNamespaceToBundleRange.containsKey(broker)
                            && brokerToNamespaceToBundleRange.get(broker).containsKey(namespaceName));
    } else {
        final int finalLeastBundles = leastBundles;
        // 安全的假设：任何一个 broker 的 namespace 至少有一个 bundle。candidates 依次迭代：移除那些 broker 所属 namespace 数量大于 finalLeastBundles 的元素（broker）
        candidates.removeIf(broker -> brokerToNamespaceToBundleRange.get(broker).get(namespaceName)
                            .size() != finalLeastBundles);
    }
}

//加载 bundle 中所有的 topics
public void loadNamespaceTopics(NamespaceBundle bundle) {
    executor.submit(() -> {
        LOG.info("Loading all topics on bundle: {}", bundle);

        NamespaceName nsName = bundle.getNamespaceObject();
        List<CompletableFuture<Topic>> persistentTopics = Lists.newArrayList();
        long topicLoadStart = System.nanoTime();
        //通过 namespaceService 加载 nsName 所有 topics
        for (String topic : getNamespaceService().getListOfPersistentTopics(nsName)) {
            try {
                TopicName topicName = TopicName.get(topic);
                //这里就是把 topic 分配到 bundle 中，并且加载 topic （即尝试获取，如果没有，则创建 topic ，当然这里加载用于持久化的 topic）
                if (bundle.includes(topicName)) {
                    //这个方法因为跟负载均衡没有直接关联，故没有深入，事实上，这是 pulsar 负载均衡的根基之一——轻量级的 topic 转移
                    CompletableFuture<Topic> future = brokerService.getOrCreateTopic(topic);
                    if (future != null) {
                        persistentTopics.add(future);
                    }
                }
            } catch (Throwable t) {
                LOG.warn("Failed to preload topic {}", topic, t);
            }
        }
        //如果持久化 topic 集不为空
        if (!persistentTopics.isEmpty()) {
            //等待所有的持久化 topic 获取或创建成功，记录此次加载耗时
            FutureUtil.waitForAll(persistentTopics).thenRun(() -> {
                double topicLoadTimeSeconds = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - topicLoadStart)
                    / 1000.0;
                LOG.info("Loaded {} topics on {} -- time taken: {} seconds", persistentTopics.size(), bundle,
                         topicLoadTimeSeconds);
            });
        }
        return null;
    });
}
// 通过 candidateBroker 读取其详细地址信息
protected CompletableFuture<LookupResult> createLookupResult(String candidateBroker) throws Exception {

    CompletableFuture<LookupResult> lookupFuture = new CompletableFuture<>();
    try {
        checkArgument(StringUtils.isNotBlank(candidateBroker), "Lookup broker can't be null " + candidateBroker);
        URI uri = new URI(candidateBroker);
        //构造 candidateBroker 相关的path
        String path = String.format("%s/%s:%s", LoadManager.LOADBALANCE_BROKERS_ROOT, uri.getHost(),uri.getPort());
        //从本地 zookeeper 异步读取数据，并通过负载均衡器的反序列化方法解析，获取到 LocalBrokerData  信息，这样就可以查找 candidateBroker 所有的地址信息了，返回 LookupResult。
        pulsar.getLocalZkCache().getDataAsync(path, pulsar.getLoadManager().get().getLoadReportDeserializer()).thenAccept(reportData -> {
            if (reportData.isPresent()) {
                ServiceLookupData lookupData = reportData.get();
                lookupFuture.complete(new LookupResult(lookupData.getWebServiceUrl(),
                                                       lookupData.getWebServiceUrlTls(), lookupData.getPulsarServiceUrl(),lookupData.getPulsarServiceUrlTls()));
            } else {
                lookupFuture.completeExceptionally(new KeeperException.NoNodeException(path));
            }
        }).exceptionally(ex -> {
            lookupFuture.completeExceptionally(ex);
            return null;
        });
    } catch (Exception e) {
        lookupFuture.completeExceptionally(e);
    }
    return lookupFuture;
}
// 此方法位于 ModularLoadManagerStrategy 接口实现类 LeastLongTermMessageRate 中，从类名就可以看出长期消息速率最小（策略），简单的选出长期内消息速率最小的 broker，来看看吧
public Optional<String> selectBroker(final Set<String> candidates, final BundleData bundleToAssign, final LoadData loadData, final ServiceConfiguration conf) {
    //清空当前缓存
    bestBrokers.clear();
    //设置正无限大
    double minScore = Double.POSITIVE_INFINITY;
    // 这里主要是给所有的候选 broker 打分，选出最好的，然后随机选择一个 broker 返回
    for (String broker : candidates) {
        final BrokerData brokerData = loadData.getBrokerData().get(broker);
        //核心方法，对 broker 打分
        final double score = getScore(brokerData, conf);
        if (score == Double.POSITIVE_INFINITY) {
            //打印超载的 broker 数据
            final LocalBrokerData localData = brokerData.getLocalData();
            log.warn(
                "Broker {} is overloaded: CPU: {}%, MEMORY: {}%, DIRECT MEMORY: {}%, BANDWIDTH IN: {}%, "
                + "BANDWIDTH OUT: {}%",
                broker, localData.getCpu().percentUsage(), localData.getMemory().percentUsage(),
                localData.getDirectMemory().percentUsage(), localData.getBandwidthIn().percentUsage(),
                localData.getBandwidthOut().percentUsage());

        }
        //把最低分数的 broker 放入 bestBrokers 中
        if (score < minScore) {
            // 一旦遇到没有分数小于 minScore 的，则清空原 bestBrokers，并把当前 broker 放入bestBrokers 里，重置 minScore
            bestBrokers.clear();
            bestBrokers.add(broker);
            minScore = score;
        } else if (score == minScore) {
            // 当前 score 等于 minScore，放入
            bestBrokers.add(broker);
        }
    }
    if (bestBrokers.isEmpty()) {
        // 所有的 broker 都已经超载，还是把候选的 broker 集 放入 bestBroker 集，最后随机选择一个 
        bestBrokers.addAll(candidates);
    }

    if (bestBrokers.isEmpty()) {
        // 这里意味着，此时没有可用的 broker
        return Optional.empty();
    }
    //此时从 bestBroker 集中随机选择一个 broker
    return Optional.of(bestBrokers.get(ThreadLocalRandom.current().nextInt(bestBrokers.size())));
}
//通过 broker 的负载数据以及系统配置来计分
private static double getScore(final BrokerData brokerData, final ServiceConfiguration conf) {
    //负载超载阈值
    final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
    //最大资源使用率（这里就是 broker 上资源使用最大使用负载）
    final double maxUsage = brokerData.getLocalData().getMaxResourceUsage();
    //如果最大资源使用率大于负载超载阈值，则认为超载了，返回正无穷大
    if (maxUsage > overloadThreshold) {
        log.warn("Broker {} is overloaded: max usage={}", brokerData.getLocalData().getWebServiceUrl(), maxUsage);
        return Double.POSITIVE_INFINITY;
    }

    double totalMessageRate = 0;
    //这里计算预分配的 namespaceBundles 长期消息扇入扇出总数
    for (BundleData bundleData : brokerData.getPreallocatedBundleData().values()) {
        final TimeAverageMessageData longTermData = bundleData.getLongTermData();
        totalMessageRate += longTermData.getMsgRateIn() + longTermData.getMsgRateOut();
    }
    //计算 broker 的长期消息扇入扇出总数
    final TimeAverageBrokerData timeAverageData = brokerData.getTimeAverageData();
    final double timeAverageLongTermMessageRate = timeAverageData.getLongTermMsgRateIn()
        + timeAverageData.getLongTermMsgRateOut();
    //计算预估分数
    final double totalMessageRateEstimate = totalMessageRate + timeAverageLongTermMessageRate;

    if (log.isDebugEnabled()) {
        log.debug("Broker {} has long term message rate {}",brokerData.getLocalData().getWebServiceUrl(), totalMessageRateEstimate);
    }
    return totalMessageRateEstimate;
}
```

#### 3.5 负载均衡器工作流程小结

好了，到这里，整个流程很明朗了，那么现在描述整个负载均衡的工作流程：

1. 创建负载均衡器。
   1. 主要初始化负载均衡器的数据结构，并初始化数据上报线程。
2. 初始化负载均衡器。
   1. 读取本地 zookeeper 中 /loadbalance/brokers 节点的缓存，并监控其数据变更。一旦变更，则回调 reapDeadBrokerPreallocations 和  updateAll 方法。前者是清理已经宕机或停机的 broker 的预分配的资源，后者事实上调用了三个方法，其主要功能为更新 broker 主机资源、namespaceBundle 集的负载数据，如果当前 broker 是领导者，还会对 namespaceBundle集的负载数据与设定的阈值进行比对，一旦发现超载的 namespaceBundle ，则发送拆分 namespaceBundle 命令给其所属的 broker 进行拆分作业。
   2. 读取本地 zookeeper 中当前 broker 缓存数据，并监控其数据变更。一旦变更，则回调 updateAll 方法。
   3. 启动 broker 所在主机资源负载收集任务，定时把计算数据更新到缓存。
   4. 创建 namespaceBundle 过载判断服务，主要用于后来的判断 namespaceBundle 所用资源与配置阈值是否过载。
   5. 创建 namespaceBundle 拆分策略任务（BundleSplitterTask），主要提供拆分执行方法。
   6. 创建 namespaceBundle 分配服务（ModularLoadManagerStrategy），主要用于把拆分后的 namespaceBundle 附加到合适的 broker 上。 
   7. 创建 简单的资源分配任务（SimpleResourceAllocationPolicies），即  namespace 隔离策略，主要用于主、从、共享 broker 资源隔离（通过Java正则表达式来打标签）以及执行自动故障转移策略。
   8. 新增 broker 过滤器（BrokerVersionFilter），这里主要部署时，滚动升级。
   9. 如果启用 broker 故障域，则立即刷新故障域（refreshBrokerToFailureDomainMap），监控故障域列表，注册回调方法（refreshBrokerToFailureDomainMap），故障域主要用来逻辑把 broker 集群隔离开来，避免影响全局。
3. 启动负载均衡器

      1. 更新本地 broker 负载数据，即获取主机负载数据和namespaceBundle状态数据，更新到 loadData 中。
      2. 再次尝试注册   /loadbalance/brokers 节点，已存在节点异常忽略。把当前主机的地址信息注册到  /loadbalance/brokers 节点下（与 zookeeper 失去连接即把主机信息删除），这里还有非常特别的处理，注册主机信息时，如果出现节点已存在异常，这检查会话信息，是否建立两个会话，如果会话ID有两个，则抛异常，否则清空当前节点的负载数据。
      3. 在 zookeeper  上的 /loadbalance/broker-time-average 节点，注册主机信息，并在主机节点清空短长期负载数据。
      4. 调用 updateAll  方法更新负载数据。
         * 更新所有的 broker 负载数据：读取集群内所有存活的 broker，依次从 zookeeper 上的 /loadbalance/brokers/IP:Port 节点读取 LocalBrokerData 数据，并更新集群内所有的 broker 负载数据（BrokerData），移除停机或宕机的负载数据，新增新加入的 broker 负载数据。
         * 更新所有的 broker 上 namespaceBundle 负载数据：因为已经更新所有的 broker 负载数据，其包含 namespaceBundleStats 数据，通过读取其，来更新 loadData 的 bundleData 数据，loadData  的 BrokerData 中 预分配 namespaceBundle 数据（preallocatedBundleData），以及更新负载时间聚合数据（TimeAverageData），并且更新 broker 到 namespace 到 namespaceBundle 的映射关系 （brokerToNamespaceToBundleRange）。
         * 通过以上负载数据更新，就可以检查负载超载问题。如果配置没有启用自动 namespaceBundle 拆分功能 或 broker 选举服务为空 或 当前 broker 不是领导者（leader），则直接返回。所以，当前 broker 必须是 `leader` 才有权限执行 namespaceBundle 拆分任务：
           1.  先拿到 BundleSplitterTask 对象锁，调用 findBundlesToSplit 方法把以上负载数据与一系列的配置阈值比较，得出超载的 namespaceBundle 集。
           2. 对上个步骤获取的 namespaceBundle 集合进行迭代：
              1. 获取 adminClient 的 namespace 服务，发 splitNamespaceBundle 命令给当前 broker 把超载的 namespaceBundle 拆分（如果 namespaceBundle 不属于当前 broker，会重定向到  namespace 所有者 broker 上执行命令）。
              2.  从 loadData ，localData 移除相应的 namespaceBundle 信息。
              3.  从 namespaceBundleFactory 刷新 namespaceBundle 缓存信息。
              4.  从 zookeeper 上 /loadbalance/bundle-data/~~namespaceBundle~~ 删除此节点。

​           5.  更新最后刷新时间

到此，基本上整个负载均衡器工作流程都关注到了，从以上源码详细分析可知，这只是展示了负载均衡器的部分功能。那么还有其他功能没有覆盖到，请往下看：

* 向 zookeeper 上报 broker 负载数据

  如果启用负载均衡器，则启动负载均衡定时任务，通过定时执行（默认是5秒）LoadReportUpdaterTask 任务，运行负载均衡器 org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl 类 writeBrokerDataOnZooKeeper 方法执行数据上报任务，具体如下：

  1. 同样是调用负载均衡器 updateLocalBrokerData 方法（此方法上文已有描述，不再单独说明），主要功能是更新当前主机、当前 broker 上 namespaceBundle 集负载信息到内存中。
  2. 检查是否需要把本地负载数据更新到 zookeeper 上：
     *  检查负载报告最大更新间隔时间 loadBalancerReportUpdateMaxIntervalMinutes 是否已经小于距离上一次更新数据到 zookeeper 时间。如果是，则立即返回 true，否则继续判定。
     * 通过计算上一次负载数据与本地负载数据的最大变动百分比 maxChange，来对比负载报告更新最大阈值百分比 loadBalancerReportUpdateThresholdPercentage，如果前者大于阈值，则立即返回  true，否则返回 false。
  3. 根据步骤2，来决定是否立即上报本地负载数据到 zookeeper，刷新最新更新时间，并且把本次负载数据 localData 更新到 lastData，并清空一些数据。

* 实际执行 namespaceBundle 拆分任务

​        位于 admin 模块的 namespace 中，以对外接口形式提供服务。总结如下：

1. 验证 namespace 在 tenant 是否可用。

   2. 验证当前用户是否有权限访问此接口。
   3. 验证 namespace 是否属于当前 broker，如果非全局（global）namespace，还需验证 tenant  中所允许的集群集是否包含 namespace 所在的集群（cluster）。
   4. 验证策略信息读写情况，并且验证下是否能连接上全局（global） zookeeper。
   5. 验证被拆分的 namespaceBundle 是否所属当前的 broker，如果不是，则会重定向到属于 namespaceBundle 的 broker 上。
   6. 真正调用拆分方法，并且默认重试7次，如果7次还没拆分成功，则抛异常：
      1. 默认把当前的 namespaceBundle 拆分2个小的 namespaceBundle，并尝试把新产生的2个小 namespaceBundle 分配到当前 broker。
      2. 把拆分后的 namespaceBundles 更新到 zookeeper 上，并刷新策略信息。
      3. 如果出现拆分异常，则重试（超过7次不再重试，返回）。
      4. 把原 namespaceBundle 在 broker 上缓存禁用。
      5. 读取原 namespaceBundle 上所有的 topic，重新分配到2个小的 namespaceBundle 上，并更新本地的 namespace 、namespaceBundle 、topic 映射关系，刷新 bundleStat 信息。
      6. 如果设置了 `unload`参数，则卸载 2个小的 namespaceBundle,卸载主要是做如下：
         * 把 namespaceBundle 禁用，禁止有新的生产者和消费者连接。
         * 关闭 namespaceBundle 上所有的 topic，包括 topic 上所有的副本连接器、生产者、消费者、ledger（非持久化的 topic 无需）等，并在 broker 缓存中移除 topic。
         * 删除 zookeeper 上的 /namespace/~~namebundle~~ 节点，并从本地缓存移除其相关信息。

* 拆分后的 namespaceBundle 重新分配到新的 broker 上

​        因为篇幅，前面方法**validateTopicOwnership**调用并没有给出到底是哪个组件调用，在什么时候调用。事实上，它是 broker 的一个命令`LOOKUP`对应的处理方法 `handleLookup`, 客户端在生成或订阅指定 topic 时，需要查找 topic 所属哪个 broker。这时候通过 topic 全限定名相关的 namespace 来确定落在哪个namespaceBundle 上，然后通过查找 namespaceBundle 所属 broker 来确定分配情况，具体过程如下（这里假定请求的 topic 恰好落在前文已拆分且已被卸载的 namespaceBundle）：

1. 由 topic 中的 namespace 最终确定所属 namespaceBundle （这里是一致性哈希算法，上文已有交代）。
2. 尝试从当前 broker 的缓存中读取 namespaceBundle 信息，如果找到，则直接返回（即 namespaceBundle 已经分配给当前的 broker）。如果未找到，则尝试异步为 namespaceBundle 搜索候选 broker（searchForCandidateBroker）：
   1.  检查 namespaceBundle 所属的 namespace 是否为 Heartbeat 或 SLAMonitor namespace，如果是，则选择当前 broker 为候选 broker，否则继续。
   2. 如果负载均衡器的集中分配标志为 false（目前默认为 true），或当前 broker 被选举为领导者，或 当前集群中领导者非存活状态，则通过调用负载均衡器方法`getLeastLoaded`返回负载最小的的 broker，如果为空，则返回空，否则以返回的最优 broker 为候选 broker。实际上，其调用的是 `ModularLoadManagerImpl` 的 `selectBrokerForAssignment` 方法，此方法工作流程如下：
      1.  从 `preallocatedBundleToBroker` 中读取 namespaceBundle，是否已经为其分配 broker，如果找到了，直接返回，否则继续。
      2. 从 zookeeper 中读取 namespaceBundle 的 bundleData，并清空 `brokerCandidateCache`。
      3. 迭代存活的 broker 集，根据指定策略（即 namespaceBundle 所在的 namespace 中的策略），选择合适的 broker 给 namespaceBundle，放入 `brokerCandidateCache`，详细如下：
         1. 如果设置了隔离策略（isIsolationPoliciesPresent）来区分主（Primary）从（Secondary） broker，否则，根据持久化、非持久化 的topic 和 broker 本身属性来区分 从 broker。
         2. 如果设置了隔离策略，只有主 broker 才能放入候选 broker 集（`brokerCandidateCache`），中，又根据 namespace 策略故障转移（目前只有 MinAvailablePolicy）方式，来决定从 broker 也能放入候选 broker 集，否则从 broker 也无条件放入候选 broker 集。
      4. 迭代候选 broker 集，计算 broker 上当前 topic 总数 和预分配 topic总数之和，如果超过配置 broker 设定最大 topic 数（loadBalancerBrokerMaxTopics），则过滤掉（即从候选 broker 集移除）。否则，继续。
      5. 继续迭代候选 broker 集，过滤反亲和性组过滤以及故障域过滤，详细如下：
         1. 如果 namespaceBundle 所属的 namespace 没有设置反亲和性组或为空，则直接返回。否则，通过 `brokerToNamespaceToBundleRange` 计算出 broker 上 antiAffinityGroup 总数，如果计算出为 null，直接返回。
         2. 如果启用了故障域，则迭代候选 broker 集，通过上节计算出的结果，计算出各故障域中 namespace 总数，再通过计算结果找出包含 namespace 数最小故障域。最后保留包含 namespace 数最少的 候选 broker 集。
         3. 继续迭代候选 broker 集，找出反亲和性组中包含最少的 候选 broker 集。
      6. 迭代候选 broker 集，找出包含 namespace 最小值，把候选 broker 集中包含大于 namespace 数的 broker 全部移除。
      7. 执行 BrokerFilter 过滤链（目前主要用于过滤低于当前最新 broker 版本的，应用场景就是滚动升级），如果执行过程出现异常，则重新执行步骤3。
      8. 如果候选 broker 集为空，则重新执行步骤3。
      9. 执行 ModularLoadManagerStrategy，迭代候选 broker 集，通过计算其每个 broker 负载数据得出负载率，选出负载最小的 broker 。详细如下：
         1. 通过 broker ，获取其负载数据BrokerData，并得出最大资源使用率（maxResourceUsage），大于系统设置的 broker负载超载使用率（ loadBalancerBrokerOverloadedThresholdPercentage / 100.0），则返回 Double.POSITIVE_INFINITY，否则通过计算 PreallocatedBundleData 和 brokerData 的长期采样消息扇入扇出之和，得出负载资源使用率。
         2. 通过以上步骤计算，就可以选出资源使用率最低的 broker 集，把它放入 bestBroker 集中。
         3. 如果 bestBroker 为空，则意味着所有的候选 broker 都超载了，那么就把候选 broker 集全部放入 bestBroker 中，如果此时 bestBroker  还为空，则只能返回空了，否则从bestBroker 随机选择一个 broker 返回。
      10. 如果此时没有从步骤9选出最优 broker，则直接返回空。否则，把选出最优的 broker 进行超载判断，如果超载，则再重新执行步骤3，然后执行步骤9，再一次选出最优的 broker。
      11. 此时，更新PreallocatedBundleData （Map<namespaceBundle，bundleData>）、preallocatedBundleToBroker （Map<namespaceBundle，broker>）、brokerToNamespaceToBundleRange（Map<broker,Map<namespace,set<namespaceBundle>>>），返回最优的 broker。
   3. 否则，如果 `authoritative` 为 true，则领导者强制当前 broker 为候选 broker，否则把当前处于领导者角色的 broker  为候选 broker。
   4. 如果候选 broker 等于当前的 broker（即地址信息一样），则尝试把 namespaceBundle 收为当前 broker 所有，读取 namespaceBundle 状态信息，如果处于禁用状态，则抛异常。否则提交一个异步任务：读取 namespaceBundle 对应的 namespace 所有的 topic 列表，执行 topic 分配到 namespaceBundle 任务，任务提交后，返回查询结果（LookupResult(ownerInfo)，即返回当前 broker 地址）。
   5. 否则，通过候选 broker 的地址信息，尝试从本地 zookeeper 读取其完整的地址信息，如果读取成功，则同样返回结果（LookupResult），否则抛没有此节点异常（比如此节点停机或宕机了）。
3. 到达这里，清理下垃圾，返回，数据放在（future中）。

   

## 总结

​        本文详细的分析 pulsar 的 broker 端整个负载均衡过程，作为Cloud Native设计的消息系统，负载均衡尤其重要。当然，也支持一些非常好的特性，如自动故障转移、支持持久化非持久化 broker 隔离、故障域逻辑分组、反亲和性组、主从组等，其中几个特性都是为了更好的进行隔离作用。当然了，作为展望，完全可以更进一步，本身 pulsar 已经是计算、存储分离的架构，无状态、有状态组件已经完全分离，这使得负载均衡器更好的调度资源，broker 、bookkeeper 均容器化，负载均衡器通过 k8s进行调度管理（更加智能的水平伸缩），实现智能化的云系统。