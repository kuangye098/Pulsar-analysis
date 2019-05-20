# Pulsar 源码解析

说明：

1. 源码版本采用的是 2.3.1 。
2. 中英文对照表如下：

英文|中文（或作用）
:--:|---:
Client|客户端
Producer|生产者
Consumer|消费者
Topic|主题
Schema|架构（结构化类型）
Broker|消息代理服务器
Partition|分区
DLQ|死信队列

## 1. Producer 使用简单例子

以下是 Producer 最简单的发送消息例子，源码解析就按照这个顺序抽丝剥茧，步步深入。

```java
// 创建一个Pulsar客户端，直接指定Pulsar Broker的IP地址和端口
PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

//创建一个生产者，并指定Topic为persistent://my-tenant/my-ns/my-topic，消息格式为字节数组
Producer<byte[]> producer = client.newProducer().topic("persistent://my-tenant/my-ns/my-topic").create();

//发送消息
for (int i = 0; i < 10; i++) {
    producer.send("my-message".getBytes());
}

//关闭客户端
client.close();
```

## 2. Pulsar Client 接口

PulsarClient 是 Pulsar 的Java专用客户端，定义了客户端所有操作，如下：

```java
// 默认构造器模式实现，用于新建给定配置的Pulsar实例
public static ClientBuilder builder() {
    return DefaultImplementation.newClientBuilder();
}

//创建生产者构造器，其用于创建给定配置的生产者，默认生产数据类型为字节数组
ProducerBuilder<byte[]> newProducer();

//同上，这里强生产数据泛型，并可指定系统序列化方式
<T> ProducerBuilder<T> newProducer(Schema<T> schema);

//创建消费者构造器，其用于创建给定配置订阅特定Topic的消费者，默认消费数据类型为字节数组
ConsumerBuilder<byte[]> newConsumer();

//同上，这里强调消费数据泛型，并可指定系统反序列化方式
<T> ConsumerBuilder<T> newConsumer(Schema<T> schema);

//创建读取者构造器，其用于创建给定配置读取特定Topic的读取者，默认读取数据类型为字节数组（它与消费者的区别在于，它不需要向broker发送Ack消息）
ReaderBuilder<byte[]> newReader();

//同上，这里强调读取数据为泛型，并可指定系统反序列化方式
<T> ReaderBuilder<T> newReader(Schema<T> schema);

//更新客户端服务URL
void updateServiceUrl(String serviceUrl) throws PulsarClientException;

//获取指定Topic的分区信息
CompletableFuture<List<String>> getPartitionsForTopic(String topic);

//和平关闭PulsarClient，释放所有资源
void close() throws PulsarClientException;

//异步，同上
CompletableFuture<Void> closeAsync();

//立即关闭客户端，不管资源是否被释放
void shutdown() throws PulsarClientException;
```

## 3. Pulsar Client 配置项

```java
//broker或discovery-service或pulsar-proxy地址
private String serviceUrl;
//这里可以根据实际需求，动态提供serviceUrl
@JsonIgnore
private ServiceUrlProvider serviceUrlProvider;
//认证接口，默认是禁用的
@JsonIgnore
private Authentication authentication = new AuthenticationDisabled();
//操作超时时间
private long operationTimeoutMs = 30000;
//状态收集时间间隔
private long statsIntervalSeconds = 60;
//生产者网络IO线程数
private int numIoThreads = 1;
//消费者订阅线程数
private int numListenerThreads = 1;
//每个broker（与客户端）连接数
private int connectionsPerBroker = 1;
//是否延迟发送（Nagle算法，不延迟，意味着立即发送）
private boolean useTcpNoDelay = true;
//是否用TLS
private boolean useTls = false;
//TLS受信任证书路径
private String tlsTrustCertsFilePath = "";
//TLS是否允许不安全连接
private boolean tlsAllowInsecureConnection = false;
//TLS是否启用主机名校验
private boolean tlsHostnameVerificationEnable = false;
//Lookup请求并发数
private int concurrentLookupRequest = 5000;
//最大Lookup请求数
private int maxLookupRequest = 50000;
//每个连接最大拒绝请求数（？是不是每个连接最大请求数超过N时，则拒绝发送）
private int maxNumberOfRejectedRequestPerConnection = 50;
//保活间隔时间
private int keepAliveIntervalSeconds = 30;
//连接超时时间
private int connectionTimeoutMs = 10000;
```

## 4. PulsarClient 的实现

### 1.PulsarClientImpl 的属性

PulsarClientImpl 是个有状态的管理类，主要功能如下:

 1. 实现PulsarClient接口
 2. 管理生产者、消费者的生命周期
 3. 提供一些公共服务，如LookupService

```java
//客户端配置数据
private final ClientConfigurationData conf;
//查找服务
private LookupService lookup;
//连接池
private final ConnectionPool cnxPool;
//定时器
private final Timer timer;
//线程池提供者
private final ExecutorProvider externalExecutorProvider;
//客户端状态
private AtomicReference<State> state = new AtomicReference<>();
//生产者对象池
private final IdentityHashMap<ProducerBase<?>, Boolean> producers;
//消费者对象池
private final IdentityHashMap<ConsumerBase<?>, Boolean> consumers;
//生产者ID、消费者ID、请求ID生成器
private final AtomicLong producerIdGenerator = new AtomicLong();
private final AtomicLong consumerIdGenerator = new AtomicLong();
private final AtomicLong requestIdGenerator = new AtomicLong();
private final EventLoopGroup eventLoopGroup;
```

### 2. PulsarClientImpl 的构造方法

```java
//构造方法1 根据配置创建EventLoopGroup
public PulsarClientImpl(ClientConfigurationData conf) throws PulsarClientException {
    this(conf, getEventLoopGroup(conf));
}

//构造方法2 根据配置创建连接池
public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
    this(conf, eventLoopGroup, new ConnectionPool(conf, eventLoopGroup));
}

public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool)
        throws PulsarClientException {
    //配置、serverUrl、eventLoopGroup均不能为空
    if (conf == null || isBlank(conf.getServiceUrl()) || eventLoopGroup == null) {
        throw new PulsarClientException.InvalidConfigurationException("Invalid client configuration");
    }
    this.eventLoopGroup = eventLoopGroup;
    this.conf = conf;
    //启动认证方法
    conf.getAuthentication().start();
    this.cnxPool = cnxPool;
    externalExecutorProvider = new ExecutorProvider(conf.getNumListenerThreads(), getThreadFactory("pulsar-external-listener"));
    //根据协议选择使用哪种方式提供查找服务
    if (conf.getServiceUrl().startsWith("http")) {
        lookup = new HttpLookupService(conf, eventLoopGroup);
    } else {
        lookup = new BinaryProtoLookupService(this, conf.getServiceUrl(), conf.isUseTls(), externalExecutorProvider.getExecutor());
    }
    //定时器
    timer = new HashedWheelTimer(getThreadFactory("pulsar-timer"), 1, TimeUnit.MILLISECONDS);
    producers = Maps.newIdentityHashMap();
    consumers = Maps.newIdentityHashMap();
    //设置状态为打开
    state.set(State.Open);
}
```

这里就有两个重要的组件，ConntionPool 和 LookupService ，下面我们来简单的解析下。

### 3. Pulsar Client 接口方法实现

实现 Pulsar Client 接口，有2个关键组件，如下：

#### 1.ConntionPool 连接池

这里只描述下属性，具体使用方法将会在 Pulsar Producer 章节分析

```java
/**
 * 主要是管理客户端与broker或proxy的连接
 * */
//核心容器，用于存客户端与broker或proxy的连接
protected final ConcurrentHashMap<InetSocketAddress, ConcurrentMap<Integer, CompletableFuture<ClientCnx>>> pool;
private final Bootstrap bootstrap;
private final EventLoopGroup eventLoopGroup;
//每个主机最大连接
private final int maxConnectionsPerHosts;
//域名解析器
protected final DnsNameResolver dnsResolver;

//就一个核心方法，原型如下：
public CompletableFuture<ClientCnx> getConnection(InetSocketAddress logicalAddress,
            InetSocketAddress physicalAddress);
```

#### 2. LookupService 服务

查找服务有两个协议实现，一个是Http协议实现，一个是native(TCP)协议实现

主要有如下功能:

```java
/**
 * 查找服务
 */
public interface LookupService extends AutoCloseable {
//用于动态更新serviceUrl
void updateServiceUrl(String serviceUrl) throws PulsarClientException;
//根据Topic来获取活的broker地址
public CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> getBroker(TopicName topicName);
//根据Topic获取Topic分区信息
public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(TopicName topicName);
//根据Topic获取Schema信息
public CompletableFuture<Optional<SchemaInfo>> getSchema(TopicName topicName);
//获取serviceUrl
public String getServiceUrl();
//获取Namespace下所有的topic
public CompletableFuture<List<String>> getTopicsUnderNamespace(NamespaceName namespace, Mode mode);
}
```

#### 3. 实现 Pulsar Client 接口方法

这里的生产者、消费者、读取者创建构造者实例方法签名都是一样的，分为两类：

1. 默认Schema为BYTES。

2. 自定义Schema，创建者指定Schema。

然后构造者都有一个默认实现方法**xx create() throws PulsarClientException**，分别来创建生产者、消费者、读取者实例，并完成实例的初始化，而此方法最终调用的为**createXXAsync**系列方法。下面，将讲解核心方法的实现。

#### 1. 创建生产者

```java
// 1. 创建生产者构造者实例（PulsarClientImpl类）。
@Override
public ProducerBuilder<byte[]> newProducer() {
    return new ProducerBuilderImpl<>(this, Schema.BYTES);
}
// 2. 切换 ProducerBuilderImpl<T> 类，构造方法
public ProducerBuilderImpl(PulsarClientImpl client, Schema<T> schema) {
        this(client, new ProducerConfigurationData(), schema);
}

// 3. 切换 ProducerBuilderImpl<T> 类，同步创建一个生产者
@Override
public Producer<T> create() throws PulsarClientException {
    try {
        return createAsync().get();
    } catch (ExecutionException e) {
        Throwable t = e.getCause();
        if (t instanceof PulsarClientException) {
            throw (PulsarClientException) t;
        } else {
            throw new PulsarClientException(t);
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PulsarClientException(e);
    }
}

@Override
public CompletableFuture<Producer<T>> createAsync() {
    if (conf.getTopicName() == null) {
        return FutureUtil
                .failedFuture(new IllegalArgumentException("Topic name must be set on the producer builder"));
    }

    try {
        //设置消息路由模式，有3种
        //默认为轮询
        setMessageRoutingMode();
    } catch(PulsarClientException pce) {
        return FutureUtil.failedFuture(pce);
    }

    return interceptorList == null || interceptorList.size() == 0 ?
            client.createProducerAsync(conf, schema, null) :
            client.createProducerAsync(conf, schema, new ProducerInterceptors<>(interceptorList));
}

// 4. 切换到 PulsarClientImpl 类 ，异步创建生产者实例。

public <T> CompletableFuture<Producer<T>> createProducerAsync(ProducerConfigurationData conf, Schema<T> schema,
          ProducerInterceptors<T> interceptors) {
    //配置为空
    if (conf == null) {
        return FutureUtil.failedFuture(
            new PulsarClientException.InvalidConfigurationException("Producer configuration undefined"));
    }
    //如果Schema为自动消费者Schema实例，由于这里是生产者，故抛异常
    if (schema instanceof AutoConsumeSchema) {
        return FutureUtil.failedFuture(
            new PulsarClientException.InvalidConfigurationException("AutoConsumeSchema is only used by consumers to detect schemas automatically"));
    }
    //Pulsar 客户端是否初始化完成，如果状态不是Open，则抛异常：客户端已关闭。
    if (state.get() != State.Open) {
        return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed : state = " + state.get()));
    }

    String topic = conf.getTopicName();
    //对Topic进行格式校验
    if (!TopicName.isValid(topic)) {
        return FutureUtil.failedFuture(
            new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
    }
    //自动获取Schema
    if (schema instanceof AutoProduceBytesSchema) {
        AutoProduceBytesSchema autoProduceBytesSchema = (AutoProduceBytesSchema) schema;
        //这里就是查找服务，通过Topic查询注册的Schema，如果没注册，则默认Schema.BYTES
        return lookup.getSchema(TopicName.get(conf.getTopicName()))
                .thenCompose(schemaInfoOptional -> {
                    if (schemaInfoOptional.isPresent()) {
                        autoProduceBytesSchema.setSchema(Schema.getSchema(schemaInfoOptional.get()));
                    } else {
                        autoProduceBytesSchema.setSchema(Schema.BYTES);
                    }
                    return createProducerAsync(topic, conf, schema, interceptors);
                });
    } else {
        return createProducerAsync(topic, conf, schema, interceptors);
    }

}

private <T> CompletableFuture<Producer<T>> createProducerAsync(String topic,
                                                            ProducerConfigurationData conf,
                                                            Schema<T> schema,
                                                            ProducerInterceptors<T> interceptors) {
CompletableFuture<Producer<T>> producerCreatedFuture = new CompletableFuture<>();
    //通过查找服务获取Topic元数据，这里主要是确定Topic是否分区，以此来确定用哪个底层生产者实例
    getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
        }

        ProducerBase<T> producer;
        if (metadata.partitions > 1) {
            producer = new PartitionedProducerImpl<>(PulsarClientImpl.this, topic, conf, metadata.partitions,
                    producerCreatedFuture, schema, interceptors);
        } else {
            producer = new ProducerImpl<>(PulsarClientImpl.this, topic, conf, producerCreatedFuture, -1, schema, interceptors);
        }

        synchronized (producers) {
            producers.put(producer, Boolean.TRUE);
        }
    }).exceptionally(ex -> {
        log.warn("[{}] Failed to get partitioned topic metadata: {}", topic, ex.getMessage());
        producerCreatedFuture.completeExceptionally(ex);
        return null;
    });

    return producerCreatedFuture;
}
```

到此，创建生产者实例就结束了，接下来章节会详细描述，生产者怎么实现的。接下来，看看消费者的创建。

#### 2. 创建消费者（订阅）

```java

// 1. 消费者构造者实例（PulsarClientImpl 类）。
@Override
public ConsumerBuilder<byte[]> newConsumer() {
    return new ConsumerBuilderImpl<>(this, Schema.BYTES);
}

// ConsumerBuilderImpl 构造方法
public ConsumerBuilderImpl(PulsarClientImpl client, Schema<T> schema) {
    this(client, new ConsumerConfigurationData<T>(), schema);
}

// 2. 消费者开始订阅 ConsumerBuilderImpl
// 这里实际上，调用的是异步订阅方法
@Override
public Consumer<T> subscribe() throws PulsarClientException {
    try {
        return subscribeAsync().get();
    } catch (ExecutionException e) {
        Throwable t = e.getCause();
        if (t instanceof PulsarClientException) {
            throw (PulsarClientException) t;
        } else {
            throw new PulsarClientException(t);
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PulsarClientException(e);
    }
}

@Override
public CompletableFuture<Consumer<T>> subscribeAsync() {
    //检查 Topic 是否有效
    if (conf.getTopicNames().isEmpty() && conf.getTopicsPattern() == null) {
        return FutureUtil
                .failedFuture(new InvalidConfigurationException("Topic name must be set on the consumer builder"));
    }
    //检查订阅名不能为空
    if (StringUtils.isBlank(conf.getSubscriptionName())) {
        return FutureUtil.failedFuture(
                new InvalidConfigurationException("Subscription name must be set on the consumer builder"));
    }
    //检查是否有拦截器
    return interceptorList == null || interceptorList.size() == 0 ?
            client.subscribeAsync(conf, schema, null) :
            client.subscribeAsync(conf, schema, new ConsumerInterceptors<>(interceptorList));
}

// 这里切换到 PulsarClientImpl 类，实现订阅前置检查，实际功能委托给 消费者实现类
public <T> CompletableFuture<Consumer<T>> subscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
    // 检查 Client 状态是否打开
    if (state.get() != State.Open) {
        return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
    }
    // 配置不能为空
    if (conf == null) {
        return FutureUtil.failedFuture(
                new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
    }
    //检查 Topic 是否合法
    if (!conf.getTopicNames().stream().allMatch(TopicName::isValid)) {
        return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name"));
    }
  
    //订阅名不能为空
    if (isBlank(conf.getSubscriptionName())) {
        return FutureUtil
                .failedFuture(new PulsarClientException.InvalidConfigurationException("Empty subscription name"));
    }

    //读取压缩特性仅仅被用订阅持久化 Topic，并且订阅类型为独占或故障转移模式
    if (conf.isReadCompacted() && (!conf.getTopicNames().stream()
            .allMatch(topic -> TopicName.get(topic).getDomain() == TopicDomain.persistent)
            || (conf.getSubscriptionType() != SubscriptionType.Exclusive
                    && conf.getSubscriptionType() != SubscriptionType.Failover))) {
        return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                "Read compacted can only be used with exclusive of failover persistent subscriptions"));
    }

    // 事件监听器只允许在故障转移订阅模式下
    if (conf.getConsumerEventListener() != null && conf.getSubscriptionType() != SubscriptionType.Failover) {
        return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                "Active consumer listener is only supported for failover subscription"));
    }

    //   如果已经设置了 正则表达式订阅 Topic，就不能指定具体的 Topic
    if (conf.getTopicsPattern() != null) {
        if (!conf.getTopicNames().isEmpty()){
            return FutureUtil
                .failedFuture(new IllegalArgumentException("Topic names list must be null when use topicsPattern"));
        }
        //正则表达式订阅
        return patternTopicSubscribeAsync(conf, schema, interceptors);
    } else if (conf.getTopicNames().size() == 1) {
        //单 Topic 订阅
        return singleTopicSubscribeAsync(conf, schema, interceptors);
    } else {
        //多 Topic 订阅
        return multiTopicSubscribeAsync(conf, schema, interceptors);
    }
}

//单 Topic 订阅
private <T> CompletableFuture<Consumer<T>> singleTopicSubscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
    // 如果 schema 是 AutoConsumeSchema 的实例，尝试自动获取 Schema 类型
    if (schema instanceof AutoConsumeSchema) {
        AutoConsumeSchema autoConsumeSchema = (AutoConsumeSchema) schema;
        return lookup.getSchema(TopicName.get(conf.getSingleTopic()))
                .thenCompose(schemaInfoOptional -> {
                    //目前只支持SchemaType.AVRO
                    if (schemaInfoOptional.isPresent() && schemaInfoOptional.get().getType() == SchemaType.AVRO) {
                        GenericSchema genericSchema = GenericSchema.of(schemaInfoOptional.get());
                        log.info("Auto detected schema for topic {} : {}",
                            conf.getSingleTopic(), new String(schemaInfoOptional.get().getSchema(), UTF_8));
                        autoConsumeSchema.setSchema(genericSchema);
                        return doSingleTopicSubscribeAsync(conf, schema, interceptors);
                    } else {
                        return FutureUtil.failedFuture(
                            new PulsarClientException.LookupException("Currently schema detection only works for topics with avro schemas"));
                    }
                });
    } else {
        return doSingleTopicSubscribeAsync(conf, schema, interceptors);
    }
}

private <T> CompletableFuture<Consumer<T>> doSingleTopicSubscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
    //查看是否已有订阅存在（基于订阅名和自订阅类型为共享模式）
    Optional<ConsumerBase<T>> subscriber = subscriptionExist(conf);
    if (subscriber.isPresent()) {
        return CompletableFuture.completedFuture(subscriber.get());
    }

    CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();

    String topic = conf.getSingleTopic();

    getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
        }

        ConsumerBase<T> consumer;
        // 从执行器列表中获取下一个单线程版执行器
        ExecutorService listenerThread = externalExecutorProvider.getExecutor();
        if (metadata.partitions > 1) {
            //这里注意，多分区 Topic 也对应着多 Topic 消费者实现
            consumer = MultiTopicsConsumerImpl.createPartitionedConsumer(PulsarClientImpl.this, conf,
                listenerThread, consumerSubscribedFuture, metadata.partitions, schema, interceptors);
        } else {
            //单 Topic 消费者实现
            consumer = ConsumerImpl(PulsarClientImpl.this, topic, conf, listenerThread, -1,consumerSubscribedFuture, SubscriptionMode.Durable, null, schema, interceptors);
        }

        synchronized (consumers) {
            consumers.put(consumer, Boolean.TRUE);
        }
    }).exceptionally(ex -> {
        log.warn("[{}] Failed to get partitioned topic metadata", topic, ex);
        consumerSubscribedFuture.completeExceptionally(ex);
        return null;
    });

    return consumerSubscribedFuture;
}

// 多 Topic 订阅，这里是真正的多 Topic 订阅，但可能其中0或多个 Topic 是多分区
private <T> CompletableFuture<Consumer<T>> multiTopicSubscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
    Optional<ConsumerBase<T>> subscriber = subscriptionExist(conf);
    if (subscriber.isPresent()) {
        return CompletableFuture.completedFuture(subscriber.get());
    }

    CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();

    ConsumerBase<T> consumer = new MultiTopicsConsumerImpl<>(PulsarClientImpl.this, conf,
            externalExecutorProvider.getExecutor(), consumerSubscribedFuture, schema, interceptors);

    synchronized (consumers) {
        consumers.put(consumer, Boolean.TRUE);
    }

    return consumerSubscribedFuture;
}

//Topic 正则表达式订阅
private <T> CompletableFuture<Consumer<T>> patternTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
        Schema<T> schema, ConsumerInterceptors<T> interceptors) {
    String regex = conf.getTopicsPattern().pattern();
    Mode subscriptionMode = convertRegexSubscriptionMode(conf.getRegexSubscriptionMode());
    TopicName destination = TopicName.get(regex);
    //有正则表达式解析出 Namespace ，例如：persistent://public/default/.*
    NamespaceName namespaceName = destination.getNamespaceObject();
    //检查是否已存在相同的订阅
    Optional<ConsumerBase<T>> subscriber = subscriptionExist(conf);
    if (subscriber.isPresent()) {
        return CompletableFuture.completedFuture(subscriber.get());
    }

    CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();
    //根据 Namespace 和 订阅模式来查找所有符合条件的 Topic
    lookup.getTopicsUnderNamespace(namespaceName, subscriptionMode)
        .thenAccept(topics -> {
            if (log.isDebugEnabled()) {
                log.debug("Get topics under namespace {}, topics.size: {}", namespaceName.toString(), topics.size());
                topics.forEach(topicName ->
                    log.debug("Get topics under namespace {}, topic: {}", namespaceName.toString(), topicName));
            }
            //这里进行正则表达式过滤
            List<String> topicsList = topicsPatternFilter(topics, conf.getTopicsPattern());
            //配置好新的 Topic
            conf.getTopicNames().addAll(topicsList);
            ConsumerBase<T> consumer = new PatternMultiTopicsConsumerImpl<T>(conf.getTopicsPattern(),
                PulsarClientImpl.this,
                conf,
                externalExecutorProvider.getExecutor(),
                consumerSubscribedFuture,
                schema, subscriptionMode, interceptors);

            synchronized (consumers) {
                consumers.put(consumer, Boolean.TRUE);
            }
        })
        .exceptionally(ex -> {
            log.warn("[{}] Failed to get topics under namespace", namespaceName);
            consumerSubscribedFuture.completeExceptionally(ex);
            return null;
        });

    return consumerSubscribedFuture;
}

// 从原Topic列表中匹配 ‘Topic正则表达式’，结果返回仅仅是 Topic 名，无分区部分
public static List<String> topicsPatternFilter(List<String> original, Pattern topicsPattern) {
    final Pattern shortenedTopicsPattern = topicsPattern.toString().contains("://")
        ? Pattern.compile(topicsPattern.toString().split("\\:\\/\\/")[1]) : topicsPattern;

    return original.stream()
        .map(TopicName::get)
        .map(TopicName::toString)
        .filter(topic -> shortenedTopicsPattern.matcher(topic.split("\\:\\/\\/")[1]).matches())
        .collect(Collectors.toList());
}

```

到此，创建消费者实例就结束了，已经引出了4个不同的消费者实现，接下来章节会详细描述，消费者怎么实现的。另外，这里附录 PulsarClientImpl的一些方法实现。

#### 创建读取者

```java
@Override
public <T> ProducerBuilder<T> newProducer(Schema<T> schema) {
    return new ProducerBuilderImpl<>(this, schema);
}

@Override
public <T> ConsumerBuilder<T> newConsumer(Schema<T> schema) {
    return new ConsumerBuilderImpl<>(this, schema);
}

@Override
public ReaderBuilder<byte[]> newReader() {
    return new ReaderBuilderImpl<>(this, Schema.BYTES);
}

@Override
public <T> ReaderBuilder<T> newReader(Schema<T> schema) {
    return new ReaderBuilderImpl<>(this, schema);
}

@Override
public synchronized void updateServiceUrl(String serviceUrl) throws PulsarClientException {
    log.info("Updating service URL to {}", serviceUrl);
    //设置配置文件新的serviceUrl
    conf.setServiceUrl(serviceUrl);
    // 更新本地serviceUrl的配置
    lookup.updateServiceUrl(serviceUrl);
    //由于服务地址变更，关闭所有与broker或proxy连接
    cnxPool.closeAllConnections();
}

@Override
public CompletableFuture<List<String>> getPartitionsForTopic(String topic) {
    //获取Topic的元数据，返回Topic Partition 信息
    return getPartitionedTopicMetadata(topic).thenApply(metadata -> {
        if (metadata.partitions > 1) {
            TopicName topicName = TopicName.get(topic);
            List<String> partitions = new ArrayList<>(metadata.partitions);
            for (int i = 0; i < metadata.partitions; i++) {
                partitions.add(topicName.getPartition(i).toString());
            }
            return partitions;
        } else {
            return Collections.singletonList(topic);
        }
    });
}
```

### 3. Pulsar Client 的关闭

关闭 Pulsar Client 分为2个步骤：

1. 设置客户端为**State.Closing**状态，并依次异步关闭生产者和消费者。
2. Step 1 正常执行完后，关闭 客户端 本身的资源，成功后设置**State.Closed**状态。

#### 1. 优雅的关闭

```java

//同步关闭，实际上就是调用异步关闭方法，等待完成
@Override
public void close() throws PulsarClientException {
    try {
        closeAsync().get();
    } catch (ExecutionException e) {
        Throwable t = e.getCause();
        if (t instanceof PulsarClientException) {
            throw (PulsarClientException) t;
        } else {
            throw new PulsarClientException(t);
        }
    } catch (InterruptedException e) {
        throw new PulsarClientException(e);
    }
}

//异步关闭
@Override
public CompletableFuture<Void> closeAsync() {
    log.info("Client closing. URL: {}", lookup.getServiceUrl());
    //校验状态，如果不是Open状态，抛异常
    if (!state.compareAndSet(State.Open, State.Closing)) {
        return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
    }

    final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    List<CompletableFuture<Void>> futures = Lists.newArrayList();
    //依次（异步）关闭生产者
    synchronized (producers) {
        //拷贝新的列表，因为关闭时将触发从map移除元素，使迭代器失效
        List<ProducerBase<?>> producersToClose = Lists.newArrayList(producers.keySet());
        producersToClose.forEach(p -> futures.add(p.closeAsync()));
    }

    synchronized (consumers) {
        List<ConsumerBase<?>> consumersToClose = Lists.newArrayList(consumers.keySet());
        consumersToClose.forEach(c -> futures.add(c.closeAsync()));
    }

    //等待所有的生产者消费者正常关闭，这样就优雅的关闭客户端了
    FutureUtil.waitForAll(futures).thenRun(() -> {
        try {
            shutdown();
            closeFuture.complete(null);
            state.set(State.Closed);
        } catch (PulsarClientException e) {
            closeFuture.completeExceptionally(e);
        }
    }).exceptionally(exception -> {
        closeFuture.completeExceptionally(exception);
        return null;
    });

    return closeFuture;
}

@Override
public void shutdown() throws PulsarClientException {
    try {
        //关闭查找
        lookup.close();
        //关闭连接池
        cnxPool.close();
        //定时器关闭
        timer.stop();
        //额外执行线程池关闭
        externalExecutorProvider.shutdownNow();
        //认证服务关闭
        conf.getAuthentication().close();
    } catch (Throwable t) {
        log.warn("Failed to shutdown Pulsar client", t);
        throw new PulsarClientException(t);
    }
}
```

## 5. Pulsar Producer 的配置项

```java
//创建生产者实例
Producer<T> create() throws PulsarClientException;
//异步创建生产者实例
CompletableFuture<Producer<T>> createAsync();
//从给定config读取配置
ProducerBuilder<T> loadConf(Map<String, Object> config);
//克隆当前的生产者构造器
ProducerBuilder<T> clone();
//设置生产者发消息的Topic
ProducerBuilder<T> topic(String topicName);
//设置生产者名称
ProducerBuilder<T> producerName(String producerName);
//设置发送消息超时时间
ProducerBuilder<T> sendTimeout(int sendTimeout, TimeUnit unit);
//设置最大正处理消息数
ProducerBuilder<T> maxPendingMessages(int maxPendingMessages);
//设置分区总正处理消息数（实际上，对于分区Topic，每个分区最大正处理消息数为min(maxPendingMessages,maxPendingMessagesAcrossPartitions/partitionNum)）
ProducerBuilder<T> maxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions);
//如果启用，队列满了阻塞（生产者发消息）
ProducerBuilder<T> blockIfQueueFull(boolean blockIfQueueFull);
//设置(分区生产者)消息路由模式
ProducerBuilder<T> messageRoutingMode(MessageRoutingMode messageRoutingMode);
//设置发送消息时，选择分区的哈希计算函数
ProducerBuilder<T> hashingScheme(HashingScheme hashingScheme);
//设置压缩类型
ProducerBuilder<T> compressionType(CompressionType compressionType);
//设置自定义消息路由策略
ProducerBuilder<T> messageRouter(MessageRouter messageRouter);
//是否启用自动批量（发送消息）模式
ProducerBuilder<T> enableBatching(boolean enableBatching);
//设置加密消息的密钥读取器
ProducerBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader);
//增加公共加密key，用于生产者加密数据的key
ProducerBuilder<T> addEncryptionKey(String key);
//设置加密失败时的动作
ProducerBuilder<T> cryptoFailureAction(ProducerCryptoFailureAction action);
//设置批量消息发送最大延迟
ProducerBuilder<T> batchingMaxPublishDelay(long batchDelay, TimeUnit timeUnit);
//设置批量消息最大消息数
ProducerBuilder<T> batchingMaxMessages(int batchMessagesMaxMessagesPerBatch);
//设置生产者消息初始序列id
ProducerBuilder<T> initialSequenceId(long initialSequenceId);
//设置生产者的key-value属性
ProducerBuilder<T> property(String key, String value);
//同上
ProducerBuilder<T> properties(Map<String, String> properties);
//设置生产者拦截器
ProducerBuilder<T> intercept(ProducerInterceptor<T> ... interceptors);
//如果启用，分区生产者将在运行时自动发现新增分区。 这仅适用于Partitioned Topics。
ProducerBuilder<T> autoUpdatePartitions(boolean autoUpdate);
```

## 6. Pulsar Producer 接口

```java
//获取Topic
String getTopic();
//获取生产者名称
String getProducerName();
//发送消息
MessageId send(T message) throws PulsarClientException;
//异步发送消息
CompletableFuture<MessageId> sendAsync(T message);
//刷新客户端的消息缓存并阻塞，直到所有消息成功保存。
void flush() throws PulsarClientException;
//异步刷新，功能同上
CompletableFuture<Void> flushAsync();
//创建一个新消息构造器
TypedMessageBuilder<T> newMessage();
//获取生产者最后一个序列ID
long getLastSequenceId();
//获取消费者状态
ProducerStats getStats();
//关闭生产者并且释放所有分配资源
void close() throws PulsarClientException;
//异步关闭，功能同上
CompletableFuture<Void> closeAsync();
//判断生产者是否已连接上broker
boolean isConnected();
```

## 7. Pulsar Producer 的实现

生产者根据无分区和分区的 Topic 分为两个实现：ProducerImpl 和 PartitionedProducerImpl 。

### 1. 无分区 Producer 实现，类继承如图

![ProducerImpl](./images/20190411200209.png)

* HandleState 抽象类，用于管理生产者状态，并且持有Topic和PulsarClient实例
* ProducerBase 抽象类，实现部分 Producer 接口，生产者拦截器调用，抽象出 Producer 公共实现，绝大部分实现都放在更具体的实现类中。
* ProducerImpl 无分区 Producer 实现类，主要实现消息发送，加密，批量消息发送等。

#### 1. ProducerImpl 的属性

```java
//生产者ID，用于标识单个连接
private final long producerId;
//消息ID生成器
private volatile long msgIdGenerator;
//待处理消息队列
private final BlockingQueue<OpSendMsg> pendingMessages;
//待处理回调队列
private final BlockingQueue<OpSendMsg> pendingCallbacks;
//信号量，用于控制发送消息频率
private final Semaphore semaphore;
//发送超时定时器
private volatile Timeout sendTimeout = null;
//批量发送超时定时器
private volatile Timeout batchMessageAndSendTimeout = null;
//创建生产者超时时间
private long createProducerTimeout;
//每次批量最大消息数
private final int maxNumMessagesInBatch;
//批量消息容器
private final BatchMessageContainer batchMessageContainer;
//最新消息发送（状态）标记
private CompletableFuture<MessageId> lastSendFuture = CompletableFuture.completedFuture(null);
// 全局唯一生产者名称
private String producerName;
//连接ID
private String connectionId;
private String connectedSince;
//分区索引（ID）
private final int partitionIndex;
//生产者状态记录器
private final ProducerStatsRecorder stats;
//消息压缩类型
private final CompressionCodec compressor;
//最后一个发送消息ID
private volatile long lastSequenceIdPublished;
//消息加密器
private MessageCrypto msgCrypto = null;
//KEY生成任务
private ScheduledFuture<?> keyGeneratorTask = null;
//元数据容器
private final Map<String, String> metadata;
//
private Optional<byte[]> schemaVersion = Optional.empty();
//连接句柄
private final ConnectionHandler connectionHandler;
//原子字段更新器，消息ID
private static final AtomicLongFieldUpdater<ProducerImpl> msgIdGeneratorUpdater = AtomicLongFieldUpdater.newUpdater(ProducerImpl.class, "msgIdGenerator");
```

#### 2. 构造方法（实例化过程）

构造过程也是**与服务器建立连接**的过程。

```java

public ProducerImpl(PulsarClientImpl client, String topic, ProducerConfigurationData conf,
                CompletableFuture<Producer<T>> producerCreatedFuture, int partitionIndex, Schema<T> schema,
                ProducerInterceptors<T> interceptors) {
    //这里producerCratedFuture，用于实现异步创建
    super(client, topic, conf, producerCreatedFuture, schema, interceptors);
    this.producerId = client.newProducerId();
    this.producerName = conf.getProducerName();
    this.partitionIndex = partitionIndex;
    //初始化数组阻塞队列，这里有限制深度
    this.pendingMessages = Queues.newArrayBlockingQueue(conf.getMaxPendingMessages());
    //同上
    this.pendingCallbacks = Queues.newArrayBlockingQueue(conf.getMaxPendingMessages());
    //新建信号量，限制信号量大小，并且是公平的（意味着排队）
    this.semaphore = new Semaphore(conf.getMaxPendingMessages(), true);
    //选择压缩方法
    this.compressor = CompressionCodecProvider.getCompressionCodec(conf.getCompressionType());
    //初始化最后发送序列ID和消息ID
    if (conf.getInitialSequenceId() != null) {
        long initialSequenceId = conf.getInitialSequenceId();
        this.lastSequenceIdPublished = initialSequenceId;
        this.msgIdGenerator = initialSequenceId + 1;
    } else {
        this.lastSequenceIdPublished = -1;
        this.msgIdGenerator = 0;
    }
    //如果开启消息加密，则加载加密方法和密钥加载器
    if (conf.isEncryptionEnabled()) {
        String logCtx = "[" + topic + "] [" + producerName + "] [" + producerId + "]";
        this.msgCrypto = new MessageCrypto(logCtx, true);

        // Regenerate data key cipher at fixed interval
        keyGeneratorTask = client.eventLoopGroup().scheduleWithFixedDelay(() -> {
            try {
                msgCrypto.addPublicKeyCipher(conf.getEncryptionKeys(), conf.getCryptoKeyReader());
            } catch (CryptoException e) {
                if (!producerCreatedFuture.isDone()) {
                    log.warn("[{}] [{}] [{}] Failed to add public key cipher.", topic, producerName, producerId);
                    producerCreatedFuture.completeExceptionally(e);
                }
            }
        }, 0L, 4L, TimeUnit.HOURS);
    }

    //如果设置发消息超时时间，则初始化消息发送超时任务
    if (conf.getSendTimeoutMs() > 0) {
        sendTimeout = client.timer().newTimeout(this, conf.getSendTimeoutMs(), TimeUnit.MILLISECONDS);
    }
    //创建生产者超时时间
    this.createProducerTimeout = System.currentTimeMillis() + client.getConfiguration().getOperationTimeoutMs();
    //是否启用批量消息特性
    if (conf.isBatchingEnabled()) {
        this.maxNumMessagesInBatch = conf.getBatchingMaxMessages();
        this.batchMessageContainer = new BatchMessageContainer(maxNumMessagesInBatch,
                CompressionCodecProvider.convertToWireProtocol(conf.getCompressionType()), topic, producerName);
    } else {
        this.maxNumMessagesInBatch = 1;
        this.batchMessageContainer = null;
    }
    //初始化生产者状态记录器
    if (client.getConfiguration().getStatsIntervalSeconds() > 0) {
        stats = new ProducerStatsRecorderImpl(client, conf, this);
    } else {
        stats = ProducerStatsDisabled.INSTANCE;
    }

    if (conf.getProperties().isEmpty()) {
        metadata = Collections.emptyMap();
    } else {
        metadata = Collections.unmodifiableMap(new HashMap<>(conf.getProperties()));
    }
    //创建连接处理器
    this.connectionHandler = new ConnectionHandler(this,
        new Backoff(100, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS, Math.max(100, conf.getSendTimeoutMs() - 100), TimeUnit.MILLISECONDS),
        this);
    //开始连接broker
    grabCnx();
}
```

ConnectionHandler类，具体如下：

```java
//用于原子更新ClientCnx
private static final AtomicReferenceFieldUpdater<ConnectionHandler, ClientCnx> CLIENT_CNX_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ConnectionHandler.class, ClientCnx.class, "clientCnx");
//封装底层的客户端控制上下文，是实际的处理网络连接的
@SuppressWarnings("unused")
private volatile ClientCnx clientCnx = null;
//客户端句柄状态
protected final HandlerState state;
//用于自动重连的时间组件
protected final Backoff backoff;
//connection 用于回调
interface Connection {
    void connectionFailed(PulsarClientException exception);
    void connectionOpened(ClientCnx cnx);
}

protected Connection connection;
```

开始连接 broker ，grabCnx方法实际上调用的是ConnectionHandle的grabCnx方法，如下：

```java
protected void grabCnx() {
    //如果ClientCnx不为空，则直接返回，因为已经设置过了，这时候就忽略重连请求
    if (CLIENT_CNX_UPDATER.get(this) != null) {
        log.warn("[{}] [{}] Client cnx already set, ignoring reconnection request", state.topic, state.getHandlerName());
        return;
    }
    //判定下是否能重连，只有Uninitialized、Connecting、Ready才能重连
    if (!isValidStateForReconnection()) {
        // Ignore connection closed when we are shutting down
        log.info("[{}] [{}] Ignoring reconnection request (state: {})", state.topic, state.getHandlerName(), state.getState());
        return;
    }

    //这时候，客户端开始获取连接 ， 如果连接发送异常，则延后重连
    try {
        state.client.getConnection(state.topic)
                .thenAccept(cnx -> connection.connectionOpened(cnx)) //
                .exceptionally(this::handleConnectionError);
    } catch (Throwable t) {
        log.warn("[{}] [{}] Exception thrown while getting connection: ", state.topic, state.getHandlerName(), t);
        reconnectLater(t);
    }
}
```

连接主方法，调用的是PulsarClientImpl类的 getConnection 方法：

```java
protected CompletableFuture<ClientCnx> getConnection(final String topic) {
        //解析Topic
        TopicName topicName = TopicName.get(topic);
        //从查找服务里读取存活的broker 地址，异步返回 ClientCnx ，根据成功与失败回调 Connection 接口中2个方法。
        return lookup.getBroker(topicName)
                .thenCompose(pair -> cnxPool.getConnection(pair.getLeft(), pair.getRight()));
}
```

通过查找服务中 getBroker 方法获取存活的 broker 地址，有两种实现，一个是 Http 协议实现，一个是 Native（TCP）实现，这里只取一个实现分析，TCP实现的：BinaryProtoLookupService 类，如下：

```java

public CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> getBroker(TopicName topicName) {
    //域名解析到对应的IP地址信息
    return findBroker(serviceNameResolver.resolveHost(), false, topicName);
}

private CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> findBroker(InetSocketAddress socketAddress,
        boolean authoritative, TopicName topicName) {
    CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> addressFuture = new CompletableFuture<>();
    //连接池连接目标IP地址
    client.getCnxPool().getConnection(socketAddress).thenAccept(clientCnx -> {
        long requestId = client.newRequestId();
        //连接成功，发送 Lookup 命令
        ByteBuf request = Commands.newLookup(topicName.toString(), authoritative, requestId);
        clientCnx.newLookup(request, requestId).thenAccept(lookupDataResult -> {
            //成功返回
            URI uri = null;
            try {
                // (1) 通过查找应答数据，连接Url
                if (useTls) {
                    uri = new URI(lookupDataResult.brokerUrlTls);
                } else {
                    String serviceUrl = lookupDataResult.brokerUrl;
                    uri = new URI(serviceUrl);
                }

                InetSocketAddress responseBrokerAddress = InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());

                // (2) 如果查找应答命令需要重定向，则重定向新地址，继续查找 broker 地址
                if (lookupDataResult.redirect) {
                    findBroker(responseBrokerAddress, lookupDataResult.authoritative, topicName)
                            .thenAccept(addressPair -> {
                                addressFuture.complete(addressPair);
                            }).exceptionally((lookupException) -> {
                                // lookup failed
                                log.warn("[{}] lookup failed : {}", topicName.toString(),
                                        lookupException.getMessage(), lookupException);
                                addressFuture.completeExceptionally(lookupException);
                                return null;
                            });
                } else {
                    // (3) 查看是否通过 proxy 连接
                    if (lookupDataResult.proxyThroughServiceUrl) {
                        // 通过 proxy 连接
                        addressFuture.complete(Pair.of(responseBrokerAddress, socketAddress));
                    } else {
                        // 正常情况直连 broker
                        addressFuture.complete(Pair.of(responseBrokerAddress, responseBrokerAddress));
                    }
                }

            } catch (Exception parseUrlException) {
                // url解析失败异常
                log.warn("[{}] invalid url {} : {}", topicName.toString(), uri, parseUrlException.getMessage(),
                        parseUrlException);
                addressFuture.completeExceptionally(parseUrlException);
            }
        }).exceptionally((sendException) -> {
            // 查找命令请求失败
            log.warn("[{}] failed to send lookup request : {}", topicName.toString(), sendException.getMessage());
            if (log.isDebugEnabled()) {
                log.warn("[{}] Lookup response exception: {}", topicName.toString(), sendException);
            }

            addressFuture.completeExceptionally(sendException);
            return null;
        });
    }).exceptionally(connectionException -> {
        //连接失败
        addressFuture.completeExceptionally(connectionException);
        return null;
    });
    return addressFuture;
}

```

上面获取到连接地址后，调用ConnectionPool中的 getConnection 方法获取连接，如下：

```java
//连接池容器的定义
protected final ConcurrentHashMap<InetSocketAddress, ConcurrentMap<Integer, CompletableFuture<ClientCnx>>> pool;


 /*从连接池获取连接,这连接可能是从连接池里获取，或者是直接创建
 关于两个地址的原因，当没有代理层时，2个地址是相同的，其实第一个地址作为broker 的标记，第二个地址去连接，当有代理层时，两个地址不一样的，实际上，连接池使用logicalAddress作为决定是否重用特定连接。*/
 public CompletableFuture<ClientCnx> getConnection(InetSocketAddress logicalAddress,
        InetSocketAddress physicalAddress) {
    //如果每个主机最大连接数为0，则禁用连接池，直接创建连接
    if (maxConnectionsPerHosts == 0) {
        // Disable pooling
        return createConnection(logicalAddress, physicalAddress, -1);
    }
    //如果不为0，则产生一个随机数，maxConnectionsPerHosts取余，生成一个randomKey
    final int randomKey = signSafeMod(random.nextInt(), maxConnectionsPerHosts);
    //连接池里如果logicalAddress作为key，没有对应的value，则创建新ConcurrentMap<Integer, CompletableFuture<ClientCnx>对象，如果randomKey作为key，没有对应的value，则使用createConnection(logicalAddress, physicalAddress, randomKey)方法创建一个CompletableFuture<ClientCnx>
    return pool.computeIfAbsent(logicalAddress, a -> new ConcurrentHashMap<>()) //
            .computeIfAbsent(randomKey, k -> createConnection(logicalAddress, physicalAddress, randomKey));
}
```

好了，到达这里，才真正触及到连接 broker 的核心方法以及子方法（也是在ConnectionPool类里），如下：

```java
private CompletableFuture<ClientCnx> createConnection(InetSocketAddress logicalAddress, InetSocketAddress physicalAddress, int connectionKey) {

    if (log.isDebugEnabled()) {
        log.debug("Connection for {} not found in cache", logicalAddress);
    }

    final CompletableFuture<ClientCnx> cnxFuture = new CompletableFuture<ClientCnx>();

    // DNS解析主机名，返回IP数组，以此连接IP，只要一连接成功就返回，否则继续重试下一IP，如果所有IP重试完，还是没连接上，则抛异常
    createConnection(physicalAddress).thenAccept(channel -> {
        log.info("[{}] Connected to server", channel);

        channel.closeFuture().addListener(v -> {
            // 如果连接关闭，则清理垃圾（主要是从ConnectionPool里移除对应的对象）
            if (log.isDebugEnabled()) {
                log.debug("Removing closed connection from pool: {}", v);
            }
            cleanupConnection(logicalAddress, connectionKey, cnxFuture);
        });

        // 这里已经连接上broker，但是需要等待直到连接握手完成
        final ClientCnx cnx = (ClientCnx) channel.pipeline().get("handler");
        if (!channel.isActive() || cnx == null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Connection was already closed by the time we got notified", channel);
            }
            cnxFuture.completeExceptionally(new ChannelException("Connection already closed"));
            return;
        }

        if (!logicalAddress.equals(physicalAddress)) {
            //当正在通过代理连接时，需要在ClientCnx对象中设置目标broker，这为了在发送CommandConnect命令时指定目标broker。在阶段发生在CliectCnx.connectActive()里，在完成本方法调用后，将会立即调用CliectCnx.connectActive()
            cnx.setTargetBroker(logicalAddress);
        }

        //保存远程主机名，主要在处理握手包时，TLS校验主机名是否正确
        cnx.setRemoteHostName(physicalAddress.getHostName());
        //这里就是等待与broker握手完成
        cnx.connectionFuture().thenRun(() -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Connection handshake completed", cnx.channel());
            }
            cnxFuture.complete(cnx);
        }).exceptionally(exception -> {
            log.warn("[{}] Connection handshake failed: {}", cnx.channel(), exception.getMessage());
            cnxFuture.completeExceptionally(exception);
            cleanupConnection(logicalAddress, connectionKey, cnxFuture);
            cnx.ctx().close();
            return null;
        });
    }).exceptionally(exception -> {
        eventLoopGroup.execute(() -> {
            log.warn("Failed to open connection to {} : {}", physicalAddress, exception.getMessage());
            cleanupConnection(logicalAddress, connectionKey, cnxFuture);
            cnxFuture.completeExceptionally(new PulsarClientException(exception));
        });
        return null;
    });

    return cnxFuture;
}

private CompletableFuture<Channel> createConnection(InetSocketAddress unresolvedAddress) {
    String hostname = unresolvedAddress.getHostString();
    int port = unresolvedAddress.getPort();

    // DNS解析出IP列表--> 以此尝试连接IP，只要有成功的就返回
    return resolveName(hostname)
            .thenCompose(inetAddresses -> connectToResolvedAddresses(inetAddresses.iterator(), port));
}

//DNS解析IP列表
CompletableFuture<List<InetAddress>> resolveName(String hostname) {
    CompletableFuture<List<InetAddress>> future = new CompletableFuture<>();
    dnsResolver.resolveAll(hostname).addListener((Future<List<InetAddress>> resolveFuture) -> {
        if (resolveFuture.isSuccess()) {
            future.complete(resolveFuture.get());
        } else {
            future.completeExceptionally(resolveFuture.cause());
        }
    });
    return future;
}

//连接解析出的IP
private CompletableFuture<Channel> connectToResolvedAddresses(Iterator<InetAddress> unresolvedAddresses, int port) {
    CompletableFuture<Channel> future = new CompletableFuture<>();

    connectToAddress(unresolvedAddresses.next(), port).thenAccept(channel -> {
        // 这里成功连接到服务器上
        future.complete(channel);
    }).exceptionally(exception -> {
        //连接异常，切换尝试切换下一IP
        //判定迭代器是否还有IP地址
        if (unresolvedAddresses.hasNext()) {
            // 如果有，则继续尝试连接新的IP地址，递归调用
            connectToResolvedAddresses(unresolvedAddresses, port).thenAccept(channel -> {
                future.complete(channel);
            }).exceptionally(ex -> {
                // 这里已经解除递归调用
                future.completeExceptionally(ex);
                return null;
            });
        } else {
            // 这里表示没有IP地址了，这返回连接抛出的异常
            future.completeExceptionally(exception);
        }
        return null;
    });

    return future;
}

//业务最底层调用，连接远程主机，当然，在建立连接过程中，将会触发通道激活方法
private CompletableFuture<Channel> connectToAddress(InetAddress ipAddress, int port{
    CompletableFuture<Channel> future = new CompletableFuture<>();
    bootstrap.connect(ipAddress, port).addListener((ChannelFuture channelFuture) -> {
        if (channelFuture.isSuccess()) {
            future.complete(channelFuture.channel());
        } else {
            future.completeExceptionally(channelFuture.cause());
        }
    });

    return future;
}


与 broker 连接中，通道将激活，客户端将触发握手命令，如下：

```java
 @Override
public void channelActive(ChannelHandlerContext ctx) throws Exception {
    //如果设置保活间隔时间，这里将启动保活任务。
    super.channelActive(ctx);
    //前面说了，客户端可能直连broker，也可能通过代理连接broker。
    //如果proxyToTargetBrokerAddress为空，则直连broker，否则就通过代理连接broker
    if (proxyToTargetBrokerAddress == null) {
        if (log.isDebugEnabled()) {
            log.debug("{} Connected to broker", ctx.channel());
        }
    } else {
        log.info("{} Connected through proxy to target broker at {}", ctx.channel(), proxyToTargetBrokerAddress);
    }
    // 发送连接命令，此命令两个主要功能
    //1。 拿到broker协议版本 2 。当通过代理连接broker时，提供目标 broker 地址
    ctx.writeAndFlush(newConnectCommand())
            .addListener(future -> {
                if (future.isSuccess()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Complete: {}", future.isSuccess());
                    }
                    state = State.SentConnectFrame;
                } else {
                    log.warn("Error during handshake", future.cause());
                    ctx.close();
                }
            });
}
```

处理已连接应答，意味着完成连接建立，如下

```java
protected void handleConnected(CommandConnected connected) {

    //启用TLS 主机名验证 ，如果验证失败，则关闭连接
    if (isTlsHostnameVerificationEnable && remoteHostName != null && !verifyTlsHostName(remoteHostName, ctx)) {
        log.warn("[{}] Failed to verify hostname of {}", ctx.channel(), remoteHostName);
        ctx.close();
        return;
    }

    checkArgument(state == State.SentConnectFrame);

    if (log.isDebugEnabled()) {
        log.debug("{} Connection is ready", ctx.channel());
    }
    // 设置远程服务器（broker）协议版本，并设置连接完成标志，意味着完成连接
    remoteEndpointProtocolVersion = connected.getProtocolVersion();
    connectionFuture.complete(null);
    state = State.Ready;
}
```

//如果连接成功，则执行 Connection 接口中的 connectionOpened 方法，如下：

```java
@Override
public void connectionOpened(final ClientCnx cnx) {
    // 在 CliectCnx 注册生产者之前设置 ClientCnx 引用，是为了创建生产者之前释放 cnx 对象，
    // 生成一个新的 cnx 对象
    connectionHandler.setClientCnx(cnx);
    // 向 ClientCnx 注册 生产者对象
    cnx.registerProducer(producerId, this);

    log.info("[{}] [{}] Creating producer on cnx {}", topic, producerName, cnx.ctx().channel());

    long requestId = client.newRequestId();

    SchemaInfo schemaInfo = null;
    if (schema != null) {
        if (schema.getSchemaInfo() != null) {
            if (schema.getSchemaInfo().getType() == SchemaType.JSON) {
                // 为了向后兼容目的，JSONSchema最初基于JSON模式标准为pojo生成了一个模式，但现在已经对每个模式进行了标准化（处理）以生成基于Avro的模式
                if (Commands.peerSupportJsonSchemaAvroFormat(cnx.getRemoteEndpointProtocolVersion())) {
                    schemaInfo = schema.getSchemaInfo();
                } else if (schema instanceof JSONSchema){
                    JSONSchema jsonSchema = (JSONSchema) schema;
                    schemaInfo = jsonSchema.getBackwardsCompatibleJsonSchemaInfo();
                } else {
                    schemaInfo = schema.getSchemaInfo();
                }
            } else if (schema.getSchemaInfo().getType() == SchemaType.BYTES) {
                // Schema.BYTES 时不需要设置schemaInfo
                schemaInfo = null;
            } else {
                schemaInfo = schema.getSchemaInfo();
            }
        }
    }

    //向 broker 注册生产者
    cnx.sendRequestWithId(
            Commands.newProducer(topic, producerId, requestId, producerName, conf.isEncryptionEnabled(), metadata,
                    schemaInfo),
            requestId).thenAccept(response -> {
                // 这里表示注册成功
                String producerName = response.getProducerName();
                long lastSequenceId = response.getLastSequenceId();
                schemaVersion = Optional.ofNullable(response.getSchemaVersion());

                // 重新连接到 broker ，并且 清空发送的消息。重新发送正处理的消息，设置 cnx 对象，有新消息将立即发送。
                synchronized (ProducerImpl.this) {
                    if (getState() == State.Closing || getState() == State.Closed) {
                        // 当正在重连的时候，生产者将被关闭，关闭连接确保 broker 释放生产者相关资源
                        cnx.removeProducer(producerId);
                        cnx.channel().close();
                        return;
                    }
                    // 重置定时重连器
                    resetBackoff();

                    log.info("[{}] [{}] Created producer on cnx {}", topic, producerName, cnx.ctx().channel());
                    connectionId = cnx.ctx().channel().toString();
                    connectedSince = DateFormatter.now();

                    if (this.producerName == null) {
                        this.producerName = producerName;
                    }

                    if (this.msgIdGenerator == 0 && conf.getInitialSequenceId() == null) {
                        //仅更新序列ID生成器（如果尚未修改）。 这意味着我们只想在第一次建立连接时更新id生成器，并忽略 broker 在后续生成器重新连接中发送的序列ID
                        this.lastSequenceIdPublished = lastSequenceId;
                        this.msgIdGenerator = lastSequenceId + 1;
                    }

                    if (!producerCreatedFuture.isDone() && isBatchMessagingEnabled()) {
                        //如果启用批量消息发送，则创建定时器，用于触发批量发消息任务
                        client.timer().newTimeout(batchMessageAndSendTask, conf.getBatchingMaxPublishDelayMicros(),
                                TimeUnit.MICROSECONDS);
                    }
                    // 用新的 cnx 发送正处理的消息
                    resendMessages(cnx);
                }
            }).exceptionally((e) -> {
                // 注册生产者异常
                Throwable cause = e.getCause();
                cnx.removeProducer(producerId);
                if (getState() == State.Closing || getState() == State.Closed) {
                    // 当正在重连的时候，生产者将被关闭，关闭连接确保 broker 释放生产者相关资源
                    cnx.channel().close();
                    return null;
                }
                log.error("[{}] [{}] Failed to create producer: {}", topic, producerName, cause.getMessage());

                // 生产者 Topic 配额超出异常，可能连接已到上限
                if (cause instanceof PulsarClientException.ProducerBlockedQuotaExceededException) {
                    synchronized (this) {
                        log.warn("[{}] [{}] Topic backlog quota exceeded. Throwing Exception on producer.", topic,
                                producerName);

                        if (log.isDebugEnabled()) {
                            log.debug("[{}] [{}] Pending messages: {}", topic, producerName,
                                    pendingMessages.size());
                        }
                        // 把正处理的消息全部异常响应
                        PulsarClientException bqe = new PulsarClientException.ProducerBlockedQuotaExceededException(
                                "Could not send pending messages as backlog exceeded");
                        failPendingMessages(cnx(), bqe);
                    }
                // 生产者 Topic 积压配额错误
                } else if (cause instanceof PulsarClientException.ProducerBlockedQuotaExceededError) {
                    log.warn("[{}] [{}] Producer is blocked on creation because backlog exceeded on topic.",
                            producerName, topic);
                }
                // Topic 已关闭异常
                if (cause instanceof PulsarClientException.TopicTerminatedException) {
                    // 生产者状态 设置 State.Terminated 
                    setState(State.Terminated);
                    // 把正处理的消息全部异常响应
                    failPendingMessages(cnx(), (PulsarClientException) cause);
                    // 生产者创建设置异常
                    producerCreatedFuture.completeExceptionally(cause);
                    // 清理资源
                    client.cleanupProducer(this);
                } else if (producerCreatedFuture.isDone() || //
                (cause instanceof PulsarClientException && connectionHandler.isRetriableError((PulsarClientException) cause)
                        && System.currentTimeMillis() < createProducerTimeout)) {
                    // 只要已创建成功过生产者一次，或者还在创建超时时间内，出现可重试异常，那么就可以稍后重连
                    reconnectLater(cause);
                } else {
                    // 设置生产者创建失败
                    setState(State.Failed);
                    producerCreatedFuture.completeExceptionally(cause);
                    client.cleanupProducer(this);
                }

                return null;
            });
}

// 注册失败时，可能要把正处理的消息全部设置成异常失败
private void failPendingMessages(ClientCnx cnx, PulsarClientException ex) {
    if (cnx == null) {
        final AtomicInteger releaseCount = new AtomicInteger();
        pendingMessages.forEach(op -> {
            releaseCount.addAndGet(op.numMessagesInBatch);
            try {
                op.callback.sendComplete(ex);
            } catch (Throwable t) {
                log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic, producerName,
                        op.sequenceId, t);
            }
            ReferenceCountUtil.safeRelease(op.cmd);
            op.recycle();
        });
        semaphore.release(releaseCount.get());
        pendingMessages.clear();
        pendingCallbacks.clear();
         // 如果批量消息发送启用，则也设置
        if (isBatchMessagingEnabled()) {
            failPendingBatchMessages(ex);
        }
    } else {
        // 如果还有连接，那么应该在事件循环线程上执行回调和循环（调用），以避免任何竞争条件，因为我们也在这个线程上写消息
        cnx.ctx().channel().eventLoop().execute(() -> {
            synchronized (ProducerImpl.this) {
                failPendingMessages(null, ex);
            }
        });
    }
}

// 批量消息里面也设置为异常
private void failPendingBatchMessages(PulsarClientException ex) {
    if (batchMessageContainer.isEmpty()) {
        return;
    }
    int numMessagesInBatch = batchMessageContainer.numMessagesInBatch;
    semaphore.release(numMessagesInBatch);
    try {
        batchMessageContainer.firstCallback.sendComplete(ex);
    } catch (Throwable t) {
        log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic, producerName,
                batchMessageContainer.sequenceId, t);
    }
    ReferenceCountUtil.safeRelease(batchMessageContainer.getBatchedSingleMessageMetadataAndPayload());
    batchMessageContainer.clear();
}

//否则，执行 HandelConnectionError 方法，但实际上同样是 Connection 接口的 connectionFailed 方法，如下：

private Void handleConnectionError(Throwable exception) {
    log.warn("[{}] [{}] Error connecting to broker: {}", state.topic, state.getHandlerName(), exception.getMessage());
    connection.connectionFailed(new PulsarClientException(exception));

    State state = this.state.getState();
    // 如果客户端状态为 State.Uninitialized 或 State.Connecting  或  State.Ready，
    // 则稍后重连
    if (state == State.Uninitialized || state == State.Connecting || state == State.Ready) {
        reconnectLater(exception);
    }

    return null;
}

@Override
public void connectionFailed(PulsarClientException exception) {
    // 如果超时，并且还没触发异常，则设置客户端状态为State.Failed,
    //并移除创建的producer对象
    if (System.currentTimeMillis() > createProducerTimeout
            && producerCreatedFuture.completeExceptionally(exception)) {
        log.info("[{}] Producer creation failed for producer {}", topic, producerId);
        setState(State.Failed);
        client.cleanupProducer(this);
    }
}

//向 broker 注册新生产者成功处理
protected void handleProducerSuccess(CommandProducerSuccess success) {
    checkArgument(state == State.Ready);

    if (log.isDebugEnabled()) {
        log.debug("{} Received producer success response from server: {} - producer-name: {}", ctx.channel(),
                success.getRequestId(), success.getProducerName());
    }
    long requestId = success.getRequestId();
    CompletableFuture<ProducerResponse> requestFuture = pendingRequests.remove(requestId);
    // 这里表示处理完成，返回 broker 应答
    if (requestFuture != null) {
        requestFuture.complete(new ProducerResponse(success.getProducerName(), success.getLastSequenceId(), success.getSchemaVersion().toByteArray()));
    } else {
        log.warn("{} Received unknown request id from server: {}", ctx.channel(), success.getRequestId());
    }
}
```

自此，生产者已与 broker 或 通过 proxy 与 broker 建立连接，下面我们将梳理发消息流程。

#### 3. Producer 发送消息流程

构建一个消息，先看看消息的构建接口

```java
public interface TypedMessageBuilder<T> extends Serializable {

//同步发送消息，并返回消息ID信息
MessageId send() throws PulsarClientException;

//异步发送
CompletableFuture<MessageId> sendAsync();

//设置消息路由Key
TypedMessageBuilder<T> key(String key);

//设置消息（体）
TypedMessageBuilder<T> value(T value);

//给消息增加一个属性
TypedMessageBuilder<T> property(String name, String value);

//把Map所有Key-Value放入消息属性中
TypedMessageBuilder<T> properties(Map<String, String> properties);

//设置事件时间
TypedMessageBuilder<T> eventTime(long timestamp);

//设置唯一序号（主要用于去重）
TypedMessageBuilder<T> sequenceId(long sequenceId);
}

//设置（覆盖）全局集群列表（geo-replication clusters）
TypedMessageBuilder<T> replicationClusters(List<String> clusters);

//禁用集群（geo-replication clusters）
TypedMessageBuilder<T> disableReplication();
```

接口唯一实现类：TypedMessageBuilderImpl，属性信息如下：

```java
//生产者基类，用于发送消息
private final ProducerBase<T> producer;
//消息元数据构建器
private final MessageMetadata.Builder msgMetadataBuilder = MessageMetadata.newBuilder();
//设置消息的Schema
private final Schema<T> schema;
//消息的缓冲区
private ByteBuffer content;
```

其简单的例子:

```java
producer.newMessage().key(myKey).value(myValue).send();
```

此时，send()方法如下:

```java
//1. TypedMessageBuilderImpl类，发送消息入口
@Override
public MessageId send() throws PulsarClientException {
    return producer.send(getMessage());
}

public Message<T> getMessage() {
    return (Message<T>) MessageImpl.create(msgMetadataBuilder, content, schema);
}
```

从这里可以看出，构造出消息，通过生产者基类发送方法，返回消息ID信息，这里，涉及到两个新接口，如下：

```java
public interface Message<T> {
    //获取消息属性集
    Map<String, String> getProperties();
    //是否存在名叫name属性
    boolean hasProperty(String name);
    //获取名叫name的属性
    String getProperty(String name);
    //获取消息原始负载字节数组
    byte[] getData();
    //获取消息反序列化对象，根据配置Schema
    T getValue();
    //获取这消息唯一消息ID
    MessageId getMessageId();
    //获取消息发送时间
    long getPublishTime();
    //获取与消息关联的世界时间，如果无事件关联，则返回0
    long getEventTime();
    //获取与消息关联的序列号
    long getSequenceId();
    //获取生产者名称
    String getProducerName();
    //检查消息是否有Key
    boolean hasKey();
    //消息的Key
    String getKey();
    //检查消息key是否已被base64解码
    boolean hasBase64EncodedKey();
    //获取Key的字节数组，如果Key已经被base64编码，它将解码后返回。
    //否则，如果Key是文本格式，这方法将返回UTF-8编码的字节数组。
    byte[] getKeyBytes();
    //返回消息发布的Topic
    String getTopicName();
    //返回加密和压缩信息，用它可以解密被加密的消息体
    Optional<EncryptionContext> getEncryptionCtx();
    //返回消息消费重试次数
    int getRedeliveryCount();
}


public interface MessageId extends Comparable<MessageId>, Serializable {
    //消息ID序列化成字节数组
    byte[] toByteArray();
    //字节数组反序列化消息ID
    public static MessageId fromByteArray(byte[] data) throws IOException {
        return DefaultImplementation.newMessageIdFromByteArray(data);
    }
    //反序列化消息ID并带有Topic信息，这里用于多Topic情况
    public static MessageId fromByteArrayWithTopic(byte[] data, String topicName) throws IOException {
        return DefaultImplementation.newMessageIdFromByteArrayWithTopic(data, topicName);
    }
}

```

简单的介绍两个新的接口后，接着继续解析发消息的流程：

```java
//2. ProducerBase类,调用的是异步发送方法，实际调用的是internalSendAsync方法
@Override
public MessageId send(Message<T> message) throws PulsarClientException {
    try {
        // 把消息放入缓冲区
        CompletableFuture<MessageId> sendFuture = internalSendAsync(message);

        if (!sendFuture.isDone()) {
            // 当请求还没完成时（主要是尝试触发批量消息发送）
            triggerFlush();
        }
        //等待消息发送完毕，返回
        return sendFuture.get();
    } catch (ExecutionException e) {
        Throwable t = e.getCause();
        if (t instanceof PulsarClientException) {
            throw (PulsarClientException) t;
        } else {
            throw new PulsarClientException(t);
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PulsarClientException(e);
    }
}

//3. 发送消息
@Override
CompletableFuture<MessageId> internalSendAsync(Message<T> message) {

    CompletableFuture<MessageId> future = new CompletableFuture<>();
    //执行拦截器调用
    MessageImpl<T> interceptorMessage = (MessageImpl<T>) beforeSend(message);
    //Retain the buffer used by interceptors callback to get message. Buffer will release after complete interceptors.
    interceptorMessage.getDataBuffer().retain();
    //设置用户属性
    if (interceptors != null) {
        interceptorMessage.getProperties();
    }
    sendAsync(interceptorMessage, new SendCallback() {
        SendCallback nextCallback = null;
        MessageImpl<?> nextMsg = null;
        long createdAt = System.nanoTime();

        @Override
        public CompletableFuture<MessageId> getFuture() {
            return future;
        }

        @Override
        public SendCallback getNextSendCallback() {
            return nextCallback;
        }

        @Override
        public MessageImpl<?> getNextMessage() {
            return nextMsg;
        }

        @Override
        public void sendComplete(Exception e) {
            try {
                if (e != null) {
                    stats.incrementSendFailed();
                    onSendAcknowledgement(interceptorMessage, null, e);
                    future.completeExceptionally(e);
                } else {
                    onSendAcknowledgement(interceptorMessage, interceptorMessage.getMessageId(), null);
                    future.complete(interceptorMessage.getMessageId());
                    stats.incrementNumAcksReceived(System.nanoTime() - createdAt);
                }
            } finally {
                interceptorMessage.getDataBuffer().release();
            }

            while (nextCallback != null) {
                SendCallback sendCallback = nextCallback;
                MessageImpl<?> msg = nextMsg;
                //Retain the buffer used by interceptors callback to get message. Buffer will release after complete interceptors.
                //通过拦截器回调获取消息，增加缓冲区引用，完成拦截器调用后，将释放
                try {
                    msg.getDataBuffer().retain();
                    if (e != null) {
                        stats.incrementSendFailed();
                        //触发消息发送成功，broker 应答成功时拦截器调用
                        onSendAcknowledgement((Message<T>) msg, null, e);
                        sendCallback.getFuture().completeExceptionally(e);
                    } else {
                        onSendAcknowledgement((Message<T>) msg, msg.getMessageId(), null);
                        sendCallback.getFuture().complete(msg.getMessageId());
                        stats.incrementNumAcksReceived(System.nanoTime() - createdAt);
                    }
                    nextMsg = nextCallback.getNextMessage();
                    nextCallback = nextCallback.getNextSendCallback();
                } finally {
                    msg.getDataBuffer().release();
                }
            }
        }

        @Override
        public void addCallback(MessageImpl<?> msg, SendCallback scb) {
            nextMsg = msg;
            nextCallback = scb;
        }
    });
    return future;
}

public void sendAsync(Message<T> message, SendCallback callback) {
    //检查是否MessageImpl实例
    checkArgument(message instanceof MessageImpl);
    //检查生产者状态是否正常
    if (!isValidProducerState(callback)) {
        return;
    }
    //如果启用队列满则阻塞标志，则请求信号量（阻塞）
    //否则，试着请求信号量，如果信号量没有获取成功，则立即返回，并抛异常
    if (!canEnqueueRequest(callback)) {
        return;
    }

    MessageImpl<T> msg = (MessageImpl<T>) message;
    MessageMetadata.Builder msgMetadataBuilder = msg.getMessageBuilder();
    ByteBuf payload = msg.getDataBuffer();

    // 如果启用压缩，则压缩，否则用相同的一块缓冲区
    int uncompressedSize = payload.readableBytes();
    ByteBuf compressedPayload = payload;
    // 如果批量消息没有启用，（这里表示没启用批量消息）
    if (!isBatchMessagingEnabled()) {
        //压缩
        compressedPayload = compressor.encode(payload);
        //释放原消息体
        payload.release();
    }
    int compressedSize = compressedPayload.readableBytes();

    // 校验消息大小 
    // 如果消息大小已经大于允许的最大消息大小，则抛异常
    if (compressedSize > PulsarDecoder.MaxMessageSize) {
        compressedPayload.release();
        String compressedStr = (!isBatchMessagingEnabled() && conf.getCompressionType() != CompressionType.NONE)
                ? "Compressed"
                : "";
        PulsarClientException.InvalidMessageException invalidMessageException =
                new PulsarClientException.InvalidMessageException(
                        format("%s Message payload size %d cannot exceed %d bytes", compressedStr, compressedSize,
                                PulsarDecoder.MaxMessageSize));
        // 设置回调异常，表示消息已经超大
        callback.sendComplete(invalidMessageException);
        return;
    }

    //不能重用相同的消息（TODO）
    if (!msg.isReplicated() && msgMetadataBuilder.hasProducerName()) {
        PulsarClientException.InvalidMessageException invalidMessageException =
                new PulsarClientException.InvalidMessageException("Cannot re-use the same message");
        callback.sendComplete(invalidMessageException);
        compressedPayload.release();
        return;
    }

    //设置Schema版本
    if (schemaVersion.isPresent()) {
        msgMetadataBuilder.setSchemaVersion(ByteString.copyFrom(schemaVersion.get()));
    }

    try {
        //同步代码块（用对象监视器），确保消息有序发送，这里与后面的消息应答处理相呼应
        synchronized (this) {
            long sequenceId;
            //是否设置了序列ID
            if (!msgMetadataBuilder.hasSequenceId()) {
                sequenceId = msgIdGeneratorUpdater.getAndIncrement(this);
                msgMetadataBuilder.setSequenceId(sequenceId);
            } else {
                sequenceId = msgMetadataBuilder.getSequenceId();
            }
            //如果没有设置发布时间
            if (!msgMetadataBuilder.hasPublishTime()) {
                msgMetadataBuilder.setPublishTime(System.currentTimeMillis());

                checkArgument(!msgMetadataBuilder.hasProducerName());

                msgMetadataBuilder.setProducerName(producerName);
                //设置压缩类型
                if (conf.getCompressionType() != CompressionType.NONE) {
                    msgMetadataBuilder.setCompression(
                            CompressionCodecProvider.convertToWireProtocol(conf.getCompressionType()));
                }
                //保存未压缩前的消息大小
                msgMetadataBuilder.setUncompressedSize(uncompressedSize);
            }
            //如果批量消息发送被启用
            if (isBatchMessagingEnabled()) {
                //检查批量消息容器是否还有空间
                if (batchMessageContainer.hasSpaceInBatch(msg)) {
                    //如果有，则把当前消息放入容器（重点）
                    batchMessageContainer.add(msg, callback);
                    lastSendFuture = callback.getFuture();
                    payload.release();
                    //如果已经达到批量最大消息数或者达到批量最大消息消息，则触发批量发送消息动作（重点）
                    if (batchMessageContainer.numMessagesInBatch == maxNumMessagesInBatch
                            || batchMessageContainer.currentBatchSizeBytes >= BatchMessageContainer.MAX_MESSAGE_BATCH_SIZE_BYTES) {
                        batchMessageAndSend();
                    }
                } else {//如果没有，则立即执行批量消息发送（重点）
                    doBatchSendAndAdd(msg, callback, payload);
                }
            } else {//如果没有启用批量消息（也就是单个消费发送逻辑）
                //加密消息
                ByteBuf encryptedPayload = encryptMessage(msgMetadataBuilder, compressedPayload);

                MessageMetadata msgMetadata = msgMetadataBuilder.build();
                //发送消息命令序列化
                ByteBufPair cmd = sendMessage(producerId, sequenceId, 1, msgMetadata, encryptedPayload);
                //回收对象，放回对象池
                msgMetadataBuilder.recycle();
                msgMetadata.recycle();
                //保存发送消息状态信息
                final OpSendMsg op = OpSendMsg.create(msg, cmd, sequenceId, callback);
                op.setNumMessagesInBatch(1);
                op.setBatchSizeByte(encryptedPayload.readableBytes());
                //放入正在发送消息队列，用于校验应答是否超时
                pendingMessages.put(op);
                //记录最后一次消息的futrue
                lastSendFuture = callback.getFuture();

                // 在验证之前检查连接是否仍然正常（连接），避免读取空值
                ClientCnx cnx = cnx();
                if (isConnected()) {
                    //如果还有正常的连接，消息将被立即发送，否则尝试建立新的连接
                    cmd.retain();
                    //这里才是把消息放入通道，发送出去
                    cnx.ctx().channel().eventLoop().execute(WriteInEventLoopCallback.create(this, cnx, op));
                    //更新状态
                    stats.updateNumMsgsSent(op.numMessagesInBatch, op.batchSizeByte);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Connection is not ready -- sequenceId {}", topic, producerName,
                                sequenceId);
                    }
                }
            }
        }
    } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        semaphore.release();
        callback.sendComplete(new PulsarClientException(ie));
    } catch (PulsarClientException e) {
        semaphore.release();
        callback.sendComplete(e);
    } catch (Throwable t) {
        semaphore.release();
        callback.sendComplete(new PulsarClientException(t));
    }
}

//  用于批量消息时
void add(MessageImpl<?> msg, SendCallback callback) {

    if (log.isDebugEnabled()) {
        log.debug("[{}] [{}] add message to batch, num messages in batch so far {}", topicName, producerName,
                numMessagesInBatch);
    }

    //容器内，第一条消息进来执行初始化操作
    if (++numMessagesInBatch == 1) {
        //批量消息中，不同消息的一些属性是公共的，所以这里我们以第一条消息的一些数据作为初始化数据，并初始化序列ID
        sequenceId = Commands.initBatchMessageMetadata(messageMetadata, msg.getMessageBuilder());
        this.firstCallback = callback;
        //分配批量消息缓冲区
        batchedMessageMetadataAndPayload = PooledByteBufAllocator.DEFAULT
                .buffer(Math.min(maxBatchSize, MAX_MESSAGE_BATCH_SIZE_BYTES), PulsarDecoder.MaxMessageSize);
    }

    //这里构建一个回调链，这回调链可参考 SendCallback 接口
    if (previousCallback != null) {
        previousCallback.addCallback(msg, callback);
    }
    previousCallback = callback;
    //记录当前消息的大小，然后累计历史消息大小
    currentBatchSizeBytes += msg.getDataBuffer().readableBytes();
    PulsarApi.MessageMetadata.Builder msgBuilder = msg.getMessageBuilder();
    //把当前消息序列化批量消息中
    batchedMessageMetadataAndPayload = Commands.serializeSingleMessageInBatchWithPayload(msgBuilder,
            msg.getDataBuffer(), batchedMessageMetadataAndPayload);
    //把当前消息放入消息队列
    messages.add(msg);
    //回收对象
    msgBuilder.recycle();
}

// 在入队列之前，必须申请到信号量（这里主要防止内存溢出，当然也一定程度也保护 broker）
private void batchMessageAndSend() {
    if (log.isDebugEnabled()) {
        log.debug("[{}] [{}] Batching the messages from the batch container with {} messages", topic, producerName,
                batchMessageContainer.numMessagesInBatch);
    }
    OpSendMsg op = null;
    int numMessagesInBatch = 0;
    try {
        //如果批量消息中消息队列不为空
        if (!batchMessageContainer.isEmpty()) {
            //批量消息总数
            numMessagesInBatch = batchMessageContainer.numMessagesInBatch;
            ByteBuf compressedPayload = batchMessageContainer.getCompressedBatchMetadataAndPayload();
            long sequenceId = batchMessageContainer.sequenceId;
            //尝试加密消息
            ByteBuf encryptedPayload = encryptMessage(batchMessageContainer.messageMetadata, compressedPayload);
            ByteBufPair cmd = sendMessage(producerId, sequenceId, batchMessageContainer.numMessagesInBatch,
                    batchMessageContainer.setBatchAndBuild(), encryptedPayload);
            //构造发送消息命令
            op = OpSendMsg.create(batchMessageContainer.messages, cmd, sequenceId,
                    batchMessageContainer.firstCallback);

            op.setNumMessagesInBatch(batchMessageContainer.numMessagesInBatch);
            op.setBatchSizeByte(batchMessageContainer.currentBatchSizeBytes);
            //清理批量消息容器，下一次使用
            batchMessageContainer.clear();
            //放入待处理消息队列，用于检查应答是否超时
            pendingMessages.put(op);
            //检查与 broker 连接是否正常
            if (isConnected()) {
                //如果有正常连接，这消息立即发送，否则，尝试创建新连接（再发送）
                cmd.retain();
                cnx().ctx().channel().eventLoop().execute(WriteInEventLoopCallback.create(this, cnx(), op));
                stats.updateNumMsgsSent(numMessagesInBatch, op.batchSizeByte);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Connection is not ready -- sequenceId {}", topic, producerName,
                            sequenceId);
                }
            }
        }
    } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        //释放信号量，以便重发
        semaphore.release(numMessagesInBatch);
        if (op != null) {
            // 中断异常，设置回调为中断异常
            op.callback.sendComplete(new PulsarClientException(ie));
        }
    } catch (PulsarClientException e) {
        Thread.currentThread().interrupt();
        semaphore.release(numMessagesInBatch);
        if (op != null) {
            // 客户端本身异常 ，设置回调为客户端本身异常
            op.callback.sendComplete(e);
        }
    } catch (Throwable t) {
        semaphore.release(numMessagesInBatch);
        log.warn("[{}] [{}] error while closing out batch -- {}", topic, producerName, t);
        if (op != null) {
            //任何异常，包装为客户端异常，并设置回调客户端本身异常
            op.callback.sendComplete(new PulsarClientException(t));
        }
    }
}

//触发批量发送，并把当前消息放入新的批量消息容器
private void doBatchSendAndAdd(MessageImpl<T> msg, SendCallback callback, ByteBuf payload) {
    if (log.isDebugEnabled()) {
        log.debug("[{}] [{}] Closing out batch to accomodate large message with size {}", topic, producerName,
                msg.getDataBuffer().readableBytes());
    }
    batchMessageAndSend();
    batchMessageContainer.add(msg, callback);
    lastSendFuture = callback.getFuture();
    payload.release();
}

//  WriteInEventLoopCallback 类中核心代码，仅仅是通过 channel 发送消息。
@Override
public void run() {
    if (log.isDebugEnabled()) {
        log.debug("[{}] [{}] Sending message cnx {}, sequenceId {}", producer.topic, producer.producerName, cnx,
                sequenceId);
    }

    try {
        cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
    } finally {
        recycle();
    }
}

//到了这里，发送成功或失败的处理逻辑哪去了？在 ClientCnx类中，有两个方法

//处理 broker 返回应答出错
@Override
protected void handleSendError(CommandSendError sendError) {
    log.warn("{} Received send error from server: {} : {}", ctx.channel(), sendError.getError(),
            sendError.getMessage());

    long producerId = sendError.getProducerId();
    long sequenceId = sendError.getSequenceId();

    switch (sendError.getError()) {
    // 校验和错误，客户端将尝试跟原来的校验和对比，如果匹配则重发，否则忽略。
    case ChecksumError:
        producers.get(producerId).recoverChecksumError(this, sequenceId);
        break;

    // Topic 已被销毁
    case TopicTerminatedError:
        producers.get(producerId).terminated(this);
        break;

    default:
        // 默认情况下，作为短暂的（未知的）错误，关闭连接，（让客户端自动）重连
        ctx.close();
    }
}

//处理 broker 返回应答成功
@Override
protected void handleSendReceipt(CommandSendReceipt sendReceipt) {
    checkArgument(state == State.Ready);

    long producerId = sendReceipt.getProducerId();
    long sequenceId = sendReceipt.getSequenceId();
    long ledgerId = -1;
    long entryId = -1;
    if (sendReceipt.hasMessageId()) {
        ledgerId = sendReceipt.getMessageId().getLedgerId();
        entryId = sendReceipt.getMessageId().getEntryId();
    }

    if (ledgerId == -1 && entryId == -1) {
        log.warn("[{}] Message has been dropped for non-persistent topic producer-id {}-{}", ctx.channel(),
                producerId, sequenceId);
    }

    if (log.isDebugEnabled()) {
        log.debug("{} Got receipt for producer: {} -- msg: {} -- id: {}:{}", ctx.channel(), producerId, sequenceId,
                ledgerId, entryId);
    }
    // 找到生产者，处理 broker 应答（也就是回执）
    producers.get(producerId).ackReceived(this, sequenceId, ledgerId, entryId);
}

// 客户端实际处理 broker 的应答
void ackReceived(ClientCnx cnx, long sequenceId, long ledgerId, long entryId) {
    OpSendMsg op = null;
    boolean callback = false;
    //同步代码块（对象监视器）
    synchronized (this) {
        // 从正处理消息队列查看一消息发送指令
        op = pendingMessages.peek();
        //如果为空，则意味着已超时处理，返回
        if (op == null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Got ack for timed out msg {}", topic, producerName, sequenceId);
            }
            return;
        }

        long expectedSequenceId = op.sequenceId;
        if (sequenceId > expectedSequenceId) {
            log.warn("[{}] [{}] Got ack for msg. expecting: {} - got: {} - queue-size: {}", topic, producerName,
                    expectedSequenceId, sequenceId, pendingMessages.size());
            // 这种情况（不知什么情况会出现），应该强制关闭连接，消息应该在新连接里重新传输
            cnx.channel().close();
        } else if (sequenceId < expectedSequenceId) {
            // 不管这种应答，因为这消息已经被超时处理
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Got ack for timed out msg {} last-seq: {}", topic, producerName, sequenceId,
                        expectedSequenceId);
            }
        } else {
            // 消息已被正常处理
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Received ack for msg {} ", topic, producerName, sequenceId);
            }
            //移除正在处理消息队列一消息发送指令
            pendingMessages.remove();
            semaphore.release(op.numMessagesInBatch);
            callback = true;
            pendingCallbacks.add(op);
        }
    }
    if (callback) {
        op = pendingCallbacks.poll();
        if (op != null) {
            //保存最新发布序列ID
            lastSequenceIdPublished = op.sequenceId + op.numMessagesInBatch - 1;
            //生成消息ID
            op.setMessageId(ledgerId, entryId, partitionIndex);
            try {
                // 消息成功投递，执行回调
                op.callback.sendComplete(null);
            } catch (Throwable t) {
                log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic, producerName,
                        sequenceId, t);
            }
            ReferenceCountUtil.safeRelease(op.cmd);
            op.recycle();
        }
    }
}

//发送超时在哪里处理？
// 有 两处 ，如下：
// 1. ClientCnx 构造的时，会启动检查超时任务,这里专门检查带 requestId 的命令
// 2. ProducerImpl 有个 run 方法，它是通过检查正处理消息队列来进行超时检查的。

// 超时任务
this.timeoutTask = this.eventLoopGroup.scheduleAtFixedRate(() -> checkRequestTimeout(), operationTimeoutMs,
                operationTimeoutMs, TimeUnit.MILLISECONDS);

// ClientCnx 内 检查超时请求，用于注册生产者、查找请求等
private void checkRequestTimeout() {
    // 循环
    while (!requestTimeoutQueue.isEmpty()) {
        // 从队列检索头部
        RequestTime request = requestTimeoutQueue.peek();
        if (request == null || (System.currentTimeMillis() - request.creationTimeMs) < operationTimeoutMs) {
            // 如果没有请求超时则退出循环。
            break;
        }
        request = requestTimeoutQueue.poll();
        CompletableFuture<ProducerResponse> requestFuture = pendingRequests.remove(request.requestId);
        // 尝试设置请求为超时异常
        if (requestFuture != null && !requestFuture.isDone()
                && requestFuture.completeExceptionally(new TimeoutException(
                        request.requestId + " lookup request timedout after ms " + operationTimeoutMs))) {
            log.warn("{} request {} timed out after {} ms", ctx.channel(), request.requestId, operationTimeoutMs);
        } else {
            //请求已正常处理
        }
    }
}

// ProducerImpl 内，检查超时请求，主要用于消息发送超时检查
@Override
public void run(Timeout timeout) throws Exception {
    if (timeout.isCancelled()) {
        return;
    }

    long timeToWaitMs;

    synchronized (this) {
        // 如果当前处于正在关闭或已关闭状态则忽略本次超时检查
        if (getState() == State.Closing || getState() == State.Closed) {
            return;
        }
        //取出第一个消息（如果第一个消息都超时了，意味着后面的消息肯定全部超时了）
        OpSendMsg firstMsg = pendingMessages.peek();
        if (firstMsg == null) {
            // 如果没有正在处理的消息，则重置超时时间为配置值
            timeToWaitMs = conf.getSendTimeoutMs();
        } else {
            //如果至少有一个消息，则计算超时时间点差值与当前时间
            long diff = (firstMsg.createdAt + conf.getSendTimeoutMs()) - System.currentTimeMillis();
            if (diff <= 0) {
                // 如果差值小于或等于0，则意味着消息已（发送或应答）超时，每个消息都设置回调为超时异常，并情况正处理消息队列
                log.info("[{}] [{}] Message send timed out. Failing {} messages", topic, producerName,
                        pendingMessages.size());

                PulsarClientException te = new PulsarClientException.TimeoutException(
                        "Could not send message to broker within given timeout");
                failPendingMessages(cnx(), te);
                stats.incrementSendFailed(pendingMessages.size());
                // 一旦正处理消息队列被清空，则重置超时时间为配置值
                timeToWaitMs = conf.getSendTimeoutMs();
            } else {
                //如果差值大于0，则设置此时间为超时时间（意味着下次这时间间隔将再次检查，而不是配置值）
                timeToWaitMs = diff;
            }
        }

        sendTimeout = client.timer().newTimeout(this, timeToWaitMs, TimeUnit.MILLISECONDS);
    }
}

```

以上，梳理了 Producer 发送消息的全过程，并还顺带分析了请求超时的处理，接下来，将分析 Producer 关闭流程。

#### 4. Producer 关闭

```java

@Override
public CompletableFuture<Void> closeAsync() {
    //获取和更新 Producer 状态,如果状态是已关闭，直接返回已关闭，否则直接返回正在关闭
    final State currentState = getAndUpdateState(state -> {
        if (state == State.Closed) {
            return state;
        }
        return State.Closing;
    });

    //如果当前状态是已关闭或正在关闭，则直接返回关闭成功
    if (currentState == State.Closed || currentState == State.Closing) {
        return CompletableFuture.completedFuture(null);
    }

    //如果定时器不为空，则取消
    Timeout timeout = sendTimeout;
    if (timeout != null) {
        timeout.cancel();
        sendTimeout = null;
    }

    //批量消息发送定时器不为空，则取消
    Timeout batchTimeout = batchMessageAndSendTimeout;
    if (batchTimeout != null) {
        batchTimeout.cancel();
        batchMessageAndSendTimeout = null;
    }

    // 取消数据加密key加载任务
    if (keyGeneratorTask != null && !keyGeneratorTask.isCancelled()) {
        keyGeneratorTask.cancel(false);
    }

    stats.cancelStatsTimeout();
    //Producer 已关闭，设置关闭状态，并且把正处理消息全部设置为 producer 已关闭异常
    ClientCnx cnx = cnx();
    if (cnx == null || currentState != State.Ready) {
        log.info("[{}] [{}] Closed Producer (not connected)", topic, producerName);
        synchronized (this) {
            setState(State.Closed);
            client.cleanupProducer(this);
            PulsarClientException ex = new PulsarClientException.AlreadyClosedException(
                    "Producer was already closed");
            pendingMessages.forEach(msg -> {
                msg.callback.sendComplete(ex);
                msg.cmd.release();
                msg.recycle();
            });
            pendingMessages.clear();
        }

        return CompletableFuture.completedFuture(null);
    }
 
    // 生成 producer 关闭命令，向 broker 注销自己
    long requestId = client.newRequestId();
    ByteBuf cmd = Commands.newCloseProducer(producerId, requestId);

    CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    cnx.sendRequestWithId(cmd, requestId).handle((v, exception) -> {
        cnx.removeProducer(producerId);
        if (exception == null || !cnx.ctx().channel().isActive()) {
            // 要么 已经成功从 broker 接收到关闭 producer 的应答命令，要么 此时与broker连接已经被破坏，无论什么情况， producer 将关闭（设置状态为 关闭，并且把正处理的消息都是否，其队列将被清理，其他的资源也被释放）
            synchronized (ProducerImpl.this) {
                log.info("[{}] [{}] Closed Producer", topic, producerName);
                setState(State.Closed);
                pendingMessages.forEach(msg -> {
                    msg.cmd.release();
                    msg.recycle();
                });
                pendingMessages.clear();
            }

            closeFuture.complete(null);
            client.cleanupProducer(this);
        } else {
            closeFuture.completeExceptionally(exception);
        }

        return null;
    });

    return closeFuture;
}

// 同步关闭，只是调用异步关闭接口
@Override
public void close() throws PulsarClientException {
    try {
        closeAsync().get();
    } catch (ExecutionException e) {
        Throwable t = e.getCause();
        if (t instanceof PulsarClientException) {
            throw (PulsarClientException) t;
        } else {
            throw new PulsarClientException(t);
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PulsarClientException(e);
    }
}


```

到这里，已经分析了无分区 Producer 实现，接下来，将分析多分区 Producer 实现。

### 2. 分区 Producer 实现，如图

![PartitionedProducerImpl](./images/20190411200014.png)

分区 Producer 的实现基本上跟无分区 Producer 实现类似，只是少继承了一部分类，而且它就是使用了无分区 Producer 作为实际与 broker 通信的主体，所以实现较为简单，主要在于处理多 无分区 Producer 协作和Topic 分区变更逻辑。

#### 1. 分区 Producer 的属性

```java
//每个分区一个无分区 Producer
private List<ProducerImpl<T>> producers;
//消息路由策略
private MessageRouter routerPolicy;
//Producer 状态记录器
private final ProducerStatsRecorderImpl stats;
//Topic 元数据
private TopicMetadata topicMetadata;

//用于超时检查以及订阅分区变更增长消息
private volatile Timeout partitionsAutoUpdateTimeout = null;
//Topic 分区变更监听器
TopicsPartitionChangedListener topicsPartitionChangedListener;
//分区自动更新future
CompletableFuture<Void> partitionsAutoUpdateFuture = null;
```

#### 2. 分区 Producer 的构造（包含初始化）

```java

public PartitionedProducerImpl(PulsarClientImpl client, String topic, ProducerConfigurationData conf, int numPartitions,
        CompletableFuture<Producer<T>> producerCreatedFuture, Schema<T> schema, ProducerInterceptors<T> interceptors) {
    //同 ProducerImpl 一样
    super(client, topic, conf, producerCreatedFuture, schema, interceptors);
    //这里跟Topic分区一样数量
    this.producers = Lists.newArrayListWithCapacity(numPartitions);
    this.topicMetadata = new TopicMetadataImpl(numPartitions);
    this.routerPolicy = getMessageRouter();
    stats = client.getConfiguration().getStatsIntervalSeconds() > 0 ? new ProducerStatsRecorderImpl() : null;

    //计算每个分区最大正处理消息数
    int maxPendingMessages = Math.min(conf.getMaxPendingMessages(),
            conf.getMaxPendingMessagesAcrossPartitions() / numPartitions);
    conf.setMaxPendingMessages(maxPendingMessages);
    //每个分区启动一个 无分区 Producer，用于发消息
    start();

    // 启动自动监控 Topic 分区增长任务
    if (conf.isAutoUpdatePartitions()) {
        topicsPartitionChangedListener = new TopicsPartitionChangedListener();
        partitionsAutoUpdateTimeout = client.timer()
            .newTimeout(partitionsAutoUpdateTimerTask, 1, TimeUnit.MINUTES);
    }
}

private void start() {
    AtomicReference<Throwable> createFail = new AtomicReference<Throwable>();
    AtomicInteger completed = new AtomicInteger();
    //开始创建分区个数的Producer
    for (int partitionIndex = 0; partitionIndex < topicMetadata.numPartitions(); partitionIndex++) {
        // 从缓存中获取分区名字
        String partitionName = TopicName.get(topic).getPartition(partitionIndex).toString();
        //创建无分区Producer
        ProducerImpl<T> producer = new ProducerImpl<>(client, partitionName, conf, new CompletableFuture<>(),
                partitionIndex, schema, interceptors);
        producers.add(producer);
        //如果创建完成，立即执行
        producer.producerCreatedFuture().handle((prod, createException) -> {
            //有异常，则设置
            if (createException != null) {
                setState(State.Failed);
                createFail.compareAndSet(null, createException);
            }
            //如果所有分区对应的Producer都创建成功，则分区 Producer 才标记为成功，否则抛其中一个创建失败的异常，并且要关闭已成功创建的对应分区的 Producer。
            if (completed.incrementAndGet() == topicMetadata.numPartitions()) {
                //如果这里为null，则意味着分区 Producer 创建成功，并设置State.Ready
                if (createFail.get() == null) {
                    setState(State.Ready);
                    log.info("[{}] Created partitioned producer", topic);
                    producerCreatedFuture().complete(PartitionedProducerImpl.this);
                } else {
                    //否则创建失败，返回创建异常，并且关闭已创建成功的无分区 Producer
                    log.error("[{}] Could not create partitioned producer.", topic, createFail.get().getCause());
                    closeAsync().handle((ok, closeException) -> {
                        producerCreatedFuture().completeExceptionally(createFail.get());
                        client.cleanupProducer(this);
                        return null;
                    });
                }
            }

            return null;
        });
    }

}
```

#### 3. 分区 Producer 发送消息流程

通过无分区 Producer 可知，实际上，发送的逻辑是通过覆盖**internalSendAsync**方法实现的，如下：

```java

@Override
CompletableFuture<MessageId> internalSendAsync(Message<T> message) {
//状态检查
    switch (getState()) {
    case Ready:
    case Connecting:
        break; // Ok
    case Closing:
    case Closed:
        return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Producer already closed"));
    case Terminated:
        return FutureUtil.failedFuture(new PulsarClientException.TopicTerminatedException("Topic was terminated"));
    case Failed:
    case Uninitialized:
        return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
    }
    //通过路由策略，选择消息发送到哪个分区
    int partition = routerPolicy.choosePartition(message, topicMetadata);
    //预防性检查分区数落点
    checkArgument(partition >= 0 && partition < topicMetadata.numPartitions(),
            "Illegal partition index chosen by the message routing policy: " + partition);
    //根据分区索引来获取 Producer ，然后发消息到此无分区 Producer 中，达到增加吞吐量目的
    return producers.get(partition).internalSendAsync(message);
}

// 这里介绍下路由接口，其核心方法为choosePartition(Message,TopicMetadata),用于消息选择哪个分区落地
public interface MessageRouter extends Serializable {

    @Deprecated
    default int choosePartition(Message<?> msg) {
        throw new UnsupportedOperationException("Use #choosePartition(Message, TopicMetadata) instead");
    }

    default int choosePartition(Message<?> msg, TopicMetadata metadata) {
        return choosePartition(msg);
    }
}

// 消息路由典型实现

@Override
public int choosePartition(Message<?> msg, TopicMetadata topicMetadata) {
    // 如果消息设置了Key，则用Key的hash值除以Topic分区数取余，其值如果小于0，则加分区数
    if (msg.hasKey()) {
        return signSafeMod(hash.makeHash(msg.getKey()), topicMetadata.numPartitions());
    }

    if (isBatchingEnabled) { 
    // 如果批量消息发送启用，则基于maxBatchingDelayMs边界计算（假定其为1ms，当前时间为Nms，那么N到N+1ms产生的消息都将会放入一个分区）
        long currentMs = clock.millis();
        return signSafeMod(currentMs / maxBatchingDelayMs + startPtnIdx, topicMetadata.numPartitions());
    } else {
        //否则，根据分区索引自增除以Topic分区数取余，其值如果小于0，则加分区数
        return signSafeMod(PARTITION_INDEX_UPDATER.getAndIncrement(this), topicMetadata.numPartitions());
    }
}

至此，分区 Producer 发送消息流程已经明朗，它的绝大多数实现都在无分区 Producer 实现，主要改进在于，Topic 分成 N 个分区，每个分区使用无分区 Producer 与broker通信，这意味着，只要网络不是瓶颈的情况下，与无分区Topic相比，理论上会提供N倍吞吐量。

```

#### 4. 分区 Producer 分区扩容

随着业务量的增长，某个 Topic 它的吞吐量已经达到上限，这时，增加它的分区数不失为一个好办法，下面介绍下分区数增长时的逻辑。

```java

// 分区 Producer 构造时，会根据参数AutoUpdatePartitions来决定是否自动监视Topic的变更事件，
// 也就是每一分钟，检查一次分区数
if (conf.isAutoUpdatePartitions()) {
    topicsPartitionChangedListener = new TopicsPartitionChangedListener();
    partitionsAutoUpdateTimeout = client.timer()
        .newTimeout(partitionsAutoUpdateTimerTask, 1, TimeUnit.MINUTES);
}

private TimerTask partitionsAutoUpdateTimerTask = new TimerTask() {
    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled() || getState() != State.Ready) {
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}]  run partitionsAutoUpdateTimerTask for partitioned producer: {}", topic);
        }

        // 如果上一个更新还没有完成，本次不做任何事情
        if (partitionsAutoUpdateFuture == null || partitionsAutoUpdateFuture.isDone()) {
            partitionsAutoUpdateFuture = topicsPartitionChangedListener.onTopicsExtended(ImmutableList.of(topic));
        }

        // 执行一次周期的检查
        partitionsAutoUpdateTimeout = client.timer()
            .newTimeout(partitionsAutoUpdateTimerTask, 1, TimeUnit.MINUTES);
    }
};

// 当Topic的分区数变更时，此监听器将被触发
private class TopicsPartitionChangedListener implements PartitionsChangedListener {
    // 检查 Topic 分区的与过去的改变，增加新的 Topic 的分区（目前只支持新增分区）
    @Override
    public CompletableFuture<Void> onTopicsExtended(Collection<String> topicsExtended) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        //预防性检查
        if (topicsExtended.isEmpty() || !topicsExtended.contains(topic)) {
            future.complete(null);
            return future;
        }

        client.getPartitionsForTopic(topic).thenCompose(list -> {
            //上一次的分区数
            int oldPartitionNumber = topicMetadata.numPartitions();
            //本次查询得出的分区数
            int currentPartitionNumber = list.size();

            if (log.isDebugEnabled()) {
                log.debug("[{}] partitions number. old: {}, new: {}",
                    topic, oldPartitionNumber, currentPartitionNumber);
            }

            //如果分区数没有改变，则结束本次
            if (oldPartitionNumber == currentPartitionNumber) {
                future.complete(null);
                return future;
            } else if (oldPartitionNumber < currentPartitionNumber) {//新分区数大于老分区数（即分区扩容）
                List<CompletableFuture<Producer<T>>> futureList = list
                    .subList(oldPartitionNumber, currentPartitionNumber)
                    .stream()//找出新增的分区
                    .map(partitionName -> {
                        //获取索引号
                        int partitionIndex = TopicName.getPartitionIndex(partitionName);
                        ProducerImpl<T> producer =
                            new ProducerImpl<>(client,
                                partitionName, conf, new CompletableFuture<>(),
                                partitionIndex, schema, interceptors);
                        producers.add(producer);
                        //返回创建futrue
                        return producer.producerCreatedFuture();
                    }).collect(Collectors.toList());
                //等待所有的新的生产者创建成功
                FutureUtil.waitForAll(futureList)
                    .thenAccept(finalFuture -> {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] success create producers for extended partitions. old: {}, new: {}",
                                topic, oldPartitionNumber, currentPartitionNumber);
                        }
                        //创建成功后，设置新的分区信息
                        topicMetadata = new TopicMetadataImpl(currentPartitionNumber);
                        future.complete(null);
                    })
                    .exceptionally(ex -> {
                        // 如果有异常发生，则关闭新创建的生产者，并移除相关资源，等待下一次检查的到来
                        log.warn("[{}] fail create producers for extended partitions. old: {}, new: {}",
                            topic, oldPartitionNumber, currentPartitionNumber);
                        List<ProducerImpl<T>> sublist = producers.subList(oldPartitionNumber, producers.size());
                        sublist.forEach(newProducer -> newProducer.closeAsync());
                        sublist.clear();
                        future.completeExceptionally(ex);
                        return null;
                    });
                return null;
            } else {
                // 暂时不支持分区数减少操作，即Topic分区缩容操作
                log.error("[{}] not support shrink topic partitions. old: {}, new: {}",
                    topic, oldPartitionNumber, currentPartitionNumber);
                future.completeExceptionally(new NotSupportedException("not support shrink topic partitions"));
            }
            return future;
        });

        return future;
    }
}

//通过 Topic 来获取其分区数
@Override
public CompletableFuture<List<String>> getPartitionsForTopic(String topic) {
    return getPartitionedTopicMetadata(topic).thenApply(metadata -> {
        if (metadata.partitions > 1) {
            TopicName topicName = TopicName.get(topic);
            List<String> partitions = new ArrayList<>(metadata.partitions);
            for (int i = 0; i < metadata.partitions; i++) {
                partitions.add(topicName.getPartition(i).toString());
            }
            return partitions;
        } else {
            return Collections.singletonList(topic);
        }
    });
}

```

到这里，Pulsar Producer 已经解析完毕，架构上跟传统的MQ Producer 的职责一样，但是在源代码上实现还是非常优雅的，层次非常鲜明，代码模式用的恰大好处，很容易阅读。

## Pulsar Consumer源码解析

Pulsar Consumer 也属于 Pulsar Client组件，因为在 Pulsar Producer 时候已经对Client有所解析，这里，将直接开门入山，不再做铺垫了。

### 1. Consumer 简单例子

```java
public class SampleConsumer {
    public static void main(String[] args) throws PulsarClientException, InterruptedException {

        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

        Consumer<byte[]> consumer = pulsarClient.newConsumer() //
                .topic("persistent://my-tenant/my-ns/my-topic") //
                .subscriptionName("my-subscription-name").subscribe();

        Message<byte[]> msg = null;

        for (int i = 0; i < 100; i++) {
            msg = consumer.receive();
            // do something
            System.out.println("Received: " + new String(msg.getData()));
        }

        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        pulsarClient.close();
    }
}
```

### 2. Consumer 配置项

直接切换到消费者构造器，如下：

```java
// 跟 Producer 一样，Consumer 一样有两个接口：一个默认Schema.BYTES，用于兼容原来的客户端，一个自定义Schema类型的，用于自定义。
@Override
public ConsumerBuilder<byte[]> newConsumer() {
    return new ConsumerBuilderImpl<>(this, Schema.BYTES);
}

public <T> ConsumerBuilder<T> newConsumer(Schema<T> schema) {
    return new ConsumerBuilderImpl<>(this, schema);
}
```

这里，引出了 ConsumerBuilder 类，绝大部分是 Consumer 的配置，当然还包括 Cousumer 的最主要方法，它的接口：

```java

//克隆当前消费者Builder ，使用场景如下：
/**
ConsumerBuilder<String> builder = client.newConsumer(Schema.STRING)
        .subscriptionName("my-subscription-name")
        .subscriptionType(SubscriptionType.Shared)
        .receiverQueueSize(10);

Consumer<String> consumer1 = builder.clone().topic("my-topic-1").subscribe();
Consumer<String> consumer2 = builder.clone().topic("my-topic-2").subscribe();
不同的Topic下，一些相同的配置进行订阅
*/
ConsumerBuilder<T> clone();
//从Map中读取配置信息
ConsumerBuilder<T> loadConf(Map<String, Object> config);
//订阅指定Topic，如果订阅不存在，则创建新的订阅，默认配置下，这订阅将从Topic的尾部开始接受消息，可以看 {@link #subscriptionInitialPosition(SubscriptionInitialPosition)} 通过此方法来配置订阅行为，即订阅开始时从什么位置开始读取消息。
Consumer<T> subscribe() throws PulsarClientException;
//异步订阅，实现逻辑跟同步订阅一直，一旦创建订阅后，即使消费者未连接，它也会保留数据和订阅光标。
CompletableFuture<Consumer<T>> subscribeAsync();
//设置多个 Topic，消费者将消费多个 Topic 中的消息。（注意，由于ACL原因，这里多个Topic 只能是同一个 Namespace 中的）
ConsumerBuilder<T> topic(String... topicNames);
//同上
ConsumerBuilder<T> topics(List<String> topicNames);
//设置 Topic 正则表达式订阅,消费者将通过这个表达式匹配 Topic 名并自动订阅。（这里所有的  Topic 只能在同一 Namespace）
ConsumerBuilder<T> topicsPattern(Pattern topicsPattern);
//同上
ConsumerBuilder<T> topicsPattern(String topicsPattern);
//设置订阅名
ConsumerBuilder<T> subscriptionName(String subscriptionName);
//设置未确认消息的确认超时时间，截断为最接近的毫秒。 超时时间需要大于10秒。 默认情况下，确认超时被禁用，这意味着除非消费者崩溃，否则不会重新传递传递给消费者的消息。当启用确认超时时，如果在指定的超时时间内未确认消息，则将重新传递给消费者（在共享订阅的情况下可能传递给其他消费者）。
ConsumerBuilder<T> ackTimeout(long ackTimeout, TimeUnit timeUnit);
//订阅类型，目前支持3种：Exclusive（独占）默认配置，Failover（失败转移），Shared（共享）
ConsumerBuilder<T> subscriptionType(SubscriptionType subscriptionType);
//设置消费者监听器，一旦有消息到来，将自动调用此接口
ConsumerBuilder<T> messageListener(MessageListener<T> messageListener);
//加密配置信息读取器，读取的加密配置将用于加密消息
ConsumerBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader);
//加密失败执行的动作，参考枚举类ConsumerCryptoFailureAction
ConsumerBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action);
//设置接收队列大小，默认值为1000（大多数场景推荐值）
ConsumerBuilder<T> receiverQueueSize(int receiverQueueSize);
//消息确认分组提交（类似于批量确认的意思）如果设置为0，则意味着立即发送确认消息，否则按照配置的间隔时间确认，默认值为100ms确认
ConsumerBuilder<T> acknowledgmentGroupTime(long delay, TimeUnit unit);
//跨分区时，设置最大总消息队列大小，默认为50000
ConsumerBuilder<T> maxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions);
//设置消费者名称
ConsumerBuilder<T> consumerName(String consumerName);
//消费者事件监听器，消费组接收状态改变（如：消费组里有成员宕机），应用程序依靠这种机制能对状态变更做出反应
ConsumerBuilder<T> consumerEventListener(ConsumerEventListener consumerEventListener);
//如果启用，则使用者将从 Compact Topic 中读取消息，而不是读 Topic 中完整消息积压。 这意味着，如果 Topic 已被压缩，则使用者将只看到主题中每个键的最新值，直到 Topic 积压的消息全部已经压缩为止。 除此之外，消息将照常发送。readCompacted只能启用对持久 Topic 的订阅，这些 Topic 只能有一个活动消费者（即失败或独占模式订阅）。 尝试在订阅非持久性 Topic 或共享订阅时启用它，将导致订阅调用抛出PulsarClientException。
ConsumerBuilder<T> readCompacted(boolean readCompacted);
//当通过正则表达式订阅时，系统将通过设置指定时间间隔来自动发现新的符合正则表达式的 Topic。此参数就是设置时间间隔使用的。（注意，这里最新时间单位为分钟，最小值为1分钟）
ConsumerBuilder<T> patternAutoDiscoveryPeriod(int periodInMinutes);
//订阅共享模式时有效，如果消费者有更高的优先级（priorityLevel），将优先分发消息。（其中，0为最高优先级，以此类推）在共享订阅模式下，如果拥有许可（permits），broker 将首先将消息分派给最高优先级消费者，否则代理将考虑下一个优先级消费者。 如果订阅具有带有priorityLevel 0的consumer-A和带有priorityLevel 1的Consumer-B，则broker将仅将消息分派给consumer-A，直到它用完permit，然后broker开始将消息分派给Consumer-B。请看如下例子：
 Consumer PriorityLevel Permits
  C1       0             2
  C2       0             1
  C3       0             1
  C4       1             2
  C5       1             1
  Broker 消息分发到 Consumer 的顺序为: C1, C2, C3, C1, C4, C5, C4
ConsumerBuilder<T> priorityLevel(int priorityLevel);
// 设置消费者的键值对
ConsumerBuilder<T> property(String key, String value);
//指定Map设置消费者的键值对
ConsumerBuilder<T> properties(Map<String, String> properties);
//当创建新订阅时，指定从什么位置开始都读取消息
ConsumerBuilder<T> subscriptionInitialPosition(SubscriptionInitialPosition subscriptionInitialPosition);
//设置订阅模式，用于决定消费者订阅持久化、非持久化和所有的 Topic，此参数只用于正则表达式订阅方式中
ConsumerBuilder<T> subscriptionTopicsMode(RegexSubscriptionMode regexSubscriptionMode);
//设置消费者拦截链
ConsumerBuilder<T> intercept(ConsumerInterceptor<T> ...interceptors);
//设置死信消息策略，这里可以设置当消费一定次数时，消息将会放入指定的死信队列，死信队列 Topic 的默认名为{TopicName}-{Subscription}-DLQ，当然，这里也可以指定死信队列名称
ConsumerBuilder<T> deadLetterPolicy(DeadLetterPolicy deadLetterPolicy);
//如果启用，消费者将自动订阅新增的分区的变更（这里分区只能增长）
ConsumerBuilder<T> autoUpdatePartitions(boolean autoUpdate);

```

### 2. Comsumer 的接口

```java
//返回 Topic
String getTopic();
//返回订阅名
String getSubscription();
//不再订阅
void unsubscribe() throws PulsarClientException;
//异步操作，功能同上
CompletableFuture<Void> unsubscribeAsync();
//接收消息
Message<T> receive() throws PulsarClientException;
//异步操作，功能同上
CompletableFuture<Message<T>> receiveAsync();
//指定时间内接收消息，超过则返回空
Message<T> receive(int timeout, TimeUnit unit) throws PulsarClientException;
//确认单个消息
void acknowledge(Message<?> message) throws PulsarClientException;
//确认单个消息
void acknowledge(MessageId messageId) throws PulsarClientException;
//确认接收流中的所有消息（并包括）提供传入参数的消息。此方法将阻塞，直到确认已发送给broker。 之后，消息将不会重新传递给此消费者。当订阅类型设置为 ConsumerShared 时，不能使用累积确认。 它等同于调用asyncAcknowledgeCumulative（Message）并等待触发回调。
void acknowledgeCumulative(Message<?> message) throws PulsarClientException;
//功能同上
void acknowledgeCumulative(MessageId messageId) throws PulsarClientException;
//异步确认单个消息
CompletableFuture<Void> acknowledgeAsync(Message<?> message);
//同上
CompletableFuture<Void> acknowledgeAsync(MessageId messageId);
//异步累积确认接口，功能参考同步接口
CompletableFuture<Void> acknowledgeCumulativeAsync(Message<?> message);
//同上
CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId);
//返回消费状态
ConsumerStats getStats();
//关闭消费者
void close() throws PulsarClientException;
//异步操作，功能同上
CompletableFuture<Void> closeAsync();
//如果 Topic 终止并且消费者已经消费了 Topic 中的所有消息，则返回true，请注意，这不是简单的意味着消费者追上生产者最新发布的消息，而不是 Topic 真的需要 “终止”
boolean hasReachedEndOfTopic();
//重新传递所有未确认的消息。 在故障转移（Failover）模式下，如果消费者对于给定 Topic 处于不活动状态，则会忽略该请求。 在共享（Shared）模式下，要重传的消息将被均匀分布在所有连接的消费者中。 这是一个非阻塞调用，不会抛出异常。 如果连接中断，重新连接后将重新传递消息。
void redeliverUnacknowledgedMessages();
//指定消息重置当前消费者的订阅。注意，此操作只能在非分区 Topic 上，当然，多分区 Topic 时消费者可以操作单分区
void seek(MessageId messageId) throws PulsarClientException;
//异步操作，功能同上
CompletableFuture<Void> seekAsync(MessageId messageId);
//消费者是否已连接到 Broker
boolean isConnected();
//返回消费者名称
String getConsumerName();
//停止从 broker 请求新消息直到 resume 方法被调用。注意，这可能导致 receive() 方法阻塞直到 resume 方法被调用，此时，broker 会推送新的消息过来。
void pause();
//恢复从 broker 请求消息
void resume();
```

### 3. Consumer 接口实现

Cousumer 接口实现有3个典型实现：ConsumerImpl 、MultiTopicsConsumerImpl、PatternMultiTopicConsumerImpl。其继承结构跟 Producer 非常类似。现在就依次开始解析。

#### 1. ConsumerImpl 实现解析

##### 1. 类继承图，如下

![ComsumerImpl类图](./images/20190422162436.png)

基本上，跟 ProducerImpl 类继承结构一样。

##### 2. ConsumerImpl 的属性

```java
//最大未确认消息数
private static final int MAX_REDELIVER_UNACKNOWLEDGED = 1000;
//消费者ID
final long consumerId;
//已传递给应用程序的消息数。每隔一段时间，这数字将发送 broker 通知我们准备（存储到消息队列中）获得更多的消息。（可用许可数）
@SuppressWarnings("rawtypes")
private static final AtomicIntegerFieldUpdater<ConsumerImpl> AVAILABLE_PERMITS_UPDATER = AtomicIntegerFieldUpdater
        .newUpdater(ConsumerImpl.class, "availablePermits");
@SuppressWarnings("unused")
private volatile int availablePermits = 0;
//上一次出队列消息
protected volatile MessageId lastDequeuedMessage = MessageId.earliest;
//上一次在broker消息ID
private volatile MessageId lastMessageIdInBroker = MessageId.earliest;
// 订阅超时时间
private long subscribeTimeout;
//分区索引号
private final int partitionIndex;
//接收队列阈值？
private final int receiverQueueRefillThreshold;
//读写锁
private final ReadWriteLock lock = new ReentrantReadWriteLock();
//未确认消息跟踪器
private final UnAckedMessageTracker unAckedMessageTracker;
//确认组提交
private final AcknowledgmentsGroupingTracker acknowledgmentsGroupingTracker;
//消费状态记录器
protected final ConsumerStatsRecorder stats;
//优先级
private final int priorityLevel;
//订阅模式
private final SubscriptionMode subscriptionMode;
//批量消息ID
private volatile BatchMessageIdImpl startMessageId;
//消费 Topic 末尾标志
private volatile boolean hasReachedEndOfTopic;
//消息解密器
private final MessageCrypto msgCrypto;
//元数据
private final Map<String, String> metadata;
//是否压缩
private final boolean readCompacted;
//订阅初始化Position
private final SubscriptionInitialPosition subscriptionInitialPosition;
//连接处理器
private final ConnectionHandler connectionHandler;
//Topic信息
private final TopicName topicName;
//Topic名称无分区信息
private final String topicNameWithoutPartition;
//死信消息容器
private final Map<MessageIdImpl, List<MessageImpl<T>>> possibleSendToDeadLetterTopicMessages;
//死信队列策略
private final DeadLetterPolicy deadLetterPolicy;
//死信消息生产者
private Producer<T> deadLetterProducer;
//暂停标志
protected volatile boolean paused;

enum SubscriptionMode {
    //保存订阅进度
    Durable,

    //不会保存订阅进度
    NonDurable
}
```

##### 3. ConsumerImpl 构造方法

ConsumerImpl 构造方法基本与 ProducerImpl 类似，都是调用先调用父类构造方法，然后初始化基础数据和一些服务，最后开始连接 broker 或 Proxy，完成构造初始化。

```java
protected ConsumerImpl(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
                ExecutorService listenerExecutor, int partitionIndex, CompletableFuture<Consumer<T>> subscribeFuture,
                SubscriptionMode subscriptionMode, MessageId startMessageId, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
    super(client, topic, conf, conf.getReceiverQueueSize(), listenerExecutor, subscribeFuture, schema, interceptors);
    //生成消费者ID
    this.consumerId = client.newConsumerId();
    //订阅模式
    this.subscriptionMode = subscriptionMode;
    //确定是否有起始消息ID
    this.startMessageId = startMessageId != null ? new BatchMessageIdImpl((MessageIdImpl) startMessageId) : null;
    //可用许可设置为0
    AVAILABLE_PERMITS_UPDATER.set(this, 0);
    this.subscribeTimeout = System.currentTimeMillis() + client.getConfiguration().getOperationTimeoutMs();
    //消费者 Topic 分区索引
    this.partitionIndex = partitionIndex;
    //接收队列？阈值（接收队列大小的一半）
    this.receiverQueueRefillThreshold = conf.getReceiverQueueSize() / 2;
    //优先级
    this.priorityLevel = conf.getPriorityLevel();
    this.readCompacted = conf.isReadCompacted();
    //设置订阅初始化时，游标开始位置
    this.subscriptionInitialPosition = conf.getSubscriptionInitialPosition();
    //消费状态收集定时时间
    if (client.getConfiguration().getStatsIntervalSeconds() > 0) {
        stats = new ConsumerStatsRecorderImpl(client, conf, this);
    } else {
        stats = ConsumerStatsDisabled.INSTANCE;
    }

    //消息确认超时时间
    if (conf.getAckTimeoutMillis() != 0) {
        //时间单位
        if (conf.getTickDurationMillis() > 0) {
            this.unAckedMessageTracker = new UnAckedMessageTracker(client, this, conf.getAckTimeoutMillis(), conf.getTickDurationMillis());
        } else {
            this.unAckedMessageTracker = new UnAckedMessageTracker(client, this, conf.getAckTimeoutMillis());
        }
    } else {
        this.unAckedMessageTracker = UnAckedMessageTracker.UNACKED_MESSAGE_TRACKER_DISABLED;
    }

    // 如果还没创建，则创建消息解密器
    if (conf.getCryptoKeyReader() != null) {
        this.msgCrypto = new MessageCrypto(String.format("[%s] [%s]", topic, subscription), false);
    } else {
        this.msgCrypto = null;
    }

    //初始化属性或设置属性为不可变更
    if (conf.getProperties().isEmpty()) {
        metadata = Collections.emptyMap();
    } else {
        metadata = Collections.unmodifiableMap(new HashMap<>(conf.getProperties()));
    }

    //连接处理器，这里跟 Producer 一样
    this.connectionHandler = new ConnectionHandler(this,
        new Backoff(100, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS, 0, TimeUnit.MILLISECONDS),
        this);

    //读取 Topic 信息，判定 Topic 是持久化或非持久化的
    this.topicName = TopicName.get(topic);
    if (this.topicName.isPersistent()) {
        //分组（定时）确认消息
        this.acknowledgmentsGroupingTracker =
            new PersistentAcknowledgmentsGroupingTracker(this, conf, client.eventLoopGroup());
    } else {
        //非持久化 Topic，不需要确认消息。
        this.acknowledgmentsGroupingTracker =
            NonPersistentAcknowledgmentGroupingTracker.of();
    }

    //如果死信消息策略不为空，这里主要是设置重发次数和死信消息所发送的 Topic 名称
    if (conf.getDeadLetterPolicy() != null) {
        possibleSendToDeadLetterTopicMessages = new ConcurrentHashMap<>();
        if (StringUtils.isNotBlank(conf.getDeadLetterPolicy().getDeadLetterTopic())) {
            this.deadLetterPolicy = DeadLetterPolicy.builder()
                    .maxRedeliverCount(conf.getDeadLetterPolicy().getMaxRedeliverCount())
                    .deadLetterTopic(conf.getDeadLetterPolicy().getDeadLetterTopic())
                    .build();
        } else {
            this.deadLetterPolicy = DeadLetterPolicy.builder()
                    .maxRedeliverCount(conf.getDeadLetterPolicy().getMaxRedeliverCount())
                    .deadLetterTopic(String.format("%s-%s-DLQ", topic, subscription))
                    .build();
        }
    } else {
        deadLetterPolicy = null;
        possibleSendToDeadLetterTopicMessages = null;
    }

    //无分区信息的 Topic 信息
    topicNameWithoutPartition = topicName.getPartitionedTopicName();
    // 这里跟 Producer 构造方法一样，发起与 broker 连接，直到获取到连接结果。这里唯一不同的是就是对 Connection 两个方法的实现。
    grabCnx();
}

// 因为这里涉及了 ConsumerBase 类，也看看它的构造方法。

protected ConsumerBase(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
                        int receiverQueueSize, ExecutorService listenerExecutor,
                        CompletableFuture<Consumer<T>> subscribeFuture, Schema<T> schema, ConsumerInterceptors interceptors) {
    super(client, topic);
    this.maxReceiverQueueSize = receiverQueueSize;
    this.subscription = conf.getSubscriptionName();
    this.conf = conf;
    //如果没设置消费者名称，则随机生成
    this.consumerName = conf.getConsumerName() == null ? ConsumerName.generateRandomName() : conf.getConsumerName();
    this.subscribeFuture = subscribeFuture;
    this.listener = conf.getMessageListener();
    this.consumerEventListener = conf.getConsumerEventListener();
    //如果接收队列小于等于1，则使用数组阻塞队列，否则，使用可增长的数组阻塞队列
    if (receiverQueueSize <= 1) {
        this.incomingMessages = Queues.newArrayBlockingQueue(1);
    } else {
        this.incomingMessages = new GrowableArrayBlockingQueue<>();
    }

    this.listenerExecutor = listenerExecutor;
    this.pendingReceives = Queues.newConcurrentLinkedQueue();
    this.schema = schema;
    this.interceptors = interceptors;
}

// ConsumerBase 类也是跟 ProducerBase 非常类似，同样是继承 HandlerState 类。因为前面已经分析过 HandlerState 类，故这里不再描述。

//前面说到，grabCnx() 方法跟 ProducerImpl 中的是一模一样的，只不过它们对 Connection 接口的实现部分不一样而已，那下面来分析一下，Connection 接口实现细节。

//ConsumerImpl 实现了Connection接口的connectionOpened方法，如下：
@Override
public void connectionOpened(final ClientCnx cnx) {
    //原子设置 cnx
    setClientCnx(cnx);
    //把自己注册到 ClientCnx 消费者容器中
    cnx.registerConsumer(consumerId, this);

    log.info("[{}][{}] Subscribing to topic on cnx {}", topic, subscription, cnx.ctx().channel());
    // 生成请求ID
    long requestId = client.newRequestId();

    int currentSize;
    synchronized (this) {
        //当前消息队列大小
        currentSize = incomingMessages.size();
        startMessageId = clearReceiverQueue();
        if (possibleSendToDeadLetterTopicMessages != null) {
            possibleSendToDeadLetterTopicMessages.clear();
        }
    }

    boolean isDurable = subscriptionMode == SubscriptionMode.Durable;
    MessageIdData startMessageIdData;
    if (isDurable) {
        //对于通常的持久化订阅，重启时，broker 决定起始消息ID。 TODO
        startMessageIdData = null;
    } else {
        // 对于非持久化订阅，重启时直接取下一消息实体
        MessageIdData.Builder builder = MessageIdData.newBuilder();
        builder.setLedgerId(startMessageId.getLedgerId());
        builder.setEntryId(startMessageId.getEntryId());
        if (startMessageId instanceof BatchMessageIdImpl) {
            builder.setBatchIndex(((BatchMessageIdImpl) startMessageId).getBatchIndex());
        }

        startMessageIdData = builder.build();
        builder.recycle();
    }

    SchemaInfo si = schema.getSchemaInfo();
    if (si != null && SchemaType.BYTES == si.getType()) {
        // 为了兼容，当 Schema 类型为 Schema.BYTES 时，SchemaInfo 为 null。
        si = null;
    }
    //创建新订阅命令
    ByteBuf request = Commands.newSubscribe(topic, subscription, consumerId, requestId, getSubType(), priorityLevel,
            consumerName, isDurable, startMessageIdData, metadata, readCompacted, InitialPosition.valueOf(subscriptionInitialPosition.getValue()), si);
    if (startMessageIdData != null) {
        startMessageIdData.recycle();
    }

    //执行请求
    cnx.sendRequestWithId(request, requestId).thenRun(() -> {
        synchronized (ConsumerImpl.this) {
            //尝试把cnx状态转变成 Ready 状态
            if (changeToReadyState()) {
                // 转换成功，则把可用许可设置为0
                consumerIsReconnectedToBroker(cnx, currentSize);
            } else {
                //如果重连的时候，转换失败，设置消费者状态为 Closed 状态，并关闭连接确保 broker 清理消费者（资源）
                setState(State.Closed);
                cnx.removeConsumer(consumerId);
                cnx.channel().close();
                return;
            }
        }
        //重置重连超时时间
        resetBackoff();
        //尝试把 subscribeFuture 设置 complete 状态，如果成功，则认为是第一次连接
        boolean firstTimeConnect = subscribeFuture.complete(this);
        //如果消费者（订阅的Topic）是无分区 或 是分区但已重连过，并且接收队列不为 0，发送流控命令到 broker，（通知 broker 可以推送消息到消费者了）
        if (!(firstTimeConnect && partitionIndex > -1) && conf.getReceiverQueueSize() != 0) {
            sendFlowPermitsToBroker(cnx, conf.getReceiverQueueSize());
        }
    }).exceptionally((e) -> {
        //连接打开时，出现异常
        cnx.removeConsumer(consumerId);
        if (getState() == State.Closing || getState() == State.Closed) {
            //如果消费者状态为 Closed 或 Closing 状态时，关闭连接确保 broker 清理消费者（资源）
            cnx.channel().close();
            return null;
        }
        log.warn("[{}][{}] Failed to subscribe to topic on {}", topic, subscription, cnx.channel().remoteAddress());
        //如果 异常是 PulsarClientException 导致的，其属于可重试异常，并还没有订阅超时时间， 那么稍后重连
        if (e.getCause() instanceof PulsarClientException && getConnectionHandler().isRetriableError((PulsarClientException) e.getCause())
                && System.currentTimeMillis() < subscribeTimeout) {
            reconnectLater(e.getCause());
            return null;
        }

        if (!subscribeFuture.isDone()) {
            // 不能创建新的消费者，操作失败，状态设置 Failed，设置异常，并清理资源
            setState(State.Failed);
            subscribeFuture.completeExceptionally(e);
            client.cleanupConsumer(this);
        } else {
            // 消费者已连接和订阅，但是出现一些错误，稍后重试
            reconnectLater(e.getCause());
        }
        return null;
    });
}


/**
   *清除内部接收队列并返回队列中第一条消息的消息ID，该消息是应用尚未看到（处理）的
   */
private BatchMessageIdImpl clearReceiverQueue() {
    List<Message<?>> currentMessageQueue = new ArrayList<>(incomingMessages.size());
    //把阻塞队列数据全部按顺序放入当前消息队列，就是ArrayList容器
    incomingMessages.drainTo(currentMessageQueue);
    //如果不为空
    if (!currentMessageQueue.isEmpty()) {
        // 取第一条消息
        MessageIdImpl nextMessageInQueue = (MessageIdImpl) currentMessageQueue.get(0).getMessageId();
        BatchMessageIdImpl previousMessage;
        if (nextMessageInQueue instanceof BatchMessageIdImpl) {
            // 获取前一个消息在当前批量消息中
            previousMessage = new BatchMessageIdImpl(nextMessageInQueue.getLedgerId(),
                    nextMessageInQueue.getEntryId(), nextMessageInQueue.getPartitionIndex(),
                    ((BatchMessageIdImpl) nextMessageInQueue).getBatchIndex() - 1);
        } else {
            // 获取前一个消息在当前消息实体中
            previousMessage = new BatchMessageIdImpl(nextMessageInQueue.getLedgerId(),
                    nextMessageInQueue.getEntryId() - 1, nextMessageInQueue.getPartitionIndex(), -1);
        }

        return previousMessage;
    } else if (!lastDequeuedMessage.equals(MessageId.earliest)) {
        // 如果队列为空，最后出队消息不等于最早消息ID，则使用最后出队消息作为起始消息
        return new BatchMessageIdImpl((MessageIdImpl) lastDequeuedMessage);
    } else {
        // 如果没消息接收过或已经被消费者处理，下一个消息还是用起始消息
        return startMessageId;
    }
}

//重连完成，设置可用许可为0
protected void consumerIsReconnectedToBroker(ClientCnx cnx, int currentQueueSize) {
    log.info("[{}][{}] Subscribed to topic on {} -- consumer: {}", topic, subscription,
            cnx.channel().remoteAddress(), consumerId);

    AVAILABLE_PERMITS_UPDATER.set(this, 0);
}

//发送流控命令到 broker
void sendFlowPermitsToBroker(ClientCnx cnx, int numMessages) {
    if (cnx != null) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Adding {} additional permits", topic, subscription, numMessages);
        }

        cnx.ctx().writeAndFlush(Commands.newFlow(consumerId, numMessages), cnx.ctx().voidPromise());
    }
}

```

至此，Comsumer 已经连接 Broker 或 通过 Proxy 连接到 Broker ，并且已经准备接收 Broker 推送消息过来。

##### 4. ComsumerImpl 接收消息

消费者接收消息的API非常简单，根据前面的消费者简单例子，如下：

```java

Message<byte[]> msg = consumer.receive();

```

下面来详细解析。

```java

//ConsumerBase 抽象类中方法，它实际上调用internalReceive()抽象方法，这个方法由各个消费者实现，这里只是作一些状态检查。
@Override
public Message<T> receive() throws PulsarClientException {
    if (listener != null) {
        throw new PulsarClientException.InvalidConfigurationException(
                "Cannot use receive() when a listener has been set");
    }

    switch (getState()) {
    case Ready:
    case Connecting:
        break; // Ok
    case Closing:
    case Closed:
        throw new PulsarClientException.AlreadyClosedException("Consumer already closed");
    case Terminated:
        throw new PulsarClientException.AlreadyClosedException("Topic was terminated");
    case Failed:
    case Uninitialized:
        throw new PulsarClientException.NotConnectedException();
    default:
        break;
    }

    return internalReceive();
}

//ConsumerImpl 实现internalReceive方法
@Override
protected Message<T> internalReceive() throws PulsarClientException {
    Message<T> message;
    try {
        //消息队列阻塞调用，等待消息到来
        message = incomingMessages.take();
        //把消息放入跟踪未确认消息记录器
        trackMessage(message);
        //调用消息拦截器(这里不再详细分析)
        Message<T> interceptMsg = beforeConsume(message);
        //处理消息
        messageProcessed(interceptMsg);
        return interceptMsg;
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        stats.incrementNumReceiveFailed();
        throw new PulsarClientException(e);
    }
}

protected void trackMessage(Message<?> msg) {
    if (msg != null) {
        MessageId messageId = msg.getMessageId();
        if (conf.getAckTimeoutMillis() > 0 && messageId instanceof MessageIdImpl) {
            MessageIdImpl id = (MessageIdImpl)messageId;
            if (id instanceof BatchMessageIdImpl) {
                // 对于批量消息实现，不需要每个消息都放入跟踪器
                id = new MessageIdImpl(id.getLedgerId(), id.getEntryId(), getPartitionIndex());
            }
            unAckedMessageTracker.add(id);
        }
    }
}

protected synchronized void messageProcessed(Message<?> msg) {
    ClientCnx currentCnx = cnx();
    ClientCnx msgCnx = ((MessageImpl<?>) msg).getCnx();
    lastDequeuedMessage = msg.getMessageId();
    //当前消息属于老队列中的，重连后将被清除。
    if (msgCnx != currentCnx) {
        return;
    }
    //增加可用许可
    increaseAvailablePermits(currentCnx);
    stats.updateNumMsgsReceived(msg);

    if (conf.getAckTimeoutMillis() != 0) {
        //为通过客户端接收的消息重置定时器
        MessageIdImpl id = (MessageIdImpl) msg.getMessageId();
        if (id instanceof BatchMessageIdImpl) {
            id = new MessageIdImpl(id.getLedgerId(), id.getEntryId(), getPartitionIndex());
        }
        if (partitionIndex != -1) {
           //不再跟踪此消息，TopicsConsumer将从现在开始注意
            unAckedMessageTracker.remove(id);
        } else {
            unAckedMessageTracker.add(id);
        }
    }
}

void increaseAvailablePermits(ClientCnx currentCnx) {
    increaseAvailablePermits(currentCnx, 1);
}

//增加可用许可
private void increaseAvailablePermits(ClientCnx currentCnx, int delta) {
    int available = AVAILABLE_PERMITS_UPDATER.addAndGet(this, delta);

    while (available >= receiverQueueRefillThreshold && !paused) {
        if (AVAILABLE_PERMITS_UPDATER.compareAndSet(this, available, 0)) {
            //根据可用许可，通知 broker 推送消息
            sendFlowPermitsToBroker(currentCnx, available);
            break;
        } else {
            available = AVAILABLE_PERMITS_UPDATER.get(this);
        }
    }
}


//异步接收消息方法，其实调用internalReceiveAsync()抽象方法，被调用的方法由各个消费者实现，这里只是进行一些状态检查。
@Override
public CompletableFuture<Message<T>> receiveAsync() {

    if (listener != null) {
        return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                "Cannot use receive() when a listener has been set"));
    }

    switch (getState()) {
    case Ready:
    case Connecting:
        break; // Ok
    case Closing:
    case Closed:
        return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Consumer already closed"));
    case Terminated:
        return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Topic was terminated"));
    case Failed:
    case Uninitialized:
        return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
    }

    return internalReceiveAsync();
}

//ConsumerImpl 实现internalReceiveAsync方法
@Override
protected CompletableFuture<Message<T>> internalReceiveAsync() {

    CompletableFuture<Message<T>> result = new CompletableFuture<>();
    Message<T> message = null;
    try {
        //写锁（这里加锁为了pendingReceives容器的isEmpty()方法准确度）
        lock.writeLock().lock();
        //尝试从本地消息队列获取消息
        message = incomingMessages.poll(0, TimeUnit.MILLISECONDS);
        if (message == null) {
            //如果没有消息，则把 Futrue 则放入正处理接收队列（等消息到达，再进行处理）
            pendingReceives.add(result);
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        result.completeExceptionally(e);
    } finally {
        lock.writeLock().unlock();
    }

    //如果有消息，接下来的处理跟同步获取消息逻辑一样
    if (message != null) {
        trackMessage(message);
        Message<T> interceptMsg = beforeConsume(message);
        messageProcessed(interceptMsg);
        result.complete(interceptMsg);
    }

    return result;
}

```

自此，消息接收逻辑已经分析完了，还是比较简单的，但是有一个没有交代清楚，就是本地消息队列（**incomingMessages**）是怎么放入消息的呢？接下来，分析下。

再看一下incomeingMessages的初始化,实际上，它依赖配置**receiverQueueSize**，根据大小来决定用什么阻塞数组队列。

```java
if (receiverQueueSize <= 1) {
    this.incomingMessages = Queues.newArrayBlockingQueue(1);
} else {
    this.incomingMessages = new GrowableArrayBlockingQueue<>();
}
```

好了，通信框架有个**handleMessage**方法，来看看，如下：

```java
//ClientCnx 类，这里就是处理消息接收的地方
@Override
protected void handleMessage(CommandMessage cmdMessage, ByteBuf headersAndPayload) {
    checkArgument(state == State.Ready);

    if (log.isDebugEnabled()) {
        log.debug("{} Received a message from the server: {}", ctx.channel(), cmdMessage);
    }
    //通过消费者ID，获取消费者实例
    ConsumerImpl<?> consumer = consumers.get(cmdMessage.getConsumerId());
    if (consumer != null) {
        //这里是
        consumer.messageReceived(cmdMessage.getMessageId(), cmdMessage.getRedeliveryCount(), headersAndPayload, this);
    }
}

//ConusmerImpl 类
void messageReceived(MessageIdData messageId, int redeliveryCount, ByteBuf headersAndPayload, ClientCnx cnx) {
    if (log.isDebugEnabled()) {
        log.debug("[{}][{}] Received message: {}/{}", topic, subscription, messageId.getLedgerId(),
                messageId.getEntryId());
    }

    MessageIdImpl msgId = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), getPartitionIndex());
    //检查此消息是否已确认，如果已确认，则忽略（丢弃）
    if (acknowledgmentsGroupingTracker.isDuplicate(msgId)) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Ignoring message as it was already being acked earlier by same consumer {}/{}",
                    topic, subscription, msgId);
        }
        //如果系统队列大小设置为0，则立即通知 broker 推送消息过来
        if (conf.getReceiverQueueSize() == 0) {
            increaseAvailablePermits(cnx);
        }
        return;
    }

    MessageMetadata msgMetadata = null;
    ByteBuf payload = headersAndPayload;
    //进行消息校验，是否已损坏
    if (!verifyChecksum(headersAndPayload, messageId)) {
        // 消息校验失败，则丢弃这个消息，并通知 broker 重发
        discardCorruptedMessage(messageId, cnx, ValidationError.ChecksumMismatch);
        return;
    }

    try {
        //尝试解析消息
        msgMetadata = Commands.parseMessageMetadata(payload);
    } catch (Throwable t) {
        //解析异常，则丢弃这个消息，并通知 broker 重发
        discardCorruptedMessage(messageId, cnx, ValidationError.ChecksumMismatch);
        return;
    }

    // 如果需要，解密消息
    ByteBuf decryptedPayload = decryptPayloadIfNeeded(messageId, msgMetadata, payload, cnx);
    //消息是否已解密
    boolean isMessageUndecryptable = isMessageUndecryptable(msgMetadata);
    //消息已丢弃或 CryptoKeyReader 接口没有实现
    if (decryptedPayload == null) {
        return;
    }

    // 解压已解密的消息，并且释放已解密的字节缓存
    ByteBuf uncompressedPayload = isMessageUndecryptable ? decryptedPayload.retain()
            : uncompressPayloadIfNeeded(messageId, msgMetadata, decryptedPayload, cnx);
    decryptedPayload.release();
    if (uncompressedPayload == null) {
        // 一旦消息解压错误，则丢弃
        return;
    }

    final int numMessages = msgMetadata.getNumMessagesInBatch();

    // 如果消息没有解密，它不能解析成一个批量消息，故增加加密上下文到消息，返回未加密的消息体
    if (isMessageUndecryptable || (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch())) {
        //构建一个消息
        final MessageImpl<T> message = new MessageImpl<>(topicName.toString(), msgId,  msgMetadata, uncompressedPayload, createEncryptionContext(msgMetadata), cnx, schema, redeliveryCount);
        uncompressedPayload.release();
        msgMetadata.recycle();

        lock.readLock().lock();
        try {
            // 消息入本地队列，当应用调用 receive() 方法时可以获取它。
            // 如果参数ReceiverQueueSize设置为0，那么如果没有人等待（消息到来），消息将丢弃
            // 如果调用asyncReceive方法等待，这将通知回调，不会把消息放入到本地队列

            //如果死信消息策略不为空，可能发送消息给死信 Topic 对应的容器不为空，且当前投递次数已经大于等于死信消息策略设置的最大投递次数，则把消息放入死信容器，等待发给死信对应的死信 Topic，接下来有小节会有详细分析死信消息的处理。
            if (deadLetterPolicy != null && possibleSendToDeadLetterTopicMessages != null && redeliveryCount >= deadLetterPolicy.getMaxRedeliverCount()) {
                possibleSendToDeadLetterTopicMessages.put((MessageIdImpl)message.getMessageId(), Collections.singletonList(message));
            }
            //如果正处理接收队列不为空（就是上面提到的等待消息到来的futrue）
            if (!pendingReceives.isEmpty()) {
                //跟踪消息未确认
                trackMessage(message);
                //通知正处理消息队列中的等待任务并执行回调
                notifyPendingReceivedCallback(message, null);
            } else if (canEnqueueMessage(message)) {
                //直接放入本地消息队列
                incomingMessages.add(message);
            }
        } finally {
            lock.readLock().unlock();
        }
    } else {
        // 处理批量消息入队列，批处理解压缩所有的消息
        receiveIndividualMessagesFromBatch(msgMetadata, redeliveryCount, uncompressedPayload, messageId, cnx);

        uncompressedPayload.release();
        msgMetadata.recycle();
    }

    //如果监听器被设置，则触发，注意，这里意味着拦截器先触发，监听器后触发
    if (listener != null) {
        triggerListener(numMessages);
    }
}

void notifyPendingReceivedCallback(final Message<T> message, Exception exception) {
    if (pendingReceives.isEmpty()) {
        return;
    }

    //拉取一个等待任务
    final CompletableFuture<Message<T>> receivedFuture = pendingReceives.poll();
    if (receivedFuture == null) {
        return;
    }

    if (exception != null) {
        listenerExecutor.execute(() -> receivedFuture.completeExceptionally(exception));
        return;
    }

    if (message == null) {
        IllegalStateException e = new IllegalStateException("received message can't be null");
        listenerExecutor.execute(() -> receivedFuture.completeExceptionally(e));
        return;
    }

    //如果队列大小设置为0，将立即调用拦截器和完成接收回调
    if (conf.getReceiverQueueSize() == 0) {
        interceptAndComplete(message, receivedFuture);
        return;
    }

    // 记录应用处理了一条消息的事件。并尝试发送一个 Flow 命令通知 broker 可以推送消息了
    messageProcessed(message);
    interceptAndComplete(message, receivedFuture);
}

private void interceptAndComplete(final Message<T> message, final CompletableFuture<Message<T>> receivedFuture) {
    // 调用拦截器
    final Message<T> interceptMessage = beforeConsume(message);
    //设置完成标志
    listenerExecutor.execute(() -> receivedFuture.complete(interceptMessage));
}

protected void triggerListener(int numMessages) {
    // 在一隔离的线程中触发消息监听器的通知，避免在消息处理发生时阻塞网络线程
    listenerExecutor.execute(() -> {
        for (int i = 0; i < numMessages; i++) {
            try {
                Message<T> msg = internalReceive(0, TimeUnit.MILLISECONDS);
                // 当消息为null时，则结束循环（意味着本轮触发完成，消息队列已经被清空）
                if (msg == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Message has been cleared from the queue", topic, subscription);
                    }
                    break;
                }
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{}] Calling message listener for message {}", topic, subscription,
                                msg.getMessageId());
                    }
                    listener.received(ConsumerImpl.this, msg);
                } catch (Throwable t) {
                    log.error("[{}][{}] Message listener error in processing message: {}", topic, subscription,
                            msg.getMessageId(), t);
                }

            } catch (PulsarClientException e) {
                log.warn("[{}] [{}] Failed to dequeue the message for listener", topic, subscription, e);
                return;
            }
        }
    });
}

//如果需要，则解密加密的消息
private ByteBuf decryptPayloadIfNeeded(MessageIdData messageId, MessageMetadata msgMetadata, ByteBuf payload,
        ClientCnx currentCnx) {

    if (msgMetadata.getEncryptionKeysCount() == 0) {
        return payload.retain();
    }

    // 如果密钥读取器没有配置，按照系统配置进行处理
    if (conf.getCryptoKeyReader() == null) {
        switch (conf.getCryptoFailureAction()) {
            case CONSUME: //即使不解密或解密失败，继续给应用消费
                log.warn("[{}][{}][{}] CryptoKeyReader interface is not implemented. Consuming encrypted message.",
                        topic, subscription, consumerName);
                return payload.retain();
            case DISCARD: //解密失败，丢弃当前消息，并通知broker推送消息
                log.warn(
                        "[{}][{}][{}] Skipping decryption since CryptoKeyReader interface is not implemented and config is set to discard",
                        topic, subscription, consumerName);
                discardMessage(messageId, currentCnx, ValidationError.DecryptionError);
                return null;
            case FAIL: //消息投递失败，忽略它，等待系统确认
                MessageId m = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), partitionIndex);
                log.error(
                        "[{}][{}][{}][{}] Message delivery failed since CryptoKeyReader interface is not implemented to consume encrypted message",
                            topic, subscription, consumerName, m);
                unAckedMessageTracker.add(m);
                return null;
        }
    }

    //解密消息
    ByteBuf decryptedData = this.msgCrypto.decrypt(msgMetadata, payload, conf.getCryptoKeyReader());
    // 如果解密成功，直接返回
    if (decryptedData != null) {
        return decryptedData;
    }

    //解密失败时，系统根据配置处理
    switch (conf.getCryptoFailureAction()) {
        case CONSUME:
            // 注意，即使配置（CryptoFailureAction）为消费选项，批量消息也会解密失败，这里往下传。
            log.warn("[{}][{}][{}][{}] Decryption failed. Consuming encrypted message since config is set to consume.",
                    topic, subscription, consumerName, messageId);
            return payload.retain();
        case DISCARD:
            //CryptoFailureAction配置为丢弃，如果解密失败，（意味着不会给上层应用），系统将自动确认，跳过这个消息
            log.warn("[{}][{}][{}][{}] Discarding message since decryption failed and config is set to discard", topic,
                    subscription, consumerName, messageId);
            discardMessage(messageId, currentCnx, ValidationError.DecryptionError);
            return null;
        case FAIL:
            //CryptoFailureAction配置为失败选项，不能解密消息，消息投递失败。
            MessageId m = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), partitionIndex);
            log.error(
                    "[{}][{}][{}][{}] Message delivery failed since unable to decrypt incoming message",
                        topic, subscription, consumerName, m);
            unAckedMessageTracker.add(m);
            return null;
    }
    return null;
}

// 如果需要，解压缩消息
private ByteBuf uncompressPayloadIfNeeded(MessageIdData messageId, MessageMetadata msgMetadata, ByteBuf payload,
        ClientCnx currentCnx) {
    // 压缩类型
    CompressionType compressionType = msgMetadata.getCompression();
    CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
    //未压缩前的消息体大小
    int uncompressedSize = msgMetadata.getUncompressedSize();
    //当前消息体大小
    int payloadSize = payload.readableBytes();
    //消息体是否大于最大消息尺寸大小
    if (payloadSize > PulsarDecoder.MaxMessageSize) {
        log.error("[{}][{}] Got corrupted payload message size {} at {}", topic, subscription, payloadSize,
                messageId);
        //丢弃此消息，并尝试通知 broker 推送消息
        discardCorruptedMessage(messageId, currentCnx, ValidationError.UncompressedSizeCorruption);
        return null;
    }

    try {
        //解压
        ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
        return uncompressedPayload;
    } catch (IOException e) {
        log.error("[{}][{}] Failed to decompress message with {} at {}: {}", topic, subscription, compressionType,
                messageId, e.getMessage(), e);
        //解压异常，丢弃此消息，并尝试通知 broker 推送消息
        discardCorruptedMessage(messageId, currentCnx, ValidationError.DecompressionError);
        return null;
    }
}

//验证校验和
private boolean verifyChecksum(ByteBuf headersAndPayload, MessageIdData messageId) {
    // 判定是否有校验和
    if (hasChecksum(headersAndPayload)) {
        //获取原始校验和
        int checksum = readChecksum(headersAndPayload);
        //重新计算校验和
        int computedChecksum = computeChecksum(headersAndPayload);
        //如果不匹配，则校验不通过
        if (checksum != computedChecksum) {
            log.error(
                    "[{}][{}] Checksum mismatch for message at {}:{}. Received checksum: 0x{}, Computed checksum: 0x{}",
                    topic, subscription, messageId.getLedgerId(), messageId.getEntryId(),
                    Long.toHexString(checksum), Integer.toHexString(computedChecksum));
            return false;
        }
    }

    return true;
}


void receiveIndividualMessagesFromBatch(MessageMetadata msgMetadata, int redeliveryCount, ByteBuf uncompressedPayload,
        MessageIdData messageId, ClientCnx cnx) {
    int batchSize = msgMetadata.getNumMessagesInBatch();

    MessageIdImpl batchMessage = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(),
            getPartitionIndex());
    // 创建批量消息确认器
    BatchMessageAcker acker = BatchMessageAcker.newAcker(batchSize);
    List<MessageImpl<T>> possibleToDeadLetter = null;
    // 死信消息策略，是否已达到投递次数上限
    if (deadLetterPolicy != null && redeliveryCount >= deadLetterPolicy.getMaxRedeliverCount()) {
        possibleToDeadLetter = new ArrayList<>();
    }
    int skippedMessages = 0;
    try {
        for (int i = 0; i < batchSize; ++i) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] processing message num - {} in batch", subscription, consumerName, i);
            }
            //创建一个单消息建造者
            PulsarApi.SingleMessageMetadata.Builder singleMessageMetadataBuilder = PulsarApi.SingleMessageMetadata
                    .newBuilder();
            //从批量消息缓存中分离出第（i+1）个消息
            ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                    singleMessageMetadataBuilder, i, batchSize);

            if (subscriptionMode == SubscriptionMode.NonDurable && startMessageId != null
                    && messageId.getLedgerId() == startMessageId.getLedgerId()
                    && messageId.getEntryId() == startMessageId.getEntryId()
                    && i <= startMessageId.getBatchIndex()) {
                // 如果消费进度非持久化、起始消息不为空、
                //当前消息ledgerId等于起始消息的ledgerID,其EntryId等于起始消息的EntryID，并且索引号小于等于批量索引，则跳过， TODO
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Ignoring message from before the startMessageId", subscription,
                            consumerName);
                }

                ++skippedMessages;
                continue;
            }
            // TODO
            if (singleMessageMetadataBuilder.getCompactedOut()) {
                singleMessagePayload.release();
                singleMessageMetadataBuilder.recycle();

                ++skippedMessages;
                continue;
            }

            BatchMessageIdImpl batchMessageIdImpl = new BatchMessageIdImpl(messageId.getLedgerId(),
                    messageId.getEntryId(), getPartitionIndex(), i, acker);
            // 构建当前消息
            final MessageImpl<T> message = new MessageImpl<>(topicName.toString(), batchMessageIdImpl,
                    msgMetadata, singleMessageMetadataBuilder.build(), singleMessagePayload,
                    createEncryptionContext(msgMetadata), cnx, schema, redeliveryCount);
            //可能放入死信消息容器不为空，则把当前消息放入
            if (possibleToDeadLetter != null) {
                possibleToDeadLetter.add(message);
            }
            lock.readLock().lock();
            try {
                //这里跟处理单个消息逻辑一致
                if (pendingReceives.isEmpty()) {
                    incomingMessages.add(message);
                } else {
                    notifyPendingReceivedCallback(message, null);
                }
            } finally {
                lock.readLock().unlock();
            }
            singleMessagePayload.release();
            singleMessageMetadataBuilder.recycle();
        }
    } catch (IOException e) {
        log.warn("[{}] [{}] unable to obtain message in batch", subscription, consumerName);
        discardCorruptedMessage(messageId, cnx, ValidationError.BatchDeSerializeError);
    }
    // 放入死信消息容器
    if (possibleToDeadLetter != null && possibleSendToDeadLetterTopicMessages != null){
        possibleSendToDeadLetterTopicMessages.put(batchMessage, possibleToDeadLetter);
    }

    if (log.isDebugEnabled()) {
        log.debug("[{}] [{}] enqueued messages in batch. queue size - {}, available queue size - {}", subscription,
                consumerName, incomingMessages.size(), incomingMessages.remainingCapacity());
    }

    //如果跳过了skippedMessages 个消息，则释放skippedMessages个可用许可
    //尝试通知broker，推送消息过来
    if (skippedMessages > 0) {
        increaseAvailablePermits(cnx, skippedMessages);
    }
}

```

到这里，已经详细了介绍了消费者如何接受消息，如何解压，解密消息，如何处理单、批量消息，并且如何把消息准备好，抛给上层应用，接下来，将详细介绍消费者的消息确认。

##### 5. ConsumerImpl 消息确认

消息确认，顾名思义就是上层应用已经把当前消息业务处理完毕了，发消息通知给 broker 服务器，表示此消息已经完成消费，不再需要推送给消费端了，内部实现其实是记录了消费偏移，Pulsar系统叫游标，解析 broker 时，将详细讲解，这里只是提一下。前面已经有提到消息确认接口，那么接下来，将详细介绍接口实现。

```java
// ConsumerBase 类实现了消息确认接口，但是具体功能实现是下放到各个实现类
@Override
public void acknowledge(Message<?> message) throws PulsarClientException {
    try {
        acknowledge(message.getMessageId());
    } catch (NullPointerException npe) {
        throw new PulsarClientException.InvalidMessageException(npe.getMessage());
    }
}

@Override
public void acknowledge(MessageId messageId) throws PulsarClientException {
    try {
        acknowledgeAsync(messageId).get();
    } catch (ExecutionException e) {
        Throwable t = e.getCause();
        if (t instanceof PulsarClientException) {
            throw (PulsarClientException) t;
        } else {
            throw new PulsarClientException(t);
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PulsarClientException(e);
    }
}

@Override
public void acknowledgeCumulative(Message<?> message) throws PulsarClientException {
    try {
        acknowledgeCumulative(message.getMessageId());
    } catch (NullPointerException npe) {
        throw new PulsarClientException.InvalidMessageException(npe.getMessage());
    }
}

@Override
public void acknowledgeCumulative(MessageId messageId) throws PulsarClientException {
    try {
        acknowledgeCumulativeAsync(messageId).get();
    } catch (ExecutionException e) {
        Throwable t = e.getCause();
        if (t instanceof PulsarClientException) {
            throw (PulsarClientException) t;
        } else {
            throw new PulsarClientException(t);
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PulsarClientException(e);
    }
}

@Override
public CompletableFuture<Void> acknowledgeAsync(Message<?> message) {
    try {
        return acknowledgeAsync(message.getMessageId());
    } catch (NullPointerException npe) {
        return FutureUtil.failedFuture(new PulsarClientException.InvalidMessageException(npe.getMessage()));
    }
}

@Override
public CompletableFuture<Void> acknowledgeCumulativeAsync(Message<?> message) {
    try {
        return acknowledgeCumulativeAsync(message.getMessageId());
    } catch (NullPointerException npe) {
        return FutureUtil.failedFuture(new PulsarClientException.InvalidMessageException(npe.getMessage()));
    }
}

@Override
public CompletableFuture<Void> acknowledgeAsync(MessageId messageId) {
    return doAcknowledge(messageId, AckType.Individual, Collections.emptyMap());
}

@Override
public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId) {
    if (!isCumulativeAcknowledgementAllowed(conf.getSubscriptionType())) {
        return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                "Cannot use cumulative acks on a non-exclusive subscription"));
    }
    return doAcknowledge(messageId, AckType.Cumulative, Collections.emptyMap());
}

//从以上接口可以看出，从功能上分实际上只有2类接口，一类是单个消息确认，一类是消息累计确认。来看看他们的共同实现doAcknowledge方法

// ConsumerImpl 的实现
@Override
protected CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType,
                                                Map<String,Long> properties) {
    checkArgument(messageId instanceof MessageIdImpl);
    //状态检查
    if (getState() != State.Ready && getState() != State.Connecting) {
        //消费者还未准备完毕
        stats.incrementNumAcksFailed();
        PulsarClientException exception = new PulsarClientException("Consumer not ready. State: " + getState());
        //回调拦截器异常
        if (AckType.Individual.equals(ackType)) {
            onAcknowledge(messageId, exception);
        } else if (AckType.Cumulative.equals(ackType)) {
            onAcknowledgeCumulative(messageId, exception);
        }
        return FutureUtil.failedFuture(exception);
    }

    //批量消息确认处理
    if (messageId instanceof BatchMessageIdImpl) {
        if (markAckForBatchMessage((BatchMessageIdImpl) messageId, ackType, properties)) {
            // 批量容器里所有消息已经被 broker确认通过sendAcknowledge()
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] acknowledging message - {}, acktype {}", subscription, consumerName, messageId,
                        ackType);
            }
        } else {
            // 批量容器中海油其他消息还在等待确认
            return CompletableFuture.completedFuture(null);
        }
    }
    //单消息确认处理
    return sendAcknowledge(messageId, ackType, properties);
}


//发送确认消息
private CompletableFuture<Void> sendAcknowledge(MessageId messageId, AckType ackType,
                                                    Map<String,Long> properties) {
    MessageIdImpl msgId = (MessageIdImpl) messageId;
    // 如果确认类型为单个确认
    if (ackType == AckType.Individual) {
        //如果当前消息ID为批量消息ID
        if (messageId instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;

            stats.incrementNumAcksSent(batchMessageId.getBatchSize());
            //移除未确认跟踪器中对应的消息
            unAckedMessageTracker.remove(new MessageIdImpl(batchMessageId.getLedgerId(),
                    batchMessageId.getEntryId(), batchMessageId.getPartitionIndex()));
            //移除死信队列容器中对应的消息
            if (possibleSendToDeadLetterTopicMessages != null) {
                possibleSendToDeadLetterTopicMessages.remove(new MessageIdImpl(batchMessageId.getLedgerId(),
                        batchMessageId.getEntryId(), batchMessageId.getPartitionIndex()));
            }
        } else {
            //单个消息处理，同上
            unAckedMessageTracker.remove(msgId);
            if (possibleSendToDeadLetterTopicMessages != null) {
                possibleSendToDeadLetterTopicMessages.remove(msgId);
            }
            stats.incrementNumAcksSent(1);
        }
        //执行单个消息确认拦截器
        onAcknowledge(messageId, null);
    } else if (ackType == AckType.Cumulative) {
        //执行累积消息确认拦截器
        onAcknowledgeCumulative(messageId, null);
        stats.incrementNumAcksSent(unAckedMessageTracker.removeMessagesTill(msgId));
    }
    //把未确认的放入确认组提交跟踪器（这个接口实现的持久化确认，会发送确认消息到 broker）
    acknowledgmentsGroupingTracker.addAcknowledgment(msgId, ackType, properties);

    //消费者确认操作将会立即成功，在任何情况，如果没有发送确认消息到 broker （也有网络原因），这个消息将被重新投递
    return CompletableFuture.completedFuture(null);
}

boolean markAckForBatchMessage(BatchMessageIdImpl batchMessageId, AckType ackType,
                                Map<String,Long> properties) {
    boolean isAllMsgsAcked;
    //单个确认
    if (ackType == AckType.Individual) {
        isAllMsgsAcked = batchMessageId.ackIndividual();
    } else { // 累计确认
        isAllMsgsAcked = batchMessageId.ackCumulative();
    }
    int outstandingAcks = 0;
    if (log.isDebugEnabled()) {
        outstandingAcks = batchMessageId.getOutstandingAcksInSameBatch();
    }

    int batchSize = batchMessageId.getBatchSize();
    // 批量容器里所有消息是否已经全部确认
    if (isAllMsgsAcked) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] can ack message to broker {}, acktype {}, cardinality {}, length {}", subscription,
                    consumerName, batchMessageId, ackType, outstandingAcks, batchSize);
        }
        //如果是的，返回true
        return true;
    } else {
        //如果确认类型为累计确认，且前一个批量累计确认还没有完成
        if (AckType.Cumulative == ackType
            && !batchMessageId.getAcker().isPrevBatchCumulativelyAcked()) {
            //把前一个批量消息给累计确认了
            sendAcknowledge(batchMessageId.prevBatchMessageId(), AckType.Cumulative, properties);
            //确认完后，设置前一个批量消息已确认
            batchMessageId.getAcker().setPrevBatchCumulativelyAcked(true);
        } else {
            //前一个批量累计已确认，立即执行当前批量消息确认
            onAcknowledge(batchMessageId, null);
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] cannot ack message to broker {}, acktype {}, pending acks - {}", subscription,
                    consumerName, batchMessageId, ackType, outstandingAcks);
        }
    }
    return false;
}

//PersistentAcknowledgmentsGroupingTracker 类的具体实现

public void addAcknowledgment(MessageIdImpl msgId, AckType ackType, Map<String, Long> properties) {
    if (acknowledgementGroupTimeMicros == 0 || !properties.isEmpty()) {
        // 如果延迟为0或者有属性关联到当前消息，就不能组确认提交，将立即发送确认命令给Broker。幸运的是，这是一个不常见的情况，因为它仅用于压缩订阅。
        doImmediateAck(msgId, ackType, properties);
    } else if (ackType == AckType.Cumulative) {
        doCumulativeAck(msgId);
    } else {
        // 单个确认，把消息ID放入正处理单个确认队列
        pendingIndividualAcks.add(msgId);
        //如果组提交个数超过配置数，就主动刷新
        if (pendingIndividualAcks.size() >= MAX_ACK_GROUP_SIZE) {
            flush();
        }
    }
}

//累计确认
private void doCumulativeAck(MessageIdImpl msgId) {
    // 并发处理来自不同线程的更新
    while (true) {
        //最后累计确认消息ID
        MessageIdImpl lastCumlativeAck = this.lastCumulativeAck;
        if (msgId.compareTo(lastCumlativeAck) > 0) {
            if (LAST_CUMULATIVE_ACK_UPDATER.compareAndSet(this, lastCumlativeAck, msgId)) {
                // 成功更新了最后一次累积确认。 说明当前消息是新的累积确认消息（也就是说可以发送确认命令）
                cumulativeAckFulshRequired = true;
                return;
            }
        } else {
            // 消息ID已完成最后一次确认（也就是之前累计的消息都已确认完毕）
            return;
        }
    }
}

// 单个确认
private boolean doImmediateAck(MessageIdImpl msgId, AckType ackType, Map<String, Long> properties) {
    ClientCnx cnx = consumer.getClientCnx();

    if (cnx == null) {
        return false;
    }
    // 组成单个确认请求
    final ByteBuf cmd = Commands.newAck(consumer.consumerId, msgId.getLedgerId(), msgId.getEntryId(), ackType, null,
            properties);

    cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
    return true;
}

/**
 * 刷新所有正（准备）确认的消息发送到 broker
 */
public void flush() {
    if (log.isDebugEnabled()) {
        log.debug("[{}] Flushing pending acks to broker: last-cumulative-ack: {} -- individual-acks: {}", consumer,
                lastCumulativeAck, pendingIndividualAcks);
    }

    ClientCnx cnx = consumer.getClientCnx();

    if (cnx == null) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Cannot flush pending acks since we're not connected to broker", consumer);
        }
        return;
    }

    //累积确认刷新请求标志
    if (cumulativeAckFulshRequired) {
        //这里发送最后累积消息确认命令到broker
        ByteBuf cmd = Commands.newAck(consumer.consumerId, lastCumulativeAck.ledgerId, lastCumulativeAck.entryId,
                AckType.Cumulative, null, Collections.emptyMap());
        cnx.ctx().write(cmd, cnx.ctx().voidPromise());
        cumulativeAckFulshRequired = false;
    }

    // 刷新所有的单个消息确认
    if (!pendingIndividualAcks.isEmpty()) {
        //检查broker是否支持多消息确认协议
        if (Commands.peerSupportsMultiMessageAcknowledgment(cnx.getRemoteEndpointProtocolVersion())) {
            // 打包所有的单个消息确认放入一个确认命令
            List<Pair<Long, Long>> entriesToAck = new ArrayList<>(pendingIndividualAcks.size());
            while (true) {
                MessageIdImpl msgId = pendingIndividualAcks.pollFirst();
                if (msgId == null) {
                    break;
                }

                entriesToAck.add(Pair.of(msgId.getLedgerId(), msgId.getEntryId()));
            }
            //发送
            cnx.ctx().write(Commands.newMultiMessageAck(consumer.consumerId, entriesToAck),
                    cnx.ctx().voidPromise());
        } else {
            // 如果与较老版本的 broker 通信，那只能单个消息依次发送（意味着传输更大的包）
            while (true) {
                MessageIdImpl msgId = pendingIndividualAcks.pollFirst();
                if (msgId == null) {
                    break;
                }

                cnx.ctx().write(Commands.newAck(consumer.consumerId, msgId.getLedgerId(), msgId.getEntryId(),
                        AckType.Individual, null, Collections.emptyMap()), cnx.ctx().voidPromise());
            }
        }
    }
    //刷新缓冲区，发送消息
    cnx.ctx().flush();
}

```

本小节，主要讲消息被上层应用处理后，要向 broker 确认已表示此消息已被上层应用成功消费。这里为了提高确认性能，有多个地方优化：批量确认，累积确认，批量和累积确认一起发送等手段。大大增加了效率和减轻 broker 的压力。

##### 6. ComsumerImpl 消费超时（重试）与死信队列

跟踪消息超时组件其实在上一小节已经出现了，那就是未确认消息跟踪器——**UnAckedMessageTracker**，每个消息接收后首先都要放入此容器里，然后其监控是否消费超时，如果超时，则移除并重新通知 broker 推送过来，而且还会跟踪消息未消费次数，如果超过次数，则会放入死信队列，然后发送系统设置的死信Topic ，另外此消息会被系统确认“消费”（意味着不再推送重新消费了，要消息只能到死信 Topic 里取）。

```java
// UnAckedMessageTracker 的属性
//消息ID-消息集（为了更好的针对消息ID索引）
protected final ConcurrentHashMap<MessageId, ConcurrentOpenHashSet<MessageId>> messageIdPartitionMap;
//双向列表 用于时间分区
protected final LinkedList<ConcurrentOpenHashSet<MessageId>> timePartitions;
//读写锁
protected final Lock readLock;
protected final Lock writeLock;
//UnAckedMessageTracker 的空实现，用于非持久化消息组件
public static final UnAckedMessageTrackerDisabled UNACKED_MESSAGE_TRACKER_DISABLED = new UnAckedMessageTrackerDisabled();
//确认超时时间
private final long ackTimeoutMillis;
//滴答时间（或者叫单位时间）
private final long tickDurationInMs;

// UnAckedMessageTracker 的构造方法
public UnAckedMessageTracker(PulsarClientImpl client, ConsumerBase<?> consumerBase, long ackTimeoutMillis, long tickDurationInMs) {
    //滴答时间必须大于0，并且确认超时时间大于等于滴答时间
    Preconditions.checkArgument(tickDurationInMs > 0 && ackTimeoutMillis >= tickDurationInMs);
    this.ackTimeoutMillis = ackTimeoutMillis;
    this.tickDurationInMs = tickDurationInMs;
    //读写锁
    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
    //这里主要用于存储消息ID和消息的映射
    this.messageIdPartitionMap = new ConcurrentHashMap<>();
    //从头部移除，尾部追加数据
    this.timePartitions = new LinkedList<>();
    //设置一个时间分区，假设确认超时时间为11000ms，滴答时间为2000ms，此时计算结果为5
    int blankPartitions = (int)Math.ceil((double)this.ackTimeoutMillis / this.tickDurationInMs);
    //创建6个时间分区容器
    for (int i = 0; i < blankPartitions + 1; i++) {
        timePartitions.add(new ConcurrentOpenHashSet<>());
    }

    //新增一个定时器任务，tickDurationInMs 为轮询间隔，也就是这里会处理消息消费超时，并通知 broker  进行重新投递 ，这里 UnAckedMessageTracker 最核心的处理
    timeout = client.timer().newTimeout(new TimerTask() {
        @Override
        public void run(Timeout t) throws Exception {
            Set<MessageId> messageIds = new HashSet<>();
            writeLock.lock();
            try {
                //新创建一个并发哈希容器，并把它追加在时间分区链表尾部，这样就确保新增了，新增的消息将都放在这尾部，然后将移除头部容器，这样始终保持创建时的个数，且有效避免了竞争关系
                timePartitions.addLast(new ConcurrentOpenHashSet<>());
                ConcurrentOpenHashSet<MessageId> headPartition = timePartitions.removeFirst();
                //如果容器内有消息，此时就认为过期了，把过期消息移除，并保留下来，为接下来通知 broker 重发这些消息打下基础
                if (!headPartition.isEmpty()) {
                    log.warn("[{}] {} messages have timed-out", consumerBase, timePartitions.size());
                    headPartition.forEach(messageId -> {
                        messageIds.add(messageId);
                        messageIdPartitionMap.remove(messageId);
                    });
                }
            } finally {
                writeLock.unlock();
            }
            //如果有过期消息，则通知 broker 重新投递消息
            if (messageIds.size() > 0) {
                consumerBase.redeliverUnacknowledgedMessages(messageIds);
            }
            //准备下一次任务定时
            timeout = client.timer().newTimeout(this, tickDurationInMs, TimeUnit.MILLISECONDS);
        }
    }, this.tickDurationInMs, TimeUnit.MILLISECONDS);
}

// UnAckedMessageTracker 添加消息
public boolean add(MessageId messageId) {
    writeLock.lock();
    try {
        //从尾部获取一个哈希set，并把它放入message map，再把消息放入哈希set
        ConcurrentOpenHashSet<MessageId> partition = timePartitions.peekLast();
        messageIdPartitionMap.put(messageId, partition);
        return partition.add(messageId);
    } finally {
        writeLock.unlock();
    }
}

// UnAckedMessageTracker 清空容器
public void clear() {
    writeLock.lock();
    try {
        messageIdPartitionMap.clear();
        timePartitions.clear();
        int blankPartitions = (int)Math.ceil((double)ackTimeoutMillis / tickDurationInMs);
        for (int i = 0; i < blankPartitions + 1; i++) {
            timePartitions.add(new ConcurrentOpenHashSet<>());
        }
    } finally {
        writeLock.unlock();
    }
}

// UnAckedMessageTracker 是否为空
boolean isEmpty() {
    readLock.lock();
    try {
        return messageIdPartitionMap.isEmpty();
    } finally {
        readLock.unlock();
    }
}

// UnAckedMessageTracker 移除某消息ID
public boolean remove(MessageId messageId) {
    writeLock.lock();
    try {
        boolean removed = false;
        ConcurrentOpenHashSet<MessageId> exist = messageIdPartitionMap.remove(messageId);
        if (exist != null) {
            removed = exist.remove(messageId);
        }
        return removed;
    } finally {
        writeLock.unlock();
    }
}

//移除比MessageId小的所有消息
public int removeMessagesTill(MessageId msgId) {
    writeLock.lock();
    try {
        int removed = 0;
        Iterator<MessageId> iterator = messageIdPartitionMap.keySet().iterator();
        while (iterator.hasNext()) {
            MessageId messageId = iterator.next();
            if (messageId.compareTo(msgId) <= 0) {
                ConcurrentOpenHashSet<MessageId> exist = messageIdPartitionMap.get(messageId);
                if (exist != null) {
                    exist.remove(messageId);
                }
                iterator.remove();
                removed ++;
            }
        }
        return removed;
    } finally {
        writeLock.unlock();
    }
}

// UnAckedMessageTracker 的核心方法

在构造方法，已经指定了核心处理逻辑，调用的核心方法其实是 ConsumerBase 类的抽象方法 redeliverUnacknowledgedMessages ，在 ConsumerImpl 类中就具体实现，下面看看它，如下：

// 主要两件事 处理过期消息，处理死信消息
@Override
public void redeliverUnacknowledgedMessages(Set<MessageId> messageIds) {
    checkArgument(messageIds.stream().findFirst().get() instanceof MessageIdImpl);

    if (conf.getSubscriptionType() != SubscriptionType.Shared) {
        // 如果订阅类型不是共享类型，则不重发单个消息
        redeliverUnacknowledgedMessages();
        return;
    }
    ClientCnx cnx = cnx();
    //判定是否已连接和版本是否支持
    if (isConnected() && cnx.getRemoteEndpointProtocolVersion() >= ProtocolVersion.v2.getNumber()) {
        int messagesFromQueue = removeExpiredMessagesFromQueue(messageIds);
        Iterable<List<MessageIdImpl>> batches = Iterables.partition(
            messageIds.stream()
                .map(messageId -> (MessageIdImpl)messageId)
                //限制当前次处理的未确认消息数
                .collect(Collectors.toSet()), MAX_REDELIVER_UNACKNOWLEDGED);
        MessageIdData.Builder builder = MessageIdData.newBuilder();
        batches.forEach(ids -> {
            List<MessageIdData> messageIdDatas = ids.stream().map(messageId -> {
                //这里就处理死信消息了
                processPossibleToDLQ(messageId);
                // 尝试从 batchMessageAckTracker 移除对应的消息
                builder.setPartition(messageId.getPartitionIndex());
                builder.setLedgerId(messageId.getLedgerId());
                builder.setEntryId(messageId.getEntryId());
                return builder.build();
            }).collect(Collectors.toList());
            //这里就是发命令通知 broker 重发本次未确认消息
            ByteBuf cmd = Commands.newRedeliverUnacknowledgedMessages(consumerId, messageIdDatas);
            cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
            messageIdDatas.forEach(MessageIdData::recycle);
        });
        //增加可用许可
        if (messagesFromQueue > 0) {
            increaseAvailablePermits(cnx, messagesFromQueue);
        }
        builder.recycle();
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] [{}] Redeliver unacked messages and increase {} permits", subscription, topic,
                    consumerName, messagesFromQueue);
        }
        return;
    }
    //如果客户端还没连接或正在连接，则忽略
    if (cnx == null || (getState() == State.Connecting)) {
        log.warn("[{}] Client Connection needs to be established for redelivery of unacknowledged messages", this);
    } else {
        //这里重连？？
        log.warn("[{}] Reconnecting the client to redeliver the messages.", this);
        cnx.ctx().close();
    }
}

//从队列移除过期消息，并返回消息数
private int removeExpiredMessagesFromQueue(Set<MessageId> messageIds) {
    int messagesFromQueue = 0;
    //从队列头部检索一个消息，但不移除
    Message<T> peek = incomingMessages.peek();
    if (peek != null) {
        MessageIdImpl messageId = getMessageIdImpl(peek);
        if (!messageIds.contains(messageId)) {
            // 如果第一个消息没过期，那么当前整个消息队列就没有消息过期
            return 0;
        }

        //开始循环移除那些过期的消息
        Message<T> message = incomingMessages.poll();
        while (message != null) {
            messagesFromQueue++;
            MessageIdImpl id = getMessageIdImpl(message);
            //当前消息可能还没过期，但是已经拿出来了，故还是要放入过期容器，然后跳出循环
            if (!messageIds.contains(id)) {
                messageIds.add(id);
                break;
            }
            message = incomingMessages.poll();
        }
    }
    return messagesFromQueue;
}

// 处理死信消息
private void processPossibleToDLQ(MessageIdImpl messageId) {
    List<MessageImpl<T>> deadLetterMessages = null;
    //如果死信消息容器不为空
    if (possibleSendToDeadLetterTopicMessages != null) {
        if (messageId instanceof BatchMessageIdImpl) {
            deadLetterMessages = possibleSendToDeadLetterTopicMessages.get(new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(),
                    getPartitionIndex()));
        } else {
            deadLetterMessages = possibleSendToDeadLetterTopicMessages.get(messageId);
        }
    }
    //通过消息ID拿到对应的消息
    if (deadLetterMessages != null) {
        //如果死信消息生产者为空，则创建
        if (deadLetterProducer == null) {
            try {
                deadLetterProducer = client.newProducer(schema)
                        .topic(this.deadLetterPolicy.getDeadLetterTopic())
                        .blockIfQueueFull(false)
                        .create();
            } catch (Exception e) {
                log.error("Create dead letter producer exception with topic: {}", deadLetterPolicy.getDeadLetterTopic(), e);
            }
        }
        if (deadLetterProducer != null) {
            try {
                //把死信消息发送到指定 死信Topic 中。
                for (MessageImpl<T> message : deadLetterMessages) {
                    deadLetterProducer.newMessage()
                            .value(message.getValue())
                            .properties(message.getProperties())
                            .send();
                }
                //把死信消息确认，意味着将不再消费（已放死信Topic上了，需要进入异常处理）
                acknowledge(messageId);
            } catch (Exception e) {
                log.error("Send to dead letter topic exception with topic: {}, messageId: {}", deadLetterProducer.getTopic(), messageId, e);
            }
        }
    }
}
```

到这里，已经把消息的消费超时和死信消息的处理都一一分析完了。接下来，将解析 Pulsar 消费者特有的功能——根据指定消息ID重置订阅进度。

##### 7. ComsumerImpl 重置订阅

根据给定的消息ID（当前 Topic 最早的消息ID和最新的消息ID之间都可以）重置订阅进度，意味着只要允许范围，可以任意跳到某个消息ID里，重新消费。下面看看接口实现，如下：

```java
//照例，同步实现调用异步接口
@Override
public void seek(MessageId messageId) throws PulsarClientException {
    try {
        seekAsync(messageId).get();
    } catch (ExecutionException | InterruptedException e) {
        throw new PulsarClientException(e);
    }
}

@Override
public CompletableFuture<Void> seekAsync(MessageId messageId) {
    //如果消费者状态为正关闭或已关闭，则抛异常
    if (getState() == State.Closing || getState() == State.Closed) {
        return FutureUtil
                .failedFuture(new PulsarClientException.AlreadyClosedException("Consumer was already closed"));
    }

    //当前消费者未连接，抛异常
    if (!isConnected()) {
        return FutureUtil.failedFuture(new PulsarClientException("Not connected to broker"));
    }

    final CompletableFuture<Void> seekFuture = new CompletableFuture<>();

    long requestId = client.newRequestId();
    MessageIdImpl msgId = (MessageIdImpl) messageId;
    //构建重置消费命令
    ByteBuf seek = Commands.newSeek(consumerId, requestId, msgId.getLedgerId(), msgId.getEntryId());
    ClientCnx cnx = cnx();

    log.info("[{}][{}] Seek subscription to message id {}", topic, subscription, messageId);
    //发送重置消息命令到 broker
    cnx.sendRequestWithId(seek, requestId).thenRun(() -> {
        log.info("[{}][{}] Successfully reset subscription to message id {}", topic, subscription, messageId);
        seekFuture.complete(null);
    }).exceptionally(e -> {
        log.error("[{}][{}] Failed to reset subscription: {}", topic, subscription, e.getCause().getMessage());
        seekFuture.completeExceptionally(e.getCause());
        return null;
    });
    return seekFuture;
}

```

消费者端实现较为简单，就发送重置命令到 broker，接下来就等着接收历史消息了。

##### 8. ComsumerImpl 暂停与恢复

顾名思义，就是暂停当前消费者接收消息，除非调用

```java
// 1. volatile 类型，保证可见性
protected volatile boolean paused;

// 2. 暂停调用即把 paused 设置为true
@Override
public void pause() {
    paused = true;
}

// 3. 恢复调用，并尝试恢复消息接收
@Override
public void resume() {
    if (paused) {
        paused = false;
        increaseAvailablePermits(cnx(), 0);
    }
}

// 4. 设置许可的时候，paused 变量决定是否拿到可用许可（意味着是否通知 broker 推送新消息到消费端）
private void increaseAvailablePermits(ClientCnx currentCnx, int delta) {
    int available = AVAILABLE_PERMITS_UPDATER.addAndGet(this, delta);

    while (available >= receiverQueueRefillThreshold && !paused) {
        if (AVAILABLE_PERMITS_UPDATER.compareAndSet(this, available, 0)) {
            sendFlowPermitsToBroker(currentCnx, available);
            break;
        } else {
            available = AVAILABLE_PERMITS_UPDATER.get(this);
        }
    }
}

```

消费端的暂停恢复逻辑实现也较为简单，这得益于 broker 和 consumer 的推送机制。接下来，将介绍消费者取消订阅功能。

##### 9. ComsumerImpl 取消订阅

当消费者不再想接收消息——取消订阅。
在 ConsumerBase 类中，有实现其同步接口，以及还有个抽象接口,由 ConsumerImpl 类实现，其实现模式跟大部分同步和异步方法类似，即同步简单的调用异步接口。如下：

```java
//ConsumerBase 类中实现
@Override
public void unsubscribe() throws PulsarClientException {
    try {
        unsubscribeAsync().get();
    } catch (ExecutionException e) {
        Throwable t = e.getCause();
        if (t instanceof PulsarClientException) {
            throw (PulsarClientException) t;
        } else {
            throw new PulsarClientException(t);
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PulsarClientException(e);
    }
}

// ConsumerImpl 类实现取消订阅
@Override
public CompletableFuture<Void> unsubscribeAsync() {
    //检查消费者当前状态，如果正在关闭或已关闭状态，直接抛异常
    if (getState() == State.Closing || getState() == State.Closed) {
        return FutureUtil
                .failedFuture(new PulsarClientException.AlreadyClosedException("Consumer was already closed"));
    }
    final CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();
    //检查消费者当前是否已连接
    if (isConnected()) {
        //设置正在关闭状态
        setState(State.Closing);
        long requestId = client.newRequestId();
        //构造取消订阅命令
        ByteBuf unsubscribe = Commands.newUnsubscribe(consumerId, requestId);
        ClientCnx cnx = cnx();
        cnx.sendRequestWithId(unsubscribe, requestId).thenRun(() -> {
            //broker 成功应答，移除连接池中的 当前消费者
            cnx.removeConsumer(consumerId);
            //关闭未确认消息跟踪器
            unAckedMessageTracker.close();
            //如果有死信消息容器，则清空
            if (possibleSendToDeadLetterTopicMessages != null) {
                possibleSendToDeadLetterTopicMessages.clear();
            }
            //清理客户端中消费者信息
            client.cleanupConsumer(ConsumerImpl.this);
            log.info("[{}][{}] Successfully unsubscribed from topic", topic, subscription);
            //设置消费者已关闭标志，unsubscribeFuture 标志完成
            setState(State.Closed);
            unsubscribeFuture.complete(null);
        }).exceptionally(e -> {
            //取消订阅异常
            log.error("[{}][{}] Failed to unsubscribe: {}", topic, subscription, e.getCause().getMessage());
            setState(State.Ready);
            unsubscribeFuture.completeExceptionally(e.getCause());
            return null;
        });
    } else {
        //设置没有连接 broker 异常
        unsubscribeFuture.completeExceptionally(new PulsarClientException("Not connected to broker"));
    }
    return unsubscribeFuture;
}

```

##### 10. ComsumerImpl 关闭

实现消费者最后一个功能接口——关闭消费者。同步和异步关闭接口，基本上跟取消订阅接口实现方式一致，这里就不再具体分析。那前面的取消订阅接口其功能最终也是关闭消费者，那么它们有什么不同？来，解析一下。

```java

// ConsumerBase 类中，实现同步关闭接口实际上调用的是异步接口
@Override
public void close() throws PulsarClientException {
    try {
        closeAsync().get();
    } catch (ExecutionException e) {
        Throwable t = e.getCause();
        if (t instanceof PulsarClientException) {
            throw (PulsarClientException) t;
        } else {
            throw new PulsarClientException(t);
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PulsarClientException(e);
    }
}

// ConsumberImpl 类中，实现的异步关闭接口
@Override
public CompletableFuture<Void> closeAsync() {
    //减少一引用计数，并判断引用是否已经为0，如果为0，返回true，否则返回false（也就是说引用数不为0，可以关闭）
    if (!shouldTearDown()) {
        return CompletableFuture.completedFuture(null);
    }

    //如果消费者状态是正在关闭或已关闭，则关闭未确认消息跟踪器，并且清空死信消息队列
    if (getState() == State.Closing || getState() == State.Closed) {
        unAckedMessageTracker.close();
        if (possibleSendToDeadLetterTopicMessages != null) {
            possibleSendToDeadLetterTopicMessages.clear();
        }
        return CompletableFuture.completedFuture(null);
    }

    //如果 与 broker不再连接了，则设置消费者为已关闭状态，并关闭未确认消息跟踪器，清空死信消息队列，客户端清理消费者信息
    if (!isConnected()) {
        log.info("[{}] [{}] Closed Consumer (not connected)", topic, subscription);
        setState(State.Closed);
        unAckedMessageTracker.close();
        if (possibleSendToDeadLetterTopicMessages != null) {
            possibleSendToDeadLetterTopicMessages.clear();
        }
        client.cleanupConsumer(this);
        return CompletableFuture.completedFuture(null);
    }

    //如果状态收集定时器被设置，则取消
    stats.getStatTimeout().ifPresent(Timeout::cancel);
    //设置为正在关闭状态
    setState(State.Closing);
    //消息确认组跟踪器关闭（主要取消定时器和把当前队列要确认消息发送出去）
    acknowledgmentsGroupingTracker.close();

    long requestId = client.newRequestId();

    CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    ClientCnx cnx = cnx();
    //如果客户端连接已释放
    if (null == cnx) {
        cleanupAtClose(closeFuture);
    } else {
        //发送关闭消费者命令到 broker
        ByteBuf cmd = Commands.newCloseConsumer(consumerId, requestId);
        cnx.sendRequestWithId(cmd, requestId).handle((v, exception) -> {
            //移除消费者信息
            cnx.removeConsumer(consumerId);
            if (exception == null || !cnx.ctx().channel().isActive()) {
                //成功关闭连接
                cleanupAtClose(closeFuture);
            } else {
                //异常关闭，返回
                closeFuture.completeExceptionally(exception);
            }
            return null;
        });
    }

    return closeFuture;
}

protected synchronized boolean shouldTearDown() {
    return refCount > 0 ? refCount-- == 0 : refCount == 0;
}

//消费者关闭时，清理
private void cleanupAtClose(CompletableFuture<Void> closeFuture) {
    log.info("[{}] [{}] Closed consumer", topic, subscription);
    setState(State.Closed);
    unAckedMessageTracker.close();
    if (possibleSendToDeadLetterTopicMessages != null) {
        possibleSendToDeadLetterTopicMessages.clear();
    }
    closeFuture.complete(null);
    client.cleanupConsumer(this);
    // 通知应用，所有正接收的消息全部设置（状态）失败
    failPendingReceive();
}

//如果监听器还没关闭，则拉取本地消息队列中消息，依次设置消费者已关闭异常
private void failPendingReceive() {
    lock.readLock().lock();
    try {
        if (listenerExecutor != null && !listenerExecutor.isShutdown()) {
            while (!pendingReceives.isEmpty()) {
                CompletableFuture<Message<T>> receiveFuture = pendingReceives.poll();
                if (receiveFuture != null) {
                    receiveFuture.completeExceptionally(
                            new PulsarClientException.AlreadyClosedException("Consumer is already closed"));
                } else {
                    break;
                }
            }
        }
    } finally {
        lock.readLock().unlock();
    }
}

```

由此可见，取消订阅和关闭消费者有共同点：关闭未确认消息跟踪器和清空死信队列，都会清理连接池和客户端消费者容器中的本身的信息，并且关闭当前消费者。不同点很明显：取消订阅只是发取消订阅命令给broker，而关闭消费者会发送关闭消费者命令，把当前本地接收队列中消息全部置为消费者已关闭异常，且会关闭与 broker 的连接。

### 2. MultiTopicsConsumerImpl 实现解析

到此为止，消费者的全功能接口都已被 ConsumerImpl  实现。但是在前面的章节，消费者根据底层对 Topic个数支持实际上有3个实现，前面的章节已经解析了 ConsumerImpl 实现，现在讲解析 MultiTopicsConsumerImpl 类对 Consumer 接口的实现。

#### 1. 类继承图,如下

![MultiTopicsConsumerImpl 类继承图](images/20190429201150.png)

基本上，跟 ConsumerImpl 类继承结构类似，只是少了继承 Connection 接口，因为实际上 MultiTopicsConsumerImpl 的实现订阅时调用 ConsumerImpl 的订阅接口 ， 屏蔽了连接相关操作 ，故不再需要继承 Connection 接口。

#### 2. MultiTopicsConsumerImpl 的属性

```java
//保存 Namespace 信息，因为即使多 Topic 并不能跨 Namespace 消费
protected NamespaceName namespaceName;

// Map <topic+partition, consumer>，当消息确认时，消费者通过 Topic 名来查找消费者
private final ConcurrentHashMap<String, ConsumerImpl<T>> consumers;

// Map <topic, numPartitions> 存储每个 Topic 分区的数量
protected final ConcurrentHashMap<String, Integer> topics;

// 分区消费者队列：当调用receiveAsync()方法时因共享消息队列满了被阻塞了
private final ConcurrentLinkedQueue<ConsumerImpl<T>> pausedConsumers;

// 共享队列的阈值，当共享队列的大小低于这阈值时，我们将恢复被暂停的分区消费者接收消息
private final int sharedQueueResumeThreshold;

// Topic 分区个数总数，简单的 Topic 为1，分区的 Topic 等于分区个数
AtomicInteger allTopicPartitionsNumber;

// 定时器关联自动检测和订阅新增的分区
private volatile Timeout partitionsAutoUpdateTimeout = null;
// Topic 分区变更监听器
TopicsPartitionChangedListener topicsPartitionChangedListener;
CompletableFuture<Void> partitionsAutoUpdateFuture = null;

//读写锁用于保证 pausedConsumers 中的消费者数是准确的
private final ReadWriteLock lock = new ReentrantReadWriteLock();
private final ConsumerStatsRecorder stats;
// 未确认消息跟踪器
private final UnAckedMessageTracker unAckedMessageTracker;
private final ConsumerConfigurationData<T> internalConfig;

```

#### 3. MultiTopicsConsumerImpl 的构造方法

```java
// 很多组件跟之前的 ConsumerImpl 类似，相同的组件就不再作介绍
MultiTopicsConsumerImpl(PulsarClientImpl client, ConsumerConfigurationData<T> conf, ExecutorService listenerExecutor,
                        CompletableFuture<Consumer<T>> subscribeFuture, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
    // ConsumerBase 构造方法
    super(client, "TopicsConsumerFakeTopicName" + ConsumerName.generateRandomName(), conf, Math.max(2, conf.getReceiverQueueSize()), listenerExecutor, subscribeFuture, schema, interceptors);

    checkArgument(conf.getReceiverQueueSize() > 0,
        "Receiver queue size needs to be greater than 0 for Topics Consumer");

    this.topics = new ConcurrentHashMap<>();
    this.consumers = new ConcurrentHashMap<>();
    this.pausedConsumers = new ConcurrentLinkedQueue<>();
    this.sharedQueueResumeThreshold = maxReceiverQueueSize / 2;
    this.allTopicPartitionsNumber = new AtomicInteger(0);
    //未确认消息超时时间，这里讲在下面章节具体讲，其实在 ConsumerImpl 已经接触过了，只是略有不同
    if (conf.getAckTimeoutMillis() != 0) {
        if (conf.getTickDurationMillis() > 0) {
            this.unAckedMessageTracker = new UnAckedTopicMessageTracker(client, this, conf.getAckTimeoutMillis(), conf.getTickDurationMillis());
        } else {
            this.unAckedMessageTracker = new UnAckedTopicMessageTracker(client, this, conf.getAckTimeoutMillis());
        }
    } else {
        this.unAckedMessageTracker = UnAckedMessageTracker.UNACKED_MESSAGE_TRACKER_DISABLED;
    }

    this.internalConfig = getInternalConsumerConfig();
    this.stats = client.getConfiguration().getStatsIntervalSeconds() > 0 ? new ConsumerStatsRecorderImpl() : null;

    // 启动跟踪和自动订阅 Topic 分区增长
    if (conf.isAutoUpdatePartitions()) {
        topicsPartitionChangedListener = new TopicsPartitionChangedListener();
        partitionsAutoUpdateTimeout = client.timer()
            .newTimeout(partitionsAutoUpdateTimerTask, 1, TimeUnit.MINUTES);
    }

    //如果没有配置 Topics 信息，则立即返回（调用订阅方法时会指定 Topic 信息）
    if (conf.getTopicNames().isEmpty()) {
        this.namespaceName = null;
        setState(State.Ready);
        subscribeFuture().complete(MultiTopicsConsumerImpl.this);
        return;
    }

    //多 Topic 订阅，他们的 Namespace 必须相同，这是系统隔离设计所要求的
    checkArgument(conf.getTopicNames().isEmpty() || topicNamesValid(conf.getTopicNames()), "Topics should have same namespace.");
    this.namespaceName = conf.getTopicNames().stream().findFirst()
            .flatMap(s -> Optional.of(TopicName.get(s).getNamespaceObject())).get();

    List<CompletableFuture<Void>> futures = conf.getTopicNames().stream().map
    (this::subscribeAsync)//依次调用异步订阅 Topic
            .collect(Collectors.toList());
    FutureUtil.waitForAll(futures) //等待所有订阅完成
        .thenAccept(finalFuture -> {
            //如果 Topic 总分区数大于最大接收队列大小，则调整最大接收队列大小为总分区数
            if (allTopicPartitionsNumber.get() > maxReceiverQueueSize) {
                setMaxReceiverQueueSize(allTopicPartitionsNumber.get());
            }
            //设置状态为准备
            setState(State.Ready)
            // 已经成功创建 N 个消费者，所以现在开始接收消息
            startReceivingMessages(new ArrayList<>(consumers.values()));
            log.info("[{}] [{}] Created topics consumer with {} sub-consumers",
                topic, subscription, allTopicPartitionsNumber.get());
            // 返回本对象
            subscribeFuture().complete(MultiTopicsConsumerImpl.this);
        })
        .exceptionally(ex -> {//异常处理
            log.warn("[{}] Failed to subscribe topics: {}", topic, ex.getMessage());
            subscribeFuture.completeExceptionally(ex);
            return null;
        });
}


 // 订阅一或多个 Topic
public CompletableFuture<Void> subscribeAsync(String topicName) {
    // 检验 Topic 是否可用
    if (!topicNameValid(topicName)) {
        return FutureUtil.failedFuture(
            new PulsarClientException.AlreadyClosedException("Topic name not valid"));
    }

    // 状态检验
    if (getState() == State.Closing || getState() == State.Closed) {
        return FutureUtil.failedFuture(
            new PulsarClientException.AlreadyClosedException("Topics Consumer was already closed"));
    }

    CompletableFuture<Void> subscribeResult = new CompletableFuture<>();
    //  获取 Topic 元数据
    client.getPartitionedTopicMetadata(topicName)
         //根据元数据开始订阅
        .thenAccept(metadata -> subscribeTopicPartitions(subscribeResult, topicName, metadata.partitions))
        .exceptionally(ex1 -> {
            log.warn("[{}] Failed to get partitioned topic metadata: {}", topicName, ex1.getMessage());
            subscribeResult.completeExceptionally(ex1);
            return null;
        });

    return subscribeResult;
}

// 创建订阅单个的 Topic 消费者，首先，创建没有指定 Topic 的消费者，再订阅已知分区数的多分区 Topic
public static <T> MultiTopicsConsumerImpl<T> createPartitionedConsumer(PulsarClientImpl client, ConsumerConfigurationData<T> conf, ExecutorService listenerExecutor,CompletableFuture<Consumer<T>> subscribeFuture,int numPartitions, Schema<T> schema, ConsumerInterceptors<T> interceptors) {
    checkArgument(conf.getTopicNames().size() == 1, "Should have only 1 topic for partitioned consumer");

    // 获取 Topic 名，虽然从配置对象移除，所以构造方法仅仅创建一个消费者，并没有订阅。
    ConsumerConfigurationData cloneConf = conf.clone();
    String topicName = cloneConf.getSingleTopic();
    cloneConf.getTopicNames().remove(topicName);

    CompletableFuture<Consumer> future = new CompletableFuture<>();
    //创建订阅多分区 Topic 消费者
    MultiTopicsConsumerImpl consumer = new MultiTopicsConsumerImpl(client, cloneConf, listenerExecutor, future, schema, interceptors);
    //订阅 Topic
    future.thenCompose(c -> ((MultiTopicsConsumerImpl)c).subscribeAsync(topicName, numPartitions))
        .thenRun(()-> subscribeFuture.complete(consumer))
        .exceptionally(e -> {
            log.warn("Failed subscription for createPartitionedConsumer: {} {}, e:{}",
                topicName, numPartitions,  e);
            subscribeFuture.completeExceptionally(((Throwable)e).getCause());
            return null;
        });;
    return consumer;
}

// 订阅一或多个指定 Topic ，但是此时（必须）知道分区数
private CompletableFuture<Void> subscribeAsync(String topicName, int numberPartitions) {
    if (!topicNameValid(topicName)) {
        return FutureUtil.failedFuture(
            new PulsarClientException.AlreadyClosedException("Topic name not valid"));
    }

    if (getState() == State.Closing || getState() == State.Closed) {
        return FutureUtil.failedFuture(
            new PulsarClientException.AlreadyClosedException("Topics Consumer was already closed"));
    }

    CompletableFuture<Void> subscribeResult = new CompletableFuture<>();
    subscribeTopicPartitions(subscribeResult, topicName, numberPartitions);

    return subscribeResult;
}

// 订阅的 Topic 可能是未分区或多分区
private void subscribeTopicPartitions(CompletableFuture<Void> subscribeResult, String topicName, int numPartitions) {
    if (log.isDebugEnabled()) {
        log.debug("Subscribe to topic {} metadata.partitions: {}", topicName, numPartitions);
    }

    List<CompletableFuture<Consumer<T>>> futureList;
    // 处理多分区订阅
    if (numPartitions > 1) {
        this.topics.putIfAbsent(topicName, numPartitions);
        allTopicPartitionsNumber.addAndGet(numPartitions);

        int receiverQueueSize = Math.min(conf.getReceiverQueueSize(),
            conf.getMaxTotalReceiverQueueSizeAcrossPartitions() / numPartitions);
        ConsumerConfigurationData<T> configurationData = getInternalConsumerConfig();
        configurationData.setReceiverQueueSize(receiverQueueSize);
        //为每个 （多分区）Topic 的分区创建一个消费者，这里指一个 多分区Topic 订阅
        futureList = IntStream
            .range(0, numPartitions)
            .mapToObj(
                partitionIndex -> {
                    String partitionName = TopicName.get(topicName).getPartition(partitionIndex).toString();
                    CompletableFuture<Consumer<T>> subFuture = new CompletableFuture<>();
                    ConsumerImpl<T> newConsumer = new ConsumerImpl<>(client, partitionName, configurationData,
                        client.externalExecutorProvider().getExecutor(), partitionIndex, subFuture, schema, interceptors);
                    consumers.putIfAbsent(newConsumer.getTopic(), newConsumer);
                    return subFuture;
                })
            .collect(Collectors.toList());
    } else {
        //无分区的 Topic 创建一个订阅
        this.topics.putIfAbsent(topicName, 1);
        allTopicPartitionsNumber.incrementAndGet();

        CompletableFuture<Consumer<T>> subFuture = new CompletableFuture<>();
        ConsumerImpl<T> newConsumer = new ConsumerImpl<>(client, topicName, internalConfig,
            client.externalExecutorProvider().getExecutor(), 0, subFuture, schema, interceptors);
        consumers.putIfAbsent(newConsumer.getTopic(), newConsumer);

        futureList = Collections.singletonList(subFuture);
    }
    // 等待所有消费者订阅执行成功
    FutureUtil.waitForAll(futureList)
        .thenAccept(finalFuture -> {
            //这里修正接收队列的大小，至少也要每个订阅者接收一个消息
            if (allTopicPartitionsNumber.get() > maxReceiverQueueSize) {
                setMaxReceiverQueueSize(allTopicPartitionsNumber.get());
            }
            int numTopics = this.topics.values().stream().mapToInt(Integer::intValue).sum();
            //这里校验 Topic 的分区数和消费者的订阅数是否一致
            checkState(allTopicPartitionsNumber.get() == numTopics,
                "allTopicPartitionsNumber " + allTopicPartitionsNumber.get()
                    + " not equals expected: " + numTopics);

            // 这意味着已经成功创建了消费者，这里（子消费者）依次通知 broker 可以推送消息，消费者已准备好接收消息，此方法上一章节已分析，这里不再分析了
            startReceivingMessages(
                consumers.values().stream()
                    .filter(consumer1 -> {
                        String consumerTopicName = consumer1.getTopic();
                        if (TopicName.get(consumerTopicName).getPartitionedTopicName().equals(
                            TopicName.get(topicName).getPartitionedTopicName().toString())) {
                            return true;
                        } else {
                            return false;
                        }
                    })
                    .collect(Collectors.toList()));
            //如果无异常，则表示创建成功
            subscribeResult.complete(null);
            log.info("[{}] [{}] Success subscribe new topic {} in topics consumer, partitions: {}, allTopicPartitionsNumber: {}",
                topic, subscription, topicName, numPartitions, allTopicPartitionsNumber.get());
            if (this.namespaceName == null) {
                this.namespaceName = TopicName.get(topicName).getNamespaceObject();
            }
            return;
        })
        .exceptionally(ex -> {
            handleSubscribeOneTopicError(topicName, ex, subscribeResult);
            return null;
        });
}

// 当订阅新 Topic 失败时，要解除那些已订阅成功的子消费者所订阅的分区（简单的说就是，对于多分区 Topic，要么订阅全部成功，要么就全部失败）
private void handleSubscribeOneTopicError(String topicName, Throwable error, CompletableFuture<Void> subscribeFuture) {
    log.warn("[{}] Failed to subscribe for topic [{}] in topics consumer {}", topic, topicName, error.getMessage());

    client.externalExecutorProvider().getExecutor().submit(() -> {
        AtomicInteger toCloseNum = new AtomicInteger(0);
        consumers.values().stream().filter(consumer1 -> {
            String consumerTopicName = consumer1.getTopic();
            if (TopicName.get(consumerTopicName).getPartitionedTopicName().equals(topicName)) {
                toCloseNum.incrementAndGet();
                return true;
            } else {
                return false;
            }
        }).collect(Collectors.toList()).forEach(consumer2 -> {
            //调用消费者异步关闭方法
            consumer2.closeAsync().whenComplete((r, ex) -> {
                consumer2.subscribeFuture().completeExceptionally(error);
                allTopicPartitionsNumber.decrementAndGet();
                consumers.remove(consumer2.getTopic());
                if (toCloseNum.decrementAndGet() == 0) {
                    log.warn("[{}] Failed to subscribe for topic [{}] in topics consumer, subscribe error: {}",
                        topic, topicName, error.getMessage());
                    topics.remove(topicName);
                    // 这里再一次校验分区数 和 订阅数是否一致
                    checkState(allTopicPartitionsNumber.get() == consumers.values().size());
                    subscribeFuture.completeExceptionally(error);
                }
                return;
            });
        });
    });
}


//向各个消费者发送 可用许可命令 ，通知 broker 推送消息到来
private void startReceivingMessages(List<ConsumerImpl<T>> newConsumers) {
    if (log.isDebugEnabled()) {
        log.debug("[{}] startReceivingMessages for {} new consumers in topics consumer, state: {}",
            topic, newConsumers.size(), getState());
    }
    if (getState() == State.Ready) {
        newConsumers.forEach(consumer -> {
            consumer.sendFlowPermitsToBroker(consumer.getConnectionHandler().cnx(), conf.getReceiverQueueSize());
            receiveMessageFromConsumer(consumer);
        });
    }
}

//检查 Topic 集合是合法的
// - 检查每个 Topic 是合法
// - 每个 Topic 所属的 Namespace 是相同的
// - Topic 名是唯一的
private static boolean topicNamesValid(Collection<String> topics) {
    // 多 Topic 订阅，Topic 的个数必然要大于1。
    checkState(topics != null && topics.size() >= 1,
        "topics should should contain more than 1 topic");

    // 取第一个 Topic 的 Namespace
    final String namespace = TopicName.get(topics.stream().findFirst().get()).getNamespace();
    //依次检查 Topic 集合
    Optional<String> result = topics.stream()
        .filter(topic -> {
            // Topic 合法性检查
            boolean topicInvalid = !TopicName.isValid(topic);
            if (topicInvalid) {
                return true;
            }
            //每个 Topic 所属的 Namespace 与第一个 Topic 是相同的
            String newNamespace =  TopicName.get(topic).getNamespace();
            if (!namespace.equals(newNamespace)) {
                return true;
            } else {
                return false;
            }
        }).findFirst();

    if (result.isPresent()) {
        log.warn("[{}] Received invalid topic name.  {}/{}", result.get());
        return false;
    }

    // 检查各个 Topic 名是否唯一
    HashSet<String> set = new HashSet<>(topics);
    if (set.size() == topics.size()) {
        return true;
    } else {
        log.warn("Topic names not unique. unique/all : {}/{}", set.size(), topics.size());
        return false;
    }
}

```

从这里可以看出，构造方法主要功能如下：

* 初始化相关的容器和组件
* 如果配置了 Topics 相关信息，则进行合法性校验，否则将直接返回。
* 根据 Topics 配置调用 ConsumerImpl 的订阅方法
* 全部订阅成功后，则向每个消费者发送 FlowPermitsToBroker 命令，通知 broker 推送消息

#### 4. MultiTopicsConsumerImpl 接收消息

构造方法已经通知 broker 推送消息过来，那么消费者就要开始接收消息，前面的 ConsumerImpl 已展示怎么用API接收消息。跟 ConsumerImpl 相同，只要实现**internalReceiveXX**相关方法即可。

```java

@Override
protected Message<T> internalReceive() throws PulsarClientException {
    Message<T> message;
    try {
        //阻塞消息队列，等待消息到来
        message = incomingMessages.take();
        //检查是否是 TopicMessageImpl 实例
        checkState(message instanceof TopicMessageImpl);
        // 放入未确认消息跟踪器
        unAckedMessageTracker.add(message.getMessageId());
        //如果需要，恢复暂停消费者队列中消费者接收消息
        resumeReceivingFromPausedConsumersIfNeeded();
        return message;
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PulsarClientException(e);
    }
}

private void resumeReceivingFromPausedConsumersIfNeeded() {
    lock.readLock().lock();
    try {
        //如果消息队列中消息数小于等于共享队列恢复阈值，并且暂停消费者队列不为空，此时就可以唤醒消费者通知 broker 推送消息
        if (incomingMessages.size() <= sharedQueueResumeThreshold && !pausedConsumers.isEmpty()) {
            while (true) {
                ConsumerImpl<T> consumer = pausedConsumers.poll();
                if (consumer == null) {
                    break;
                }

                // 指定消费者接收消息，因为这里用到了读锁，所以另外一线程执行
                client.eventLoopGroup().execute(() -> {
                    receiveMessageFromConsumer(consumer);
                });
            }
        }
    } finally {
        lock.readLock().unlock();
    }
}

private void receiveMessageFromConsumer(ConsumerImpl<T> consumer) {
    //异步接收消息
    consumer.receiveAsync().thenAccept(message -> {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Receive message from sub consumer:{}",
                topic, subscription, consumer.getTopic());
        }

        // 处理接收到的消息，把它放入队列触发监听器或执行异步回调
        messageReceived(consumer, message);

        lock.writeLock().lock();
        try {
            //如果消息队列中消息数已经大于等于最大接收队列大小或者消息队列中消息数大于共享队列恢复阈值且暂停消费者队列不为空
            int size = incomingMessages.size();
            if (size >= maxReceiverQueueSize
                    || (size > sharedQueueResumeThreshold && !pausedConsumers.isEmpty())) {
                // 将此消费者标记为稍后恢复：如果共享队列中没有剩余空间，或者是否已暂停任何消费者（为已暂停的消费者创造公平机会）
                pausedConsumers.add(consumer);
            } else {
                // 如果队列未满，继续调用 ConsumerImpl 的方法 receiveAsync()（接收消息）。 这里使用不同的线程以避免递归和堆栈溢出
                client.eventLoopGroup().execute(() -> {
                    receiveMessageFromConsumer(consumer);
                });
            }
        } finally {
            lock.writeLock().unlock();
        }
    });
}

//消费者消息接收
private void messageReceived(ConsumerImpl<T> consumer, Message<T> message) {
    checkArgument(message instanceof MessageImpl);
    lock.writeLock().lock();
    try {
        //带有 Topic 信息的消息
        TopicMessageImpl<T> topicMessage = new TopicMessageImpl<>(
            consumer.getTopic(), consumer.getTopicNameWithoutPartition(), message);
        //放入跟踪器
        unAckedMessageTracker.add(topicMessage.getMessageId());

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Received message from topics-consumer {}",
                topic, subscription, message.getMessageId());
        }

        // 如果异步接收队列有正等待任务，取出相关任务，直接设置消息触发回调，而不再把消息放入消息队列（这里是异步接收方法产生的正处理接收队列）
        if (!pendingReceives.isEmpty()) {
            CompletableFuture<Message<T>> receivedFuture = pendingReceives.poll();
            listenerExecutor.execute(() -> receivedFuture.complete(topicMessage));
        } else {
            // 将消息入队列，以便上层应用调用 receive() 方法时能检索消息。（如果队列已满，则堵塞并）等待消息队列有足够空间插入消息。但这里的操作不应阻塞，因为MultiTopicsSumerImpl 使用 GrowableArrayBlockingQueue 队列，一种可自动扩容的阻塞消息队列
            incomingMessages.put(topicMessage);
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    } finally {
        lock.writeLock().unlock();
    }

    //监听器处理
    if (listener != null) {
        // 在单独的线程中触发消息监听器上的通知，以避免在消息处理时阻塞网络线程。
        listenerExecutor.execute(() -> {
            Message<T> msg;
            try {
                //取消息
                msg = internalReceive();
            } catch (PulsarClientException e) {
                log.warn("[{}] [{}] Failed to dequeue the message for listener", topic, subscription, e);
                return;
            }

            try {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Calling message listener for message {}",
                        topic, subscription, message.getMessageId());
                }
                //执行回调
                listener.received(MultiTopicsConsumerImpl.this, msg);
            } catch (Throwable t) {
                log.error("[{}][{}] Message listener error in processing message: {}",
                    topic, subscription, message, t);
            }
        });
    }
}

//具有超时控制的消息获取方法（跟上面的方法基本类似，只是加入了超时控制）
@Override
protected Message<T> internalReceive(int timeout, TimeUnit unit) throws PulsarClientException {
    Message<T> message;
    try {
        message = incomingMessages.poll(timeout, unit);
        if (message != null) {
            checkArgument(message instanceof TopicMessageImpl);
            unAckedMessageTracker.add(message.getMessageId());
        }
        resumeReceivingFromPausedConsumersIfNeeded();
        return message;
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PulsarClientException(e);
    }
}

//这里是异步接收
@Override
protected CompletableFuture<Message<T>> internalReceiveAsync() {
    CompletableFuture<Message<T>> result = new CompletableFuture<>();
    Message<T> message;
    try {
        lock.writeLock().lock();
        //立即从消息队列拉取一条消息（不阻塞）
        message = incomingMessages.poll(0, TimeUnit.SECONDS);
        //如果消息为空，则把当前结果放入正处理接收队列
        if (message == null) {
            pendingReceives.add(result);
        } else {
            //其他情况没什么区别，跟前面方法处理逻辑一致
            checkState(message instanceof TopicMessageImpl);
            unAckedMessageTracker.add(message.getMessageId());
            resumeReceivingFromPausedConsumersIfNeeded();
            result.complete(message);
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        result.completeExceptionally(new PulsarClientException(e));
    } finally {
        lock.writeLock().unlock();
    }

    return result;
}
```

以上就是消息接收方法的实现，有人会说，怎么这么简单，实际上，大部分功能已经委托给 ConsumerImpl 实现，这里主要是控制消息队列不被填满、消息少了通知 broker 推送消息而已。然后消息处理流程基本与 ConsumerImpl 类似。

#### 5. MultiTopicsConsumerImpl 消息确认

在 ConsumerImpl 类中已经详解消息确认大部分逻辑，现这里直接给出 doAcknowledge 方法实现。

```java

@Override
protected CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType,
                                                Map<String,Long> properties) {

    // 检测消息ID是否是 TopicMessageIdImpl 实例
    checkArgument(messageId instanceof TopicMessageIdImpl);
    TopicMessageIdImpl topicMessageId = (TopicMessageIdImpl) messageId;
    // 状态检查
    if (getState() != State.Ready) {
        return FutureUtil.failedFuture(new PulsarClientException("Consumer already closed"));
    }

    // 如果是累积确认
    if (ackType == AckType.Cumulative) {
        Consumer individualConsumer = consumers.get(topicMessageId.getTopicPartitionName());
        if (individualConsumer != null) {
            MessageId innerId = topicMessageId.getInnerMessageId();
            //这里调用的还是 ConsumerImpl 类 acknowledgeCumulativeAsync 方法。
            return individualConsumer.acknowledgeCumulativeAsync(innerId);
        } else {
            //没找到相关消费者信息，则认为是没连接
            return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
        }
    } else {
        // 单个确认逻辑，为什么这里不进行 消费者为空判断？
        ConsumerImpl<T> consumer = consumers.get(topicMessageId.getTopicPartitionName());

        MessageId innerId = topicMessageId.getInnerMessageId();
        //处理逻辑还是跟 consumer 相同
        return consumer.doAcknowledge(innerId, ackType, properties)
            .thenRun(() ->
                unAckedMessageTracker.remove(topicMessageId));
    }
}

```

消息确认基本上解玩完了，这里的只是作一转发动作，其关键在于根据 Topic 信息找到消费者，再调用其消息确认方法。

#### 6. MultiTopicsConsumerImpl 消费超时（重试）与死信队列

在 ConsumerImpl 分析中已经知道，处理消费超时与死信消息的逻辑在 UnAckedMessageTracker 类中实现，这里就不再详细分析全部过程，只关注那些变化部分。在 MultiTopicsConsumerImpl 构造方法可知，UnAckedTopicMessageTracker 初始化跟 UnAckedMessageTracker 一样，实际上前者继承后者，只是多了一个 removeTopicMessages 方法，然后重写**redeliverUnacknowledgedMessages**方法，如下：

```java
// 移除 Topic （名）相关的 TopicMessageIdImpl 消息
public int removeTopicMessages(String topicName) {
    writeLock.lock();
    try {
        int removed = 0;
        Iterator<MessageId> iterator = messageIdPartitionMap.keySet().iterator();
        while (iterator.hasNext()) {
            MessageId messageId = iterator.next();
            if (messageId instanceof TopicMessageIdImpl &&
                    ((TopicMessageIdImpl)messageId).getTopicPartitionName().contains(topicName)) {
                ConcurrentOpenHashSet<MessageId> exist = messageIdPartitionMap.get(messageId);
                if (exist != null) {
                    exist.remove(messageId);
                }
                iterator.remove();
                removed ++;
            }
        }
        return removed;
    } finally {
        writeLock.unlock();
    }
}

@Override
public void redeliverUnacknowledgedMessages(Set<MessageId> messageIds) {
    checkArgument(messageIds.stream().findFirst().get() instanceof TopicMessageIdImpl);

    if (conf.getSubscriptionType() != SubscriptionType.Shared) {
        // 如果订阅类型不是共享式，则不重新投递单个消息
        redeliverUnacknowledgedMessages();
        return;
    }
    //根据消息ID从消息队列移除过期消息，这个方法与 ConsumerImpl 中的相同
    removeExpiredMessagesFromQueue(messageIds);
    messageIds.stream().map(messageId -> (TopicMessageIdImpl)messageId)
        //以 Topic 的各个分区名分组，这样接下来就保证同一分区消息只用其分区 Topic 对应的消费者处理
        .collect(Collectors.groupingBy(TopicMessageIdImpl::getTopicPartitionName, Collectors.toSet()))
        .forEach((topicName, messageIds1) ->
            consumers.get(topicName)
                .redeliverUnacknowledgedMessages(messageIds1.stream()
                    .map(mid -> mid.getInnerMessageId()).collect(Collectors.toSet())));
    //如果需要，恢复暂停队列中的消费者接收消息，这个方法与 ConsumerImpl 中的相同
    resumeReceivingFromPausedConsumersIfNeeded();
}

// 这里其实跟 ConsumerImpl 实现一样，只是消息为 TopicMessageImpl 的实例
private void removeExpiredMessagesFromQueue(Set<MessageId> messageIds) {
    Message<T> peek = incomingMessages.peek();
    if (peek != null) {
        if (!messageIds.contains(peek.getMessageId())) {
            return;
        }

        Message<T> message = incomingMessages.poll();
        checkState(message instanceof TopicMessageImpl);
        while (message != null) {
            MessageId messageId = message.getMessageId();
            if (!messageIds.contains(messageId)) {
                messageIds.add(messageId);
                break;
            }
            message = incomingMessages.poll();
        }
    }
}

//重新投递无确认消息
@Override
public void redeliverUnacknowledgedMessages() {
    lock.writeLock().lock();
    try {
        //所有的子消费者都进行重新投递无确认消息调用，并把消息队列和未确认消息跟踪器清空
        consumers.values().stream().forEach(consumer -> consumer.redeliverUnacknowledgedMessages());
        incomingMessages.clear();
        unAckedMessageTracker.clear();
    } finally {
        lock.writeLock().unlock();
    }
    //尝试恢复消息接收
    resumeReceivingFromPausedConsumersIfNeeded();
}

```

到这里，基本上可以下结论，MultiTopicsConsumerImpl 实现的多 Topics  消费者订阅其实分为3种不同的情况：多个未（单）分区 Topic 订阅，1或多个多分区 Topic 订阅，1或多个未分区 Topic 和1或多个多分区 Topic 共同订阅。其核心思想在于，每个分区（未分区或单分区其实只是一个特殊的多分区）都由一个普通的消费者订阅，这样互不影响，很多接口方法均已在 ConsumerImpl 上实现，MultiTopicsConsumerImpl 大部分实现只是转发或合并数据，甚至有些特性完全依赖前者，比如，这里的**死信消息处理**。所以 MultiTopicsConsumerImpl 对死信消息的处理完全依赖 ConsumerImpl。

#### 7. MultiTopicsConsumerImpl 重置订阅

**MultiTopicsConsumerImpl** 不支持重置订阅，如下：

```java
@Override
public void seek(MessageId messageId) throws PulsarClientException {
    try {
        seekAsync(messageId).get();
    } catch (ExecutionException e) {
        throw new PulsarClientException(e.getCause());
    } catch (InterruptedException e) {
        throw new PulsarClientException(e);
    }
}

@Override
public CompletableFuture<Void> seekAsync(MessageId messageId) {
    return FutureUtil.failedFuture(new PulsarClientException("Seek operation not supported on topics consumer"));
}

```

#### 8. MultiTopicsConsumerImpl 暂停与恢复

对子消费者进行暂停与恢复控制，如下：

```java

@Override
public void pause() {
    consumers.forEach((name, consumer) -> consumer.pause());
}

@Override
public void resume() {
    consumers.forEach((name, consumer) -> consumer.resume());
}

```

子消费者暂停与恢复操作，前面章节已经具体分析，这里不再赘述。

#### 9. MultiTopicsConsumerImpl 取消订阅

提供两个方法，一个是取消所有 Topic 订阅，一个是取消指定 Topic 的订阅，也是对相关联的子消费者进行操作。如下：

```java
//取消所有订阅
@Override
public CompletableFuture<Void> unsubscribeAsync() {
    //状态检查
    if (getState() == State.Closing || getState() == State.Closed) {
        return FutureUtil.failedFuture(
                new PulsarClientException.AlreadyClosedException("Topics Consumer was already closed"));
    }
    //这里怎么没有取消定时器呢 TODO
    setState(State.Closing);
    //子消费者依次调用取消订阅
    CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();
    List<CompletableFuture<Void>> futureList = consumers.values().stream()
        .map(c -> c.unsubscribeAsync()).collect(Collectors.toList());
    //所有执行完后，进行判断
    FutureUtil.waitForAll(futureList)
        .whenComplete((r, ex) -> {
            if (ex == null) {
                //无异常，则正常关闭，并关闭未确认消息跟踪器
                setState(State.Closed);
                unAckedMessageTracker.close();
                unsubscribeFuture.complete(null);
                log.info("[{}] [{}] [{}] Unsubscribed Topics Consumer",
                    topic, subscription, consumerName);
            } else {
                //有异常，则标记为失败，返回异常消息
                setState(State.Failed);
                unsubscribeFuture.completeExceptionally(ex);
                log.error("[{}] [{}] [{}] Could not unsubscribe Topics Consumer",
                    topic, subscription, consumerName, ex.getCause());
            }
        });

    return unsubscribeFuture;
}

//取消指定 Topic 的订阅
public CompletableFuture<Void> unsubscribeAsync(String topicName) {
    //Topic 合法性校验
    checkArgument(TopicName.isValid(topicName), "Invalid topic name:" + topicName);
    // 状态校验
    if (getState() == State.Closing || getState() == State.Closed) {
        return FutureUtil.failedFuture(
            new PulsarClientException.AlreadyClosedException("Topics Consumer was already closed"));
    }
    //取消分区自动更新任务，后面没恢复这个定时器 TODO
    if (partitionsAutoUpdateTimeout != null) {
        partitionsAutoUpdateTimeout.cancel();
        partitionsAutoUpdateTimeout = null;
    }

    CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();
    String topicPartName = TopicName.get(topicName).getPartitionedTopicName();
    //这里选出跟 Topic 相关联的子消费者
    List<ConsumerImpl<T>> consumersToUnsub = consumers.values().stream()
        .filter(consumer -> {
            String consumerTopicName = consumer.getTopic();
            if (TopicName.get(consumerTopicName).getPartitionedTopicName().equals(topicPartName)) {
                return true;
            } else {
                return false;
            }
        }).collect(Collectors.toList());
    //依次调用子消费者的取消订阅方法
    List<CompletableFuture<Void>> futureList = consumersToUnsub.stream()
        .map(ConsumerImpl::unsubscribeAsync).collect(Collectors.toList());

    FutureUtil.waitForAll(futureList)
        .whenComplete((r, ex) -> {
            if (ex == null) {
                //如果正常取消订阅，则清理相关资源
                consumersToUnsub.forEach(consumer1 -> {
                    consumers.remove(consumer1.getTopic());
                    pausedConsumers.remove(consumer1);
                    allTopicPartitionsNumber.decrementAndGet();
                });

                topics.remove(topicName);
                ((UnAckedTopicMessageTracker) unAckedMessageTracker).removeTopicMessages(topicName);

                unsubscribeFuture.complete(null);
                log.info("[{}] [{}] [{}] Unsubscribed Topics Consumer, allTopicPartitionsNumber: {}",
                    topicName, subscription, consumerName, allTopicPartitionsNumber);
            } else {
                //失败则把异常返回，这里要标记失败？？
                unsubscribeFuture.completeExceptionally(ex);
                setState(State.Failed);
                log.error("[{}] [{}] [{}] Could not unsubscribe Topics Consumer",
                    topicName, subscription, consumerName, ex.getCause());
            }
        });

    return unsubscribeFuture;
}

```

#### 10. MultiTopicsConsumerImpl 关闭

异步关闭所有的子消费者。

```java
//代码结构、逻辑基本上与取消订阅类似
@Override
public CompletableFuture<Void> closeAsync() {
    if (getState() == State.Closing || getState() == State.Closed) {
        unAckedMessageTracker.close();
        return CompletableFuture.completedFuture(null);
    }
    setState(State.Closing);

    if (partitionsAutoUpdateTimeout != null) {
        partitionsAutoUpdateTimeout.cancel();
        partitionsAutoUpdateTimeout = null;
    }

    CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    List<CompletableFuture<Void>> futureList = consumers.values().stream()
        .map(c -> c.closeAsync()).collect(Collectors.toList());

    FutureUtil.waitForAll(futureList)
        .whenComplete((r, ex) -> {
            if (ex == null) {
                setState(State.Closed);
                unAckedMessageTracker.close();
                closeFuture.complete(null);
                log.info("[{}] [{}] Closed Topics Consumer", topic, subscription);
                //清理消费者资源
                client.cleanupConsumer(this);
                // 通知上层引用，正处理的消息全部置为消费者已关闭异常
                failPendingReceive();
            } else {
                setState(State.Failed);
                closeFuture.completeExceptionally(ex);
                log.error("[{}] [{}] Could not close Topics Consumer", topic, subscription,
                    ex.getCause());
            }
        });

    return closeFuture;
}

```

到这里，MultiTopicsConsumerImpl 的实现已基本分析完，这里作个总结：MultiTopicsConsumerImpl 实现的多 Topics  消费者订阅其实分为3种不同的情况：多个未（单）分区 Topic 订阅，1或多个多分区 Topic 订阅，1或多个未分区 Topic 和1或多个多分区 Topic 共同订阅。其核心思想在于，每个分区（未分区或单分区其实只是一个特殊的多分区）都由一个普通的消费者订阅，这样互不影响，很多接口方法均已在 ConsumerImpl 上实现，MultiTopicsConsumerImpl 大部分实现只是转发或合并数据，甚至有些特性完全依赖前者，比如，前面章节涉及的**死信消息处理**。从这里可以推测：消费者第三个实现——正则表达式多 Topic 消费者实现基本上有迹可循了。

### 3. PatternMultiTopicsConsumerImpl 实现解析

#### 1. PatternMultiTopicsConsumerImpl 类继承图，如下

![PatternMultiTopicsConsumerImpl类继承图](./images/20190505164346.png)

PatternMultiTopicsConsumerImpl 直接继承与 MultiTopicsConsumerImpl，另一方面也跟 MultiTopicsConsumerImpl 的继承结构非常类似，只不过多了一个继承和一个实现。

#### 2. PatternMultiTopicsConsumerImpl 的属性

```java
//正则表达式
private final Pattern topicsPattern;
//Topic 变更监听器
private final TopicsChangedListener topicsChangeListener;
//订阅模式
private final Mode subscriptionMode;
//监测正则表达式结果定时器
private volatile Timeout recheckPatternTimeout = null;

```

#### 3. PatternMultiTopicsConsumerImpl 构造方法

```java

public PatternMultiTopicsConsumerImpl(Pattern topicsPattern,
PulsarClientImpl client,ConsumerConfigurationData<T> conf, ExecutorService listenerExecutor,CompletableFuture<Consumer<T>> subscribeFuture,Schema<T> schema, Mode subscriptionMode, ConsumerInterceptors<T> interceptors) {
    // 同样调用 ConsumerBase 构造方法，前面章节有介绍，这里不再赘述
    super(client, conf, listenerExecutor, subscribeFuture, schema, interceptors);
    this.topicsPattern = topicsPattern;
    this.subscriptionMode = subscriptionMode;
    //如果 Namespace 为空，则从正则表达式 Topic 中获取
    if (this.namespaceName == null) {
        this.namespaceName = getNameSpaceFromPattern(topicsPattern);
    }
    //检查下当前正则表达式 Topic 对应的 Namespace 与 当前的 Namespace 是否一致
    checkArgument(getNameSpaceFromPattern(topicsPattern).toString().equals(this.namespaceName.toString()));

    this.topicsChangeListener = new PatternTopicsChangedListener();
    //创建定时器任务，这里注意间隔最小值
    recheckPatternTimeout = client.timer().newTimeout(this, Math.min(1, conf.getPatternAutoDiscoveryPeriod()), TimeUnit.MINUTES);
}

public static NamespaceName getNameSpaceFromPattern(Pattern pattern) {
    return TopicName.get(pattern.pattern()).getNamespaceObject();
}

interface TopicsChangedListener {
    // 取消订阅和删除消费者对象时，将触发这个回调
    CompletableFuture<Void> onTopicsRemoved(Collection<String> removedTopics);
    // 订阅和创建新消费者对象列表时，将触发这个回调
    CompletableFuture<Void> onTopicsAdded(Collection<String> addedTopics);
}

private class PatternTopicsChangedListener implements TopicsChangedListener {
    @Override
    public CompletableFuture<Void> onTopicsRemoved(Collection<String> removedTopics) {
        CompletableFuture<Void> removeFuture = new CompletableFuture<>();
        //如果移除的 Topic 集合为空，则直接返回
        if (removedTopics.isEmpty()) {
            removeFuture.complete(null);
            return removeFuture;
        }

        List<CompletableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(topics.size());
        //对移除的 Topic 集合依次异步执行取消订阅操作
        removedTopics.stream().forEach(topic -> futures.add(unsubscribeAsync(topic)));
        //如果没有异常，则成功，否则失败
        FutureUtil.waitForAll(futures)
            .thenAccept(finalFuture -> removeFuture.complete(null))
            .exceptionally(ex -> {
                log.warn("[{}] Failed to subscribe topics: {}", topic, ex.getMessage());
                removeFuture.completeExceptionally(ex);
            return null;
        });
        return removeFuture;
    }

    @Override
    public CompletableFuture<Void> onTopicsAdded(Collection<String> addedTopics) {
        CompletableFuture<Void> addFuture = new CompletableFuture<>();
        //如果新增的 Topic 集合为空，则直接返回
        if (addedTopics.isEmpty()) {
            addFuture.complete(null);
            return addFuture;
        }

        List<CompletableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(topics.size());
        //对新增的 Topic 集合依次异步执行订阅操作
        addedTopics.stream().forEach(topic -> futures.add(subscribeAsync(topic)));
        //如果没有异常，则成功，否则失败
        FutureUtil.waitForAll(futures)
            .thenAccept(finalFuture -> addFuture.complete(null))
            .exceptionally(ex -> {
                log.warn("[{}] Failed to unsubscribe topics: {}", topic, ex.getMessage());
                addFuture.completeExceptionally(ex);
                return null;
            });
        return addFuture;
    }
}

@Override
public void run(Timeout timeout) throws Exception {
    //定时器已取消，则直接返回
    if (timeout.isCancelled()) {
        return;
    }

    CompletableFuture<Void> recheckFuture = new CompletableFuture<>();
    List<CompletableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(2);
    //根据 Namespace 和 订阅模式查找所有的 Topic 集合
    client.getLookup().getTopicsUnderNamespace(namespaceName, subscriptionMode).thenAccept(topics -> {
        if (log.isDebugEnabled()) {
            log.debug("Get topics under namespace {}, topics.size: {}", namespaceName.toString(), topics.size());
            topics.forEach(topicName ->
                log.debug("Get topics under namespace {}, topic: {}", namespaceName.toString(), topicName));
        }
        //根据给定 Topic 集合和正则表达式，求出符合表达式的 Topic 集合
        List<String> newTopics = PulsarClientImpl.topicsPatternFilter(topics, topicsPattern);
        //给出老的 Topic 集合
        List<String> oldTopics = PatternMultiTopicsConsumerImpl.this.getTopics();
        //算出一分钟内新增的 Topic 集合，并回调
        futures.add(topicsChangeListener.onTopicsAdded(topicsListsMinus(newTopics, oldTopics)));
        //算出一分钟内删除的 Topic 集合，并回调
        futures.add(topicsChangeListener.onTopicsRemoved(topicsListsMinus(oldTopics, newTopics)));
        //如果无异常，则设置成功，否则设置异常
        FutureUtil.waitForAll(futures)
            .thenAccept(finalFuture -> recheckFuture.complete(null))
            .exceptionally(ex -> {
                log.warn("[{}] Failed to recheck topics change: {}", topic, ex.getMessage());
                recheckFuture.completeExceptionally(ex);
                return null;
            });
    });

    //启动下一次检查任务
    recheckPatternTimeout = client.timer().newTimeout(PatternMultiTopicsConsumerImpl.this,
        Math.min(1, conf.getPatternAutoDiscoveryPeriod()), TimeUnit.MINUTES);
}

```

从这里可以看出，PatternMultiTopicsConsumerImpl 的构造方法已然加载和初始化所有组件，其核心逻辑在于定时加载相关 Namespace 与 订阅模式的 Topic 集合，并和正则表达式求值，其值与上一次Topic集合求差集和反差集，然后回调`TopicsChangedListener`接口的两个方法**onTopicsAdded**和**onTopicsRemoved**，这样就完成了对 Topic 的新增和删除的操作，也对应着订阅和取消订阅，其他的方法均继承于 MultiTopicsConsumerImpl 类，完成对正则表达式 Topic 支持，可以说继承的非常巧妙。Pulsar Client 接口设计的非常清晰精炼，很轻量级，这有利于其他语言实现相关接口，而序列化协议采用Protobuf，很容易无缝移植到其他语言。到这里，PatternMultiTopicsConsumerImpl 解析已经完成。

自此，Pulsar Client 部分源码解析已经完结，接下来，将讲解Pulsar 的 Broker 部分。

## 3. Pulsar Broker 解析

Pulsar Broker 像很多分布式应用一样，有单机和集群启动模式。单机模式便于开发者开发调试，集群用于日常部署。这里，选取集群模式来进行详细解析，单机模式本质上就是一阉割版的集群模式。此外，还有其他管理工具，如集群元数据安装程序等。

### 1. BrokerStarter 主线程

从启动命令行可以看出，BrokerStarter 是主线程所在类，那么就拿它来解析，以下是主线程的源码：

```java

public static void main(String[] args) throws Exception {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
    //设置线程异常未catch情况下默认处理方法
    Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
        System.out.println(String.format("%s [%s] error Uncaught exception in thread %s: %s", dateFormat.format(new Date()), thread.getContextClassLoader(), thread.getName(), exception.getMessage()));
    });
    //创建 broker 启动器实例
    BrokerStarter starter = new BrokerStarter(args);
    //安装JVM退出时钩子，即调用 broker 启动器关闭
    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> {
            starter.shutdown();
        })
    );

    try {
        //调用启动器启动
        starter.start();
    } catch (Exception e) {
        log.error("Failed to start pulsar service.", e);
        Runtime.getRuntime().halt(1);
    }
    //等待启动器退出
    starter.join();
}

```

从上面源码中可看出，主要是 BrokerStarter 唱主角，而主线程只做了如下操作：

* 设置线程异常退出时执行的方法（这里是打印日志）
* 创建 BrokerStarter 实例，并把命令行参数放入（解析命令）
* 安装当JVM退出时钩子，即 BrokerStarter 关闭
* 调用 BrokerStarter 开始方法
* 等待 BrokerStarter 退出

那么接下来，就解析 BrokerStarter 源码了。

#### 1. BrokerStarter 属性

```java
//服务配置文件类
private final ServiceConfiguration brokerConfig;
//Pulsar 服务类
private final PulsarService pulsarService;
//Bookkeeper 客户端
private final BookieServer bookieServer;
//Bookeeper 自动恢复服务
private final AutoRecoveryMain autoRecoveryMain;
//Bookeeper 状态提供服务
private final StatsProvider bookieStatsProvider;
//服务器配置类
private final ServerConfiguration bookieConfig;
//函数式工作者服务
private final WorkerService functionsWorkerService;

```

#### 2. BrokerStarter 构造方法

```java

BrokerStarter(String[] args) throws Exception{
    StarterArguments starterArguments = new StarterArguments();
    JCommander jcommander = new JCommander(starterArguments);
    jcommander.setProgramName("PulsarBrokerStarter");

    // JCommander 组件解析命令行
    jcommander.parse(args);
    if (starterArguments.help) {
        jcommander.usage();
        System.exit(-1);
    }

    // 初始化 broker 配置文件
    if (isBlank(starterArguments.brokerConfigFile)) {
        jcommander.usage();
        throw new IllegalArgumentException("Need to specify a configuration file for broker");
    } else {
        brokerConfig = loadConfig(starterArguments.brokerConfigFile);
    }

    // 初始化 functions worker ，判定是否启用函数工作者服务
    if (starterArguments.runFunctionsWorker || brokerConfig.isFunctionsWorkerEnabled()) {
        WorkerConfig workerConfig;
        //初始化函数工作者配置文件
        if (isBlank(starterArguments.fnWorkerConfigFile)) {
            workerConfig = new WorkerConfig();
        } else {
            workerConfig = WorkerConfig.load(starterArguments.fnWorkerConfigFile);
        }
        // 配置工作者与本地 broker 通信
        boolean useTls = workerConfig.isUseTls();
        String localhost = "127.0.0.1";
        String pulsarServiceUrl = useTls
                ? PulsarService.brokerUrlTls(localhost, brokerConfig.getBrokerServicePortTls().get())
                : PulsarService.brokerUrl(localhost, brokerConfig.getBrokerServicePort().get());
        String webServiceUrl = useTls
                ? PulsarService.webAddressTls(localhost, brokerConfig.getWebServicePortTls().get())
                : PulsarService.webAddress(localhost, brokerConfig.getWebServicePort().get());
        workerConfig.setPulsarServiceUrl(pulsarServiceUrl);
        workerConfig.setPulsarWebServiceUrl(webServiceUrl);
        String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(
            brokerConfig.getAdvertisedAddress());
        workerConfig.setWorkerHostname(hostname);
        workerConfig.setWorkerPort(brokerConfig.getWebServicePort().get());
        workerConfig.setWorkerId(
            "c-" + brokerConfig.getClusterName()
                + "-fw-" + hostname
                + "-" + workerConfig.getWorkerPort());
        functionsWorkerService = new WorkerService(workerConfig);
    } else {
        //意味着不启用
        functionsWorkerService = null;
    }

    // 初始化 pulsar 服务
    pulsarService = new PulsarService(brokerConfig, Optional.ofNullable(functionsWorkerService));

    // 如果在命令行中没有运行 bookie （命令），则尝试从 pulsar 配置文件中读取
    if (!argsContains(args, "-rb") && !argsContains(args, "--run-bookie")) {
        checkState(starterArguments.runBookie == false,
            "runBookie should be false if has no argument specified");
        starterArguments.runBookie = brokerConfig.isEnableRunBookieTogether();
    }
    // 加载顺序同上，表示是否启用 bookie 自动恢复服务
    if (!argsContains(args, "-ra") && !argsContains(args, "--run-bookie-autorecovery")) {
        checkState(starterArguments.runBookieAutoRecovery == false,
            "runBookieAutoRecovery should be false if has no argument specified");
        starterArguments.runBookieAutoRecovery = brokerConfig.isEnableRunBookieAutoRecoveryTogether();
    }

    // 从这里可以看出，要么运行 bookie（读写） 服务，要么运行 bookie 自动恢复服务
    if ((starterArguments.runBookie || starterArguments.runBookieAutoRecovery)
        && isBlank(starterArguments.bookieConfigFile)) {
        jcommander.usage();
        throw new IllegalArgumentException("No configuration file for Bookie");
    }

    // 初始化 bookie 状态提供者
    if (starterArguments.runBookie || starterArguments.runBookieAutoRecovery) {
        checkState(isNotBlank(starterArguments.bookieConfigFile),
            "No configuration file for Bookie");
        bookieConfig = readBookieConfFile(starterArguments.bookieConfigFile);
        Class<? extends StatsProvider> statsProviderClass = bookieConfig.getStatsProviderClass();
        bookieStatsProvider = ReflectionUtils.newInstance(statsProviderClass);
    } else {
        bookieConfig = null;
        bookieStatsProvider = null;
    }

    // 如果配置文件设置当前服务器运行 bookie 服务，则初始化 bookie 服务器
    if (starterArguments.runBookie) {
        checkNotNull(bookieConfig, "No ServerConfiguration for Bookie");
        checkNotNull(bookieStatsProvider, "No Stats Provider for Bookie");
        bookieServer = new BookieServer(bookieConfig, bookieStatsProvider.getStatsLogger(""));
    } else {
        bookieServer = null;
    }

    // 如果设置 bookie 自动恢复标志，则初始化 bookie 自动恢复服务
    if (starterArguments.runBookieAutoRecovery) {
        checkNotNull(bookieConfig, "No ServerConfiguration for Bookie Autorecovery");
        autoRecoveryMain = new AutoRecoveryMain(bookieConfig);
    } else {
        autoRecoveryMain = null;
    }
}

//命令列表，这里就是支持的所有命令
@VisibleForTesting
private static class StarterArguments {
    @Parameter(names = {"-c", "--broker-conf"}, description = "Configuration file for Broker")
    private String brokerConfigFile = Paths.get("").toAbsolutePath().normalize().toString() + "/conf/broker.conf";

    @Parameter(names = {"-rb", "--run-bookie"}, description = "Run Bookie together with Broker")
    private boolean runBookie = false;

    @Parameter(names = {"-ra", "--run-bookie-autorecovery"}, description = "Run Bookie Autorecovery together with broker")
    private boolean runBookieAutoRecovery = false;

    @Parameter(names = {"-bc", "--bookie-conf"}, description = "Configuration file for Bookie")
    private String bookieConfigFile = Paths.get("").toAbsolutePath().normalize().toString() + "/conf/bookkeeper.conf";

    @Parameter(names = {"-rfw", "--run-functions-worker"}, description = "Run functions worker with Broker")
    private boolean runFunctionsWorker = false;

    @Parameter(names = {"-fwc", "--functions-worker-conf"}, description = "Configuration file for Functions Worker")
    private String fnWorkerConfigFile = Paths.get("").toAbsolutePath().normalize().toString() + "/conf/functions_worker.yml";

    @Parameter(names = {"-h", "--help"}, description = "Show this help message")
    private boolean help = false;
}

//通用配置加载器
private static ServerConfiguration readBookieConfFile(String bookieConfigFile) throws IllegalArgumentException {
    ServerConfiguration bookieConf = new ServerConfiguration();
    try {
        bookieConf.loadConf(new File(bookieConfigFile).toURI().toURL());
        bookieConf.validate();
        log.info("Using bookie configuration file {}", bookieConfigFile);
    } catch (MalformedURLException e) {
        log.error("Could not open configuration file: {}", bookieConfigFile, e);
        throw new IllegalArgumentException("Could not open configuration file");
    } catch (ConfigurationException e) {
        log.error("Malformed configuration file: {}", bookieConfigFile, e);
        throw new IllegalArgumentException("Malformed configuration file");
    }
    return bookieConf;
}

```

构造方法主要执行如下功能：

* 命令解析
* 配置文件加载与验证
* 服务初始化

#### 3. BrokerStarter 启动方法

```java

public void start() throws Exception {
    if (bookieStatsProvider != null) {
        bookieStatsProvider.start(bookieConfig);
        log.info("started bookieStatsProvider.");
    }
    if (bookieServer != null) {
        bookieServer.start();
        log.info("started bookieServer.");
    }
    if (autoRecoveryMain != null) {
        autoRecoveryMain.start();
        log.info("started bookie autoRecoveryMain.");
    }

    pulsarService.start();
    log.info("PulsarService started.");
}

```

bookieStatsProvider 、bookieServer 、autoRecoveryMain 三个组件，如果不为空，则将会启动，
pulsarService 作为核心服务必须启动。具体各个组件服务的初始化和启动过程，接下来的章节会作重点分析。

#### 4. BrokerStarter 阻塞方法

```java

public void join() throws InterruptedException {
    pulsarService.waitUntilClosed();

    if (bookieServer != null) {
        bookieServer.join();
    }
    if (autoRecoveryMain != null) {
        autoRecoveryMain.join();
    }
}

```

此方法主线程有提到，用于阻塞主线程，防止主线程退出，实现主要是信号量，等待JVM触发关闭方法，然后信号被触发，
与此同时，bookieServer 与 autoRecoveryMain 都将会关闭，这样主线程不再阻塞，执行完退出。

#### 5. BrokerStarter 关闭方法

```java

public void shutdown() {
    if (null != functionsWorkerService) {
        functionsWorkerService.stop();
        log.info("Shut down functions worker service successfully.");
    }

    pulsarService.getShutdownService().run();
    log.info("Shut down broker service successfully.");

    if (bookieStatsProvider != null) {
        bookieStatsProvider.stop();
        log.info("Shut down bookieStatsProvider successfully.");
    }
    if (bookieServer != null) {
        bookieServer.shutdown();
        log.info("Shut down bookieServer successfully.");
    }
    if (autoRecoveryMain != null) {
        autoRecoveryMain.shutdown();
        log.info("Shut down autoRecoveryMain successfully.");
    }
}

此方法主线程同样有提到，作为JVM关闭钩子中调用的，确保所有正使用的组件优雅有序退出。

```

### 2. Broker 初始化

上一章节已经了解到 BrokerStarter 所包含的组件、服务以及大概的启动过程，这里，将详细解析 WorkerService、StatsProvider、BookieServer、AutoRecoveryMain、PulsarService 这五大组件或服务是如何初始化、是怎样的启动过程。

#### 1. WorkerService 初始化

##### 1. WorkerService 属性

```java
//工作者配置
private final WorkerConfig workerConfig;
//Pulsar 客户端
private PulsarClient client;
//函数运行时管理器
private FunctionRuntimeManager functionRuntimeManager;
//函数元数据管理器
private FunctionMetaDataManager functionMetaDataManager;
//集群服务协调器
private ClusterServiceCoordinator clusterServiceCoordinator;
// DistributedLog Namespace 用于存储函数jars
private Namespace dlogNamespace;
// 存储客户端，函数用于访问状态存储器
private StorageAdminClient stateStoreAdminClient;
//成员管理器
private MembershipManager membershipManager;
//调度管理器
private SchedulerManager schedulerManager;
private boolean isInitialized = false;
//用于状态更新的定时服务
private final ScheduledExecutorService statsUpdater;
//认证服务
private AuthenticationService authenticationService;
//连接管理器
private ConnectorsManager connectorsManager;
//Pulsar admin 客户端
private PulsarAdmin brokerAdmin;
private PulsarAdmin functionAdmin;
//指标生成器
private final MetricsGenerator metricsGenerator;
//定时执行器
private final ScheduledExecutorService executor;
//log存储路径
@VisibleForTesting
private URI dlogUri;

```

##### 2. WorkerService 构造方法

主要是初始化线程池

```java

 public WorkerService(WorkerConfig workerConfig) {
    this.workerConfig = workerConfig;
    this.statsUpdater = Executors
            .newSingleThreadScheduledExecutor(new DefaultThreadFactory("worker-stats-updater"));
    this.executor = Executors.newScheduledThreadPool(10, new DefaultThreadFactory("pulsar-worker"));
    this.metricsGenerator = new MetricsGenerator(this.statsUpdater, workerConfig);
}

```

#### 2. PulsarService 初始化

Pulsar Broker 核心服务

##### 1. PulsarService 属性

```java
//服务配置
private ServiceConfiguration config = null;
//命名空间服务
private NamespaceService nsservice = null;
//Ledger 客户端工厂类（用于创建Ledger 客户端）
private ManagedLedgerClientFactory managedLedgerClientFactory = null;
//领导者选举服务
private LeaderElectionService leaderElectionService = null;
//Broker 服务
private BrokerService brokerService = null;
//Web服务，处理Http请求用
private WebService webService = null;
//WebSocket服务，用于支持WebSocket协议
private WebSocketService webSocketService = null;
//配置缓存服务
private ConfigurationCacheService configurationCacheService = null;
//本地Zookeeper缓存服务
private LocalZooKeeperCacheService localZkCacheService = null;
//Bookeeper 客户端工厂
private BookKeeperClientFactory bkClientFactory;
//Zookeeper缓存（用于本地集群配置和状态管理）
private ZooKeeperCache localZkCache;
//全局Zookeeper缓存（用于跨地域复制集群配置）
private GlobalZooKeeperCache globalZkCache;
//本地Zookeer连接服务提供者
private LocalZooKeeperConnectionService localZooKeeperConnectionProvider;
//压缩器
private Compactor compactor;

//调度线程池
private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
        new DefaultThreadFactory("pulsar"));
//调度线程池（用于ZK缓存更新回调）
private final ScheduledExecutorService cacheExecutor = Executors.newScheduledThreadPool(10,
        new DefaultThreadFactory("zk-cache-callback"));
//顺序执行线程池
private final OrderedExecutor orderedExecutor = OrderedExecutor.newBuilder().numThreads(8).name("pulsar-ordered")
        .build();
//调度线程池（用于负载管理使用，定时更新、上报系统负载信息）
private final ScheduledExecutorService loadManagerExecutor;
//调度线程池 （用于压缩）
private ScheduledExecutorService compactorExecutor;
//顺序调度器 （失去领导时执行器）
private OrderedScheduler offloaderScheduler;
//Ledger 离线加载管理器
private Offloaders offloaderManager = new Offloaders();
//Ledger 离线加载器
private LedgerOffloader offloader;
//负载报告任务
private ScheduledFuture<?> loadReportTask = null;
//负载卸载任务
private ScheduledFuture<?> loadSheddingTask = null;
//负载资源配额任务
private ScheduledFuture<?> loadResourceQuotaTask = null;
//负载管理器（原子引用）
private final AtomicReference<LoadManager> loadManager = new AtomicReference<>();
//Pulsar Admin 客户端
private PulsarAdmin adminClient = null;
//Pulsar 客户端
private PulsarClient client = null;
//Zookeeper 客户端工厂
private ZooKeeperClientFactory zkClientFactory = null;
//绑定地址
private final String bindAddress;
//通知地址
private final String advertisedAddress;
//web服务地址（http用）
private final String webServiceAddress;
//web服务地址（https用）
private final String webServiceAddressTls;
//broker服务url（原生协议TCP）
private final String brokerServiceUrl;
//broker服务url（原生协议TCP+安全加密通道）
private final String brokerServiceUrlTls;
//版本
private final String brokerVersion;
//Schema注册服务
private SchemaRegistryService schemaRegistryService = null;
//可选 工作者服务 （支持函数框架）
private final Optional<WorkerService> functionWorkerService;
//系统关闭hook
private final MessagingServiceShutdownHook shutdownService;
//指标生成器
private MetricsGenerator metricsGenerator;

public enum State {
    Init, Started, Closed
}
//Pulsar 状态
private volatile State state;
// 互斥锁（用于启动、关闭）
private final ReentrantLock mutex = new ReentrantLock();
// 监视 Pulsar 是否已关闭
private final Condition isClosedCondition = mutex.newCondition();
```

##### 2. PulsarService 构造方法

```java

//构造方法 只有服务配置参数
public PulsarService(ServiceConfiguration config) {
    this(config, Optional.empty());
}

//构造方法，增加可选函数工作者服务
public PulsarService(ServiceConfiguration config, Optional<WorkerService> functionWorkerService) {
    // 验证当前配置是否可以
    PulsarConfigurationLoader.isComplete(config);
    //设置当前状态为初始化
    state = State.Init;
    //如果绑定地址没设置，则取主机名
    this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getBindAddress());
    //同上
    this.advertisedAddress = advertisedAddress(config);
    //设置http相关ip和端口
    this.webServiceAddress = webAddress(config);
    //设置https相关ip和端口
    this.webServiceAddressTls = webAddressTls(config);
    //设置pulsar://ip:port
    this.brokerServiceUrl = brokerUrl(config);
    //设置pulsar+ssl://ip:port
    this.brokerServiceUrlTls = brokerUrlTls(config);
    //加载版本信息
    this.brokerVersion = PulsarBrokerVersionStringUtils.getNormalizedVersionString();
    this.config = config;
    //注册系统退出时的动作
    this.shutdownService = new MessagingServiceShutdownHook(this);
    //创建负载管理线程池
    this.loadManagerExecutor = Executors
            .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-load-manager"));
    this.functionWorkerService = functionWorkerService;
}

```

#### 3. BookieServer 初始化

如果配置指定`-runBookie`,那么 `BookieServer` 将会创建并启动。BookieServer 创建除了依赖配置文件，也依赖`StatsProvider`组件，其主要功能用于系统各种指标收集。`StatsProvider` 接口在 bookkeeper 中有多个实现，用户通过配置自行决定采用哪个状态提供者。

##### 1. BookieServer 属性

```java
// 服务器配置
final ServerConfiguration conf;
// Bookie Netty 服务器
BookieNettyServer nettyServer;
// 运行标志
private volatile boolean running = false;
// bookie
private final Bookie bookie;
// 关闭监视器
DeathWatcher deathWatcher;
private static final Logger LOG = LoggerFactory.getLogger(BookieServer.class);

int exitCode = ExitCode.OK;
// 请求处理器
private final RequestProcessor requestProcessor;
// 状态记录器
private final StatsLogger statsLogger;
// 未捕获异常处理方法
private volatile UncaughtExceptionHandler uncaughtExceptionHandler = null;

```

##### 2. BookieServer 构造方法

```java

public BookieServer(ServerConfiguration conf) throws IOException,
        KeeperException, InterruptedException, BookieException,
        UnavailableException, CompatibilityException, SecurityException {
    this(conf, NullStatsLogger.INSTANCE);
}

public BookieServer(ServerConfiguration conf, StatsLogger statsLogger)
        throws IOException, KeeperException, InterruptedException,
        BookieException, UnavailableException, CompatibilityException, SecurityException {
    this.conf = conf;
    //校验用户是否有权限启动
    validateUser(conf);
    String configAsString;
    try {
        configAsString = conf.asJson();
        LOG.info(configAsString);
    } catch (ParseJsonException pe) {
        LOG.error("Got ParseJsonException while converting Config to JSONString", pe);
    }
    //根据配置创建内存分配器
    ByteBufAllocator allocator = getAllocator(conf);
    this.statsLogger = statsLogger;
    //创建 Netty 服务器处理 bookie 请求
    this.nettyServer = new BookieNettyServer(this.conf, null, allocator);
    try {
        this.bookie = newBookie(conf, allocator);
    } catch (IOException | KeeperException | InterruptedException | BookieException e) {
        // interrupted on constructing a bookie
        this.nettyServer.shutdown();
        throw e;
    }
    //安全处理工厂，主要用于支持TLS
    final SecurityHandlerFactory shFactory;

    shFactory = SecurityProviderFactoryFactory
            .getSecurityProviderFactory(conf.getTLSProviderFactoryClass());
    // bookie 请求处理器，用于 netty Server
    this.requestProcessor = new BookieRequestProcessor(conf, bookie,
            statsLogger.scope(SERVER_SCOPE), shFactory, bookie.getAllocator());
    this.nettyServer.setRequestProcessor(this.requestProcessor);
}

```

#### 4. AutoRecoveryMain 初始化

##### 1. AutoRecoveryMain 属性

```java

private final ServerConfiguration conf;
// Bookeeper 客户端
final BookKeeper bkc;
// 审计员选举
final AuditorElector auditorElector;
// 副本工作者
final ReplicationWorker replicationWorker;
//自动恢复宕机监视器
final AutoRecoveryDeathWatcher deathWatcher;
int exitCode;
//正关闭标志
private volatile boolean shuttingDown = false;
//运行标志
private volatile boolean running = false;
// 未捕获异常处理器
private volatile UncaughtExceptionHandler uncaughtExceptionHandler = null;

```

##### 2. AutoRecoveryMain 构造方法

```java

public AutoRecoveryMain(ServerConfiguration conf) throws IOException,
        InterruptedException, KeeperException, UnavailableException,
        CompatibilityException {
    this(conf, NullStatsLogger.INSTANCE);
}

public AutoRecoveryMain(ServerConfiguration conf, StatsLogger statsLogger)
        throws IOException, InterruptedException, KeeperException, UnavailableException,
        CompatibilityException {
    this.conf = conf;
    // 创建 bookkeeper 客户端
    this.bkc = Auditor.createBookKeeperClient(conf);
    MetadataClientDriver metadataClientDriver = bkc.getMetadataClientDriver();
    // 设置会话状态监听器（这里用于连接 bookkeeper 会话过期，将关闭AutoRecoveryMain）
    metadataClientDriver.setSessionStateListener(() -> {
        LOG.error("Client connection to the Metadata server has expired, so shutting down AutoRecoveryMain!");
        shutdown(ExitCode.ZK_EXPIRED);
    });
    // 创建审计员选举服务
    auditorElector = new AuditorElector(
        Bookie.getBookieAddress(conf).toString(),
        conf,
        bkc,
        statsLogger.scope(AUDITOR_SCOPE),
        false);
    // 创建副本工作者服务（复制数据）
    replicationWorker = new ReplicationWorker(
        conf,
        bkc,
        false,
        statsLogger.scope(REPLICATION_WORKER_SCOPE));
    // 创建自动恢复宕机监视器
    deathWatcher = new AutoRecoveryDeathWatcher(this);
}

```

### 3. Broker 启动过程

#### 1. WorkerService 启动过程

```java

public void start(URI dlogUri) throws InterruptedException {
    log.info("Starting worker {}...", workerConfig.getWorkerId());
    //创建新的 broker admin 客户端（包括认证相关，TLS配置）
    this.brokerAdmin = Utils.getPulsarAdminClient(workerConfig.getPulsarWebServiceUrl(),
            workerConfig.getClientAuthenticationPlugin(), workerConfig.getClientAuthenticationParameters(),
            workerConfig.getTlsTrustCertsFilePath(), workerConfig.isTlsAllowInsecureConnection());
    //优先使用function service url
    final String functionWebServiceUrl = StringUtils.isNotBlank(workerConfig.getFunctionWebServiceUrl())
            ? workerConfig.getFunctionWebServiceUrl()
            : workerConfig.getWorkerWebAddress();
    //创建新的 function admin 客户端（包括认证相关，TLS配置）
    this.functionAdmin = Utils.getPulsarAdminClient(functionWebServiceUrl,
            workerConfig.getClientAuthenticationPlugin(), workerConfig.getClientAuthenticationParameters(),
            workerConfig.getTlsTrustCertsFilePath(), workerConfig.isTlsAllowInsecureConnection());
    // 打印配置
    try {
        log.info("Worker Configs: {}", new ObjectMapper().writerWithDefaultPrettyPrinter()
                .writeValueAsString(workerConfig));
    } catch (JsonProcessingException e) {
        log.warn("Failed to print worker configs with error {}", e.getMessage(), e);
    }

    //创建 dlog namespace 存储函数包
    this.dlogUri = dlogUri;
    //这里创建 DistributedLog 配置文件
    DistributedLogConfiguration dlogConf = Utils.getDlogConf(workerConfig);
    try {
        this.dlogNamespace = NamespaceBuilder.newBuilder()
                .conf(dlogConf)
                .clientId("function-worker-" + workerConfig.getWorkerId())
                .uri(this.dlogUri)
                .build();
    } catch (Exception e) {
        log.error("Failed to initialize dlog namespace {} for storing function packages",
                dlogUri, e);
        throw new RuntimeException(e);
    }

    //创建状态存储客户端用于存储函数状态
    if (workerConfig.getStateStorageServiceUrl() != null) {
        StorageClientSettings clientSettings = StorageClientSettings.newBuilder()
            .serviceUri(workerConfig.getStateStorageServiceUrl())
            .build();
        this.stateStoreAdminClient = StorageClientBuilder.newBuilder()
            .withSettings(clientSettings)
            .buildAdmin();
    }

    // 初始化函数元数据管理器
    try {
        //这里创建一个 Pulsar 客户端
        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(this.workerConfig.getPulsarServiceUrl());
        if (isNotBlank(workerConfig.getClientAuthenticationPlugin())
                && isNotBlank(workerConfig.getClientAuthenticationParameters())) {
            clientBuilder.authentication(workerConfig.getClientAuthenticationPlugin(),
                    workerConfig.getClientAuthenticationParameters());
        }
        clientBuilder.enableTls(workerConfig.isUseTls());
        clientBuilder.allowTlsInsecureConnection(workerConfig.isTlsAllowInsecureConnection());
        clientBuilder.tlsTrustCertsFilePath(workerConfig.getTlsTrustCertsFilePath());
        clientBuilder.enableTlsHostnameVerification(workerConfig.isTlsHostnameVerificationEnable());
        this.client = clientBuilder.build();
        log.info("Created Pulsar client");

        //创建调度管理器
        this.schedulerManager = new SchedulerManager(this.workerConfig, this.client, this.brokerAdmin,
                this.executor);

        //创建函数元数据管理器
        this.functionMetaDataManager = new FunctionMetaDataManager(
                this.workerConfig, this.schedulerManager, this.client);
        //创建连接池管理器
        this.connectorsManager = new ConnectorsManager(workerConfig);

        //创建成员管理器
        this.membershipManager = new MembershipManager(this, this.client);

        //创建函数运行时管理器
        this.functionRuntimeManager = new FunctionRuntimeManager(
                this.workerConfig, this, this.dlogNamespace, this.membershipManager, connectorsManager, functionMetaDataManager);

        //协调管理器设置管理器
        this.schedulerManager.setFunctionMetaDataManager(this.functionMetaDataManager);
        this.schedulerManager.setFunctionRuntimeManager(this.functionRuntimeManager);
        this.schedulerManager.setMembershipManager(this.membershipManager);

        //初始化函数元数据管理器
        this.functionMetaDataManager.initialize();

        //初始化函数运行时管理器
        this.functionRuntimeManager.initialize();
        //创建认证服务
        authenticationService = new AuthenticationService(PulsarConfigurationLoader.convertFrom(workerConfig));

        log.info("Start cluster services...");
        //集群服务协调器
        this.clusterServiceCoordinator = new ClusterServiceCoordinator(
                this.workerConfig.getWorkerId(),
                membershipManager);
        //增加成员监控任务
        this.clusterServiceCoordinator.addTask("membership-monitor",
                this.workerConfig.getFailureCheckFreqMs(),
                () -> membershipManager.checkFailures(
                        functionMetaDataManager, functionRuntimeManager, schedulerManager));
        //启动集群服务
        this.clusterServiceCoordinator.start();

        //启动函数运行时管理器
        this.functionRuntimeManager.start();

        //表明函数工作者服务已启动完毕
        this.isInitialized = true;
        //创建连接管理器（主要加载 Source、Sink 连接类配置）
        this.connectorsManager = new ConnectorsManager(workerConfig);

    } catch (Throwable t) {
        log.error("Error Starting up in worker", t);
        throw new RuntimeException(t);
    }
}

//Pulsar Admin 将在后面章节详细解析，此方法构建一个 Pulsar Admin 客户端
public static PulsarAdmin getPulsarAdminClient(String pulsarWebServiceUrl, String authPlugin, String authParams, String tlsTrustCertsFilePath, boolean allowTlsInsecureConnection) {
    try {
        PulsarAdminBuilder adminBuilder = PulsarAdmin.builder().serviceHttpUrl(pulsarWebServiceUrl);
        if (isNotBlank(authPlugin) && isNotBlank(authParams)) {
            adminBuilder.authentication(authPlugin, authParams);
        }
        if (isNotBlank(tlsTrustCertsFilePath)) {
            adminBuilder.tlsTrustCertsFilePath(tlsTrustCertsFilePath);
        }
        adminBuilder.allowTlsInsecureConnection(allowTlsInsecureConnection);
        return adminBuilder.build();
    } catch (PulsarClientException e) {
        log.error("Error creating pulsar admin client", e);
        throw new RuntimeException(e);
    }
}

```

#### 2. PulsarService 启动过程

```java

public void start() throws PulsarServerException {
    mutex.lock();
    // 打印系统版本信息
    LOG.info("Starting Pulsar Broker service; version: '{}'", ( brokerVersion != null ? brokerVersion : "unknown" )  );
    LOG.info("Git Revision {}", PulsarBrokerVersionStringUtils.getGitSha());
    LOG.info("Built by {} on {} at {}",
                PulsarBrokerVersionStringUtils.getBuildUser(),
                PulsarBrokerVersionStringUtils.getBuildHost(),
                PulsarBrokerVersionStringUtils.getBuildTime());

    try {
        // 状态检查
        if (state != State.Init) {
            throw new PulsarServerException("Cannot start the service once it was stopped");
        }

        // 1. 首先连接本地Zookeeper，并注册系统关闭hook
        localZooKeeperConnectionProvider = new LocalZooKeeperConnectionService(getZooKeeperClientFactory(),
                config.getZookeeperServers(), config.getZooKeeperSessionTimeoutMillis());
        localZooKeeperConnectionProvider.start(shutdownService);

        //2. 初始化和启动访问配置仓库，包括本地集群缓存和全局集群缓存
        this.startZkCacheService();
        //3. 创建 bookkeeper 客户端工厂
        this.bkClientFactory = newBookKeeperClientFactory();
        //   创建管理 Ledger 管理客户端工厂
        managedLedgerClientFactory = new ManagedLedgerClientFactory(config, getZkClient(), bkClientFactory);
        //4. 创建并初始化 broker 服务
        this.brokerService = new BrokerService(this);

        //5. 创建并设置负载管理器
        this.loadManager.set(LoadManager.create(this));

        //6. 启动领导者选举服务
        startLeaderElectionService();

        //7. 创建并启动 Namespace 服务
        this.startNamespaceService();
        //8. 创建ledger离线加载器
        this.offloader = createManagedLedgerOffloader(this.getConfiguration());
        //9. 启动 broker 服务
        brokerService.start();
        //10. 创建并初始化Web服务
        this.webService = new WebService(this);
        Map<String, Object> attributeMap = Maps.newHashMap();
        attributeMap.put(WebService.ATTRIBUTE_PULSAR_NAME, this);
        Map<String, Object> vipAttributeMap = Maps.newHashMap();
        vipAttributeMap.put(VipStatus.ATTRIBUTE_STATUS_FILE_PATH, this.config.getStatusFilePath());
        vipAttributeMap.put(VipStatus.ATTRIBUTE_IS_READY_PROBE, new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                // Ensure the VIP status is only visible when the broker is fully initialized
                return state == State.Started;
            }
        });
        //这里添加请求服务
        this.webService.addRestResources("/", VipStatus.class.getPackage().getName(), false, vipAttributeMap);
        this.webService.addRestResources("/", "org.apache.pulsar.broker.web", false, attributeMap);
        this.webService.addRestResources("/admin", "org.apache.pulsar.broker.admin.v1", true, attributeMap);
        this.webService.addRestResources("/admin/v2", "org.apache.pulsar.broker.admin.v2", true, attributeMap);
        this.webService.addRestResources("/admin/v3", "org.apache.pulsar.broker.admin.v3", true, attributeMap);
        this.webService.addRestResources("/lookup", "org.apache.pulsar.broker.lookup", true, attributeMap);

        this.webService.addServlet("/metrics",
                new ServletHolder(new PrometheusMetricsServlet(this, config.isExposeTopicLevelMetricsInPrometheus(), config.isExposeConsumerLevelMetricsInPrometheus())),
                false, attributeMap);

        if (config.isWebSocketServiceEnabled()) {
            // 11. 如果Websocket启用，则创建和启动Websocket服务
            this.webSocketService = new WebSocketService(
                    new ClusterData(webServiceAddress, webServiceAddressTls, brokerServiceUrl, brokerServiceUrlTls),
                    config);
            this.webSocketService.start();

            final WebSocketServlet producerWebSocketServlet = new WebSocketProducerServlet(webSocketService);
            this.webService.addServlet(WebSocketProducerServlet.SERVLET_PATH,
                    new ServletHolder(producerWebSocketServlet), true, attributeMap);
            this.webService.addServlet(WebSocketProducerServlet.SERVLET_PATH_V2,
                    new ServletHolder(producerWebSocketServlet), true, attributeMap);

            final WebSocketServlet consumerWebSocketServlet = new WebSocketConsumerServlet(webSocketService);
            this.webService.addServlet(WebSocketConsumerServlet.SERVLET_PATH,
                    new ServletHolder(consumerWebSocketServlet), true, attributeMap);
            this.webService.addServlet(WebSocketConsumerServlet.SERVLET_PATH_V2,
                    new ServletHolder(consumerWebSocketServlet), true, attributeMap);

            final WebSocketServlet readerWebSocketServlet = new WebSocketReaderServlet(webSocketService);
            this.webService.addServlet(WebSocketReaderServlet.SERVLET_PATH,
                    new ServletHolder(readerWebSocketServlet), true, attributeMap);
            this.webService.addServlet(WebSocketReaderServlet.SERVLET_PATH_V2,
                    new ServletHolder(readerWebSocketServlet), true, attributeMap);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Attempting to add static directory");
        }
        this.webService.addStaticResources("/static", "/static");
        // 12. 注册Namespace心跳和启动（确认自己的 Namespace，加载和创建分配自己的 Topic）
        this.nsservice.registerBootstrapNamespaces();
        // 13. 创建Schema注册服务
        schemaRegistryService = SchemaRegistryService.create(this);
        // 14. 启动Web服务器，接受请求
        webService.start();
        // 15. 创建指标生成器
        this.metricsGenerator = new MetricsGenerator(this);
        // 16. 启动负载管理器服务
        this.startLoadManagementService();

        state = State.Started;
        // 17.尝试注册 SLA Namespace 
        acquireSLANamespace();

        // 18. 如果需要，启动函数工作者服务
        this.startWorkerService();
        // 至此，消息服务已经启动完成，等待生产者、消费者连接。
        LOG.info("messaging service is ready, bootstrap service on port={}, broker url={}, cluster={}, configs={}",
                config.getWebServicePort().get(), brokerServiceUrl, config.getClusterName(),
                ReflectionToStringBuilder.toString(config));
    } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        throw new PulsarServerException(e);
    } finally {
        mutex.unlock();
    }
}

```

#### 3. BookieServer 启动过程

```java

public void start() throws IOException, UnavailableException, InterruptedException, BKException {
    // 启动 bookie
    this.bookie.start();
    // 当bookie启动不成功时，则快速失败
    if (!this.bookie.isRunning()) {
        exitCode = bookie.getExitCode();
        this.requestProcessor.close();
        return;
    }
    //netty 服务器启动
    this.nettyServer.start();

    running = true;
    //宕机或关闭监视器
    deathWatcher = new DeathWatcher(conf);
    //设置未捕获异常处理器
    if (null != uncaughtExceptionHandler) {
        deathWatcher.setUncaughtExceptionHandler(uncaughtExceptionHandler);
    }
    //启动监视器
    deathWatcher.start();
    //等待250ms，保证 bookie 完全启动，以正常提供服务
    TimeUnit.MILLISECONDS.sleep(250);
}

```

#### 4. AutoRecoveryMain 启动过程

```java

public void start() throws UnavailableException {
    //启动审计员选举服务
    auditorElector.start();
    //启动副本工作者服务
    replicationWorker.start();
    if (null != uncaughtExceptionHandler) {
        deathWatcher.setUncaughtExceptionHandler(uncaughtExceptionHandler);
    }
    //启动监视器
    deathWatcher.start();
    //启动完毕
    running = true;
}

```

到此为止，Pulsar Broker启动流程已基本已经分析完毕，但限于篇幅，只是解析了各服务启动情况。没有再分析各服务内组件的启动流程。

### 4. Broker Client 接口实现

在前面的章节中，已经分析了 Pulsar Client 以及相关的 Producer 和 Consumer ，接下来的接口分析就按照原来的
思路，即按照消息发送、消息消费两个核心展开，以时序来依次分析其方法调用，然后再分析其特性。

#### 1. 公共方法实现

##### 1. 获取 Schema 信息

```java

@Override
protected void handleGetSchema(CommandGetSchema commandGetSchema) {
    if (log.isDebugEnabled()) {
        log.debug("Received CommandGetSchema call from {}", remoteAddress);
    }

    long requestId = commandGetSchema.getRequestId();
    // 默认空 SchemaVersion
    SchemaVersion schemaVersion = SchemaVersion.Latest;
    // 如果请求命令有 SchemaVersion 版本设置
    if (commandGetSchema.hasSchemaVersion()) {
        schemaVersion = schemaService.versionFromBytes(commandGetSchema.getSchemaVersion().toByteArray());
    }
    // Schema 名，如果获取失败，则为不可用 Topic 名
    String schemaName;
    try {
        schemaName = TopicName.get(commandGetSchema.getTopic()).getSchemaName();
    } catch (Throwable t) {
        ctx.writeAndFlush(
                Commands.newGetSchemaResponseError(requestId, ServerError.InvalidTopicName, t.getMessage()));
        return;
    }
    // 从 bookkeeper 读取 Schema 信息
    schemaService.getSchema(schemaName, schemaVersion).thenAccept(schemaAndMetadata -> {
        if (schemaAndMetadata == null) {
            ctx.writeAndFlush(Commands.newGetSchemaResponseError(requestId, ServerError.TopicNotFound,
                    "Topic not found or no-schema"));
        } else {
            ctx.writeAndFlush(Commands.newGetSchemaResponse(requestId,
                    SchemaInfoUtil.newSchemaInfo(schemaName, schemaAndMetadata.schema), schemaAndMetadata.version));
        }
    }).exceptionally(ex -> {
        ctx.writeAndFlush(
                Commands.newGetSchemaResponseError(requestId, ServerError.UnknownError, ex.getMessage()));
        return null;
    });
}

```

##### 2. 获取 PartitionedTopicMetadata 信息

```java

@Override
protected void handlePartitionMetadataRequest(CommandPartitionedTopicMetadata partitionMetadata) {
    final long requestId = partitionMetadata.getRequestId();
    if (log.isDebugEnabled()) {
        log.debug("[{}] Received PartitionMetadataLookup from {} for {}", partitionMetadata.getTopic(),
                remoteAddress, requestId);
    }
    // 校验 Topic 合法性
    TopicName topicName = validateTopicName(partitionMetadata.getTopic(), requestId, partitionMetadata);
    if (topicName == null) {
        return;
    }
    // 获取 Lookup 命令信号量，用于控制并发数
    final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
    //尝试获取
    if (lookupSemaphore.tryAcquire()) {
        // 如果认证与授权被启用时，当连接时候，如果认证角色是代理的角色之一时，必须强制如下规则
        // * originalPrincipal 不能为空
        // * originalPrincipal 不能是以 proxy 身份
        if (invalidOriginalPrincipal(originalPrincipal)) {
            final String msg = "Valid Proxy Client role should be provided for getPartitionMetadataRequest ";
            log.warn("[{}] {} with role {} and proxyClientAuthRole {} on topic {}", remoteAddress, msg, authRole,
                    originalPrincipal, topicName);
            ctx.writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.AuthorizationError,
                    msg, requestId));
            lookupSemaphore.release();
            return;
        }
        CompletableFuture<Boolean> isProxyAuthorizedFuture;
        //如果授权服务启用，并且原所有者不为空，则检查是否有 lookup 权限
        if (service.isAuthorizationEnabled() && originalPrincipal != null) {
            isProxyAuthorizedFuture = service.getAuthorizationService()
                    .canLookupAsync(topicName, authRole, authenticationData);
        } else {
            isProxyAuthorizedFuture = CompletableFuture.completedFuture(true);
        }
        String finalOriginalPrincipal = originalPrincipal;
        isProxyAuthorizedFuture.thenApply(isProxyAuthorized -> {
                 //有权限
                if (isProxyAuthorized) {
                getPartitionedTopicMetadata(getBrokerService().pulsar(),
                                            authRole, finalOriginalPrincipal, authenticationData,
                        topicName).handle((metadata, ex) -> {
                                if (ex == null) {
                                    //成功获取到分区信息，这些写应答命令
                                    int partitions = metadata.partitions;
                                    ctx.writeAndFlush(Commands.newPartitionMetadataResponse(partitions, requestId));
                                } else {
                                    //异常处理
                                    if (ex instanceof PulsarClientException) {
                                        log.warn("Failed to authorize {} at [{}] on topic {} : {}", getRole(),
                                                remoteAddress, topicName, ex.getMessage());
                                        ctx.writeAndFlush(Commands.newPartitionMetadataResponse(
                                                ServerError.AuthorizationError, ex.getMessage(), requestId));
                                    } else {
                                        log.warn("Failed to get Partitioned Metadata [{}] {}: {}", remoteAddress,
                                                topicName, ex.getMessage(), ex);
                                        ServerError error = (ex instanceof RestException)
                                                && ((RestException) ex).getResponse().getStatus() < 500
                                                        ? ServerError.MetadataError : ServerError.ServiceNotReady;
                                        ctx.writeAndFlush(Commands.newPartitionMetadataResponse(error,
                                                ex.getMessage(), requestId));
                                    }
                                }
                                lookupSemaphore.release();
                                return null;
                            });
                } else {
                    //客户端没有权限获取分区元数据
                    final String msg = "Proxy Client is not authorized to Get Partition Metadata";
                    log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, authRole, topicName);
                    ctx.writeAndFlush(
                            Commands.newPartitionMetadataResponse(ServerError.AuthorizationError, msg, requestId));
                    lookupSemaphore.release();
                }
                return null;
        }).exceptionally(ex -> {
            final String msg = "Exception occured while trying to authorize get Partition Metadata";
            log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, authRole, topicName);
            ctx.writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.AuthorizationError, msg, requestId));
            lookupSemaphore.release();
            return null;
        });
    } else {
        //获取失败， 返回 Lookup 命令请求数太多，稍后再试
        if (log.isDebugEnabled()) {
            log.debug("[{}] Failed Partition-Metadata lookup due to too many lookup-requests {}", remoteAddress,
                    topicName);
        }
        ctx.writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.TooManyRequests,
                "Failed due to too many pending lookup requests", requestId));
    }
}

// 获取 Topic 的分区元信息
public static CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(PulsarService pulsar,
            String clientAppId, String originalPrincipal, AuthenticationDataSource authenticationData, TopicName topicName) {
    CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();
    try {
        // 1. 权限校验
        try {
            checkAuthorization(pulsar, topicName, clientAppId, authenticationData);
        } catch (RestException e) {
            try {
                //租户校验
                validateAdminAccessForTenant(pulsar, clientAppId, originalPrincipal, topicName.getTenant());
            } catch (RestException authException) {
                log.warn("Failed to authorize {} on cluster {}", clientAppId, topicName.toString());
                throw new PulsarClientException(String.format("Authorization failed %s on topic %s with error %s",
                        clientAppId, topicName.toString(), authException.getMessage()));
            }
        } catch (Exception ex) {
            // 抛出来的异常如果没有包装成 PulsarClientException，这未知错误将被标记为内部服务器错误
            log.warn("Failed to authorize {} on cluster {} with unexpected exception {}", clientAppId,
                    topicName.toString(), ex.getMessage(), ex);
            throw ex;
        }

        String path = path(PARTITIONED_TOPIC_PATH_ZNODE, topicName.getNamespace(), topicName.getDomain().toString(),
                topicName.getEncodedLocalName());

        //验证global-namespace包含本地/对等集群：如果存在对等/本地集群，则查找可以提供/重定向请求，否则分区元数据请求将失败，所以，客户端在创建生产者/消费者时也将失败
        checkLocalOrGetPeerReplicationCluster(pulsar, topicName.getNamespaceObject())
                // 通过集群数据查询 Topic 的分区信息
                .thenCompose(res -> fetchPartitionedTopicMetadataAsync(pulsar, path)).thenAccept(metadata -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Total number of partitions for topic {} is {}", clientAppId, topicName,
                                metadata.partitions);
                    }
                    metadataFuture.complete(metadata);
                }).exceptionally(ex -> {
                    metadataFuture.completeExceptionally(ex.getCause());
                    return null;
                });
    } catch (Exception ex) {
        metadataFuture.completeExceptionally(ex);
    }
    return metadataFuture;
}

// 检查本地货对等副本集群
public static CompletableFuture<ClusterData> checkLocalOrGetPeerReplicationCluster(PulsarService pulsarService,
            NamespaceName namespace) {
    // 如果 Namespace 不是全局的，则直接返回
    if (!namespace.isGlobal()) {
        return CompletableFuture.completedFuture(null);
    }
    final CompletableFuture<ClusterData> validationFuture = new CompletableFuture<>();
    final String localCluster = pulsarService.getConfiguration().getClusterName();
    final String path = AdminResource.path(POLICIES, namespace.toString());
    //查询 Namespace 策略信息
    pulsarService.getConfigurationCache().policiesCache().getAsync(path).thenAccept(policiesResult -> {
        if (policiesResult.isPresent()) {
            // 获取策略信息
            Policies policies = policiesResult.get();
            // 策略中副本集群为空，则返回异常
            if (policies.replication_clusters.isEmpty()) {
                String msg = String.format(
                        "Namespace does not have any clusters configured : local_cluster=%s ns=%s",
                        localCluster, namespace.toString());
                log.warn(msg);
                validationFuture.completeExceptionally(new RestException(Status.PRECONDITION_FAILED, msg));
            // 策略中副本集群不包含本地集群，则继续查询策略副本中的集群信息
            } else if (!policies.replication_clusters.contains(localCluster)) {
                ClusterData ownerPeerCluster = getOwnerFromPeerClusterList(pulsarService,
                        policies.replication_clusters);
                if (ownerPeerCluster != null) {
                    // 找到对等集群数据
                    validationFuture.complete(ownerPeerCluster);
                    return;
                }
                //未找到 Namespace 中本地集群信息
                String msg = String.format(
                        "Namespace missing local cluster name in clusters list: local_cluster=%s ns=%s clusters=%s",
                        localCluster, namespace.toString(), policies.replication_clusters);

                log.warn(msg);
                validationFuture.completeExceptionally(new RestException(Status.PRECONDITION_FAILED, msg));
            } else {
                // 这里返回正常成功
                validationFuture.complete(null);
            }
        } else {
            // 策略信息没有找到，返回异常
            String msg = String.format("Policies not found for %s namespace", namespace.toString());
            log.error(msg);
            validationFuture.completeExceptionally(new RestException(Status.NOT_FOUND, msg));
        }
    }).exceptionally(ex -> {
        String msg = String.format("Failed to validate global cluster configuration : cluster=%s ns=%s  emsg=%s",
                localCluster, namespace, ex.getMessage());
        log.error(msg);
        validationFuture.completeExceptionally(new RestException(ex));
        return null;
    });
    return validationFuture;
}

 private static ClusterData getOwnerFromPeerClusterList(PulsarService pulsar, Set<String> replicationClusters) {
    String currentCluster = pulsar.getConfiguration().getClusterName();
    if (replicationClusters == null || replicationClusters.isEmpty() || isBlank(currentCluster)) {
        return null;
    }

    try {
        //从集群缓存中查询本地集群信息
        Optional<ClusterData> cluster = pulsar.getConfigurationCache().clustersCache()
                .get(path("clusters", currentCluster));
        // 集群信息为空或者对等集群名称为空，则返回空
        if (!cluster.isPresent() || cluster.get().getPeerClusterNames() == null) {
            return null;
        }
        // 从本地集群中全局配置匹配副本集群信息，如果匹配成功，则继续取集群信息，获取失败则抛异常，匹配失败则返回空
        for (String peerCluster : cluster.get().getPeerClusterNames()) {
            if (replicationClusters.contains(peerCluster)) {
                return pulsar.getConfigurationCache().clustersCache().get(path("clusters", peerCluster))
                        .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                                "Peer cluster " + peerCluster + " data not found"));
            }
        }
    } catch (Exception e) {
        log.error("Failed to get peer-cluster {}-{}", currentCluster, e.getMessage());
        if (e instanceof RestException) {
            throw (RestException) e;
        } else {
            throw new RestException(e);
        }
    }
    return null;
}

protected static CompletableFuture<PartitionedTopicMetadata> fetchPartitionedTopicMetadataAsync(
            PulsarService pulsar, String path) {
    CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();
    try {
        // 从 Zk 缓存中获取 Topic 分区信息
        pulsar.getGlobalZkCache().getDataAsync(path, new Deserializer<PartitionedTopicMetadata>() {
            @Override
            public PartitionedTopicMetadata deserialize(String key, byte[] content) throws Exception {
                return jsonMapper().readValue(content, PartitionedTopicMetadata.class);
            }
        }).thenAccept(metadata -> {
            // 如果 Topic 没有找到分区信息，则它不是分区 Topic
            if (metadata.isPresent()) {
                metadataFuture.complete(metadata.get());
            } else {
                metadataFuture.complete(new PartitionedTopicMetadata());//默认0分区，也就是未分区
            }
        }).exceptionally(ex -> {
            metadataFuture.completeExceptionally(ex);
            return null;
        });
    } catch (Exception e) {
        metadataFuture.completeExceptionally(e);
    }
    return metadataFuture;
}

```

##### 3. 获取 Broker Address

```java

@Override
protected void handleLookup(CommandLookupTopic lookup) {
    final long requestId = lookup.getRequestId();
    //是否认证
    final boolean authoritative = lookup.getAuthoritative();
    if (log.isDebugEnabled()) {
        log.debug("[{}] Received Lookup from {} for {}", lookup.getTopic(), remoteAddress, requestId);
    }

    TopicName topicName = validateTopicName(lookup.getTopic(), requestId, lookup);
    if (topicName == null) {
        return;
    }
    // 获取 Lookup 命令信号量，用于控制并发数
    final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
    // 尝试请求
    if (lookupSemaphore.tryAcquire()) {
        // 如果认证与授权被启用时，当连接时候，如果认证角色是代理的角色之一时，必须强制如下规则
        // * originalPrincipal 不能为空
        // * originalPrincipal 不能是以 proxy 身份
        if (invalidOriginalPrincipal(originalPrincipal)) {
            final String msg = "Valid Proxy Client role should be provided for lookup ";
            log.warn("[{}] {} with role {} and proxyClientAuthRole {} on topic {}", remoteAddress, msg, authRole,
                    originalPrincipal, topicName);
            ctx.writeAndFlush(newLookupErrorResponse(ServerError.AuthorizationError, msg, requestId));
            lookupSemaphore.release();
            return;
        }
        CompletableFuture<Boolean> isProxyAuthorizedFuture;
        if (service.isAuthorizationEnabled() && originalPrincipal != null) {
            isProxyAuthorizedFuture = service.getAuthorizationService().canLookupAsync(topicName, authRole,
                    authenticationData);
        } else {
            isProxyAuthorizedFuture = CompletableFuture.completedFuture(true);
        }
        String finalOriginalPrincipal = originalPrincipal;
        isProxyAuthorizedFuture.thenApply(isProxyAuthorized -> {
            //已授权
            if (isProxyAuthorized) {
                lookupTopicAsync(getBrokerService().pulsar(), topicName, authoritative,
                        finalOriginalPrincipal != null ? finalOriginalPrincipal : authRole, authenticationData,
                        requestId).handle((lookupResponse, ex) -> {
                            if (ex == null) {
                                ctx.writeAndFlush(lookupResponse);
                            } else {
                                // it should never happen
                                log.warn("[{}] lookup failed with error {}, {}", remoteAddress, topicName,
                                        ex.getMessage(), ex);
                                ctx.writeAndFlush(newLookupErrorResponse(ServerError.ServiceNotReady,
                                        ex.getMessage(), requestId));
                            }
                            lookupSemaphore.release();
                            return null;
                        });
            } else {
                // 未授权异常
                final String msg = "Proxy Client is not authorized to Lookup";
                log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, authRole, topicName);
                ctx.writeAndFlush(newLookupErrorResponse(ServerError.AuthorizationError, msg, requestId));
                lookupSemaphore.release();
            }
            return null;
        }).exceptionally(ex -> {
            // 未授权异常
            final String msg = "Exception occured while trying to authorize lookup";
            log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, authRole, topicName, ex);
            ctx.writeAndFlush(newLookupErrorResponse(ServerError.AuthorizationError, msg, requestId));
            lookupSemaphore.release();
            return null;
        });
    } else {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Failed lookup due to too many lookup-requests {}", remoteAddress, topicName);
        }
        ctx.writeAndFlush(newLookupErrorResponse(ServerError.TooManyRequests,
                "Failed due to too many pending lookup requests", requestId));
    }
}

// 通过 Topic 名查找 所属的存活的 broker 地址
public static CompletableFuture<ByteBuf> lookupTopicAsync(PulsarService pulsarService, TopicName topicName,
        boolean authoritative, String clientAppId, AuthenticationDataSource authenticationData, long requestId) {

    final CompletableFuture<ByteBuf> validationFuture = new CompletableFuture<>();
    final CompletableFuture<ByteBuf> lookupfuture = new CompletableFuture<>();
    final String cluster = topicName.getCluster();

    // (1) 验证集群信息
    getClusterDataIfDifferentCluster(pulsarService, cluster, clientAppId).thenAccept(differentClusterData -> {

        if (differentClusterData != null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Redirecting the lookup call to {}/{} cluster={}", clientAppId,
                        differentClusterData.getBrokerServiceUrl(), differentClusterData.getBrokerServiceUrlTls(),
                        cluster);
            }
            //表示要重定向
            validationFuture.complete(newLookupResponse(differentClusterData.getBrokerServiceUrl(),
                    differentClusterData.getBrokerServiceUrlTls(), true, LookupType.Redirect, requestId, false));
        } else {
            // (2) 客户端授权
            try {
                checkAuthorization(pulsarService, topicName, clientAppId, authenticationData);
            } catch (RestException authException) {
                log.warn("Failed to authorized {} on cluster {}", clientAppId, topicName.toString());
                validationFuture.complete(newLookupErrorResponse(ServerError.AuthorizationError,
                        authException.getMessage(), requestId));
                return;
            } catch (Exception e) {
                log.warn("Unknown error while authorizing {} on cluster {}", clientAppId, topicName.toString());
                validationFuture.completeExceptionally(e);
                return;
            }
            // (3) 验证全局 Namespace ，上一小节以有分析，这里不再赘述
            checkLocalOrGetPeerReplicationCluster(pulsarService, topicName.getNamespaceObject())
                    .thenAccept(peerClusterData -> {
                        if (peerClusterData == null) {
                            // (4) 所有校验已通过，初始化 lookup
                            validationFuture.complete(null);
                            return;
                        }
                        // 如果存在对等集群数据，则表示该对等集群拥有 namespace，并且请求应重定向到该对等集群
                        if (StringUtils.isBlank(peerClusterData.getBrokerServiceUrl())
                                && StringUtils.isBlank(peerClusterData.getBrokerServiceUrl())) {
                            validationFuture.complete(newLookupErrorResponse(ServerError.MetadataError,
                                    "Redirected cluster's brokerService url is not configured", requestId));
                            return;
                        }
                        validationFuture.complete(newLookupResponse(peerClusterData.getBrokerServiceUrl(),
                                peerClusterData.getBrokerServiceUrlTls(), true, LookupType.Redirect, requestId,
                                false));

                    }).exceptionally(ex -> {
                        validationFuture.complete(
                                newLookupErrorResponse(ServerError.MetadataError, ex.getMessage(), requestId));
                        return null;
                    });
        }
    }).exceptionally(ex -> {
        validationFuture.completeExceptionally(ex);
        return null;
    });

    // 一旦初始化 lookup 命令，验证完成
    validationFuture.thenAccept(validaitonFailureResponse -> {
        //如果验证通过有数据，则直接返回
        if (validaitonFailureResponse != null) {
            lookupfuture.complete(validaitonFailureResponse);
        } else {
            //走到这里表示前面调用并没有查到到数据
            pulsarService.getNamespaceService().getBrokerServiceUrlAsync(topicName, authoritative)
                    .thenAccept(lookupResult -> {

                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Lookup result {}", topicName.toString(), lookupResult);
                        }
                        // 如果这里还是查询结果无数据，则表示当前无 broker 可用
                        if (!lookupResult.isPresent()) {
                            lookupfuture.complete(newLookupErrorResponse(ServerError.ServiceNotReady,
                                    "No broker was available to own " + topicName, requestId));
                            return;
                        }

                        LookupData lookupData = lookupResult.get().getLookupData();
                        // 如果要重定向，则判定下是否要授权（如果是领导者，则需要）
                        if (lookupResult.get().isRedirect()) {
                            boolean newAuthoritative = isLeaderBroker(pulsarService);
                            lookupfuture.complete(
                                    newLookupResponse(lookupData.getBrokerUrl(), lookupData.getBrokerUrlTls(),
                                            newAuthoritative, LookupType.Redirect, requestId, false));
                        } else {
                            // 在单机模式下运行时，我们希望通过服务URL重定向客户端，以便通知的地址配置不再相关。
                            boolean redirectThroughServiceUrl = pulsarService.getConfiguration()
                                    .isRunningStandalone();

                            lookupfuture.complete(newLookupResponse(lookupData.getBrokerUrl(),
                                    lookupData.getBrokerUrlTls(), true /* authoritative */, LookupType.Connect,
                                    requestId, redirectThroughServiceUrl));
                        }
                    }).exceptionally(ex -> {
                        if (ex instanceof CompletionException && ex.getCause() instanceof IllegalStateException) {
                            log.info("Failed to lookup {} for topic {} with error {}", clientAppId,
                                    topicName.toString(), ex.getCause().getMessage());
                        } else {
                            log.warn("Failed to lookup {} for topic {} with error {}", clientAppId,
                                    topicName.toString(), ex.getMessage(), ex);
                        }
                        //服务暂不可用
                        lookupfuture.complete(
                                newLookupErrorResponse(ServerError.ServiceNotReady, ex.getMessage(), requestId));
                        return null;
                    });
        }

    }).exceptionally(ex -> {
        if (ex instanceof CompletionException && ex.getCause() instanceof IllegalStateException) {
            log.info("Failed to lookup {} for topic {} with error {}", clientAppId, topicName.toString(),
                    ex.getCause().getMessage());
        } else {
            log.warn("Failed to lookup {} for topic {} with error {}", clientAppId, topicName.toString(),
                    ex.getMessage(), ex);
        }
        //服务暂不可用
        lookupfuture.complete(newLookupErrorResponse(ServerError.ServiceNotReady, ex.getMessage(), requestId));
        return null;
    });

    return lookupfuture;
}

protected static CompletableFuture<ClusterData> getClusterDataIfDifferentCluster(PulsarService pulsar,
        String cluster, String clientAppId) {

    CompletableFuture<ClusterData> clusterDataFuture = new CompletableFuture<>();
    // 简单校验集群信息
    if (!isValidCluster(pulsar, cluster)) {
        try {
            // 这里代码为了兼容 v1 namespace 版本的格式：prop/cluster/namespaces
            if (!pulsar.getConfiguration().getClusterName().equals(cluster)) {
                // 从配置缓存中读取集群信息，将重定向此集群
                pulsar.getConfigurationCache().clustersCache().getAsync(path("clusters", cluster))
                        .thenAccept(clusterDataResult -> {
                            if (clusterDataResult.isPresent()) {
                                clusterDataFuture.complete(clusterDataResult.get());
                            } else {
                                log.warn("[{}] Cluster does not exist: requested={}", clientAppId, cluster);
                                clusterDataFuture.completeExceptionally(new RestException(Status.NOT_FOUND,
                                        "Cluster does not exist: cluster=" + cluster));
                            }
                        }).exceptionally(ex -> {
                            clusterDataFuture.completeExceptionally(ex);
                            return null;
                        });
            } else {
                clusterDataFuture.complete(null);
            }
        } catch (Exception e) {
            clusterDataFuture.completeExceptionally(e);
        }
    } else {
        clusterDataFuture.complete(null);
    }
    return clusterDataFuture;
}

//判定是不是领导者 broker
protected static boolean isLeaderBroker(PulsarService pulsar) {

    String leaderAddress = pulsar.getLeaderElectionService().getCurrentLeader().getServiceUrl();

    String myAddress = pulsar.getWebServiceAddress();

    return myAddress.equals(leaderAddress);
}

```

到此，获取Topic 的分区方法逻辑已完成分析，主要是权限认证，集群数据验证，然后从zk中读取信息。

#### 2. Broker Producer 接口实现

##### 1. newConnectCommand 命令实现

当生产者通道被激活时，会主动发``newConnectCommand``命令给 broker 或 通过 proxy 给 broker ，表示要建立新连接。

```java

protected void handleConnect(CommandConnect connect) {
    //状态检查
    checkArgument(state == State.Start);
    //启用了身份认证
    if (service.isAuthenticationEnabled()) {
        try {
            String authMethod = "none";
            //有认证方法名
            if (connect.hasAuthMethodName()) {
                authMethod = connect.getAuthMethodName();
            //系统内置认证
            } else if (connect.hasAuthMethod()) {
                authMethod = connect.getAuthMethod().name().substring(10).toLowerCase();
            }
            //认证数据
            String authData = connect.getAuthData().toStringUtf8();
            ChannelHandler sslHandler = ctx.channel().pipeline().get(PulsarChannelInitializer.TLS_HANDLER);
            SSLSession sslSession = null;
            //如果启用了SSL
            if (sslHandler != null) {
                sslSession = ((SslHandler) sslHandler).engine().getSession();
            }
            //认证（处于proxy状态的）原数据（因为连接可能是通过proxy发起的，除了对 proxy 认证外，这里还可能对客户端进行认证）
            originalPrincipal = getOriginalPrincipal(
                    connect.hasOriginalAuthData() ? connect.getOriginalAuthData() : null,
                    connect.hasOriginalAuthMethod() ? connect.getOriginalAuthMethod() : null,
                    connect.hasOriginalPrincipal() ? connect.getOriginalPrincipal() : null,
                    sslSession);
            //对客户端或 proxy 进行认证
            authenticationData = new AuthenticationDataCommand(authData, remoteAddress, sslSession);
            authRole = getBrokerService().getAuthenticationService()
                    .authenticate(authenticationData, authMethod);

            log.info("[{}] Client successfully authenticated with {} role {} and originalPrincipal {}", remoteAddress, authMethod, authRole, originalPrincipal);
        } catch (AuthenticationException e) {
            String msg = "Unable to authenticate";
            log.warn("[{}] {}: {}", remoteAddress, msg, e.getMessage());
            ctx.writeAndFlush(Commands.newError(-1, ServerError.AuthenticationError, msg));
            close();
            return;
        }
    }
    if (log.isDebugEnabled()) {
        log.debug("Received CONNECT from {}", remoteAddress);
    }
    // 返回协议版本给客户端
    ctx.writeAndFlush(Commands.newConnected(connect.getProtocolVersion()));
    // 完成连接
    state = State.Connected;
    remoteEndpointProtocolVersion = connect.getProtocolVersion();
    String version = connect.hasClientVersion() ? connect.getClientVersion() : null;
    //如果客户端版本不为空并且不为客户端默认版本值，则设置客户端版本
    if (isNotBlank(version) && !version.contains(" ")) {
        this.clientVersion = version.intern();
    }
}

private String getOriginalPrincipal(String originalAuthData, String originalAuthMethod, String originalPrincipal,
            SSLSession sslSession) throws AuthenticationException {
     //启用原认证数据
    if (authenticateOriginalAuthData) {
        if (originalAuthData != null) {
            originalPrincipal = getBrokerService().getAuthenticationService().authenticate(
                    new AuthenticationDataCommand(originalAuthData, remoteAddress, sslSession), originalAuthMethod);
        } else {
            originalPrincipal = null;
        }
    }
    return originalPrincipal;
}

//通过认证名来给出认证提供者，然后调用认证提供者中对应的认证方法，系统目前支持4种认证方式，分别为：AuthenticationProviderAthenz，
//AuthenticationProviderBase，AuthenticationProviderTLS，AuthenticationProviderToken
public String authenticate(AuthenticationDataSource authData, String authMethodName)
            throws AuthenticationException {
    AuthenticationProvider provider = providers.get(authMethodName);
    // 如果有定义好的认证提供者，则调用对应的方法
    if (provider != null) {
        return provider.authenticate(authData);
    } else {
        //如果没有找到认证提供者，且匿名用户角色访问不为空，则直接返回，否则抛不支持认证模式异常
        if (StringUtils.isNotBlank(anonymousUserRole)) {
            return anonymousUserRole;
        }
        throw new AuthenticationException("Unsupported authentication mode: " + authMethodName);
    }
}

```

##### 2. newProducer 命令实现

当客户端已成功连接上 broker 时，马上发送注册 producer 命令，broker 将接受注册，如下：

```java

protected void handleProducer(final CommandProducer cmdProducer) {
    // 状态检查
    checkArgument(state == State.Connected);
    final long producerId = cmdProducer.getProducerId();
    final long requestId = cmdProducer.getRequestId();
    // 如果存在，将用户客户端自定义的生产者名字
    final String producerName = cmdProducer.hasProducerName() ? cmdProducer.getProducerName()
            : service.generateUniqueProducerName();
    final boolean isEncrypted = cmdProducer.getEncrypted();
    final Map<String, String> metadata = CommandUtils.metadataFromCommand(cmdProducer);
    final SchemaData schema = cmdProducer.hasSchema() ? getSchema(cmdProducer.getSchema()) : null;
    // 验证 Topic 合法性
    TopicName topicName = validateTopicName(cmdProducer.getTopic(), requestId, cmdProducer);
    if (topicName == null) {
        return;
    }
    // 如果认证与授权被启用时，当连接时候，如果认证角色（authRole）是代理角色（proxyRoles）之一时，必须强制如下规则
    // * originalPrincipal 不能为空
    // * originalPrincipal 不能是以 proxy 身份
    if (invalidOriginalPrincipal(originalPrincipal)) {
        final String msg = "Valid Proxy Client role should be provided while creating producer ";
        log.warn("[{}] {} with role {} and proxyClientAuthRole {} on topic {}", remoteAddress, msg, authRole,
                originalPrincipal, topicName);
        ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
        return;
    }
    // 如果授权被启用且通过 proxy 连接时，校验是否有权限创建生产者
    CompletableFuture<Boolean> isProxyAuthorizedFuture;
    if (service.isAuthorizationEnabled() && originalPrincipal != null) {
        isProxyAuthorizedFuture = service.getAuthorizationService().canProduceAsync(topicName,
                authRole, authenticationData);
    } else {
        isProxyAuthorizedFuture = CompletableFuture.completedFuture(true);
    }
    isProxyAuthorizedFuture.thenApply(isProxyAuthorized -> {
        // proxy 授权成功
        if (isProxyAuthorized) {
            CompletableFuture<Boolean> authorizationFuture;
            // 实际上的客户端权限检验
            if (service.isAuthorizationEnabled()) {
                authorizationFuture = service.getAuthorizationService().canProduceAsync(topicName,
                        originalPrincipal != null ? originalPrincipal : authRole, authenticationData);
            } else {
                authorizationFuture = CompletableFuture.completedFuture(true);
            }
            // 客户端授权通过
            authorizationFuture.thenApply(isAuthorized -> {
                if (isAuthorized) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Client is authorized to Produce with role {}", remoteAddress, authRole);
                    }
                    CompletableFuture<Producer> producerFuture = new CompletableFuture<>();
                    //查看是否有同一 producerId 
                    CompletableFuture<Producer> existingProducerFuture = producers.putIfAbsent(producerId,
                            producerFuture);
                    //如果有，则进行判断
                    if (existingProducerFuture != null) {
                        //如果已经完成并且没有异常，直接返回创建成功
                        if (existingProducerFuture.isDone() && !existingProducerFuture.isCompletedExceptionally()) {
                            Producer producer = existingProducerFuture.getNow(null);
                            log.info("[{}] Producer with the same id is already created: {}", remoteAddress,
                                    producer);
                            ctx.writeAndFlush(Commands.newProducerSuccess(requestId, producer.getProducerName(),
                                producer.getSchemaVersion()));
                            return null;
                        } else {
                            // 这里是有更早的请求创建生产者用相同的 producerId。当客户端超时时间小于代理超时时间时，可能会发生这种情况。 我们需要等待直到上一个生产者创建请求完成或失败。
                            ServerError error = !existingProducerFuture.isDone() ? ServerError.ServiceNotReady
                                    : getErrorCode(existingProducerFuture);
                            log.warn("[{}][{}] Producer is already present on the connection", remoteAddress,
                                    topicName);
                            ctx.writeAndFlush(Commands.newError(requestId, error,
                                    "Producer is already present on the connection"));
                            return null;
                        }
                    }

                    log.info("[{}][{}] Creating producer. producerId={}", remoteAddress, topicName, producerId);
                    // 获取或创建 Topic
                    service.getOrCreateTopic(topicName.toString()).thenAccept((Topic topic) -> {
                        // 如果 Topic 有积压配额限制，则在创建生产者之前进行检查
                        if (topic.isBacklogQuotaExceeded(producerName)) {
                            IllegalStateException illegalStateException = new IllegalStateException(
                                    "Cannot create producer on topic with backlog quota exceeded");
                            BacklogQuota.RetentionPolicy retentionPolicy = topic.getBacklogQuota().getPolicy();
                            // 根据策略决定生产者动作，这里是生产者等着，直到配额可用
                            if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_request_hold) {
                                ctx.writeAndFlush(
                                        Commands.newError(requestId, ServerError.ProducerBlockedQuotaExceededError,
                                                illegalStateException.getMessage()));
                            // 这里是直接抛异常
                            } else if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_exception) {
                                ctx.writeAndFlush(Commands.newError(requestId,
                                        ServerError.ProducerBlockedQuotaExceededException,
                                        illegalStateException.getMessage()));
                            }
                            producerFuture.completeExceptionally(illegalStateException);
                            producers.remove(producerId, producerFuture);
                            return;
                        }

                        // 如果 Topic 配置加密,检查生产者发布消息是否加密消息
                        if (topic.isEncryptionRequired() && !isEncrypted) {
                            String msg = String.format("Encryption is required in %s", topicName);
                            log.warn("[{}] {}", remoteAddress, msg);
                            ctx.writeAndFlush(Commands.newError(requestId, ServerError.MetadataError, msg));
                            return;
                        }
                        // 这里对于副本复制器，将启用nagle算法。
                        disableTcpNoDelayIfNeeded(topicName.toString(), producerName);
                        // 对于 SchemaVersion 的处理
                        CompletableFuture<SchemaVersion> schemaVersionFuture;
                        if (schema != null) {
                            schemaVersionFuture = topic.addSchema(schema);
                        } else {
                            schemaVersionFuture = topic.hasSchema().thenCompose((hasSchema) -> {
                                    CompletableFuture<SchemaVersion> result = new CompletableFuture<>();
                                    if (hasSchema && schemaValidationEnforced) {
                                        result.completeExceptionally(new IncompatibleSchemaException(
                                            "Producers cannot connect without a schema to topics with a schema"));
                                    } else {
                                        result.complete(SchemaVersion.Empty);
                                    }
                                    return result;
                                });
                        }
                        // 读取 SchemaVersion 信息异常
                        schemaVersionFuture.exceptionally(exception -> {
                            ctx.writeAndFlush(Commands.newError(requestId,
                                    BrokerServiceException.getClientErrorCode(exception.getCause()),
                                    exception.getMessage()));
                            producers.remove(producerId, producerFuture);
                            return null;
                        });
                        // 读取 SchemaVersion 成功
                        schemaVersionFuture.thenAccept(schemaVersion -> {
                            Producer producer = new Producer(topic, ServerCnx.this, producerId, producerName, authRole,
                                isEncrypted, metadata, schemaVersion);

                            try {
                                // 创建 Producer 成功，加入 Topic 
                                topic.addProducer(producer);
                                // 当前服务器已激活
                                if (isActive()) {
                                    // 尝试设置，如果成功，则意味着命令执行成功
                                    if (producerFuture.complete(producer)) {
                                        log.info("[{}] Created new producer: {}", remoteAddress, producer);
                                        ctx.writeAndFlush(Commands.newProducerSuccess(requestId, producerName,
                                            producer.getLastSequenceId(), producer.getSchemaVersion()));
                                        return;
                                    } else {
                                        // 生产者的futrue已完成，通过关闭命令
                                        producer.closeNow();
                                        log.info("[{}] Cleared producer created after timeout on client side {}",
                                            remoteAddress, producer);
                                    }
                                } else {
                                    // 当前服务器已关闭，则生产者立即关闭
                                    producer.closeNow();
                                    log.info("[{}] Cleared producer created after connection was closed: {}",
                                        remoteAddress, producer);
                                    producerFuture.completeExceptionally(
                                        new IllegalStateException("Producer created after connection was closed"));
                                }
                            } catch (BrokerServiceException ise) {
                                // Broker 服务异常
                                log.error("[{}] Failed to add producer to topic {}: {}", remoteAddress, topicName,
                                    ise.getMessage());
                                // 通知客户端
                                ctx.writeAndFlush(Commands.newError(requestId,
                                    BrokerServiceException.getClientErrorCode(ise), ise.getMessage()));
                                producerFuture.completeExceptionally(ise);
                            }

                            producers.remove(producerId, producerFuture);
                        });
                    }).exceptionally(exception -> {
                        Throwable cause = exception.getCause();
                        if (!(cause instanceof ServiceUnitNotReadyException)) {
                            // 不要为预期的异常打印堆栈跟踪信息
                            log.error("[{}] Failed to create topic {}", remoteAddress, topicName, exception);
                        }

                        // 如果客户端超时，future 将被设置为完成随即关闭。只有在尚未完成时才将错误发送回客户端。
                        if (producerFuture.completeExceptionally(exception)) {
                            ctx.writeAndFlush(Commands.newError(requestId,
                                    BrokerServiceException.getClientErrorCode(cause), cause.getMessage()));
                        }
                        producers.remove(producerId, producerFuture);

                        return null;
                    });
                } else {
                    // 客户端 无权限
                    String msg = "Client is not authorized to Produce";
                    log.warn("[{}] {} with role {}", remoteAddress, msg, authRole);
                    ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
                }
                return null;
            }).exceptionally(e -> {
                String msg = String.format("[%s] %s with role %s", remoteAddress, e.getMessage(), authRole);
                log.warn(msg);
                ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, e.getMessage()));
                return null;
            });
        } else {
            // proxy 无权限
            final String msg = "Proxy Client is not authorized to Produce";
            log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, authRole, topicName);
            ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
        }
        return null;
    }).exceptionally(ex -> {
        // 检查权限时，发生异常
        String msg = String.format("[%s] %s with role %s", remoteAddress, ex.getMessage(), authRole);
        log.warn(msg);
        ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, ex.getMessage()));
        return null;
    });
}

private final void disableTcpNoDelayIfNeeded(String topic, String producerName) {
    if (producerName != null && producerName.startsWith(replicatorPrefix)) {
        // 在用于复制目的的连接上重新启用nagle算法
        try {
            if (ctx.channel().config().getOption(ChannelOption.TCP_NODELAY).booleanValue() == true) {
                ctx.channel().config().setOption(ChannelOption.TCP_NODELAY, false);
            }
        } catch (Throwable t) {
            log.warn("[{}] [{}] Failed to remove TCP no-delay property on client cnx {}", topic, producerName,
                    ctx.channel());
        }
    }
}

//授权服务目前系统只有一个默认实现PulsarAuthorizationProvider
public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
        AuthenticationDataSource authenticationData) {
    //授权没被启用
    if (!this.conf.isAuthorizationEnabled()) {
        return CompletableFuture.completedFuture(true);
    }
    //认证提供者是否是超级用户
    if (provider != null) {
        return provider.isSuperUser(role, conf).thenComposeAsync(isSuperUser -> {
            // 是超级用户，直接返回，
            if (isSuperUser) {
                return CompletableFuture.completedFuture(true);
            } else {
                // 否则继续验证
                return provider.canProduceAsync(topicName, role, authenticationData);
            }
        });
    }
    //未配置授权服务
    return FutureUtil.failedFuture(new IllegalStateException("No authorization provider configured"));
}

// 检查授权
@Override
public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
        AuthenticationDataSource authenticationData) {
    return checkAuthorization(topicName, role, AuthAction.produce);
}

// 检查访问权限和 Topic 是否做所在的集群中
private CompletableFuture<Boolean> checkAuthorization(TopicName topicName, String role, AuthAction action) {
    return checkPermission(topicName, role, action)
            .thenApply(isPermission -> isPermission && checkCluster(topicName));
}

public CompletableFuture<Boolean> checkPermission(TopicName topicName, String role, AuthAction action) {
    CompletableFuture<Boolean> permissionFuture = new CompletableFuture<>();
    try {
        // 从配置缓存中读取 Namespace 的策略信息
        configCache.policiesCache().getAsync(POLICY_ROOT + topicName.getNamespace()).thenAccept(policies -> {
            // 策略信息不存在
            if (!policies.isPresent()) {
                if (log.isDebugEnabled()) {
                    log.debug("Policies node couldn't be found for topic : {}", topicName);
                }
            } else {
                // 授权策略获取 Namespace 授权角色
                Map<String, Set<AuthAction>> namespaceRoles = policies.get().auth_policies.namespace_auth;
                // 有角色获取对应的许可访问动作
                Set<AuthAction> namespaceActions = namespaceRoles.get(role);
                if (namespaceActions != null && namespaceActions.contains(action)) {
                    // 这角色有 Namespace 级别的访问权限
                    permissionFuture.complete(true);
                    return;
                }
                // 读取 Topic 级别的权限信息
                Map<String, Set<AuthAction>> topicRoles = policies.get().auth_policies.destination_auth
                        .get(topicName.toString());
                if (topicRoles != null) {
                    // Topic  有自定义策略
                    Set<AuthAction> topicActions = topicRoles.get(role);
                    if (topicActions != null && topicActions.contains(action)) {
                        // 这角色有 Topic 级别的访问权限
                        permissionFuture.complete(true);
                        return;
                    }
                }

                // 是否启用允许通配符授权
                if (conf.isAuthorizationAllowWildcardsMatching()) {
                    if (checkWildcardPermission(role, action, namespaceRoles)) {
                        // 这角色通过通配符匹配有 namespace 级别的访问权限
                        permissionFuture.complete(true);
                        return;
                    }

                    if (topicRoles != null && checkWildcardPermission(role, action, topicRoles)) {
                        // 这角色通过通配符匹配有 topic 级别的访问权限
                        permissionFuture.complete(true);
                        return;
                    }
                }
            }
            permissionFuture.complete(false);
        }).exceptionally(ex -> {
            log.warn("Client  with Role - {} failed to get permissions for topic - {}. {}", role, topicName,
                    ex.getMessage());
            permissionFuture.completeExceptionally(ex);
            return null;
        });
    } catch (Exception e) {
        log.warn("Client  with Role - {} failed to get permissions for topic - {}. {}", role, topicName,
                e.getMessage());
        permissionFuture.completeExceptionally(e);
    }
    return permissionFuture;
}

// 检查 Topic 是否在全局或指定集群中
private boolean checkCluster(TopicName topicName) {
    if (topicName.isGlobal() || conf.getClusterName().equals(topicName.getCluster())) {
        return true;
    } else {
        if (log.isDebugEnabled()) {
            log.debug("Topic [{}] does not belong to local cluster [{}]", topicName.toString(),
                    conf.getClusterName());
        }
        return false;
    }
}

// 通配符授权
private boolean checkWildcardPermission(String checkedRole, AuthAction checkedAction,
        Map<String, Set<AuthAction>> permissionMap) {
    for (Map.Entry<String, Set<AuthAction>> permissionData : permissionMap.entrySet()) {
        String permittedRole = permissionData.getKey();
        Set<AuthAction> permittedActions = permissionData.getValue();

        // 前缀匹配
        if (permittedRole.charAt(permittedRole.length() - 1) == '*'
                && checkedRole.startsWith(permittedRole.substring(0, permittedRole.length() - 1))
                && permittedActions.contains(checkedAction)) {
            return true;
        }

        // 后缀匹配
        if (permittedRole.charAt(0) == '*' && checkedRole.endsWith(permittedRole.substring(1))
                && permittedActions.contains(checkedAction)) {
            return true;
        }
    }
    return false;
}

// 授权之后，就开始获取或创建 Topic

public CompletableFuture<Topic> getOrCreateTopic(final String topic) {
    return getTopic(topic, true /* createIfMissing */).thenApply(Optional::get);
}

private CompletableFuture<Optional<Topic>> getTopic(final String topic, boolean createIfMissing) {
    try {
        CompletableFuture<Optional<Topic>> topicFuture = topics.get(topic);
        if (topicFuture != null) {
            if (topicFuture.isCompletedExceptionally()
                    || (topicFuture.isDone() && !topicFuture.getNow(Optional.empty()).isPresent())) {
                // 创建异常 Topic ，移除后重新创建
                topics.remove(topic, topicFuture);
            } else {
                // 如果存在已成功创建的，则直接返回
                return topicFuture;
            }
        }
        // 这里根据持久化类型来决定用哪个创建
        final boolean isPersistentTopic = TopicName.get(topic).getDomain().equals(TopicDomain.persistent);
        return topics.computeIfAbsent(topic, (topicName) -> {
                return isPersistentTopic ? this.loadOrCreatePersistentTopic(topicName, createIfMissing)
                    : createNonPersistentTopic(topicName);
        });
    } catch (IllegalArgumentException e) {
        // 参数异常
        log.warn("[{}] Illegalargument exception when loading topic", topic, e);
        return failedFuture(e);
    } catch (RuntimeException e) {
        Throwable cause = e.getCause();
        // 服务单元还未准备好
        if (cause instanceof ServiceUnitNotReadyException) {
            log.warn("[{}] Service unit is not ready when loading the topic", topic);
        } else {
            log.warn("[{}] Unexpected exception when loading topic: {}", topic, e.getMessage(), e);
        }

        return failedFuture(cause);
    }
}

 protected CompletableFuture<Optional<Topic>> loadOrCreatePersistentTopic(final String topic,
            boolean createIfMissing) throws RuntimeException {
    checkTopicNsOwnership(topic);

    final CompletableFuture<Optional<Topic>> topicFuture = new CompletableFuture<>();
    // 根据配置，不能创建持久化类型 Topic
    if (!pulsar.getConfiguration().isEnablePersistentTopics()) {
        if (log.isDebugEnabled()) {
            log.debug("Broker is unable to load persistent topic {}", topic);
        }
        topicFuture.completeExceptionally(new NotAllowedException("Broker is not unable to load persistent topic"));
        return topicFuture;
    }

    final Semaphore topicLoadSemaphore = topicLoadRequestSemaphore.get();
    // Topic 加载信号量，尝试拿到许可
    if (topicLoadSemaphore.tryAcquire()) {
        createPersistentTopic(topic, createIfMissing, topicFuture);
        topicFuture.handle((persistentTopic, ex) -> {
            // 释放许可和处理未处理的topic
            topicLoadSemaphore.release();
            createPendingLoadTopic();
            return null;
        });
    } else {
        // 否则，放入正处理队列
        pendingTopicLoadingQueue.add(new ImmutablePair<String, CompletableFuture<Optional<Topic>>>(topic, topicFuture));
        if (log.isDebugEnabled()) {
            log.debug("topic-loading for {} added into pending queue", topic);
        }
    }
    return topicFuture;
}

private void createPersistentTopic(final String topic, boolean createIfMissing, CompletableFuture<Optional<Topic>> topicFuture) {

    final long topicCreateTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    TopicName topicName = TopicName.get(topic);
    // 检查 namespace 对
    if (!pulsar.getNamespaceService().isServiceUnitActive(topicName)) {
        // namespace 正在被卸载，移除 topic 信息，返回服务还没准备好异常
        String msg = String.format("Namespace is being unloaded, cannot add topic %s", topic);
        log.warn(msg);
        pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
        topicFuture.completeExceptionally(new ServiceUnitNotReadyException(msg));
        return;
    }

    getManagedLedgerConfig(topicName).thenAccept(managedLedgerConfig -> {
        managedLedgerConfig.setCreateIfMissing(createIfMissing);
        // 一旦我们获取到配置，就可以异步打开
        managedLedgerFactory.asyncOpen(topicName.getPersistenceNamingEncoding(), managedLedgerConfig,
                new OpenLedgerCallback() {
                    // 打开成功回调
                    @Override
                    public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                        try {
                            // 创建持久化 topic
                            PersistentTopic persistentTopic = new PersistentTopic(topic, ledger,
                                    BrokerService.this);
                            // 检查副本信息
                            CompletableFuture<Void> replicationFuture = persistentTopic.checkReplication();
                            replicationFuture.thenCompose(v -> {
                                // 检查去重状态
                                return persistentTopic.checkDeduplicationStatus();
                            }).thenRun(() -> {
                                // Topic 创建成功
                                log.info("Created topic {} - dedup is {}", topic,
                                        persistentTopic.isDeduplicationEnabled() ? "enabled" : "disabled");
                                long topicLoadLatencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime())
                                        - topicCreateTimeMs;
                                pulsarStats.recordTopicLoadTimeValue(topic, topicLoadLatencyMs);
                                addTopicToStatsMaps(topicName, persistentTopic);
                                topicFuture.complete(Optional.of(persistentTopic));
                            }).exceptionally((ex) -> {
                                // 创建异常，移除集群复制器
                                log.warn(
                                        "Replication or dedup check failed. Removing topic from topics list {}, {}",
                                        topic, ex);
                                persistentTopic.stopReplProducers().whenComplete((v, exception) -> {
                                    topics.remove(topic, topicFuture);
                                    topicFuture.completeExceptionally(ex);
                                });

                                return null;
                            });
                        } catch (NamingException e) {
                            log.warn("Failed to create topic {}-{}", topic, e.getMessage());
                            pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
                            topicFuture.completeExceptionally(e);
                        }
                    }
                   //打开失败回调
                    @Override
                    public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        if (!createIfMissing && exception instanceof ManagedLedgerNotFoundException) {
                            // 仅仅试着加载 topic ，但是这 topic 不存在
                            topicFuture.complete(Optional.empty());
                        } else {
                            log.warn("Failed to create topic {}", topic, exception);
                            pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
                            topicFuture.completeExceptionally(new PersistenceException(exception));
                        }
                    }
                }, null);

    }).exceptionally((exception) -> {
        // 获取配置异常
        log.warn("[{}] Failed to get topic configuration: {}", topic, exception.getMessage(), exception);
        // 如果 createPersistentTopic 线程仅仅试着处理 futrue-result，那么在不同的线程里从 topic-map 移除 topic 信息，避免造成死锁
        pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
        topicFuture.completeExceptionally(exception);
        return null;
    });
}

// 判定服务单元是否存活
public boolean isServiceUnitActive(TopicName topicName) {
    try {
        return ownershipCache.getOwnedBundle(getBundle(topicName)).isActive();
    } catch (Exception e) {
        LOG.warn("Unable to find OwnedBundle for topic - [{}]", topicName);
        return false;
    }
}

//根据 Topic 信息获取 NamespaceBundle
public NamespaceBundle getBundle(TopicName topicName) throws Exception {
    return bundleFactory.getBundles(topicName.getNamespaceObject()).findBundle(topicName);
}

//根据 NamespaceBundle 获取 OwnedBundle
public OwnedBundle getOwnedBundle(NamespaceBundle bundle) {
    CompletableFuture<OwnedBundle> future = ownedBundlesCache.getIfPresent(ServiceUnitZkUtils.path(bundle));
    if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
        return future.join();
    } else {
        return null;
    }
}
//是否激活状态 NamespaceBundle
public boolean isActive() {
    return IS_ACTIVE_UPDATER.get(this) == TRUE;
}

// 通过 Topic 信息获取 ManagdLedgerConfig
public CompletableFuture<ManagdLedgerConfig> getManagedLedgerConfig(TopicName topicName) {
    CompletableFuture<ManagedLedgerConfig> future = new CompletableFuture<>();
    // 后台线程执行，获取策略信息可能阻塞，如果z-node没有缓存的话
    pulsar.getOrderedExecutor().executeOrdered(topicName, safeRun(() -> {
        NamespaceName namespace = topicName.getNamespaceObject();
        ServiceConfiguration serviceConfig = pulsar.getConfiguration();

        // 获取 Topic 的持久化策略
        Optional<Policies> policies = Optional.empty();
        try {
            policies = pulsar
                    .getConfigurationCache().policiesCache().get(AdminResource.path(POLICIES,
                            namespace.toString()));
        } catch (Throwable t) {
            // 获取策略时发生异常
            log.warn("Got exception when reading persistence policy for {}: {}", topicName, t.getMessage(), t);
            future.completeExceptionally(t);
            return;
        }
        // 创建 ledger 配置文件
        PersistencePolicies persistencePolicies = policies.map(p -> p.persistence).orElseGet(
                () -> new PersistencePolicies(serviceConfig.getManagedLedgerDefaultEnsembleSize(),
                        serviceConfig.getManagedLedgerDefaultWriteQuorum(),
                        serviceConfig.getManagedLedgerDefaultAckQuorum(),
                        serviceConfig.getManagedLedgerDefaultMarkDeleteRateLimit()));
        RetentionPolicies retentionPolicies = policies.map(p -> p.retention_policies).orElseGet(
                () -> new RetentionPolicies(serviceConfig.getDefaultRetentionTimeInMinutes(),
                        serviceConfig.getDefaultRetentionSizeInMB())
        );
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setEnsembleSize(persistencePolicies.getBookkeeperEnsemble());
        managedLedgerConfig.setWriteQuorumSize(persistencePolicies.getBookkeeperWriteQuorum());
        managedLedgerConfig.setAckQuorumSize(persistencePolicies.getBookkeeperAckQuorum());
        managedLedgerConfig.setThrottleMarkDelete(persistencePolicies.getManagedLedgerMaxMarkDeleteRate());
        managedLedgerConfig.setDigestType(serviceConfig.getManagedLedgerDigestType());
        managedLedgerConfig.setMaxUnackedRangesToPersist(serviceConfig.getManagedLedgerMaxUnackedRangesToPersist());
        managedLedgerConfig.setMaxUnackedRangesToPersistInZk(serviceConfig.getManagedLedgerMaxUnackedRangesToPersistInZooKeeper());
        managedLedgerConfig.setMaxEntriesPerLedger(serviceConfig.getManagedLedgerMaxEntriesPerLedger());
        managedLedgerConfig.setMinimumRolloverTime(serviceConfig.getManagedLedgerMinLedgerRolloverTimeMinutes(),TimeUnit.MINUTES);
        managedLedgerConfig.setMaximumRolloverTime(serviceConfig.getManagedLedgerMaxLedgerRolloverTimeMinutes(),TimeUnit.MINUTES);
        managedLedgerConfig.setMaxSizePerLedgerMb(2048);
        managedLedgerConfig.setMetadataOperationsTimeoutSeconds(serviceConfig.getManagedLedgerMetadataOperationsTimeoutSeconds());
        managedLedgerConfig.setReadEntryTimeoutSeconds(serviceConfig.getManagedLedgerReadEntryTimeoutSeconds());
        managedLedgerConfig.setAddEntryTimeoutSeconds(serviceConfig.getManagedLedgerAddEntryTimeoutSeconds());
        managedLedgerConfig.setMetadataEnsembleSize(serviceConfig.getManagedLedgerDefaultEnsembleSize());
        managedLedgerConfig.setMetadataWriteQuorumSize(serviceConfig.getManagedLedgerDefaultWriteQuorum());
        managedLedgerConfig.setMetadataAckQuorumSize(serviceConfig.getManagedLedgerDefaultAckQuorum());
        managedLedgerConfig.setMetadataMaxEntriesPerLedger(serviceConfig.getManagedLedgerCursorMaxEntriesPerLedger());
        managedLedgerConfig.setLedgerRolloverTimeout(serviceConfig.getManagedLedgerCursorRolloverTimeInSeconds());
        managedLedgerConfig.setRetentionTime(retentionPolicies.getRetentionTimeInMinutes(), TimeUnit.MINUTES);
        managedLedgerConfig.setRetentionSizeInMB(retentionPolicies.getRetentionSizeInMB());
        managedLedgerConfig.setLedgerOffloader(pulsar.getManagedLedgerOffloader());
        policies.ifPresent(p -> {
                long lag = serviceConfig.getManagedLedgerOffloadDeletionLagMs();
                if (p.offload_deletion_lag_ms != null) {
                    lag = p.offload_deletion_lag_ms;
                }
                managedLedgerConfig.setOffloadLedgerDeletionLag(lag, TimeUnit.MILLISECONDS);
                managedLedgerConfig.setOffloadAutoTriggerSizeThresholdBytes(p.offload_threshold);
            });

        future.complete(managedLedgerConfig);
    }, (exception) -> future.completeExceptionally(exception)));

    return future;
}

// 检查 topic 副本信息
@Override
public CompletableFuture<Void> checkReplication() {
    // 这里查看 Topic 信息，判定是不是全局 Topic
    TopicName name = TopicName.get(topic);
    // 如果不是，则直接返回
    if (!name.isGlobal()) {
        return CompletableFuture.completedFuture(null);
    }

    if (log.isDebugEnabled()) {
        log.debug("[{}] Checking replication status", name);
    }

    Policies policies = null;
    try {
        // 获取策略配置
        policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                .get(AdminResource.path(POLICIES, name.getNamespace()))
                .orElseThrow(() -> new KeeperException.NoNodeException());
    } catch (Exception e) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(new ServerMetadataException(e));
        return future;
    }

    final int newMessageTTLinSeconds = policies.message_ttl_in_seconds;

    Set<String> configuredClusters;
    //读取副本集群
    if (policies.replication_clusters != null) {
        configuredClusters = Sets.newTreeSet(policies.replication_clusters);
    } else {
        configuredClusters = Collections.emptySet();
    }

    String localCluster = brokerService.pulsar().getConfiguration().getClusterName();
    // 如果本地集群已经从全局 namespace 移除，那么将强制删除 topic，因为 pulsar 不会服务没有配置副本集群的全局 topic
    if (TopicName.get(topic).isGlobal() && !configuredClusters.contains(localCluster)) {
        log.info("Deleting topic [{}] because local cluster is not part of global namespace repl list {}",
                configuredClusters);
                // 前面章节已分析过
        return deleteForcefully();
    }

    List<CompletableFuture<Void>> futures = Lists.newArrayList();

    // 检查丢失的复制器把，启动没有准备好的复制器
    for (String cluster : configuredClusters) {
        if (cluster.equals(localCluster)) {
            continue;
        }

        if (!replicators.containsKey(cluster)) {
            futures.add(startReplicator(cluster));
        }
    }

    // 移除不在配置中，多余的复制器
    replicators.forEach((cluster, replicator) -> {
        // 更新消息存活时间
        ((PersistentReplicator) replicator).updateMessageTTL(newMessageTTLinSeconds);

        if (!cluster.equals(localCluster)) {
            if (!configuredClusters.contains(cluster)) {
                futures.add(removeReplicator(cluster));
            }
        }
    });
    // 等待复制器新增和删除完成
    return FutureUtil.waitForAll(futures);
}

//启动复制器
CompletableFuture<Void> startReplicator(String remoteCluster) {
    log.info("[{}] Starting replicator to remote: {}", topic, remoteCluster);
    final CompletableFuture<Void> future = new CompletableFuture<>();
    // 游标名=复制器前缀+远程集群名
    String name = PersistentReplicator.getReplicatorName(replicatorPrefix, remoteCluster);
    ledger.asyncOpenCursor(name, new OpenCursorCallback() {
        @Override
        public void openCursorComplete(ManagedCursor cursor, Object ctx) {
            String localCluster = brokerService.pulsar().getConfiguration().getClusterName();
            boolean isReplicatorStarted = addReplicationCluster(remoteCluster, PersistentTopic.this, cursor, localCluster);
            if (isReplicatorStarted) {
                future.complete(null);
            } else {
                future.completeExceptionally(new NamingException(
                        PersistentTopic.this.getName() + " Failed to start replicator " + remoteCluster));
            }
        }

        @Override
        public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
            future.completeExceptionally(new PersistenceException(exception));
        }

    }, null);

    return future;
}


// 增加集群复制器
protected boolean addReplicationCluster(String remoteCluster, PersistentTopic persistentTopic, ManagedCursor cursor,
            String localCluster) {
    AtomicBoolean isReplicatorStarted = new AtomicBoolean(true);
    // 如果把 remoteCluster 集群放入，创建持久化复制器
    replicators.computeIfAbsent(remoteCluster, r -> {
        try {
            return new PersistentReplicator(PersistentTopic.this, cursor, localCluster, remoteCluster,
                    brokerService);
        } catch (NamingException e) {
            isReplicatorStarted.set(false);
            log.error("[{}] Replicator startup failed due to partitioned-topic {}", topic, remoteCluster);
        }
        return null;
    });
    // 如果创建和启动失败，则移除
    if (!isReplicatorStarted.get()) {
        replicators.remove(remoteCluster);
    }
    return isReplicatorStarted.get();
}

// 持久化复制器构造方法
public PersistentReplicator(PersistentTopic topic, ManagedCursor cursor, String localCluster, String remoteCluster,
        BrokerService brokerService) throws NamingException {
    super(topic.getName(), topic.replicatorPrefix, localCluster, remoteCluster, brokerService);
    this.topic = topic;
    this.cursor = cursor;
    this.expiryMonitor = new PersistentMessageExpiryMonitor(topicName, Codec.decode(cursor.getName()), cursor);
    HAVE_PENDING_READ_UPDATER.set(this, FALSE);
    PENDING_MESSAGES_UPDATER.set(this, 0);

    readBatchSize = Math.min(
        producerQueueSize,
        topic.getBrokerService().pulsar().getConfiguration().getDispatcherMaxReadBatchSize());
    producerQueueThreshold = (int) (producerQueueSize * 0.9);
    // 启动生产者
    startProducer();
}

// 抽象的，由持久化复制器调用
public AbstractReplicator(String topicName, String replicatorPrefix, String localCluster, String remoteCluster,
            BrokerService brokerService) throws NamingException {
    validatePartitionedTopic(topicName, brokerService);
    this.brokerService = brokerService;
    this.topicName = topicName;
    this.replicatorPrefix = replicatorPrefix;
    this.localCluster = localCluster.intern();
    this.remoteCluster = remoteCluster.intern();
    this.client = (PulsarClientImpl) brokerService.getReplicationClient(remoteCluster);
    this.producer = null;
    this.producerQueueSize = brokerService.pulsar().getConfiguration().getReplicationProducerQueueSize();

    this.producerBuilder = client.newProducer() //创建生产者构造者
            .topic(topicName)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .enableBatching(false)
            .sendTimeout(0, TimeUnit.SECONDS) //
            .maxPendingMessages(producerQueueSize) //
            .producerName(getReplicatorName(replicatorPrefix, localCluster));
    STATE_UPDATER.set(this, State.Stopped);
}

// 启动生产者
public synchronized void startProducer() {
    // 状态检查，如果正在停止，则开始重新创建生产者
    if (STATE_UPDATER.get(this) == State.Stopping) {
        long waitTimeMs = backOff.next();
        if (log.isDebugEnabled()) {
            log.debug(
                    "[{}][{} -> {}] waiting for producer to close before attempting to reconnect, retrying in {} s",
                    topicName, localCluster, remoteCluster, waitTimeMs / 1000.0);
        }
        // waitTimeMs 后尝试创建生产者
        brokerService.executor().schedule(this::startProducer, waitTimeMs, TimeUnit.MILLISECONDS);
        return;
    }
    State state = STATE_UPDATER.get(this);
    // 状态更新，由已停止尝试设置为正启动中
    if (!STATE_UPDATER.compareAndSet(this, State.Stopped, State.Starting)) {
        // 没有设置成功，然后检查是否已启动完毕
        if (state == State.Started) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{} -> {}] Replicator was already running", topicName, localCluster, remoteCluster);
            }
        } else {
            // 正启动中。。
            log.info("[{}][{} -> {}] Replicator already being started. Replicator state: {}", topicName,
                    localCluster, remoteCluster, state);
        }

        return;
    }

    log.info("[{}][{} -> {}] Starting replicator", topicName, localCluster, remoteCluster);
    producerBuilder.createAsync().thenAccept(producer -> {
        readEntries(producer);
    }).exceptionally(ex -> {
        // 创建生产者异常，则继续重试
        if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopped)) {
            long waitTimeMs = backOff.next();
            log.warn("[{}][{} -> {}] Failed to create remote producer ({}), retrying in {} s", topicName,
                    localCluster, remoteCluster, ex.getMessage(), waitTimeMs / 1000.0);

            // 重试
            brokerService.executor().schedule(this::startProducer, waitTimeMs, TimeUnit.MILLISECONDS);
        } else {
            log.warn("[{}][{} -> {}] Failed to create remote producer. Replicator state: {}", topicName,
                    localCluster, remoteCluster, STATE_UPDATER.get(this), ex);
        }
        return null;
    });

}

// 读消息
@Override
protected void readEntries(org.apache.pulsar.client.api.Producer<byte[]> producer) {
    //回放游标以确保再次读取重启时发送的所有非确认消息
    cursor.rewind();
    // 取消正读取请求
    cursor.cancelPendingReadRequest();
    // 设置正读取更新为false
    HAVE_PENDING_READ_UPDATER.set(this, FALSE);
    this.producer = (ProducerImpl) producer;
    // 尝试设置启动完成标志
    if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Started)) {
        log.info("[{}][{} -> {}] Created replicator producer", topicName, localCluster, remoteCluster);
        // 重置定时器
        backOff.reset();
        // 激活游标，消息将被缓存
        this.cursor.setActive();
        // 开始读消息
        readMoreEntries();
    } else {
        // 状态设置失败，关闭生产者
        log.info(
                "[{}][{} -> {}] Replicator was stopped while creating the producer. Closing it. Replicator state: {}",
                topicName, localCluster, remoteCluster, STATE_UPDATER.get(this));
        STATE_UPDATER.set(this, State.Stopping);
        closeProducerAsync();
    }

}

// 读取更多消息
protected void readMoreEntries() {
    int availablePermits = producerQueueSize - PENDING_MESSAGES_UPDATER.get(this);
    // 可用许可
    if (availablePermits > 0) {
        // 批量读和可用许可
        int messagesToRead = Math.min(availablePermits, readBatchSize);
        // 生产者通道暂不能写
        if (!isWritable()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{} -> {}] Throttling replication traffic because producer is not writable",
                        topicName, localCluster, remoteCluster);
            }
            // 如果生产者连接中断或窗口（window，可认为是队列）已满，则设置最小消息读取大小
            messagesToRead = 1;
        }

        // 轮询读
        if (HAVE_PENDING_READ_UPDATER.compareAndSet(this, FALSE, TRUE)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{} -> {}] Schedule read of {} messages", topicName, localCluster, remoteCluster,
                        messagesToRead);
            }
            // 异步读取消息或等待
            cursor.asyncReadEntriesOrWait(messagesToRead, this, null);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{} -> {}] Not scheduling read due to pending read. Messages To Read {}", topicName,
                        localCluster, remoteCluster, messagesToRead);
            }
        }
    } else {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{} -> {}] Producer queue is full, pause reading", topicName, localCluster,
                    remoteCluster);
        }
    }
}

// 异步读取消息或等待
public void asyncReadEntriesOrWait(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx) {
    // 读必须大于0
    checkArgument(numberOfEntriesToRead > 0);
    // 状态判断
    if (isClosed()) {
        callback.readEntriesFailed(new CursorAlreadyClosedException("Cursor was already closed"), ctx);
        return;
    }
    
    // 判断有可用消息
    if (hasMoreEntries()) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Read entries immediately", ledger.getName(), name);
        }
        // 如果有可用的消息，将立即读取
        asyncReadEntries(numberOfEntriesToRead, callback, ctx);
    } else {
        // 无可用消息，那么等待
        OpReadEntry op = OpReadEntry.create(this, PositionImpl.get(readPosition), numberOfEntriesToRead, callback,
                ctx);
        // 只允许一个等待回调
        if (!WAITING_READ_OP_UPDATER.compareAndSet(this, null, op)) {
            callback.readEntriesFailed(new ManagedLedgerException("We can only have a single waiting callback"),
                    ctx);
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Deferring retry of read at position {}", ledger.getName(), name, op.readPosition);
        }

        // 在10ms内再次检查新消息，然后如果仍然没有消息可用，则注册通知
        ledger.getScheduledExecutor().schedule(safeRun(() -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Re-trying the read at position {}", ledger.getName(), name, op.readPosition);
            }
            // 这里再次检查是否有消息
            if (!hasMoreEntries()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Still no entries available. Register for notification", ledger.getName(),
                            name);
                }
                // 无论何时一旦有消息被发布，那么让 ledger 通知回调。
                ledger.waitingCursors.add(this);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Skip notification registering since we do have entries available",
                            ledger.getName(), name);
                }
            }

            // 这里再次检查是否有消息，在检查时间与注册通知时间内，消息可能被写入。
            if (hasMoreEntries()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Found more entries", ledger.getName(), name);
                }
                // 尝试取消通知请求
                if (WAITING_READ_OP_UPDATER.compareAndSet(this, op, null)) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Cancelled notification and scheduled read at {}", ledger.getName(),
                                name, op.readPosition);
                    }
                    PENDING_READ_OPS_UPDATER.incrementAndGet(this);
                    // 这里执行一个异步调用
                    ledger.asyncReadEntries(op);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] notification was already cancelled", ledger.getName(), name);
                    }
                }
            } else if (ledger.isTerminated()) {//ledger（即Topic） 已被删除
                // 此时注册了通知，但仍然没有更多可用的消息。 如果ledger已终止，需要通知游标
                callback.readEntriesFailed(new NoMoreEntriesToReadException("Topic was terminated"), ctx);
            }
        }), 10, TimeUnit.MILLISECONDS);
    }
}

// 判断是否有新消息
public boolean hasMoreEntries() {
    // 这里并没有判断 writer 与 reader 处于同一 ledger 上？？
    PositionImpl writerPosition = ledger.getLastPosition();
    if (writerPosition.getEntryId() != -1) {
        return readPosition.compareTo(writerPosition) <= 0;
    } else {
        // 当前是否有可用消息
        return getNumberOfEntries() > 0;
    }
}

// 获取当前有可用消息数量
public long getNumberOfEntries() {
    // readPosition大于lastConfirmedEntry + 1 表示无可用消息
    if (readPosition.compareTo(ledger.getLastPosition().getNext()) > 0) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Read position {} is ahead of last position {}. There are no entries to read",
                    ledger.getName(), name, readPosition, ledger.getLastPosition());
        }
        return 0;
    } else {
        //获取当前的 [readPosition,lastConfirmedEntry + 1) 区间未删除消息
        return getNumberOfEntries(Range.closedOpen(readPosition, ledger.getLastPosition().getNext()));
    }
}

// 获取指定地址区间访问的未删消息数量
protected long getNumberOfEntries(Range<PositionImpl> range) {
    // 获取此区间所有的消息
    long allEntries = ledger.getNumberOfEntries(range);

    if (log.isDebugEnabled()) {
        log.debug("[{}] getNumberOfEntries. {} allEntries: {}", ledger.getName(), range, allEntries);
    }

    long deletedEntries = 0;

    lock.readLock().lock();
    try {
        // 获取被删除的区间
        for (Range<PositionImpl> r : individualDeletedMessages.asRanges()) {
            // 被删除的区间是否处于range区间
            if (r.isConnected(range)) {
                // 取r区间与range区间交叉的区间
                Range<PositionImpl> commonEntries = r.intersection(range);
                // 取被删除的消息数
                long commonCount = ledger.getNumberOfEntries(commonEntries);
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Discounting {} entries for already deleted range {}", ledger.getName(),
                            name, commonCount, commonEntries);
                }
                deletedEntries += commonCount;
            }
        }
    } finally {
        lock.readLock().unlock();
    }

    if (log.isDebugEnabled()) {
        log.debug("[{}] Found {} entries - deleted: {}",
            ledger.getName(), allEntries - deletedEntries, deletedEntries);
    }
    return allEntries - deletedEntries;
}

// 求两个位置区间中消息总数
long getNumberOfEntries(Range<PositionImpl> range) {
    PositionImpl fromPosition = range.lowerEndpoint();
    boolean fromIncluded = range.lowerBoundType() == BoundType.CLOSED;
    PositionImpl toPosition = range.upperEndpoint();
    boolean toIncluded = range.upperBoundType() == BoundType.CLOSED;
    // 如果起始位置指针与结束位置指针于同一ledger上
    if (fromPosition.getLedgerId() == toPosition.getLedgerId()) {
        // If the 2 positions are in the same ledger
        long count = toPosition.getEntryId() - fromPosition.getEntryId() - 1;
        count += fromIncluded ? 1 : 0;
        count += toIncluded ? 1 : 0;
        return count;
    } else {
        // 不在同一ledger上，先处理边界
        // 1.对于结束指针位置，刚好是加上结束指针所在的ledger上的消息个数
        long count = 0;
        count += toPosition.getEntryId();
        count += toIncluded ? 1 : 0;

        // 2. 每个ledger消息大小减去起始位置再减一
        LedgerInfo li = ledgers.get(fromPosition.getLedgerId());
        if (li != null) {
            count += li.getEntries() - (fromPosition.getEntryId() + 1);
            count += fromIncluded ? 1 : 0;
        }

        // 3. 加上中间所有的ledger的消息数量
        for (LedgerInfo ls : ledgers.subMap(fromPosition.getLedgerId(), false, toPosition.getLedgerId(), false)
                .values()) {
            count += ls.getEntries();
        }

        return count;
    }
}

// 异步读取消息
public void asyncReadEntries(final int numberOfEntriesToRead, final ReadEntriesCallback callback,
            final Object ctx) {
    checkArgument(numberOfEntriesToRead > 0);
    if (isClosed()) {
        callback.readEntriesFailed(new ManagedLedgerException("Cursor was already closed"), ctx);
        return;
    }

    PENDING_READ_OPS_UPDATER.incrementAndGet(this);
    OpReadEntry op = OpReadEntry.create(this, PositionImpl.get(readPosition), numberOfEntriesToRead, callback, ctx);
    ledger.asyncReadEntries(op);
}

// 异步读取消息
void asyncReadEntries(OpReadEntry opReadEntry) {
    final State state = STATE_UPDATER.get(this);
    if (state == State.Fenced || state == State.Closed) {
        opReadEntry.readEntriesFailed(new ManagedLedgerFencedException(), opReadEntry.ctx);
        return;
    }

    long ledgerId = opReadEntry.readPosition.getLedgerId();

    LedgerHandle currentLedger = this.currentLedger;

    if (currentLedger != null && ledgerId == currentLedger.getId()) {
        // 当前的写 ledger不在缓存中（因为我们不想要它被自动驱逐），我们不能使用2个不同的
        // ledger 处理（读取和写入）同一分类帐。
        internalReadFromLedger(currentLedger, opReadEntry);
    } else {
        LedgerInfo ledgerInfo = ledgers.get(ledgerId);
        if (ledgerInfo == null || ledgerInfo.getEntries() == 0) {
            // 游标指向一个空的ledger，没有必要尝试打开它。 跳过此ledger并转到下一个ledger
            opReadEntry.updateReadPosition(new PositionImpl(opReadEntry.readPosition.getLedgerId() + 1, 0));
            opReadEntry.checkReadCompletion();
            return;
        }

        // 获取读 ledger 句柄开始读取
        getLedgerHandle(ledgerId).thenAccept(ledger -> {
            internalReadFromLedger(ledger, opReadEntry);
        }).exceptionally(ex -> {
            log.error("[{}] Error opening ledger for reading at position {} - {}", name, opReadEntry.readPosition,
                    ex.getMessage());
            opReadEntry.readEntriesFailed(ManagedLedgerException.getManagedLedgerException(ex.getCause()),
                    opReadEntry.ctx);
            return null;
        });
    }
}

// 获取读 ledger 句柄
CompletableFuture<ReadHandle> getLedgerHandle(long ledgerId) {
    CompletableFuture<ReadHandle> ledgerHandle = ledgerCache.get(ledgerId);
    if (ledgerHandle != null) {
        return ledgerHandle;
    }

    // 如果不存在，请再次尝试并在需要时创建
    return ledgerCache.computeIfAbsent(ledgerId, lid -> {
        // 如果还没被打开，则打开 ledger 用于读
        if (log.isDebugEnabled()) {
            log.debug("[{}] Asynchronously opening ledger {} for read", name, ledgerId);
        }
        mbean.startDataLedgerOpenOp();

        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();

        LedgerInfo info = ledgers.get(ledgerId);
        CompletableFuture<ReadHandle> openFuture = new CompletableFuture<>();
        // 如果启用云存储
        if (info != null && info.hasOffloadContext() && info.getOffloadContext().getComplete()) {
            UUID uid = new UUID(info.getOffloadContext().getUidMsb(), info.getOffloadContext().getUidLsb());
            openFuture = config.getLedgerOffloader().readOffloaded(ledgerId, uid,
                    OffloadUtils.getOffloadDriverMetadata(info));
        } else {
            // bookkeeper 读 ledger
            openFuture = bookKeeper.newOpenLedgerOp().withRecovery(!isReadOnly()).withLedgerId(ledgerId)
                    .withDigestType(config.getDigestType()).withPassword(config.getPassword()).execute();
        }
        openFuture.whenCompleteAsync((res, ex) -> {
            // 创建异常，则移除
            mbean.endDataLedgerOpenOp();
            if (ex != null) {
                ledgerCache.remove(ledgerId, promise);
                promise.completeExceptionally(createManagedLedgerException(ex));
            } else {
                // 创建成功
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Successfully opened ledger {} for reading", name, ledgerId);
                }
                promise.complete(res);
            }
        }, executor.chooseThread(name));
        return promise;
    });
}

// 这里是真正的从 ledger 读取消息
private void internalReadFromLedger(ReadHandle ledger, OpReadEntry opReadEntry) {

    long firstEntry = opReadEntry.readPosition.getEntryId();
    long lastEntryInLedger;
    final ManagedCursorImpl cursor = opReadEntry.cursor;

    PositionImpl lastPosition = lastConfirmedEntry;
    //TODO 这里翻译感觉很别扭
    if (ledger.getId() == lastPosition.getLedgerId()) {
        // 对于当前 ledger ，在 ledger 管理层，对于读可见性的最后消息是接收到确认的消息
        lastEntryInLedger = lastPosition.getEntryId();
    } else {
        // 对于其他 ledgers，已经关闭BK lastAddConfirmed是合适的
        lastEntryInLedger = ledger.getLastAddConfirmed();
    }
    
    // 起始消息ID大于最后确认消息ID
    if (firstEntry > lastEntryInLedger) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] No more messages to read from ledger={} lastEntry={} readEntry={}", name,
                    ledger.getId(), lastEntryInLedger, firstEntry);
        }
        //如果 currentLedger 为空 或 当前 currentLedger 的id不等于读 ledger的id
        if (currentLedger == null || ledger.getId() != currentLedger.getId()) {
            // 将游标放在一个 ledger 的末尾，将其移动到下一个 ledger 的开头
            Long nextLedgerId = ledgers.ceilingKey(ledger.getId() + 1);
            if (nextLedgerId != null) {
                opReadEntry.updateReadPosition(new PositionImpl(nextLedgerId, 0));
            } else {
                opReadEntry.updateReadPosition(new PositionImpl(ledger.getId() + 1, 0));
            }
        }

        opReadEntry.checkReadCompletion();
        return;
    }
    // 确定最后一消息
    long lastEntry = min(firstEntry + opReadEntry.getNumberOfEntriesToRead() - 1, lastEntryInLedger);

    if (log.isDebugEnabled()) {
        log.debug("[{}] Reading entries from ledger {} - first={} last={}", name, ledger.getId(), firstEntry,
                lastEntry);
    }
    asyncReadEntry(ledger, firstEntry, lastEntry, false, opReadEntry, opReadEntry.ctx);
    // 更新游标
    if (updateCursorRateLimit.tryAcquire()) {
        if (isCursorActive(cursor)) {
            final PositionImpl lastReadPosition = PositionImpl.get(ledger.getId(), lastEntry);
            discardEntriesFromCache(cursor, lastReadPosition);
        }
    }
}

// 异步读取消息
protected void asyncReadEntry(ReadHandle ledger, long firstEntry, long lastEntry, boolean isSlowestReader,
        OpReadEntry opReadEntry, Object ctx) {
    long timeout = config.getReadEntryTimeoutSeconds();
    boolean checkTimeout = timeout > 0;
    // 这里启用读超时
    if (checkTimeout) {
        // 增加读总数，并包装 OpReadEntry
        long readOpCount = READ_OP_COUNT_UPDATER.incrementAndGet(this);
        ReadEntryCallbackWrapper readCallback = ReadEntryCallbackWrapper.create(name, ledger.getId(), firstEntry,
                opReadEntry, readOpCount, ctx);
        // 增加定时任务，用于控制超时
        final ScheduledFuture<?> task = scheduledExecutor.schedule(() -> {
            // 验证ReadEntryCallbackWrapper对象不被bk-client回调（通过验证readOpCount）回收，如果read还没有完成则回调失败
            if (readCallback.readOpCount == readOpCount
                    && ReadEntryCallbackWrapper.READ_COMPLETED_UPDATER.get(readCallback) == FALSE) {
                log.warn("[{}]-{} read entry timeout for {}-{} after {} sec", this.name, ledger.getId(), firstEntry,
                        lastEntry, timeout);
                readCallback.readEntriesFailed(createManagedLedgerException(BKException.Code.TimeoutException), readOpCount);
            }
        }, timeout, TimeUnit.SECONDS);
        readCallback.task = task;
        entryCache.asyncReadEntry(ledger, firstEntry, lastEntry, isSlowestReader, readCallback, readOpCount);
    } else {
        entryCache.asyncReadEntry(ledger, firstEntry, lastEntry, isSlowestReader, opReadEntry, ctx);
    }
}

public void asyncReadEntry(ReadHandle lh, long firstEntry, long lastEntry, boolean isSlowestReader,
        final ReadEntriesCallback callback, Object ctx) {
    try {
        asyncReadEntry0(lh, firstEntry, lastEntry, isSlowestReader, callback, ctx);
    } catch (Throwable t) {
        log.warn("failed to read entries for {}--{}-{}", lh.getId(), firstEntry, lastEntry, t);
        // 异步读异常，与 ledger 关联的所有消息在缓存中失效（如果条目损坏，则可能会发生这种情况（entry.data由于任何竞争条件已经解除分配）
        // ，故使缓存失效并下一次从 bookie 读取消息）
        invalidateAllEntries(lh.getId());
        callback.readEntriesFailed(createManagedLedgerException(t), ctx);
    }
}

// 这里就是从 bookie 读取消息
private void asyncReadEntry0(ReadHandle lh, long firstEntry, long lastEntry, boolean isSlowestReader,
            final ReadEntriesCallback callback, Object ctx) {
    final long ledgerId = lh.getId();
    final int entriesToRead = (int) (lastEntry - firstEntry) + 1;
    final PositionImpl firstPosition = PositionImpl.get(lh.getId(), firstEntry);
    final PositionImpl lastPosition = PositionImpl.get(lh.getId(), lastEntry);

    if (log.isDebugEnabled()) {
        log.debug("[{}] Reading entries range ledger {}: {} to {}", ml.getName(), ledgerId, firstEntry, lastEntry);
    }
    // 先从缓存读消息
    Collection<EntryImpl> cachedEntries = entries.getRange(firstPosition, lastPosition);
    // 如果缓存中有足够消息
    if (cachedEntries.size() == entriesToRead) {
        long totalCachedSize = 0;
        final List<EntryImpl> entriesToReturn = Lists.newArrayListWithExpectedSize(entriesToRead);

        // 把缓存中的消息放入
        for (EntryImpl entry : cachedEntries) {
            entriesToReturn.add(EntryImpl.create(entry));
            totalCachedSize += entry.getLength();
            entry.release();
        }

        manager.mlFactoryMBean.recordCacheHits(entriesToReturn.size(), totalCachedSize);
        if (log.isDebugEnabled()) {
            log.debug("[{}] Ledger {} -- Found in cache entries: {}-{}", ml.getName(), ledgerId, firstEntry,
                    lastEntry);
        }
        // 这里终于返回，这个调用链真长
        callback.readEntriesComplete((List) entriesToReturn, ctx);

    } else {
        // 缓存中消息不够，则释放这里，重新拉取消息
        if (!cachedEntries.isEmpty()) {
            cachedEntries.forEach(entry -> entry.release());
        }

        // 从 bookkeeper 读取所有的消息
        lh.readAsync(firstEntry, lastEntry).whenCompleteAsync(
                (ledgerEntries, exception) -> {
                    // 读取异常
                    if (exception != null) {
                        if (exception instanceof BKException
                            && ((BKException)exception).getCode() == BKException.Code.TooManyRequestsException) {
                            callback.readEntriesFailed(createManagedLedgerException(exception), ctx);
                        } else {
                            ml.invalidateLedgerHandle(lh, exception);
                            ManagedLedgerException mlException = createManagedLedgerException(exception);
                            callback.readEntriesFailed(mlException, ctx);
                        }
                        return;
                    }

                    checkNotNull(ml.getName());
                    checkNotNull(ml.getExecutor());

                    try {
                        // 获取所有消息，转换List<>类型
                        long totalSize = 0;
                        final List<EntryImpl> entriesToReturn
                            = Lists.newArrayListWithExpectedSize(entriesToRead);
                        for (LedgerEntry e : ledgerEntries) {
                            EntryImpl entry = EntryImpl.create(e);
                            entriesToReturn.add(entry);
                            totalSize += entry.getLength();
                        }

                        manager.mlFactoryMBean.recordCacheMiss(entriesToReturn.size(), totalSize);
                        ml.getMBean().addReadEntriesSample(entriesToReturn.size(), totalSize);
                        // 返回
                        callback.readEntriesComplete((List) entriesToReturn, ctx);
                    } finally {
                        ledgerEntries.close();
                    }
                }, ml.getExecutor().chooseThread(ml.getName()));
    }
}

// 检查去重状态
public CompletableFuture<Void> checkDeduplicationStatus() {
    return messageDeduplication.checkStatus();
}

// 检查配置，去重功能是否启用
private CompletableFuture<Boolean> isDeduplicationEnabled() {
    TopicName name = TopicName.get(topic.getName());

    return pulsar.getConfigurationCache().policiesCache()
            .getAsync(AdminResource.path(POLICIES, name.getNamespace())).thenApply(policies -> {
                // 如果 namespace 级别策略有对应的配置，则覆盖 broker 级别配置
                if (policies.isPresent() && policies.get().deduplicationEnabled != null) {
                    return policies.get().deduplicationEnabled;
                }

                return pulsar.getConfiguration().isBrokerDeduplicationEnabled();
            });
}

// 检查状态
public CompletableFuture<Void> checkStatus() {
    return isDeduplicationEnabled().thenCompose(shouldBeEnabled -> {
        synchronized (this) {
            // 如果当前状态为正恢复或正移除，则稍后1分钟继续检查
            if (status == Status.Recovering || status == Status.Removing) {
                // If there's already a transition happening, check later for status
                pulsar.getExecutor().schedule(this::checkStatus, 1, TimeUnit.MINUTES);
                return CompletableFuture.completedFuture(null);
            }
            // 当前状态为启用，但是配置为不启用，则开始关闭去重功能
            if (status == Status.Enabled && !shouldBeEnabled) {
                // 禁用去重功能
                CompletableFuture<Void> future = new CompletableFuture<>();
                status = Status.Removing;
                // 异步删除名为 pulsar.dedup 的游标
                managedLedger.asyncDeleteCursor(PersistentTopic.DEDUPLICATION_CURSOR_NAME,
                        new DeleteCursorCallback() {
                            //删除成功
                            @Override
                            public void deleteCursorComplete(Object ctx) {
                                status = Status.Disabled;
                                managedCursor = null;
                                highestSequencedPushed.clear();
                                highestSequencedPersisted.clear();
                                future.complete(null);
                                log.info("[{}] Disabled deduplication", topic.getName());
                            }

                            @Override
                            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                                log.warn("[{}] Failed to disable deduplication: {}", topic.getName(),
                                        exception.getMessage());
                                status = Status.Failed;
                                future.completeExceptionally(exception);
                            }
                        }, null);

                return future;
            } else if (status == Status.Disabled && shouldBeEnabled) {
                // 状态为禁用，配置为启用，则开启去重功能
                CompletableFuture<Void> future = new CompletableFuture<>();
                managedLedger.asyncOpenCursor(PersistentTopic.DEDUPLICATION_CURSOR_NAME, new OpenCursorCallback() {

                    @Override
                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                        // 游标不启用缓存功能
                        cursor.setAlwaysInactive();
                        managedCursor = cursor;
                        // 恢复序列ID Map
                        recoverSequenceIdsMap().thenRun(() -> {
                            status = Status.Enabled;
                            future.complete(null);
                            log.info("[{}] Enabled deduplication", topic.getName());
                        }).exceptionally(ex -> {
                            status = Status.Failed;
                            log.warn("[{}] Failed to enable deduplication: {}", topic.getName(), ex.getMessage());
                            future.completeExceptionally(ex);
                            return null;
                        });
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                        log.warn("[{}] Failed to enable deduplication: {}", topic.getName(),
                                exception.getMessage());
                        future.completeExceptionally(exception);
                    }

                }, null);
                return future;
            } else {
                // 什么事情都不干，当前为成功状态
                return CompletableFuture.completedFuture(null);
            }
        }
    });
}

 private CompletableFuture<Void> recoverSequenceIdsMap() {
    // 从游标属性加载序列ID
    managedCursor.getProperties().forEach((k, v) -> {
        highestSequencedPushed.put(k, v);
        highestSequencedPersisted.put(k, v);
    });

    // 重放所有的消息，使所有的序列ID更新
    log.info("[{}] Replaying {} entries for deduplication", topic.getName(), managedCursor.getNumberOfEntries());
    CompletableFuture<Void> future = new CompletableFuture<>();
    replayCursor(future);
    return future;
}

private void replayCursor(CompletableFuture<Void> future) {
    // 批量读取消息数100
    managedCursor.asyncReadEntries(100, new ReadEntriesCallback() {
        @Override
        public void readEntriesComplete(List<Entry> entries, Object ctx) {
            // 读取完成，
            for (Entry entry : entries) {
                ByteBuf messageMetadataAndPayload = entry.getDataBuffer();
                MessageMetadata md = Commands.parseMessageMetadata(messageMetadataAndPayload);

                String producerName = md.getProducerName();
                long sequenceId = md.getSequenceId();
                highestSequencedPushed.put(producerName, sequenceId);
                highestSequencedPersisted.put(producerName, sequenceId);

                md.recycle();
                entry.release();
            }
            // 如果还有消息，则继续批量读取
            if (managedCursor.hasMoreEntries()) {
                pulsar.getExecutor().execute(() -> replayCursor(future));
            } else {
                // 重放完成
                future.complete(null);
            }
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            future.completeExceptionally(exception);
        }
    }, null);
}

public CompletableFuture<Void> stopReplProducers() {
    List<CompletableFuture<Void>> closeFutures = Lists.newArrayList();
    // 依次关闭复制器，即使有积压消息，也立即关闭
    replicators.forEach((region, replicator) -> closeFutures.add(replicator.disconnect()));
    return FutureUtil.waitForAll(closeFutures);
}

public CompletableFuture<Void> disconnect() {
    return disconnect(false);
}

public synchronized CompletableFuture<Void> disconnect(boolean failIfHasBacklog) {
    // 如果failIfHasBacklog为true并且有消息积压，则抛异常
    if (failIfHasBacklog && getNumberOfEntriesInBacklog() > 0) {
        CompletableFuture<Void> disconnectFuture = new CompletableFuture<>();
        disconnectFuture.completeExceptionally(new TopicBusyException("Cannot close a replicator with backlog"));
        log.debug("[{}][{} -> {}] Replicator disconnect failed since topic has backlog", topicName, localCluster,
                remoteCluster);
        return disconnectFuture;
    }

    if (STATE_UPDATER.get(this) == State.Stopping) {
        // 什么都不做，因为所有 STATE_UPDATER.set(this，Stopping) 指令后跟closeProducerAsync()，它会在某些时候将状态更改为已停止
        return CompletableFuture.completedFuture(null);
    }

    if (producer != null && (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopping)
            || STATE_UPDATER.compareAndSet(this, State.Started, State.Stopping))) {
        log.info("[{}][{} -> {}] Disconnect replicator at position {} with backlog {}", topicName, localCluster,
                remoteCluster, getReplicatorReadPosition(), getNumberOfEntriesInBacklog());
        // 关闭生产者
        return closeProducerAsync();
    }

    STATE_UPDATER.set(this, State.Stopped);
    return CompletableFuture.completedFuture(null);
}

// 检查当前 Topic 是否有 Schema 信息
public CompletableFuture<Boolean> hasSchema() {
    String base = TopicName.get(getName()).getPartitionedTopicName();
    String id = TopicName.get(base).getSchemaName();
    return brokerService.pulsar()
        .getSchemaRegistryService()
        .getSchema(id).thenApply((schema) -> schema != null);
}

//==================以下为非持久性Topic的创建=========================
private CompletableFuture<Optional<Topic>> createNonPersistentTopic(String topic) {
    CompletableFuture<Optional<Topic>> topicFuture = new CompletableFuture<>();
    // 当前配置禁止创建非持久化Topic
    if (!pulsar.getConfiguration().isEnableNonPersistentTopics()) {
        if (log.isDebugEnabled()) {
            log.debug("Broker is unable to load non-persistent topic {}", topic);
        }
        topicFuture.completeExceptionally(
                new NotAllowedException("Broker is not unable to load non-persistent topic"));
        return topicFuture;
    }
    final long topicCreateTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    NonPersistentTopic nonPersistentTopic = new NonPersistentTopic(topic, this);
    // 检查副本复制器
    CompletableFuture<Void> replicationFuture = nonPersistentTopic.checkReplication();
    replicationFuture.thenRun(() -> {
        log.info("Created topic {}", nonPersistentTopic);
        long topicLoadLatencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - topicCreateTimeMs;
        pulsarStats.recordTopicLoadTimeValue(topic, topicLoadLatencyMs);
        addTopicToStatsMaps(TopicName.get(topic), nonPersistentTopic);
        topicFuture.complete(Optional.of(nonPersistentTopic));
    });
    replicationFuture.exceptionally((ex) -> {
        log.warn("Replication check failed. Removing topic from topics list {}, {}", topic, ex);
        nonPersistentTopic.stopReplProducers().whenComplete((v, exception) -> {
            pulsar.getExecutor().execute(() -> topics.remove(topic, topicFuture));
            topicFuture.completeExceptionally(ex);
        });

        return null;
    });

    return topicFuture;
}

// 检查副本复制器，添加新增以及停止删除的副本复制器，
// 逻辑基本上跟持久化副本复制器一致，复制器的逻辑
// 基本一致，只是不会有游标操作，不会从 bookkeeper 读取数据
public CompletableFuture<Void> checkReplication() {
    TopicName name = TopicName.get(topic);
    if (!name.isGlobal()) {
        return CompletableFuture.completedFuture(null);
    }

    if (log.isDebugEnabled()) {
        log.debug("[{}] Checking replication status", name);
    }

    Policies policies = null;
    try {
        policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                .get(AdminResource.path(POLICIES, name.getNamespace()))
                .orElseThrow(() -> new KeeperException.NoNodeException());
    } catch (Exception e) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(new ServerMetadataException(e));
        return future;
    }

    Set<String> configuredClusters;
    if (policies.replication_clusters != null) {
        configuredClusters = policies.replication_clusters;
    } else {
        configuredClusters = Collections.emptySet();
    }

    String localCluster = brokerService.pulsar().getConfiguration().getClusterName();

    List<CompletableFuture<Void>> futures = Lists.newArrayList();

    // 检查丢失的副本
    for (String cluster : configuredClusters) {
        if (cluster.equals(localCluster)) {
            continue;
        }

        if (!replicators.containsKey(cluster)) {
            if (!startReplicator(cluster)) {
                return FutureUtil
                        .failedFuture(new NamingException(topic + " failed to start replicator for " + cluster));
            }
        }
    }

    replicators.forEach((cluster, replicator) -> {
        if (!cluster.equals(localCluster)) {
            if (!configuredClusters.contains(cluster)) {
                futures.add(removeReplicator(cluster));
            }
        }
    });
    return FutureUtil.waitForAll(futures);
}
// 增加副本复制器
boolean startReplicator(String remoteCluster) {
    log.info("[{}] Starting replicator to remote: {}", topic, remoteCluster);
    String localCluster = brokerService.pulsar().getConfiguration().getClusterName();
    return addReplicationCluster(remoteCluster,NonPersistentTopic.this, localCluster);
}
// 这里创建的是非持久化复制器
protected boolean addReplicationCluster(String remoteCluster, NonPersistentTopic nonPersistentTopic, String localCluster) {
    AtomicBoolean isReplicatorStarted = new AtomicBoolean(true);
    replicators.computeIfAbsent(remoteCluster, r -> {
        try {
            return new NonPersistentReplicator(NonPersistentTopic.this, localCluster, remoteCluster, brokerService);
        } catch (NamingException e) {
            isReplicatorStarted.set(false);
            log.error("[{}] Replicator startup failed due to partitioned-topic {}", topic, remoteCluster);
        }
        return null;
    });
    if (!isReplicatorStarted.get()) {
        replicators.remove(remoteCluster);
    }
    return isReplicatorStarted.get();
}
// 大体逻辑类似，这里只设置了如果队列满，是否堵塞，然后跟持久化复制器一样，启动生产者，这里都是调用抽象复制器
// 上文已有分析，这里不再赘述
public NonPersistentReplicator(NonPersistentTopic topic, String localCluster, String remoteCluster,
        BrokerService brokerService) throws NamingException {
    super(topic.getName(), topic.replicatorPrefix, localCluster, remoteCluster, brokerService);

    producerBuilder.blockIfQueueFull(false);

    startProducer();
}
// 这里是读取消息，与持久化复制器不同的是，这里不会去 bookkeeper 读消息，到这里，就直接返回了，比持久化复制器简单太多
protected void readEntries(Producer<byte[]> producer) {
    this.producer = (ProducerImpl) producer;

    if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Started)) {
        log.info("[{}][{} -> {}] Created replicator producer", topicName, localCluster, remoteCluster);
        backOff.reset();
    } else {
        log.info(
                "[{}][{} -> {}] Replicator was stopped while creating the producer. Closing it. Replicator state: {}",
                topicName, localCluster, remoteCluster, STATE_UPDATER.get(this));
        STATE_UPDATER.set(this, State.Stopping);
        closeProducerAsync();
        return;
    }
}
```

到此，总结下大致逻辑：

1. Topic 校验
2. Client 与 Proxy 权限校验
3. Topic 获取或创建
4. 检查副本复制器，启动新增或停止删除的副本复制器
5. 检查去重功能
6. 加密校验
7. SchemaVersion 校验

好了，开始进入发送消息的流程。

##### 3. handleSend 命令实现

当客户端已成功在 broker 注册时，此时 broker 就可以接收来自客户端的消息，如下：

```java

protected void handleSend(CommandSend send, ByteBuf headersAndPayload) {
    // 状态校验
    checkArgument(state == State.Connected);
    // 读取 Producer Future 
    CompletableFuture<Producer> producerFuture = producers.get(send.getProducerId());
    // 判定 Producer 是否已关闭
    if (producerFuture == null || !producerFuture.isDone() || producerFuture.isCompletedExceptionally()) {
        log.warn("[{}] Producer had already been closed: {}", remoteAddress, send.getProducerId());
        return;
    }

    Producer producer = producerFuture.getNow(null);
    if (log.isDebugEnabled()) {
        // 这里打印接收到的消息
        printSendCommandDebug(send, headersAndPayload);
    }
    // 处理非持久化消息
    if (producer.isNonPersistentTopic()) {
        // 如果达到最大并发消息限制，则避免处理非持久化消息
        if (nonPersistentPendingMessages > MaxNonPersistentPendingMessages) {
            final long producerId = send.getProducerId();
            final long sequenceId = send.getSequenceId();
            service.getTopicOrderedExecutor().executeOrdered(producer.getTopic().getName(), SafeRun.safeRun(() -> {
                // 发送回执给客户端
                ctx.writeAndFlush(Commands.newSendReceipt(producerId, sequenceId, -1, -1), ctx.voidPromise());
            }));
            producer.recordMessageDrop(send.getNumMessages());
            return;
        } else {
            nonPersistentPendingMessages++;
        }
    }
    // 开始发送操作：正处理发送请求是否已经达到最大发送限制，如果达到，则暂时禁用自动读设置
    startSendOperation();

    //保存消息
    producer.publishMessage(send.getProducerId(), send.getSequenceId(), headersAndPayload, send.getNumMessages());
}

public void startSendOperation() {
    if (++pendingSendRequest == MaxPendingSendRequests) {
        // 当达到挂起发送请求的配额时，停止从套接字读取以在客户端连接上造成反压力，可能在多个生产者之间共享。
        ctx.channel().config().setAutoRead(false);
    }
}

//=================================================================================================================
//以下是持久化消息逻辑
public void publishMessage(long producerId, long sequenceId, ByteBuf headersAndPayload, long batchSize) {
    // 生产者被关闭了，直接发送错误信息
    if (isClosed) {
        cnx.ctx().channel().eventLoop().execute(() -> {
            cnx.ctx().writeAndFlush(Commands.newSendError(producerId, sequenceId, ServerError.PersistenceError,
                    "Producer is closed"));
            cnx.completedSendOperation(isNonPersistentTopic);
        });

        return;
    }
    // 消息校验校验和，如果失败，则直接发送错误信息
    if (!verifyChecksum(headersAndPayload)) {
        cnx.ctx().channel().eventLoop().execute(() -> {
            cnx.ctx().writeAndFlush(
                    Commands.newSendError(producerId, sequenceId, ServerError.ChecksumError, "Checksum failed on the broker"));
            cnx.completedSendOperation(isNonPersistentTopic);
        });
        return;
    }
    // Topic 配置加密
    if (topic.isEncryptionRequired()) {

        headersAndPayload.markReaderIndex();
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        headersAndPayload.resetReaderIndex();

        // 检查消息是否已加密，如果没加密，直接返回错误信息
        if (msgMetadata.getEncryptionKeysCount() < 1) {
            log.warn("[{}] Messages must be encrypted", getTopic().getName());
            cnx.ctx().channel().eventLoop().execute(() -> {
                cnx.ctx().writeAndFlush(Commands.newSendError(producerId, sequenceId, ServerError.MetadataError,
                        "Messages must be encrypted"));
                cnx.completedSendOperation(isNonPersistentTopic);
            });
            return;
        }
    }
    // 增加正应答数计数
    startPublishOperation();
    //这里开始区分持久化消息和非持久化消息处理
    topic.publishMessage(headersAndPayload,
            MessagePublishContext.get(this, sequenceId, msgIn, headersAndPayload.readableBytes(), batchSize));
}

// 完成发送操作
public void completedSendOperation(boolean isNonPersistentTopic) {
    // 正读取请求等于恢复读取阈值，启用自动读数据
    if (--pendingSendRequest == ResumeReadsThreshold) {
        // Resume reading from socket
        ctx.channel().config().setAutoRead(true);
    }
    //非持久化的Topic
    if (isNonPersistentTopic) {
        nonPersistentPendingMessages--;
    }
}

// 校验校验和
private boolean verifyChecksum(ByteBuf headersAndPayload) {
    // 如果消息体里面有校验和
    if (hasChecksum(headersAndPayload)) {
        int readerIndex = headersAndPayload.readerIndex();

        try {
            // 读取消息头中的校验和
            int checksum = readChecksum(headersAndPayload);
            // 重新计算校验和
            long computedChecksum = computeChecksum(headersAndPayload);
            // 如果相等则认为消息正常，无损坏，否则则认为损坏
            if (checksum == computedChecksum) {
                return true;
            } else {
                log.error("[{}] [{}] Failed to verify checksum", topic, producerName);
                return false;
            }
        } finally {
            headersAndPayload.readerIndex(readerIndex);
        }
    } else {
        // 如果没校验和，则直接跳过
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Payload does not have checksum to verify", topic, producerName);
        }
        return true;
    }
}

//持久化消息处理
public void publishMessage(ByteBuf headersAndPayload, PublishContext publishContext) {
    if (messageDeduplication.shouldPublishNextMessage(publishContext, headersAndPayload)) {
        // 消息合法，添加到 Topic 中
        ledger.asyncAddEntry(headersAndPayload, this, publishContext);
    } else {
        // 立即应答表示重复消息
        publishContext.completed(null, -1, -1);
    }
}

// 消息去重处理
public boolean shouldPublishNextMessage(PublishContext publishContext, ByteBuf headersAndPayload) {
    // 判定是否启用
    if (!isEnabled()) {
        return true;
    }

    String producerName = publishContext.getProducerName();
    long sequenceId = publishContext.getSequenceId();
    if (producerName.startsWith(replicatorPrefix)) {
        // 消息来自复制器，需要使用原始生产者名称和序列ID进行重复数据消除，而不依赖“复制器”名称。
        int readerIndex = headersAndPayload.readerIndex();
        MessageMetadata md = Commands.parseMessageMetadata(headersAndPayload);
        producerName = md.getProducerName();
        sequenceId = md.getSequenceId();
        publishContext.setOriginalProducerName(producerName);
        publishContext.setOriginalSequenceId(sequenceId);
        headersAndPayload.readerIndex(readerIndex);
        md.recycle();
    }

    // 在map 使用同步get()和随后的put()。只有当生产者很快断开并重新连接时，这才是相关的。此时，调用可能来自不同的线程，故用同步区块
    synchronized (highestSequencedPushed) {
        // 通过生产者名称读取最后使用的序列ID，如果当前序列ID小于等于最后序列ID，则返回fales
        Long lastSequenceIdPushed = highestSequencedPushed.get(producerName);
        if (lastSequenceIdPushed != null && sequenceId <= lastSequenceIdPushed) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Message identified as duplicated producer={} seq-id={} -- highest-seq-id={}",
                        topic.getName(), producerName, sequenceId, lastSequenceIdPushed);
            }
            return false;
        }
        // 这里代表当前序列ID可用
        highestSequencedPushed.put(producerName, sequenceId);
    }
    return true;
}

// 异步添加消息
public void asyncAddEntry(ByteBuf buffer, AddEntryCallback callback, Object ctx) {
    if (log.isDebugEnabled()) {
        log.debug("[{}] asyncAddEntry size={} state={}", name, buffer.readableBytes(), state);
    }

    OpAddEntry addOperation = OpAddEntry.create(this, buffer, callback, ctx);

    // 跳到特定线程以避免来自不同线程的写入竞争（这里以 Topic 名为Key,特定的Topic请求放到一个队列）
    executor.executeOrdered(name, safeRun(() -> {
        // 正处理队列
        pendingAddEntries.add(addOperation);

        internalAsyncAddEntry(addOperation);
    }));
}

private synchronized void internalAsyncAddEntry(OpAddEntry addOperation) {
    // 状态检查
    final State state = STATE_UPDATER.get(this);
    if (state == State.Fenced) {
        addOperation.failed(new ManagedLedgerFencedException());
        return;
    } else if (state == State.Terminated) {
        addOperation.failed(new ManagedLedgerTerminatedException("Managed ledger was already terminated"));
        return;
    } else if (state == State.Closed) {
        addOperation.failed(new ManagedLedgerAlreadyClosedException("Managed ledger was already closed"));
        return;
    }
    // 没有准备好的 ledger 写入，等待新 ledger 创建
    if (state == State.ClosingLedger || state == State.CreatingLedger) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Queue addEntry request", name);
        }
    } else if (state == State.ClosedLedger) {
        long now = clock.millis();
        if (now < lastLedgerCreationFailureTimestamp + WaitTimeAfterLedgerCreationFailureMs) {
            // 拒绝写入请求，因为自从上次尝试创建新的ledger，还未等待足够多的时间。
            pendingAddEntries.remove(addOperation);
            addOperation.failed(new ManagedLedgerException("Waiting for new ledger creation to complete"));
            return;
        }

        // 没有ledger和没有正处理操作，创建新的 ledger
        if (log.isDebugEnabled()) {
            log.debug("[{}] Creating a new ledger", name);
        }
        // 尝试设置状态为正创建ledger，如果成功则创建 新的 ledger
        if (STATE_UPDATER.compareAndSet(this, State.ClosedLedger, State.CreatingLedger)) {
            this.lastLedgerCreationInitiationTimestamp = System.nanoTime();
            mbean.startDataLedgerCreateOp();
            // 异步创建ledger
            asyncCreateLedger(bookKeeper, config, digestType, this, Collections.emptyMap());
        }
    } else {
        // 状态必须为已打开
        checkArgument(state == State.LedgerOpened, "ledger=%s is not opened", state);

        // 写入当前 ledger
        addOperation.setLedger(currentLedger);
        // 统计消息数
        ++currentLedgerEntries;
        // 统计消息字节数
        currentLedgerSize += addOperation.data.readableBytes();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Write into current ledger lh={} entries={}", name, currentLedger.getId(),
                    currentLedgerEntries);
        }
        // 判定当前的 ledger 是否已满
        if (currentLedgerIsFull()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Closing current ledger lh={}", name, currentLedger.getId());
            }
            //当消息添加完成时，异步关闭 ledger
            addOperation.setCloseWhenDone(true);
            STATE_UPDATER.set(this, State.ClosingLedger);
        }

        addOperation.initiate();
    }
}

private boolean currentLedgerIsFull() {
    // 判定是否达到空间配额，两个方面：一个是每个 ledger 达到最大消息，一个是达到最大字节数
    boolean spaceQuotaReached = (currentLedgerEntries >= config.getMaxEntriesPerLedger()
            || currentLedgerSize >= (config.getMaxSizePerLedgerMb() * MegaByte));
    // 
    long timeSinceLedgerCreationMs = clock.millis() - lastLedgerCreatedTimestamp;
    boolean maxLedgerTimeReached = timeSinceLedgerCreationMs >= maximumRolloverTimeMs;
    // 配额已满或创建超时
    if (spaceQuotaReached || maxLedgerTimeReached) {
        // 这里是最小切换时间
        if (config.getMinimumRolloverTimeMs() > 0) {
            // 切换超时，这里必须换 ledger
            boolean switchLedger = timeSinceLedgerCreationMs > config.getMinimumRolloverTimeMs();
            if (log.isDebugEnabled()) {
                log.debug("Diff: {}, threshold: {} -- switch: {}", clock.millis() - lastLedgerCreatedTimestamp,
                        config.getMinimumRolloverTimeMs(), switchLedger);
            }
            return switchLedger;
        } else {
            return true;
        }
    } else {
        return false;
    }
}

public void initiate() {
    ByteBuf duplicateBuffer = data.retainedDuplicate();

    // 内部异步增加消息将会获取缓冲区的所有权并在末尾释放
    lastInitTime = System.nanoTime();
    ledger.asyncAddEntry(duplicateBuffer, this, ctx);
}

// 添加消息后，由 bookkeeper 回调
public void addComplete(int rc, final LedgerHandle lh, long entryId, Object ctx) {
    // 这里是预防性检查
    if (ledger.getId() != lh.getId()) {
        log.warn("[{}] ledgerId {} doesn't match with acked ledgerId {}", ml.getName(), ledger.getId(), lh.getId());
    }
    checkArgument(ledger.getId() == lh.getId(), "ledgerId %s doesn't match with acked ledgerId %s", ledger.getId(),
            lh.getId());
    checkArgument(this.ctx == ctx);

    this.entryId = entryId;
    if (log.isDebugEnabled()) {
        log.debug("[{}] [{}] write-complete: ledger-id={} entry-id={} size={} rc={}", this, ml.getName(),
                lh.getId(), entryId, dataLength, rc);
    }
    // 如果返回添加失败，则进行异常处理
    if (rc != BKException.Code.OK) {
        handleAddFailure(lh);
    } else {
        // 检查是否完成
        if(!checkAndCompleteTimeoutTask()) {
            return;
        }
        // 触发 addComplete 回调 在特定线程里，这里是以 ledger name 为哈希槽里，保证顺序
        ml.getExecutor().executeOrdered(ml.getName(), this);
    }
}

// 这里消息添加失败的异常处理
void handleAddFailure(final LedgerHandle ledger) {
    // 如果收到写入错误，将尝试创建新的 ledger 并重新提交挂起的写操作。 如果ledger创建失败（可能是 bookie 实例失败，将切换另外一个拥有 mangerdledger 的实例），则写入将被标记为失败。
    ml.mbean.recordAddEntryError();

    ml.getExecutor().executeOrdered(ml.getName(), SafeRun.safeRun(() -> {
        // 强制创建新的 ledger。这里用后台线程执行是为了避免请求ML lock（其实就是synchronized方法） 在BK 执行回调的时候。
        ml.ledgerClosed(ledger);
    }));
}

// 添加消息失败时，关闭原有的ledger并创建新的ledger
synchronized void ledgerClosed(final LedgerHandle lh) {
    final State state = STATE_UPDATER.get(this);
    LedgerHandle currentLedger = this.currentLedger;
    if (currentLedger == lh && (state == State.ClosingLedger || state == State.LedgerOpened)) {
        STATE_UPDATER.set(this, State.ClosedLedger);
    } else if (state == State.Closed) {
        // 如果已经关闭，则清理正处理的写请求，设置异常
        clearPendingAddEntries(new ManagedLedgerAlreadyClosedException("Managed ledger was already closed"));
        return;
    } else {
        // 为了避免不同的未完成写请求设置多个写错误，我们应该只关闭仅仅一个 ledger
        return;
    }

    long entriesInLedger = lh.getLastAddConfirmed() + 1;
    if (log.isDebugEnabled()) {
        log.debug("[{}] Ledger has been closed id={} entries={}", name, lh.getId(), entriesInLedger);
    }
    // 构建新的 ledger 信息
    if (entriesInLedger > 0) {
        LedgerInfo info = LedgerInfo.newBuilder().setLedgerId(lh.getId()).setEntries(entriesInLedger)
                .setSize(lh.getLength()).setTimestamp(clock.millis()).build();
        ledgers.put(lh.getId(), info);
    } else {
        // 如果最后一个ledger为空，则丢弃（因为这个ledger写入出问题了）
        ledgers.remove(lh.getId());
        mbean.startDataLedgerDeleteOp();
        // 异步删除
        bookKeeper.asyncDeleteLedger(lh.getId(), (rc, ctx) -> {
            mbean.endDataLedgerDeleteOp();
            log.info("[{}] Delete complete for empty ledger {}. rc={}", name, lh.getId(), rc);
        }, null);
    }
    // 检查是否已有消费完的ledger，并且删除
    trimConsumedLedgersInBackground();
    // 触发数据备份在后台
    maybeOffloadInBackground(NULL_OFFLOAD_PROMISE);
    // 如果不为空，则创建新 ledger 并处理正等待写入的消息
    if (!pendingAddEntries.isEmpty()) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Creating a new ledger", name);
        }
        STATE_UPDATER.set(this, State.CreatingLedger);
        this.lastLedgerCreationInitiationTimestamp = System.nanoTime();
        mbean.startDataLedgerCreateOp();
        // 执行创建，上文已分析，这里不再赘述
        asyncCreateLedger(bookKeeper, config, digestType, this, Collections.emptyMap());
    }
}

private void trimConsumedLedgersInBackground() {
    trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
}

private void trimConsumedLedgersInBackground(CompletableFuture<?> promise) {
    executor.executeOrdered(name, safeRun(() -> {
        internalTrimConsumedLedgers(promise);
    }));
}

private void scheduleDeferredTrimming(CompletableFuture<?> promise) {
    scheduledExecutor.schedule(safeRun(() -> trimConsumedLedgersInBackground(promise)), 100, TimeUnit.MILLISECONDS);
}

void internalTrimConsumedLedgers(CompletableFuture<?> promise) {
        // 同一时间只能有一个清理线程，这里先尝试获取互斥锁
    if (!trimmerMutex.tryLock()) {
        scheduleDeferredTrimming(promise);
        return;
    }
    // ledger 需要删除列表
    List<LedgerInfo> ledgersToDelete = Lists.newArrayList();
    // 卸载的ledger 要删除列表
    List<LedgerInfo> offloadedLedgersToDelete = Lists.newArrayList();
    synchronized (this) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Start TrimConsumedLedgers. ledgers={} totalSize={}", name, ledgers.keySet(),
                    TOTAL_SIZE_UPDATER.get(this));
        }
        // 验证状态，不能清理以关闭的 ledger
        if (STATE_UPDATER.get(this) == State.Closed) {
            log.debug("[{}] Ignoring trimming request since the managed ledger was already closed", name);
            trimmerMutex.unlock();
            promise.completeExceptionally(new ManagedLedgerAlreadyClosedException("Can't trim closed ledger"));
            return;
        }

        long slowestReaderLedgerId = -1;
        // 无游标
        if (cursors.isEmpty()) {
            // 此时，最后的Ledger将指向刚刚关闭的 ledger，因此在剪裁中包含最后Ledger的+1
            slowestReaderLedgerId = currentLedger.getId() + 1;
        } else {
            // 从游标中找出最慢的读位置
            PositionImpl slowestReaderPosition = cursors.getSlowestReaderPosition();
            if (slowestReaderPosition != null) {
                // 这里就找到了最慢所在的 ledger，清理以它为准
                slowestReaderLedgerId = slowestReaderPosition.getLedgerId();
            } else {
                // 没有找到对应的读位置，抛异常
                promise.completeExceptionally(new ManagedLedgerException("Couldn't find reader position"));
                trimmerMutex.unlock();
                return;
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Slowest consumer ledger id: {}", name, slowestReaderLedgerId);
        }

        // 拿到ledgerID之前所有的ledger列表
        for (LedgerInfo ls : ledgers.headMap(slowestReaderLedgerId, false).values()) {
            // 是否过期
            boolean expired = hasLedgerRetentionExpired(ls.getTimestamp());
            // 是否超过大小配额
            boolean overRetentionQuota = isLedgerRetentionOverSizeQuota();

            if (log.isDebugEnabled()) {
                log.debug(
                        "[{}] Checking ledger {} -- time-old: {} sec -- "
                                + "expired: {} -- over-quota: {} -- current-ledger: {}",
                        name, ls.getLedgerId(), (clock.millis() - ls.getTimestamp()) / 1000.0, expired,
                        overRetentionQuota, currentLedger.getId());
            }
            // 如果是当前的ledger，则直接跳过
            if (ls.getLedgerId() == currentLedger.getId()) {
                log.debug("[{}] ledger id skipped for deletion as it is currently being written to", name,
                        ls.getLedgerId());
                break;
            } else if (expired) { //过期的ledger
                log.debug("[{}] Ledger {} has expired, ts {}", name, ls.getLedgerId(), ls.getTimestamp());
                ledgersToDelete.add(ls);
            } else if (overRetentionQuota) {//大小超过配额
                log.debug("[{}] Ledger {} is over quota", name, ls.getLedgerId());
                ledgersToDelete.add(ls);
            } else {
                log.debug("[{}] Ledger {} not deleted. Neither expired nor over-quota", name, ls.getLedgerId());
                break;
            }
        }
        // 检查所有的ledger，判定他们是否已完成存档操作
        for (LedgerInfo ls : ledgers.values()) {
            if (isOffloadedNeedsDelete(ls.getOffloadContext()) && !ledgersToDelete.contains(ls)) {
                log.debug("[{}] Ledger {} has been offloaded, bookkeeper ledger needs to be deleted", name,
                        ls.getLedgerId());
                offloadedLedgersToDelete.add(ls);
            }
        }
        // 如果两个都为空，则立即返回
        if (ledgersToDelete.isEmpty() && offloadedLedgersToDelete.isEmpty()) {
            trimmerMutex.unlock();
            promise.complete(null);
            return;
        }
        // 状态显示正在创建中，或者 尝试获取 ledgersList 互斥锁不成功（避免另外的线程导致死锁） ，则现在放弃回收，等下一轮（间隔100ms）回收
        if (STATE_UPDATER.get(this) == State.CreatingLedger
                || !ledgersListMutex.tryLock()) { 
            scheduleDeferredTrimming(promise);
            trimmerMutex.unlock();
            return;
        }

        // 更新元数据
        for (LedgerInfo ls : ledgersToDelete) {
            // ledger 缓存中移除
            ledgerCache.remove(ls.getLedgerId());
            // 列表移除
            ledgers.remove(ls.getLedgerId());
            // 更新消息数量
            NUMBER_OF_ENTRIES_UPDATER.addAndGet(this, -ls.getEntries());
            // 更新总大小
            TOTAL_SIZE_UPDATER.addAndGet(this, -ls.getSize());
            //缓存失效
            entryCache.invalidateAllEntries(ls.getLedgerId());
        }
        // 卸载ledger处理
        for (LedgerInfo ls : offloadedLedgersToDelete) {
            LedgerInfo.Builder newInfoBuilder = ls.toBuilder();
            newInfoBuilder.getOffloadContextBuilder().setBookkeeperDeleted(true);
            // 获取第三方备份驱动名
            String driverName = OffloadUtils.getOffloadDriverName(ls,
                    config.getLedgerOffloader().getOffloadDriverName());
            // 获取第三方备份驱动元数据
            Map<String, String> driverMetadata = OffloadUtils.getOffloadDriverMetadata(ls,
                    config.getLedgerOffloader().getOffloadDriverMetadata());
            OffloadUtils.setOffloadDriverMetadata(newInfoBuilder, driverName, driverMetadata);
            ledgers.put(ls.getLedgerId(), newInfoBuilder.build());
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Updating of ledgers list after trimming", name);
        }
        // 这里异步更新 ledger 元数据
        store.asyncUpdateLedgerIds(name, getManagedLedgerInfo(), ledgersStat, new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                log.info("[{}] End TrimConsumedLedgers. ledgers={} totalSize={}", name, ledgers.size(),
                        TOTAL_SIZE_UPDATER.get(ManagedLedgerImpl.this));
                ledgersStat = stat;
                // 注意解锁顺序
                ledgersListMutex.unlock();
                trimmerMutex.unlock();
                // 元数据更新成功后，这里开始异步删除 ledger 以及 备份的ledger（也就是从云存储删除）
                for (LedgerInfo ls : ledgersToDelete) {
                    log.info("[{}] Removing ledger {} - size: {}", name, ls.getLedgerId(), ls.getSize());
                    asyncDeleteLedger(ls.getLedgerId(), ls);
                }
                // 这里直接删除备份的ledger （也就是从云存储删除）
                for (LedgerInfo ls : offloadedLedgersToDelete) {
                    log.info("[{}] Deleting offloaded ledger {} from bookkeeper - size: {}", name, ls.getLedgerId(),
                            ls.getSize());
                    asyncDeleteLedgerFromBookKeeper(ls.getLedgerId());
                }
                // 成功
                promise.complete(null);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                log.warn("[{}] Failed to update the list of ledgers after trimming", name, e);
                ledgersListMutex.unlock();
                trimmerMutex.unlock();

                promise.completeExceptionally(e);
            }
        });
    }
}

public void asyncUpdateLedgerIds(String ledgerName, ManagedLedgerInfo mlInfo, Stat stat,
        final MetaStoreCallback<Void> callback) {

    ZKStat zkStat = (ZKStat) stat;
    if (log.isDebugEnabled()) {
        log.debug("[{}] Updating metadata version={} with content={}", ledgerName, zkStat.version, mlInfo);
    }

    byte[] serializedMlInfo = mlInfo.toByteArray(); // 二进制格式
    // 这里数据存进了zk
    zk.setData(prefix + ledgerName, serializedMlInfo, zkStat.getVersion(),
            (rc, path, zkCtx, stat1) -> executor.executeOrdered(ledgerName, safeRun(() -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] UpdateLedgersIdsCallback.processResult rc={} newVersion={}", ledgerName,
                            Code.get(rc), stat != null ? stat.getVersion() : "null");
                }
                MetaStoreException status = null;
                if (rc == Code.BADVERSION.intValue()) {
                    // 自上次读取以来，已在zk上修改了内容，即版本不匹配
                    status = new BadVersionException(KeeperException.create(Code.get(rc)));
                    callback.operationFailed(status);
                } else if (rc != Code.OK.intValue()) {
                    // 更新异常
                    status = new MetaStoreException(KeeperException.create(Code.get(rc)));
                    callback.operationFailed(status);
                } else {//成功
                    callback.operationComplete(null, new ZKStat(stat1));
                }
            })), null);
}

public PositionImpl getSlowestReaderPosition() {
    long stamp = rwLock.readLock();
    try {
        return heap.isEmpty() ? null : heap.get(0).position;
    } finally {
        rwLock.unlockRead(stamp);
    }
}

// 判断是否数据转移完成，是否需要删除
private boolean isOffloadedNeedsDelete(OffloadContext offload) {
    long elapsedMs = clock.millis() - offload.getTimestamp();
    return offload.getComplete() && !offload.getBookkeeperDeleted()
            && elapsedMs > config.getOffloadLedgerDeletionLagMillis();
}

private void asyncDeleteLedger(long ledgerId, LedgerInfo info) {
    // 数据转移的还没被删除，这里暂不删除
    if (!info.getOffloadContext().getBookkeeperDeleted()) {
        // only delete if it hasn't been previously deleted for offload
        asyncDeleteLedger(ledgerId, DEFAULT_LEDGER_DELETE_RETRIES);
    }
    // 直接指定高64位和低64位的值，用户配置指定
    if (info.getOffloadContext().hasUidMsb()) {
        UUID uuid = new UUID(info.getOffloadContext().getUidMsb(), info.getOffloadContext().getUidLsb());
        cleanupOffloaded(ledgerId, uuid,
                OffloadUtils.getOffloadDriverName(info, config.getLedgerOffloader().getOffloadDriverName()),
                OffloadUtils.getOffloadDriverMetadata(info, config.getLedgerOffloader().getOffloadDriverMetadata()),
                "Trimming");
    }
}

// 从备份仓库清理 ledger 相关数据
private void cleanupOffloaded(long ledgerId, UUID uuid, String offloadDriverName, 
        Map<String, String> offloadDriverMetadata, String cleanupReason) {
    Retries.run(Backoff.exponentialJittered(TimeUnit.SECONDS.toMillis(1), TimeUnit.SECONDS.toHours(1)).limit(10),
            Retries.NonFatalPredicate,
            () -> config.getLedgerOffloader().deleteOffloaded(ledgerId, uuid, offloadDriverMetadata),
            scheduledExecutor, name).whenComplete((ignored, exception) -> {
                if (exception != null) {
                    log.warn("Error cleaning up offload for {}, (cleanup reason: {})", ledgerId, cleanupReason,
                            exception);
                }
            });
}

// 判定释放达到备份数自动触发后台归档任务
private void maybeOffloadInBackground(CompletableFuture<PositionImpl> promise) {
    if (config.getOffloadAutoTriggerSizeThresholdBytes() >= 0) {
        executor.executeOrdered(name, safeRun(() -> maybeOffload(promise)));
    }
}

private void maybeOffload(CompletableFuture<PositionImpl> finalPromise) {
    // 如果没有拿到offload互斥锁，则下一轮继续
    if (!offloadMutex.tryLock()) {
        scheduledExecutor.schedule(safeRun(() -> maybeOffloadInBackground(finalPromise)),
                                   100, TimeUnit.MILLISECONDS);
    } else {
        // 解锁 Promise
        CompletableFuture<PositionImpl> unlockingPromise = new CompletableFuture<>();
        unlockingPromise.whenComplete((res, ex) -> {
                offloadMutex.unlock();
                if (ex != null) {
                    finalPromise.completeExceptionally(ex);
                } else {
                    finalPromise.complete(res);
                }
            });

        long threshold = config.getOffloadAutoTriggerSizeThresholdBytes();
        long sizeSummed = 0; //从最新的ledger算起，总计大小
        long alreadyOffloadedSize = 0; //已归档大小
        long toOffloadSize = 0;//需要归档的大小

        ConcurrentLinkedDeque<LedgerInfo> toOffload = new ConcurrentLinkedDeque();

        // 返回 ledger 列表最新的到最老的ledger，构建 offload 列表最老的到最新的 ledger
        for (Map.Entry<Long, LedgerInfo> e : ledgers.descendingMap().entrySet()) {
            long size = e.getValue().getSize();
            sizeSummed += size;
            boolean alreadyOffloaded = e.getValue().hasOffloadContext()
                && e.getValue().getOffloadContext().getComplete();
            // 已归档的
            if (alreadyOffloaded) {
                alreadyOffloadedSize += size;
            } else if (sizeSummed > threshold) { //这里判定是否达到触发阈值
                toOffloadSize += size;
                toOffload.addFirst(e.getValue());
            }
        }
        // 看是否有归档的ledger
        if (toOffload.size() > 0) {
            log.info("[{}] Going to automatically offload ledgers {}"
                     + ", total size = {}, already offloaded = {}, to offload = {}",
                     name, toOffload.stream().map(l -> l.getLedgerId()).collect(Collectors.toList()),
                     sizeSummed, alreadyOffloadedSize, toOffloadSize);
        } else {
            // 无需进行归档操作
            log.debug("[{}] Nothing to offload, total size = {}, already offloaded = {}, threshold = {}",
                      name, sizeSummed, alreadyOffloadedSize, threshold);
        }

        offloadLoop(unlockingPromise, toOffload, PositionImpl.latest, Optional.empty());
    }
}

// 这里就是归档，各方法调用就不再展开
private void offloadLoop(CompletableFuture<PositionImpl> promise, Queue<LedgerInfo> ledgersToOffload,
        PositionImpl firstUnoffloaded, Optional<Throwable> firstError) {
    LedgerInfo info = ledgersToOffload.poll();
    // 无归档ledger任务
    if (info == null) {
        if (firstError.isPresent()) {
            promise.completeExceptionally(firstError.get());
        } else {
            // 完成
            promise.complete(firstUnoffloaded);
        }
    } else {
        long ledgerId = info.getLedgerId();
        UUID uuid = UUID.randomUUID();
        Map<String, String> extraMetadata = ImmutableMap.of("ManagedLedgerName", name);
        // 获取归档的驱动名和驱动元数据
        String driverName = config.getLedgerOffloader().getOffloadDriverName();
        Map<String, String> driverMetadata = config.getLedgerOffloader().getOffloadDriverMetadata();
        // 预保存ledgerinfo到zk
        prepareLedgerInfoForOffloaded(ledgerId, uuid, driverName, driverMetadata)
            .thenCompose((ignore) -> getLedgerHandle(ledgerId)) //这里获取读句柄（云存储）
            .thenCompose(readHandle -> config.getLedgerOffloader().offload(readHandle, uuid, extraMetadata))//这里开始写数据
            .thenCompose((ignore) -> {
                    return Retries.run(Backoff.exponentialJittered(TimeUnit.SECONDS.toMillis(1),
                                                                   TimeUnit.SECONDS.toHours(1)).limit(10),
                                       FAIL_ON_CONFLICT,
                                       () -> completeLedgerInfoForOffloaded(ledgerId, uuid),//数据如果成功写完，则更新 ledgerInfo 到zk
                                       scheduledExecutor, name)
                        .whenComplete((ignore2, exception) -> {
                                 // 如果写zk失败，则清理掉云存储上的数据
                                if (exception != null) {
                                    cleanupOffloaded(
                                        ledgerId, uuid,
                                        driverName, driverMetadata,
                                        "Metastore failure");
                                }
                            });
                })
            .whenComplete((ignore, exception) -> {
                    // 异常
                    if (exception != null) {
                        log.warn("[{}] Exception occurred during offload", name, exception);

                        PositionImpl newFirstUnoffloaded = PositionImpl.get(ledgerId, 0);
                        if (newFirstUnoffloaded.compareTo(firstUnoffloaded) > 0) {
                            newFirstUnoffloaded = firstUnoffloaded;
                        }
                        Optional<Throwable> errorToReport = firstError;
                        synchronized (ManagedLedgerImpl.this) {
                            // 如果 ledger 不再存在，则忽略异常
                            if (ledgers.containsKey(ledgerId)) {
                                errorToReport = Optional.of(firstError.orElse(exception));
                            }
                        }

                        offloadLoop(promise, ledgersToOffload,
                                    newFirstUnoffloaded,
                                    errorToReport);
                    } else {
                        // 执行归档成功，移除 ledger，并继续执行，一直处理完为止
                        ledgerCache.remove(ledgerId);
                        offloadLoop(promise, ledgersToOffload, firstUnoffloaded, firstError);
                    }
                });
    }
}

//消息添加完成后执行
public void safeRun() {
    // 从正处理队列的头部移除消息新增操作实体
    OpAddEntry firstInQueue = ml.pendingAddEntries.poll();
    // 校验是否自己
    checkArgument(this == firstInQueue);
    // 统计消息总数以及消息总大小
    ManagedLedgerImpl.NUMBER_OF_ENTRIES_UPDATER.incrementAndGet(ml);
    ManagedLedgerImpl.TOTAL_SIZE_UPDATER.addAndGet(ml, dataLength);
    if (ml.hasActiveCursors()) {
        // 如果没有活动游标，则避免缓存消息
        EntryImpl entry = EntryImpl.create(ledger.getId(), entryId, data);
        // 这里数据都已经复制过了，故释放掉
        ml.entryCache.insert(entry);
        entry.release();
    }

    // 已经使用完，释放
    data.release();

    PositionImpl lastEntry = PositionImpl.get(ledger.getId(), entryId);
    ManagedLedgerImpl.ENTRIES_ADDED_COUNTER_UPDATER.incrementAndGet(ml);
    // 更新最后确认消息
    ml.lastConfirmedEntry = lastEntry;
    // 完成后是否关闭 ledger
    if (closeWhenDone) {
        log.info("[{}] Closing ledger {} for being full", ml.getName(), ledger.getId());
        ledger.asyncClose(this, ctx);
    } else {
        //统计消息延迟
        updateLatency();
        AddEntryCallback cb = callbackUpdater.getAndSet(this, null);
        if (cb != null) {
            // 消息增加完成，执行回调
            cb.addComplete(lastEntry, ctx);
            // 通知游标以便推送消息
            ml.notifyCursors();
            // 回收自己
            this.recycle();
        }
    }
}

// 通知游标，有消息到来
void notifyCursors() {
    while (true) {
        // 从游标等待队列获取一个游标
        final ManagedCursorImpl waitingCursor = waitingCursors.poll();
        if (waitingCursor == null) {
            break;
        }
        // 通知消息可用
        executor.execute(safeRun(() -> waitingCursor.notifyEntriesAvailable()));
    }
}

// 通知游标消息到来
void notifyEntriesAvailable() {
    if (log.isDebugEnabled()) {
        log.debug("[{}] [{}] Received ml notification", ledger.getName(), name);
    }
    // 获取读操作实体，并置为null
    OpReadEntry opReadEntry = WAITING_READ_OP_UPDATER.getAndSet(this, null);

    if (opReadEntry != null) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Received notification of new messages persisted, reading at {} -- last: {}",
                    ledger.getName(), name, opReadEntry.readPosition, ledger.lastConfirmedEntry);
            log.debug("[{}] Consumer {} cursor notification: other counters: consumed {} mdPos {} rdPos {}",
                    ledger.getName(), name, messagesConsumedCounter, markDeletePosition, readPosition);
        }
        // 增加正读取总数
        PENDING_READ_OPS_UPDATER.incrementAndGet(this);
        opReadEntry.readPosition = (PositionImpl) getReadPosition();
        // 异步读取消息
        ledger.asyncReadEntries(opReadEntry);
    } else {
        // 如果没有等待通知，则忽略
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Received notification but had no pending read operation", ledger.getName(), name);
        }
    }
}

//================================================================================================================
// 非持久化消息处理逻辑

public void publishMessage(ByteBuf data, PublishContext callback) {
    // 这里就执行回调成功了
    callback.completed(null, 0L, 0L);
    ENTRIES_ADDED_COUNTER_UPDATER.incrementAndGet(this);
    // 这里直接订阅分发逻辑
    subscriptions.forEach((name, subscription) -> {
        ByteBuf duplicateBuffer = data.retainedDuplicate();
        Entry entry = create(0L, 0L, duplicateBuffer);
        // 消息内部也会 retain 数据，所以，这里 duplicateBuffer 应该释放
        duplicateBuffer.release();
        if (subscription.getDispatcher() != null) {
            subscription.getDispatcher().sendMessages(Collections.singletonList(entry));
        } else {
            //它在创建订阅时但是未创建分发者，消费者也还没新增完成。
            entry.release();
        }
    });
    // 如果副本复制器不为空，则直接往副本发送消息
    if (!replicators.isEmpty()) {
        replicators.forEach((name, replicator) -> {
            ByteBuf duplicateBuffer = data.retainedDuplicate();
            Entry entry = create(0L, 0L, duplicateBuffer);
            // 消息内部也会 retain 数据，所以，这里 duplicateBuffer 应该释放
            duplicateBuffer.release();
            ((NonPersistentReplicator) replicator).sendMessage(entry);
        });
    }
}

// 单活消费者，非持久化分发器实现。
public void sendMessages(List<Entry> entries) {
    Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
    if (currentConsumer != null && currentConsumer.getAvailablePermits() > 0 && currentConsumer.isWritable()) {
        // 推送消息
        currentConsumer.sendMessages(entries);
    } else {
        // 这里因为没有存活消费者，记录消息被删除的事件
        entries.forEach(entry -> {
            int totalMsgs = getBatchSizeforEntry(entry.getDataBuffer(), subscription, -1);
            if (totalMsgs > 0) {
                msgDrop.recordEvent();
            }
            entry.release();
        });
    }
}

// 多活消费者，非持久化分发器实现，这里与单活消费者实现只是多了一个选择消费者的过程。
// 其中，优先级0为最高优先级，1,2。。依次类推
public void sendMessages(List<Entry> entries) {
    Consumer consumer = TOTAL_AVAILABLE_PERMITS_UPDATER.get(this) > 0 ? getNextConsumer() : null;
    if (consumer != null) {
        TOTAL_AVAILABLE_PERMITS_UPDATER.addAndGet(this, -consumer.sendMessages(entries).getTotalSentMessages());
    } else {
        entries.forEach(entry -> {
            int totalMsgs = getBatchSizeforEntry(entry.getDataBuffer(), subscription, -1);
            if (totalMsgs > 0) {
                msgDrop.recordEvent();
            }
            entry.release();
        });
    }
}

public Consumer getNextConsumer() {
    if (consumerList.isEmpty() || IS_CLOSED_UPDATER.get(this) == TRUE) {
        // 如果没有消费者连接或者分发器已关闭，则直接返回
        return null;
    }
    // 如果当前消费者轮询索引大于等于消费者数，则赋值0
    if (currentConsumerRoundRobinIndex >= consumerList.size()) {
        currentConsumerRoundRobinIndex = 0;
    }

    // 获取当前索引对应的消费者优先级
    int currentRoundRobinConsumerPriority = consumerList.get(currentConsumerRoundRobinIndex).getPriorityLevel();

    //如果currentIndex不在最高级别0，则在更高级别找到可用消费者
    if (currentRoundRobinConsumerPriority != 0) {
        int higherPriorityConsumerIndex = getConsumerFromHigherPriority(currentRoundRobinConsumerPriority);
        if (higherPriorityConsumerIndex != -1) {
            currentConsumerRoundRobinIndex = higherPriorityConsumerIndex + 1;
            return consumerList.get(higherPriorityConsumerIndex);
        }
    }

    // currentIndex已经处于最高级别或者无法在更高级别找到消费者，因此，找到相同或更低级别的消费者
    int availableConsumerIndex = getNextConsumerFromSameOrLowerLevel(currentConsumerRoundRobinIndex);
    if (availableConsumerIndex != -1) {
        currentConsumerRoundRobinIndex = availableConsumerIndex + 1;
        return consumerList.get(availableConsumerIndex);
    }

    // 没有找到可用消费者
    return null;
}

// 找出更高的优先级的消费者
private int getConsumerFromHigherPriority(int targetPriority) {
    for (int i = 0; i < currentConsumerRoundRobinIndex; i++) {
        Consumer consumer = consumerList.get(i);
        if (consumer.getPriorityLevel() < targetPriority) {
            if (isConsumerAvailable(consumerList.get(i))) {
                return i;
            }
        } else {
            break;
        }
    }
    return -1;
}

// 找到下一个消费者，相同级别或更低级别
private int getNextConsumerFromSameOrLowerLevel(int currentRoundRobinIndex) {

    int targetPriority = consumerList.get(currentRoundRobinIndex).getPriorityLevel();
    // 如果无法从currentRR中找到消费者到列表中的最后消费者，则使用轮询找
    int scanIndex = currentRoundRobinIndex;
    int endPriorityLevelIndex = currentRoundRobinIndex;
    do {
        Consumer scanConsumer = scanIndex < consumerList.size() ? consumerList.get(scanIndex) : null /* 表明列表中已到末尾 */;

        // 如果到达列表的最后一个使用者，则从头开始检查列表的currentRRIndex
        if (scanConsumer == null || scanConsumer.getPriorityLevel() != targetPriority) {
            endPriorityLevelIndex = scanIndex;
            scanIndex = getFirstConsumerIndexOfPriority(targetPriority);
        } else {
            if (isConsumerAvailable(scanConsumer)) {
                return scanIndex;
            }
            scanIndex++;
        }
    } while (scanIndex != currentRoundRobinIndex);

    // 这意味着，在同一级别下没有找到消费者，所以，找比这个级别更低的可用消费者
    for (int i = endPriorityLevelIndex; i < consumerList.size(); i++) {
        if (isConsumerAvailable(consumerList.get(i))) {
            return i;
        }
    }

    return -1;
}
```

到这里，发送消息的流程基本结束，这里回顾一下总体流程：

1. 校验生产者是否已准备好
2. 校验消息的校验和（可选）
3. 校验消息加密（可选）
4. 持久化消息发送逻辑
   1. 消息去重（可选）
   2. ledger 的状态机处理，如果 ledger 的状态为关闭，则创建新的ledger ，如果为打开状态，则判定当前ledger是否已满，设置新增消息完成后关闭ledger标志，并执行消息添加操作。
   3. 消息添加操作执行完后，开始执行回调。如果添加失败，根据状态会把当前ledger的状态设置为关闭或清空等待队列，并执行如下操作：
       1. 检查游标，找出已全部消费的 ledger 列表，然后检查是否已过期，是否超过空间大小配额，形成新的 ledger 列表，准备异步卸载（归档）。
       2. 检查所有的 ledger ，确定已经被卸载的ledger （一般是放入云存储里），准备异步删除。
       3. 更新 ledger 元数据，如果成功，则执行步骤4的新的 ledger 列表，依次异步卸载，步骤5中的新的 ledger 列表异步删除。
       4. 检查所有的 ledger，根据 ledger 的元数据配置，判定是否需要卸载，以及叠加卸载的大小，是否达到卸载自动触发大小阈值，如果达到，则把更久远的 ledger 放入待卸载
       ledger 列表，根据新产生的 ledger 列表，更新 ledger 元数据，并执行卸载操作，如果成功，则更新到原数据，继续重复动作，直到列表为空。如果失败，则删除相关资源（主要是元存储上的资源）。
       5. 如果新增消息等待队列不为空，则创建新的 ledger 。
   4. 如果添加成功，检查是否完成超时，超时则直接返回，否则如下操作：
       1. 如果有存活游标，则放入消息缓存。
       2. 更新消息计数，并更新最后确认消息。
       3. 如果设置了执行完后关闭标志，则异步关闭当前ledger。
       4. 通知所有游标消息到来。
5. 非持久化消息发送逻辑
   1. 执行回调，表示发送成功
   2. 通过订阅关系，获取分发器，分发器发送消息出去。（这里分发器有2个实现，单活非持久性分发器、多活非持久性分发器，他们最大区别在于后者支持多个存活消费者，并且支持消费者优先级）
      1. 获取消费者，如果消费者不为空，则推送消息
      2. 如果消费者为空，则触发消息丢弃事件
   3. 如果副本复制器不为空，则也发送消息到远程副本。

好了，消息发送完后，这时候，需要关闭生产者，那么情况下一小结，handleCloseProducer 命令实现。

##### 4. handleCloseProducer 命令实现

```java

protected void handleCloseProducer(CommandCloseProducer closeProducer) {
    // 状态检查
    checkArgument(state == State.Connected);

    final long producerId = closeProducer.getProducerId();
    final long requestId = closeProducer.getRequestId();
    // 未找到相关生产者信息，未知错误
    CompletableFuture<Producer> producerFuture = producers.get(producerId);
    if (producerFuture == null) {
        log.warn("[{}] Producer {} was not registered on the connection", remoteAddress, producerId);
        ctx.writeAndFlush(Commands.newError(requestId, ServerError.UnknownError,
                "Producer was not registered on the connection"));
        return;
    }
    // 在创建之前关闭生产者
    if (!producerFuture.isDone() && producerFuture
            .completeExceptionally(new IllegalStateException("Closed producer before creation was complete"))) {
        //收到了在生产者（创建）实际完成之前关闭生产者的请求，已将生产者标记为失败，
        //可以告诉客户关闭操作成功。 当实际的创建操作完成时，新生产者将被丢弃。
        log.info("[{}] Closed producer {} before its creation was completed", remoteAddress, producerId);
        ctx.writeAndFlush(Commands.newSuccess(requestId));
        return;
    } else if (producerFuture.isCompletedExceptionally()) {
        // 生产者本身没有创建成功，但是可以认为关闭成功
        log.info("[{}] Closed producer {} that already failed to be created", remoteAddress, producerId);
        ctx.writeAndFlush(Commands.newSuccess(requestId));
        return;
    }

    // 正常关闭生产者
    Producer producer = producerFuture.getNow(null);
    log.info("[{}][{}] Closing producer on cnx {}", producer.getTopic(), producer.getProducerName(), remoteAddress);

    producer.close().thenAccept(v -> {
        log.info("[{}][{}] Closed producer on cnx {}", producer.getTopic(), producer.getProducerName(),
                remoteAddress);
        ctx.writeAndFlush(Commands.newSuccess(requestId));
        producers.remove(producerId, producerFuture);
    });
}

public synchronized CompletableFuture<Void> close() {
    if (log.isDebugEnabled()) {
        log.debug("Closing producer {} -- isClosed={}", this, isClosed);
    }
    // 如果关闭标记为false
    if (!isClosed) {
        isClosed = true;
        if (log.isDebugEnabled()) {
            log.debug("Trying to close producer {} -- cnxIsActive: {} -- pendingPublishAcks: {}", this,
                    cnx.isActive(), pendingPublishAcks);
        }
        // 如果通道不在活动或发布确认等待数为0，则现在关闭
        if (!cnx.isActive() || pendingPublishAcks == 0) {
            closeNow();
        }
    }
    return closeFuture;
}

//清理资源
void closeNow() {
    topic.removeProducer(this);
    cnx.removedProducer(this);

    if (log.isDebugEnabled()) {
        log.debug("Removed producer: {}", this);
    }
    closeFuture.complete(null);
}

```

关闭生产者命令还是非常简单，简单的判断和清理资源，即完成。

下面，总结下，producer 与 broker 的交互过程：

1. 指定 service_url ，与 broker 或 proxy 建立连接
2. 通过`GET_SCHEMA`命令,获取 schema 信息
3. 通过`PARTITIONED_METADATA`命令,获取 partitionedTopicMetadata 信息
4. 通过`LOOKUP`命令,获取存活的 broker 或 proxy address 信息
5. 与（重定向）新的 broker 或 proxy 建立连接，通道激活后，发送 `CONNECT` 命令
6. 连接成功后，发送消息用 `PRODUCER` 命令
7. 在 broker 上注册 producer 后，就可以发送消息了，发送消息用 `Send` 命令
8. 消息发送完毕后，关闭生产者，发送`CLOSE_PRODUCER`命令

整个交互逻辑即完成。那么目前为止，broker 的生产者接口实现已全部交代完毕。

接下来，将讲解消费者的在 broker 端实现。

#### 3. Broker Consumer 接口实现

这里，可以复习下，消费者是怎么启动，并消费消息的。
跟生产者一样，消费者 与 broker 交互时，刚开始也遵循如下操作：

1. 指定 service url ，与 broker 或 proxy 建立连接
2. 通过`GET_SCHEMA`命令,获取 schema 信息
3. 通过`PARTITIONED_METADATA`命令,获取 partitionedTopicMetadata 信息
4. 通过`LOOKUP`命令,获取存活的 broker 或 proxy address 信息
5. 与（重定向）新的 broker 或 proxy 建立连接，通道激活后，发送 `CONNECT` 命令
6. 连接成功后，发送消息用 `SUBSCRIBE` 命令

##### 1. handleSubscribe 命令实现

```java

protected void handleSubscribe(final CommandSubscribe subscribe) {
    // 类似于生产者，处理完 Connect 命令后，通道状态应该为 已连接 状态
    checkArgument(state == State.Connected);
    final long requestId = subscribe.getRequestId();
    final long consumerId = subscribe.getConsumerId();
    // 校验 Topic 合法性
    TopicName topicName = validateTopicName(subscribe.getTopic(), requestId, subscribe);
    if (topicName == null) {
        return;
    }
    // 如果认证与授权被启用时，当连接时候，如果认证角色（authRole）是代理角色（proxyRoles）之一时，必须强制如下规则
    // * originalPrincipal 不能为空
    // * originalPrincipal 不能是以 proxy 身份
    // 这里跟生产者一致
    if (invalidOriginalPrincipal(originalPrincipal)) {
        final String msg = "Valid Proxy Client role should be provided while subscribing ";
        log.warn("[{}] {} with role {} and proxyClientAuthRole {} on topic {}", remoteAddress, msg, authRole,
                originalPrincipal, topicName);
        ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
        return;
    }

    final String subscriptionName = subscribe.getSubscription();
    final SubType subType = subscribe.getSubType();
    final String consumerName = subscribe.getConsumerName();
    final boolean isDurable = subscribe.getDurable();
    final MessageIdImpl startMessageId = subscribe.hasStartMessageId() ? new BatchMessageIdImpl(
            subscribe.getStartMessageId().getLedgerId(), subscribe.getStartMessageId().getEntryId(),
            subscribe.getStartMessageId().getPartition(), subscribe.getStartMessageId().getBatchIndex())
            : null;
    final String subscription = subscribe.getSubscription();
    final int priorityLevel = subscribe.hasPriorityLevel() ? subscribe.getPriorityLevel() : 0;
    final boolean readCompacted = subscribe.getReadCompacted();
    final Map<String, String> metadata = CommandUtils.metadataFromCommand(subscribe);
    final InitialPosition initialPosition = subscribe.getInitialPosition();
    final SchemaData schema = subscribe.hasSchema() ? getSchema(subscribe.getSchema()) : null;

    CompletableFuture<Boolean> isProxyAuthorizedFuture;
    if (service.isAuthorizationEnabled() && originalPrincipal != null) {
        isProxyAuthorizedFuture = service.getAuthorizationService().canConsumeAsync(topicName, authRole,
                authenticationData, subscribe.getSubscription());
    } else {
        isProxyAuthorizedFuture = CompletableFuture.completedFuture(true);
    }
    isProxyAuthorizedFuture.thenApply(isProxyAuthorized -> {
        if (isProxyAuthorized) {
            CompletableFuture<Boolean> authorizationFuture;
            if (service.isAuthorizationEnabled()) {
                authorizationFuture = service.getAuthorizationService().canConsumeAsync(topicName,
                        originalPrincipal != null ? originalPrincipal : authRole, authenticationData,
                        subscription);
            } else {
                authorizationFuture = CompletableFuture.completedFuture(true);
            }

            authorizationFuture.thenApply(isAuthorized -> {
                if (isAuthorized) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Client is authorized to subscribe with role {}", remoteAddress, authRole);
                    }

                    log.info("[{}] Subscribing on topic {} / {}", remoteAddress, topicName, subscriptionName);
                    try {
                        Metadata.validateMetadata(metadata);
                    } catch (IllegalArgumentException iae) {
                        final String msg = iae.getMessage();
                        ctx.writeAndFlush(Commands.newError(requestId, ServerError.MetadataError, msg));
                        return null;
                    }
                    CompletableFuture<Consumer> consumerFuture = new CompletableFuture<>();
                    CompletableFuture<Consumer> existingConsumerFuture = consumers.putIfAbsent(consumerId,
                            consumerFuture);

                    if (existingConsumerFuture != null) {
                        if (existingConsumerFuture.isDone() && !existingConsumerFuture.isCompletedExceptionally()) {
                            Consumer consumer = existingConsumerFuture.getNow(null);
                            log.info("[{}] Consumer with the same id is already created: {}", remoteAddress,
                                    consumer);
                            ctx.writeAndFlush(Commands.newSuccess(requestId));
                            return null;
                        } else {
                            // There was an early request to create a consumer with same consumerId. This can happen
                            // when
                            // client timeout is lower the broker timeouts. We need to wait until the previous
                            // consumer
                            // creation request either complete or fails.
                            log.warn("[{}][{}][{}] Consumer is already present on the connection", remoteAddress,
                                    topicName, subscriptionName);
                            ServerError error = !existingConsumerFuture.isDone() ? ServerError.ServiceNotReady
                                    : getErrorCode(existingConsumerFuture);
                            ctx.writeAndFlush(Commands.newError(requestId, error,
                                    "Consumer is already present on the connection"));
                            return null;
                        }
                    }

                    service.getOrCreateTopic(topicName.toString())
                            .thenCompose(topic -> {
                                if (schema != null) {
                                    return topic.addSchemaIfIdleOrCheckCompatible(schema)
                                        .thenCompose(isCompatible -> {
                                                if (isCompatible) {
                                                    return topic.subscribe(ServerCnx.this, subscriptionName, consumerId,
                                                            subType, priorityLevel, consumerName, isDurable,
                                                            startMessageId, metadata,
                                                            readCompacted, initialPosition);
                                                } else {
                                                    return FutureUtil.failedFuture(
                                                            new IncompatibleSchemaException(
                                                                    "Trying to subscribe with incompatible schema"
                                                    ));
                                                }
                                            });
                                } else {
                                    return topic.subscribe(ServerCnx.this, subscriptionName, consumerId,
                                        subType, priorityLevel, consumerName, isDurable,
                                        startMessageId, metadata, readCompacted, initialPosition);
                                }
                            })
                            .thenAccept(consumer -> {
                                if (consumerFuture.complete(consumer)) {
                                    log.info("[{}] Created subscription on topic {} / {}", remoteAddress, topicName,
                                            subscriptionName);
                                    ctx.writeAndFlush(Commands.newSuccess(requestId), ctx.voidPromise());
                                } else {
                                    // The consumer future was completed before by a close command
                                    try {
                                        consumer.close();
                                        log.info("[{}] Cleared consumer created after timeout on client side {}",
                                                remoteAddress, consumer);
                                    } catch (BrokerServiceException e) {
                                        log.warn(
                                                "[{}] Error closing consumer created after timeout on client side {}: {}",
                                                remoteAddress, consumer, e.getMessage());
                                    }
                                    consumers.remove(consumerId, consumerFuture);
                                }

                            }) //
                            .exceptionally(exception -> {
                                if (exception.getCause() instanceof ConsumerBusyException) {
                                    if (log.isDebugEnabled()) {
                                        log.debug(
                                                "[{}][{}][{}] Failed to create consumer because exclusive consumer is already connected: {}",
                                                remoteAddress, topicName, subscriptionName,
                                                exception.getCause().getMessage());
                                    }
                                } else {
                                    log.warn("[{}][{}][{}] Failed to create consumer: {}", remoteAddress, topicName,
                                            subscriptionName, exception.getCause().getMessage(), exception);
                                }

                                // If client timed out, the future would have been completed by subsequent close.
                                // Send error
                                // back to client, only if not completed already.
                                if (consumerFuture.completeExceptionally(exception)) {
                                    ctx.writeAndFlush(Commands.newError(requestId,
                                            BrokerServiceException.getClientErrorCode(exception.getCause()),
                                            exception.getCause().getMessage()));
                                }
                                consumers.remove(consumerId, consumerFuture);

                                return null;

                            });
                } else {
                    String msg = "Client is not authorized to subscribe";
                    log.warn("[{}] {} with role {}", remoteAddress, msg, authRole);
                    ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
                }
                return null;
            }).exceptionally(e -> {
                String msg = String.format("[%s] %s with role %s", remoteAddress, e.getMessage(), authRole);
                log.warn(msg);
                ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, e.getMessage()));
                return null;
            });
        } else {
            final String msg = "Proxy Client is not authorized to subscribe";
            log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, authRole, topicName);
            ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
        }
        return null;
    }).exceptionally(ex -> {
        String msg = String.format("[%s] %s with role %s", remoteAddress, ex.getMessage(), authRole);
        if (ex.getCause() instanceof PulsarServerException) {
            log.info(msg);
        } else {
            log.warn(msg);
        }
        ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, ex.getMessage()));
        return null;
    });
}

```

### 6. Broker admin 接口实现

## 4. Pulsar Proxy 解析

## 5. Pulsar Discovery 解析

## 6. Pulsar Functions 解析

## 7. Pulsar IO 解析