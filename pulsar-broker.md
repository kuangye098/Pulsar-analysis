# Pulsar Broker 源码解析

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

#### 4. BrokerStarter 阻塞方法（即等待关闭指令）

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
与此同时，bookieServer 与 autoRecoveryMain 都将会关闭，这样主线程不再阻塞，执行完即退出。

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

此方法主线程同样有提到，其在JVM关闭钩子中调用的，确保所有正使用的组件优雅有序退出。

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
//是否已初始化
private boolean isInitialized = false;
//用于状态更新的定时服务
private final ScheduledExecutorService statsUpdater;
//认证服务
private AuthenticationService authenticationService;
//连接管理器
private ConnectorsManager connectorsManager;
//Pulsar admin 客户端
private PulsarAdmin brokerAdmin;
//函数管理端
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

//调度线程池（pulsar核心线程池）
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
//顺序调度器 （卸载服务使用）
private OrderedScheduler offloaderScheduler;
//Ledger 卸载服务管理器
private Offloaders offloaderManager = new Offloaders();
//Ledger 卸载加载器
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
//broker服务url（原生协议TCP+TLS）
private final String brokerServiceUrlTls;
//broker协议版本
private final String brokerVersion;
//Schema注册服务
private SchemaRegistryService schemaRegistryService = null;
//工作者服务（支持函数式计算框架）
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
    //校验用户是否有启动权限
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

bookies（存储层）故障自动恢复组件。其主要有两个功能：1. 选择 Auditor 领导者 2. 副本复制器（ 当有 bookie 故障时，由Auditor生成任务，副本复制器来进行数据复制）

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
    // 设置会话状态监听器（这里用于连接 zookkeeper 会话过期，将自动关闭AutoRecoveryMain）
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

    //如果状态存储服务url不为空，则尝试创建状态存储客户端用于存储函数状态
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

        //协调管理器设置元数据、运行时、成员管理器
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
        //创建集群服务协调器
        this.clusterServiceCoordinator = new ClusterServiceCoordinator(
                this.workerConfig.getWorkerId(),
                membershipManager);
        //增加成员监控任务
        this.clusterServiceCoordinator.addTask("membership-monitor",
                this.workerConfig.getFailureCheckFreqMs(),
                () -> membershipManager.checkFailures(
                        functionMetaDataManager, functionRuntimeManager, schedulerManager));
        //启动集群服务协调器
        this.clusterServiceCoordinator.start();
        //启动函数运行时管理器
        this.functionRuntimeManager.start();
        //设置函数工作者初始化标志
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
        // 身份验证
        if (isNotBlank(authPlugin) && isNotBlank(authParams)) {
            adminBuilder.authentication(authPlugin, authParams);
        }
        // 证书路径
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
    // 启动互斥锁，避免重复启动
    mutex.lock();
    // 打印系统版本信息
    LOG.info("Starting Pulsar Broker service; version: '{}'", ( brokerVersion != null ? brokerVersion : "unknown" )  );
    LOG.info("Git Revision {}", PulsarBrokerVersionStringUtils.getGitSha());
    LOG.info("Built by {} on {} at {}",
                PulsarBrokerVersionStringUtils.getBuildUser(),
                PulsarBrokerVersionStringUtils.getBuildHost(),
                PulsarBrokerVersionStringUtils.getBuildTime());

    try {
        // 状态检查，如果已是初始化状态，则抛异常
        if (state != State.Init) {
            throw new PulsarServerException("Cannot start the service once it was stopped");
        }

        // 1. 首先连接本地Zookeeper，并注册系统关闭hook
        localZooKeeperConnectionProvider = new LocalZooKeeperConnectionService(getZooKeeperClientFactory(),
                config.getZookeeperServers(), config.getZooKeeperSessionTimeoutMillis());
        localZooKeeperConnectionProvider.start(shutdownService);

        //2. 初始化和启动访问配置仓库，包括本地（local）集群缓存和全局（global）集群缓存
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
        //8. 创建 ledger 卸载管理器
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
                // 当 broker 已经完成初始化后，确保这 VIP 状态可见
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

        // 11. 如果Websocket服务启用，则创建和启动Websocket服务
        if (config.isWebSocketServiceEnabled()) {
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
        // 12. 注册 Namespace 心跳和启动（确认当前 broker 的 Namespace，加载和创建分配属于当前broker的 Topic 集合）
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
        // 解锁
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
    // 如果到达这里没异常，则设置正运行标志
    running = true;
    //创建宕机或关闭监视器
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
    // 设置未捕获异常处理器
    if (null != uncaughtExceptionHandler) {
        deathWatcher.setUncaughtExceptionHandler(uncaughtExceptionHandler);
    }
    //启动监视器
    deathWatcher.start();
    //启动完毕
    running = true;
}

```

到此为止，Pulsar Broker启动流程已基本已经分析完毕，但限于篇幅，只是解析了各服务启动情况。没有再深入分析各服务内组件的具体启动流程。

### 4. Broker Client 接口实现

在前面的章节中，已经分析了 Pulsar Client 以及相关的 Producer 和 Consumer ，接下来的接口分析就按照原来的
思路，即按照生产消息、消费消息两个核心展开，以时序来依次分析其方法调用，然后再分析其特性。

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
    // Schema 名，从 Topic 信息中获取
    String schemaName;
    try {
        schemaName = TopicName.get(commandGetSchema.getTopic()).getSchemaName();
    } catch (Throwable t) {
        ctx.writeAndFlush(
                Commands.newGetSchemaResponseError(requestId, ServerError.InvalidTopicName, t.getMessage()));
        return;
    }
    // 从 bookkeeper 中读取 Schema 信息
    schemaService.getSchema(schemaName, schemaVersion).thenAccept(schemaAndMetadata -> {
        // Schanme 元数据为空，则回复客户端异常
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

获取Schema信息较为简单，主要是从 Topic 元信息中获取 SchemaName 信息，再通过 schemaService 服务读取最新的 schema 元数据
，可以认为是 Topic 结构化信息。

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
        // 当连接 broker 时，如果认证与授权被启用时，且如果认证角色（authRole）是代理角色（proxyRoles）之一时，必须强制遵循如下规则
        // * originalPrincipal 不能为空
        // * originalPrincipal 不能是以 proxy 身份（意味着不能多重代理）
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
            // 这里代表客户端直连过来
            isProxyAuthorizedFuture = CompletableFuture.completedFuture(true);
        }
        String finalOriginalPrincipal = originalPrincipal;
        isProxyAuthorizedFuture.thenApply(isProxyAuthorized -> {
                 //有权限
                if (isProxyAuthorized) {
                    // 这里就是获取分区 Topic 元数据信息
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

// 获取分区 Topic 元信息
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

        //验证 global-namespace 包含本地/对等集群：如果存在对等/本地集群，则查找可以提供/重定向请求，否则分区 topic 元数据请求将失败，那么客户端在创建生产者/消费者时也将失败
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

// 通过全局命名空间获取集群信息
public static CompletableFuture<ClusterData> checkLocalOrGetPeerReplicationCluster(PulsarService pulsarService,
            NamespaceName namespace) {
    // 如果 Namespace 不是全局的，则直接返回
    if (!namespace.isGlobal()) {
        return CompletableFuture.completedFuture(null);
    }
    final CompletableFuture<ClusterData> validationFuture = new CompletableFuture<>();
    final String localCluster = pulsarService.getConfiguration().getClusterName();
    final String path = AdminResource.path(POLICIES, namespace.toString());
    //查询全局 Namespace 策略信息
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
            // 策略中副本集群不包含本地集群，则继续查询策略副本中本地集群信息
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
                // 这里返回正常成功（即找到副本集群配置信息）
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

// 从集群信息中获取自己的集群数据
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
        // 从本地集群中全局配置匹配副本集群信息，如果匹配成功，则返回集群信息，如果没有找到，则抛异常
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

// 如果本地集群没有分区 topic 信息，则尝试从全局 zk 中获取分区 topic 信息
protected static CompletableFuture<PartitionedTopicMetadata> fetchPartitionedTopicMetadataAsync(
            PulsarService pulsar, String path) {
    CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();
    try {
        // 从 global Zk 缓存中获取 Topic 分区信息
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

获取 PartitionedTopicMetadata 信息较为简单，主要是从 Topic 元信息中获取 PartitionedTopicMetadata 信息。

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
    // 如果没有找到对应的 topic 信息，直接返回，客户端那边会超时
    if (topicName == null) {
        return;
    }
    // 获取 Lookup 命令信号量，用于控制并发数
    final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
    // 尝试请求
    if (lookupSemaphore.tryAcquire()) {
        // 当连接 broker 时，如果认证与授权被启用时，且如果认证角色（authRole）是代理角色（proxyRoles）之一时，必须强制遵循如下规则
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
                                // 这里不会发生
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

// 通过 Topic 名查找所属的存活的 broker 地址
public static CompletableFuture<ByteBuf> lookupTopicAsync(PulsarService pulsarService, TopicName topicName,
        boolean authoritative, String clientAppId, AuthenticationDataSource authenticationData, long requestId) {

    final CompletableFuture<ByteBuf> validationFuture = new CompletableFuture<>();
    final CompletableFuture<ByteBuf> lookupfuture = new CompletableFuture<>();
    final String cluster = topicName.getCluster();

    // (1) 验证集群信息
    getClusterDataIfDifferentCluster(pulsarService, cluster, clientAppId).thenAccept(differentClusterData -> {
        // 如果 Topic 不属于当前 broker，则告诉客户端执行重定向命令
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
                        // 找到 topic 实际所在的 broker 服务地址，发送重定向命令
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
        //如果验证通过有数据（即重定向命令或错误信息），则直接返回
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

// 如果是不同集群，则获取集群数据，否则直接返回null
protected static CompletableFuture<ClusterData> getClusterDataIfDifferentCluster(PulsarService pulsar,
        String cluster, String clientAppId) {

    CompletableFuture<ClusterData> clusterDataFuture = new CompletableFuture<>();
    // 简单校验集群信息
    if (!isValidCluster(pulsar, cluster)) {
        try {
            // 这里代码为了兼容 v1 namespace 版本的格式：prop/cluster/namespaces（2.0版本这里已经被抛弃）
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

到此，通过 topic 获取 broker 地址逻辑已完成分析。其作用在于

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

1. 指定 service_url ，与 broker 或 proxy 建立连接。
2. 通过`GET_SCHEMA`命令,获取 schema 信息。
3. 通过`PARTITIONED_METADATA`命令,获取 partitionedTopicMetadata 信息。
4. 通过`LOOKUP`命令,获取存活的 broker 或 proxy address 信息。
5. 与（重定向）新的 broker 或 proxy 建立连接，通道激活后，发送 `CONNECT` 命令。
6. 连接成功后，发送消息用 `PRODUCER` 命令。
7. 在 broker 上注册 producer 后，就可以发送消息了，发送消息用 `Send` 命令。
8. 消息发送完毕后，关闭生产者，发送`CLOSE_PRODUCER`命令。

整个交互逻辑即完成。那么目前为止，broker 的生产者接口实现已全部交代完毕。

接下来，将讲解消费者的在 broker 端实现。

#### 3. Broker Consumer 接口实现

这里，可以复习下，消费者是怎么启动，并怎么消费消息。

跟生产者一样，消费者 与 broker 交互时，刚开始也遵循如下操作：

1. 指定 service url ，与 broker 或 proxy 建立连接。
2. 通过`GET_SCHEMA`命令,获取 schema 信息。
3. 通过`PARTITIONED_METADATA`命令,获取 partitionedTopicMetadata 信息。
4. 通过`LOOKUP`命令,获取存活的 broker 或 proxy address 信息。
5. 与（重定向）新的 broker 或 proxy 建立连接，通道激活后，发送 `CONNECT` 命令。
6. 连接成功后，发送消息用 `SUBSCRIBE` 命令。

因为消费者在订阅之前，其与 broker 交互流程基本一样，这里不再赘述。下面，开始讲消费者的消费流程

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
    // 当连接 broker 时，如果认证与授权被启用时，且如果认证角色（authRole）是代理角色（proxyRoles）之一时，必须强制遵循如下规则
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
            // 认证过程跟生产者完全一致，这里不再赘述
            authorizationFuture.thenApply(isAuthorized -> {
                if (isAuthorized) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Client is authorized to subscribe with role {}", remoteAddress, authRole);
                    }

                    log.info("[{}] Subscribing on topic {} / {}", remoteAddress, topicName, subscriptionName);
                    try {
                        // 校验元数据合法性，主要是数据大小不超过1Kb
                        Metadata.validateMetadata(metadata);
                    } catch (IllegalArgumentException iae) {
                        final String msg = iae.getMessage();
                        ctx.writeAndFlush(Commands.newError(requestId, ServerError.MetadataError, msg));
                        return null;
                    }
                    CompletableFuture<Consumer> consumerFuture = new CompletableFuture<>();
                    // 根据消费者ID来判断消费者是否之前已存在，这里出现的情况是可能重试导致的
                    CompletableFuture<Consumer> existingConsumerFuture = consumers.putIfAbsent(consumerId,
                            consumerFuture);
                    // 不为空的时
                    if (existingConsumerFuture != null) {
                         // 已完成，并且无异常，这里认为之前创建的消费者是成功的，故直接返回成功
                        if (existingConsumerFuture.isDone() && !existingConsumerFuture.isCompletedExceptionally()) {
                            Consumer consumer = existingConsumerFuture.getNow(null);
                            log.info("[{}] Consumer with the same id is already created: {}", remoteAddress,
                                    consumer);
                            ctx.writeAndFlush(Commands.newSuccess(requestId));
                            return null;
                        } else {
                            // 早期请求创建具有相同 consumerId 的消费者。 当客户端设置的超时时间低于 broker 本身处理的超时时间，可能会发生这种情况。 这时候，需要等到上一个消费者创建请求完成或失败，返回客户端异常，表示消费者正在连接中。
                            log.warn("[{}][{}][{}] Consumer is already present on the connection", remoteAddress,
                                    topicName, subscriptionName);
                            ServerError error = !existingConsumerFuture.isDone() ? ServerError.ServiceNotReady
                                    : getErrorCode(existingConsumerFuture);
                            ctx.writeAndFlush(Commands.newError(requestId, error,
                                    "Consumer is already present on the connection"));
                            return null;
                        }
                    }
                    // 这里生产者已经分析过，不再赘述
                    service.getOrCreateTopic(topicName.toString())
                            .thenCompose(topic -> {
                                if (schema != null) { 
                                    //schema 不为空，如果 topic 空闲（即无生产者，无消息，无生产者，不存在 schema），则新增 schema 信息，否则检查兼容性
                                    return topic.addSchemaIfIdleOrCheckCompatible(schema)
                                        .thenCompose(isCompatible -> {
                                                // 兼容
                                                if (isCompatible) {
                                                    return topic.subscribe(ServerCnx.this, subscriptionName, consumerId,
                                                            subType, priorityLevel, consumerName, isDurable,
                                                            startMessageId, metadata,
                                                            readCompacted, initialPosition);
                                                } else {
                                                    // 不兼容则抛异常：尝试订阅不兼容的 schema
                                                    return FutureUtil.failedFuture(
                                                            new IncompatibleSchemaException(
                                                                    "Trying to subscribe with incompatible schema"
                                                    ));
                                                }
                                            });
                                } else {
                                    // schema 为空，也就是兼容传统MQ模式（Bytes）
                                    return topic.subscribe(ServerCnx.this, subscriptionName, consumerId,
                                        subType, priorityLevel, consumerName, isDurable,
                                        startMessageId, metadata, readCompacted, initialPosition);
                                }
                            })
                            .thenAccept(consumer -> {
                                // 尝试执行完成，如果成功，则发命令给客户端，表示订阅成功
                                if (consumerFuture.complete(consumer)) {
                                    log.info("[{}] Created subscription on topic {} / {}", remoteAddress, topicName,
                                            subscriptionName);
                                    ctx.writeAndFlush(Commands.newSuccess(requestId), ctx.voidPromise());
                                } else {
                                    // 如果失败，则关闭自己 （因为前面的 futrue 已经执行完成）（客户端方超时导致）
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
                                // 消费者忙异常引起的
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

                                // 如果客户端超时，futrue 可能已经被关闭，尝试 future 设置异常，如果成功，则发命令通知客户端，表示 broker 服务异常。
                                if (consumerFuture.completeExceptionally(exception)) {
                                    ctx.writeAndFlush(Commands.newError(requestId,
                                            BrokerServiceException.getClientErrorCode(exception.getCause()),
                                            exception.getCause().getMessage()));
                                }
                                // 清理资源
                                consumers.remove(consumerId, consumerFuture);

                                return null;

                            });
                } else {
                    // 客户端无权限订阅
                    String msg = "Client is not authorized to subscribe";
                    log.warn("[{}] {} with role {}", remoteAddress, msg, authRole);
                    ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
                }
                return null;
            }).exceptionally(e -> {
                // 权限验证异常
                String msg = String.format("[%s] %s with role %s", remoteAddress, e.getMessage(), authRole);
                log.warn(msg);
                ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, e.getMessage()));
                return null;
            });
        } else {
            // proxy 客户端无权限订阅
            final String msg = "Proxy Client is not authorized to subscribe";
            log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, authRole, topicName);
            ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
        }
        return null;
    }).exceptionally(ex -> {
        // 权限验证异常
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


// ============================持久化 Topic ===========================================//
// 
// PersistentTopic 类，用于判断 schema 不为空，如果 topic 空闲（即无生产者，无消息，无生产者，不存在 schema），则新增 schema 信息，否则检查兼容性
public CompletableFuture<Boolean> addSchemaIfIdleOrCheckCompatible(SchemaData schema) {
    return hasSchema()
        .thenCompose((hasSchema) -> {
                if (hasSchema || isActive() || ledger.getTotalSize() != 0) {
                    // 兼容性检查
                    return isSchemaCompatible(schema);
                } else {
                    // 新增 schema 操作
                    return addSchema(schema).thenApply((ignore) -> true);
                }
            });
}

// 判定是否设置了 schema
public CompletableFuture<Boolean> hasSchema() {
    String base = TopicName.get(getName()).getPartitionedTopicName();
    String id = TopicName.get(base).getSchemaName();
    return brokerService.pulsar()
        .getSchemaRegistryService()
        .getSchema(id).thenApply((schema) -> schema != null);
}

public boolean isActive() {
    if (TopicName.get(topic).isGlobal()) {
        //如果是全局 Topic，如果有本地消费者 或 有本地生产者，那么当前 Topic 是活动的
        return !subscriptions.isEmpty() || hasLocalProducers();
    }
    // 非全局 Topic 时，这里生产者计数不为0，或订阅不为空，则认为当前 Topic 是活动的
    return USAGE_COUNT_UPDATER.get(this) != 0 || !subscriptions.isEmpty();
}
// 返回当前 Topic 消息数总大小
public long getTotalSize() {
    return TOTAL_SIZE_UPDATER.get(this);
}
// 新增 schema ，如果为空则直接返回 SchemaVersion.Empty ，否则保存到
public CompletableFuture<SchemaVersion> addSchema(SchemaData schema) {
    if (schema == null) {
        return CompletableFuture.completedFuture(SchemaVersion.Empty);
    }

    String base = TopicName.get(getName()).getPartitionedTopicName();
    String id = TopicName.get(base).getSchemaName();
    return brokerService.pulsar()
        .getSchemaRegistryService()
        .putSchemaIfAbsent(id, schema, schemaCompatibilityStrategy);
}

// 如果没有则添加，否则直接返回存在的 schema 信息
public CompletableFuture<SchemaVersion> putSchemaIfAbsent(String schemaId, SchemaData schema,
                                                              SchemaCompatibilityStrategy strategy) {
    return getSchema(schemaId)
        .thenApply(
            (existingSchema) ->
                existingSchema == null //为空
                    || existingSchema.schema.isDeleted()//已删除
                    || isCompatible(existingSchema, schema, strategy))
        .thenCompose(isCompatible -> {
                if (isCompatible) {
                    // 检查通过，注册新的 schema 信息
                    byte[] context = hashFunction.hashBytes(schema.getData()).asBytes();
                    SchemaRegistryFormat.SchemaInfo info = SchemaRegistryFormat.SchemaInfo.newBuilder()
                        .setType(Functions.convertFromDomainType(schema.getType()))
                        .setSchema(ByteString.copyFrom(schema.getData()))
                        .setSchemaId(schemaId)
                        .setUser(schema.getUser())
                        .setDeleted(false)
                        .setTimestamp(clock.millis())
                        .addAllProps(toPairs(schema.getProps()))
                        .build();
                    return schemaStorage.put(schemaId, info.toByteArray(), context);
                } else {
                    // 检查失败，抛异常
                    return FutureUtil.failedFuture(new IncompatibleSchemaException());
                }
            });
}
// 获取 schema 的最新版本
public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId) {
    return getSchema(schemaId, SchemaVersion.Latest);
}
// 从 schemaStorage 获取到 schema 信息
public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, SchemaVersion version) {
    return schemaStorage.get(schemaId, version).thenCompose(stored -> {
            // 如果为空，则直接返回null
            if (isNull(stored)) {
                return completedFuture(null);
            } else {
                // 否则，把字节数据转换成 SchemaInfo 
                return Functions.bytesToSchemaInfo(stored.data)
                    .thenApply(Functions::schemaInfoToSchema) //转换成 schemaData 
                    // 转换成 schemaAndMetadata
                    .thenApply(schema -> new SchemaAndMetadata(schemaId, schema, stored.version));
            }
        }
    );
}

public CompletableFuture<StoredSchema> get(String key, SchemaVersion version) {
    if (version == SchemaVersion.Latest) {
        return getSchema(key);
    } else {
        LongSchemaVersion longVersion = (LongSchemaVersion) version;
        return getSchema(key, longVersion.getVersion());
    }
}
// 从存储中读取 storedSchema 信息
private CompletableFuture<StoredSchema> getSchema(String schemaId) {
    // 已经有一个 schema 读操作正在进行。
    return readSchemaOperations.computeIfAbsent(schemaId, key -> {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Fetching schema from store", schemaId);
        }
        CompletableFuture<StoredSchema> future = new CompletableFuture<>();

        getSchemaLocator(getSchemaPath(schemaId)).thenCompose(locator -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Got schema locator {}", schemaId, locator);
            }
            // 如果没有，则直接返回空
            if (!locator.isPresent()) {
                return completedFuture(null);
            }

            SchemaStorageFormat.SchemaLocator schemaLocator = locator.get().locator;
            return readSchemaEntry(schemaLocator.getInfo().getPosition())
                     // 这里就获取到 schema 数据 （这个存在 bookkeeper 里面）
                    .thenApply(entry -> new StoredSchema(entry.getSchemaData().toByteArray(),
                            // 这里就获取到 schema 版本数据 （这个是 zk 节点版本产生）
                            new LongSchemaVersion(schemaLocator.getInfo().getVersion())));
        }).handleAsync((res, ex) -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Get operation completed. res={} -- ex={}", schemaId, res, ex);
            }

            // 清理已处理的操作
            readSchemaOperations.remove(schemaId, future);
            if (ex != null) {
                future.completeExceptionally(ex);
            } else {
                future.complete(res);
            }
            return null;
        });

        return future;
    });
}

// 存储位置
private static String getSchemaPath(String schemaId) {
    return SchemaPath + "/" + schemaId;
}

// 从本地 zk 缓存中读取版本信息
private CompletableFuture<Optional<LocatorEntry>> getSchemaLocator(String schema) {
    return localZkCache.getEntryAsync(schema, new SchemaLocatorDeserializer()).thenApply(optional ->
        optional.map(entry -> new LocatorEntry(entry.getKey(), entry.getValue().getVersion()))
    );
}

 private CompletableFuture<SchemaStorageFormat.SchemaEntry> readSchemaEntry(
        SchemaStorageFormat.PositionInfo position) {
    if (log.isDebugEnabled()) {
        log.debug("Reading schema entry from {}", position);
    }
     
    return openLedger(position.getLedgerId())
        .thenCompose((ledger) ->
            // 读取特定位置的消息 通过 ledger 和 entityId
            Functions.getLedgerEntry(ledger, position.getEntryId())
            // 读取完毕后，关闭 ledger
                .thenCompose(entry -> closeLedger(ledger)
                    .thenApply(ignore -> entry)
                )
                // 解析消息为 schema
        ).thenCompose(Functions::parseSchemaEntry);
}
// 打开 ledger 并返回 ledgerHandle
private CompletableFuture<LedgerHandle> openLedger(Long ledgerId) {
    final CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
    bookKeeper.asyncOpenLedger(
        ledgerId,
        BookKeeper.DigestType.fromApiDigestType(config.getManagedLedgerDigestType()),
        LedgerPassword,
        (rc, handle, ctx) -> {
            if (rc != BKException.Code.OK) {
                future.completeExceptionally(bkException("Failed to open ledger", rc, ledgerId, -1));
            } else {
                future.complete(handle);
            }
        }, null
    );
    return future;
}

// 已存在的 schema 和 新的schema 的哈希码是否相等，否则根据兼容性策略判断两者是否兼容
private boolean isCompatible(SchemaAndMetadata existingSchema, SchemaData newSchema,
                                 SchemaCompatibilityStrategy strategy) {
    HashCode existingHash = hashFunction.hashBytes(existingSchema.schema.getData());
    HashCode newHash = hashFunction.hashBytes(newSchema.getData());
    return newHash.equals(existingHash) ||
        // 如果新 schema 没有对应的兼容性检查，那么用默认的，否则用配置的兼容性策略进行检查
        compatibilityChecks.getOrDefault(newSchema.getType(), SchemaCompatibilityCheck.DEFAULT)
        .isCompatible(existingSchema.schema, newSchema, strategy);
}

// AvroSchemaCompatibilityCheck 类，Avro Schema 兼容性检查
public boolean isCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy) {
    Schema.Parser fromParser = new Schema.Parser();
    Schema fromSchema = fromParser.parse(new String(from.getData()));
    Schema.Parser toParser = new Schema.Parser();
    Schema toSchema =  toParser.parse(new String(to.getData()));

    SchemaValidator schemaValidator = createSchemaValidator(strategy, true);
    try {
        schemaValidator.validate(toSchema, Arrays.asList(fromSchema));
    } catch (SchemaValidationException e) {
        return false;
    }
    return true;
}

private static SchemaValidator createSchemaValidator(SchemaCompatibilityStrategy compatibilityStrategy,
                                              boolean onlyLatestValidator) {
    final SchemaValidatorBuilder validatorBuilder = new SchemaValidatorBuilder();
    switch (compatibilityStrategy) {
        case BACKWARD:
            return createLatestOrAllValidator(validatorBuilder.canReadStrategy(), onlyLatestValidator);
        case FORWARD:
            return createLatestOrAllValidator(validatorBuilder.canBeReadStrategy(), onlyLatestValidator);
        case FULL:
            return createLatestOrAllValidator(validatorBuilder.mutualReadStrategy(), onlyLatestValidator);
        default:
            return NeverSchemaValidator.INSTANCE;
    }
}

private static SchemaValidator createLatestOrAllValidator(SchemaValidatorBuilder validatorBuilder, boolean onlyLatest) {
    return onlyLatest ? validatorBuilder.validateLatest() : validatorBuilder.validateAll();
}

// 当有消费者或生产者或有消息，则兼容检查
public CompletableFuture<Boolean> isSchemaCompatible(SchemaData schema) {
    String base = TopicName.get(getName()).getPartitionedTopicName();
    String id = TopicName.get(base).getSchemaName();
    return brokerService.pulsar()
        .getSchemaRegistryService()
        .isCompatibleWithLatestVersion(id, schema, schemaCompatibilityStrategy);
}
// 最新版本兼容性检查
public CompletableFuture<Boolean> isCompatibleWithLatestVersion(String schemaId, SchemaData schema,
                                                                    SchemaCompatibilityStrategy strategy) {
    return checkCompatibilityWithLatest(schemaId, schema, strategy);
}

private CompletableFuture<Boolean> checkCompatibilityWithLatest(String schemaId, SchemaData schema,
                                                                   SchemaCompatibilityStrategy strategy) {
    // 上文已分析
    return getSchema(schemaId)
        .thenApply(
            (existingSchema) ->
                !(existingSchema == null || existingSchema.schema.isDeleted()) // 如果 existingSchema 不为空并且 schema 没有删除，则进行兼容性检查
                    && isCompatible(existingSchema, schema, strategy));
}


// ====================================开始订阅主体=====================================
// 
 public CompletableFuture<Consumer> subscribe(final ServerCnx cnx, String subscriptionName, long consumerId,
        SubType subType, int priorityLevel, String consumerName, boolean isDurable, MessageId startMessageId,
        Map<String, String> metadata, boolean readCompacted, InitialPosition initialPosition) {

    final CompletableFuture<Consumer> future = new CompletableFuture<>();

    try {
        // 检查 Topic 是不是当前 broker 所服务的。
        brokerService.checkTopicNsOwnership(getName());
    } catch (Exception e) {
        future.completeExceptionally(e);
        return future;
    }
     // 读压缩特性仅仅允许在独占或故障转移订阅模式
    if (readCompacted && !(subType == SubType.Failover || subType == SubType.Exclusive)) {
        future.completeExceptionally(
                new NotAllowedException("readCompacted only allowed on failover or exclusive subscriptions"));
        return future;
    }
    // 订阅名不能为空
    if (isBlank(subscriptionName)) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Empty subscription name", topic);
        }
        future.completeExceptionally(new NamingException("Empty subscription name"));
        return future;
    }
    // 消费者不支持批量消息
    if (hasBatchMessagePublished && !cnx.isBatchMessageCompatibleVersion()) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Consumer doesn't support batch-message {}", topic, subscriptionName);
        }
        future.completeExceptionally(new UnsupportedVersionException("Consumer doesn't support batch-message"));
        return future;
    }
    // 不能订阅保留订阅名（即订阅名不能带有副本前缀 或者 去重名 pulsar.dedup）
    if (subscriptionName.startsWith(replicatorPrefix) || subscriptionName.equals(DEDUPLICATION_CURSOR_NAME)) {
        log.warn("[{}] Failed to create subscription for {}", topic, subscriptionName);
        future.completeExceptionally(new NamingException("Subscription with reserved subscription name attempted"));
        return future;
    }
    // 订阅限制速率（主机名或IP，消费者名，消费者ID）
    if (cnx.getRemoteAddress() != null && cnx.getRemoteAddress().toString().contains(":")) {
        SubscribeRateLimiter.ConsumerIdentifier consumer = new SubscribeRateLimiter.ConsumerIdentifier(
                cnx.getRemoteAddress().toString().split(":")[0], consumerName, consumerId);
        // 如果订阅限速器不为空，并且订阅限制列表有指定消费者或消费许可已达上限  或 无法获取许可，则不允许订阅
        if (subscribeRateLimiter.isPresent() && !subscribeRateLimiter.get().subscribeAvailable(consumer) || !subscribeRateLimiter.get().tryAcquire(consumer)) {
            log.warn("[{}] Failed to create subscription for {} {} limited by {}, available {}",
                    topic, subscriptionName, consumer, subscribeRateLimiter.get().getSubscribeRate(),
                    subscribeRateLimiter.get().getAvailableSubscribeRateLimit(consumer));
            future.completeExceptionally(new NotAllowedException("Subscribe limited by subscribe rate limit per consumer."));
            return future;
        }
    }

    lock.readLock().lock();
    try {//Topic 是否已关闭或被删除
        if (isFenced) {
            log.warn("[{}] Attempting to subscribe to a fenced topic", topic);
            future.completeExceptionally(new TopicFencedException("Topic is temporarily unavailable"));
            return future;
        }
        // 递增消费者引用计数
        USAGE_COUNT_UPDATER.incrementAndGet(this);
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] [{}] Added consumer -- count: {}", topic, subscriptionName, consumerName,
                    USAGE_COUNT_UPDATER.get(this));
        }
    } finally {
        lock.readLock().unlock();
    }
    // 这里区分持久化游标和非持久化游标，当 isDurable 为 true时，为持久化游标
    CompletableFuture<? extends Subscription> subscriptionFuture = isDurable ? //
            getDurableSubscription(subscriptionName, initialPosition) //
            : getNonDurableSubscription(subscriptionName, startMessageId);
    // 如果为持久化，确定最大未确认消息，否则为0
    int maxUnackedMessages  = isDurable ? brokerService.pulsar().getConfiguration().getMaxUnackedMessagesPerConsumer() :0;

    subscriptionFuture.thenAccept(subscription -> {
        try {
            Consumer consumer = new Consumer(subscription, subType, topic, consumerId, priorityLevel, consumerName,
                                             maxUnackedMessages, cnx, cnx.getRole(), metadata, readCompacted, initialPosition);
            subscription.addConsumer(consumer);
            if (!cnx.isActive()) {
                consumer.close();
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] [{}] Subscribe failed -- count: {}", topic, subscriptionName,
                            consumer.consumerName(), USAGE_COUNT_UPDATER.get(PersistentTopic.this));
                }
                future.completeExceptionally(
                        new BrokerServiceException("Connection was closed while the opening the cursor "));
            } else {
                log.info("[{}][{}] Created new subscription for {}", topic, subscriptionName, consumerId);
                future.complete(consumer);
            }
        } catch (BrokerServiceException e) {
            if (e instanceof ConsumerBusyException) {
                log.warn("[{}][{}] Consumer {} {} already connected", topic, subscriptionName, consumerId,
                        consumerName);
            } else if (e instanceof SubscriptionBusyException) {
                log.warn("[{}][{}] {}", topic, subscriptionName, e.getMessage());
            }

            USAGE_COUNT_UPDATER.decrementAndGet(PersistentTopic.this);
            future.completeExceptionally(e);
        }
    }).exceptionally(ex -> {
        log.warn("[{}] Failed to create subscription for {}: ", topic, subscriptionName, ex.getMessage());
        USAGE_COUNT_UPDATER.decrementAndGet(PersistentTopic.this);
        future.completeExceptionally(new PersistenceException(ex));
        return null;
    });

    return future;
}

private CompletableFuture<Subscription> getDurableSubscription(String subscriptionName, InitialPosition initialPosition) {
    CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
    // 打开游标，订阅名经过 URL 编码后，成游标名，起始位置为 initialPostion （也就是说，会从这个位置开始推送消息）
    ledger.asyncOpenCursor(Codec.encode(subscriptionName), initialPosition, new OpenCursorCallback() {
        @Override
        public void openCursorComplete(ManagedCursor cursor, Object ctx) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Opened cursor", topic, subscriptionName);
            }
            // 这里就订阅成功了
            subscriptionFuture.complete(subscriptions.computeIfAbsent(subscriptionName,
                    name -> createPersistentSubscription(subscriptionName, cursor)));
        }

        @Override
        public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
            log.warn("[{}] Failed to create subscription for {}: {}", topic, subscriptionName, exception.getMessage());
            USAGE_COUNT_UPDATER.decrementAndGet(PersistentTopic.this);
            subscriptionFuture.completeExceptionally(new PersistenceException(exception));
            if (exception instanceof ManagedLedgerFencedException) {
                // 如果 ledger 已经关闭了，就不能继续用它了，应该关闭它重新打开。
                close();
            }
        }
    }, null);
    return subscriptionFuture;
}

// 异步打开游标
public synchronized void asyncOpenCursor(final String cursorName, final InitialPosition initialPosition,
        final OpenCursorCallback callback, final Object ctx) {
    try {
        //检查 ledger 是否打开状态
        checkManagedLedgerIsOpen();
        //检查 ledger 是否已关闭或删除
        checkFenced();
    } catch (ManagedLedgerException e) {
        callback.openCursorFailed(e, ctx);
        return;
    }
    // 查看未初始化完成游标中是否有当前游标，如果有，则直接这里设置回调，并返回
    if (uninitializedCursors.containsKey(cursorName)) {
        uninitializedCursors.get(cursorName).thenAccept(cursor -> {
            callback.openCursorComplete(cursor, ctx);
        }).exceptionally(ex -> {
            callback.openCursorFailed((ManagedLedgerException) ex, ctx);
            return null;
        });
        return;
    }
    // 根据游标名读取游标，如果不为空，则直接回调，并返回
    ManagedCursor cachedCursor = cursors.get(cursorName);
    if (cachedCursor != null) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Cursor was already created {}", name, cachedCursor);
        }
        callback.openCursorComplete(cachedCursor, ctx);
        return;
    }

    // 到这里，意味着创建新的游标并且保存它
    if (log.isDebugEnabled()) {
        log.debug("[{}] Creating new cursor: {}", name, cursorName);
    }
    final ManagedCursorImpl cursor = new ManagedCursorImpl(bookKeeper, config, this, cursorName);
    CompletableFuture<ManagedCursor> cursorFuture = new CompletableFuture<>();
    uninitializedCursors.put(cursorName, cursorFuture);
    cursor.initialize(getLastPosition(), new VoidCallback() {
        @Override
        public void operationComplete() {
            log.info("[{}] Opened new cursor: {}", name, cursor);
            // 创建游标成功，把当前游标激活
            cursor.setActive();
            // 更新确认 position （当游标正在创建的时候，忽略消息写入），如果初始化 position 为 InitialPosition.Latest，
            // 则获取最后 position 和 消息总计数，否则就 ledger 的（就是 ledger 的 entryId = -1），之前的所有ledger历史总计数
            // 这里就是初始化游标 position ，决定着订阅起始点
            cursor.initializeCursorPosition(initialPosition == InitialPosition.Latest ? getLastPositionAndCounter()
                    : getFirstPositionAndCounter());
            // 新增游标，移除等待队列中的 futrue
            synchronized (this) {
                cursors.add(cursor);
                uninitializedCursors.remove(cursorName).complete(cursor);
            }
            // 执行成功
            callback.openCursorComplete(cursor, ctx);
        }

        @Override
        public void operationFailed(ManagedLedgerException exception) {
            log.warn("[{}] Failed to open cursor: {}", name, cursor);
            // 通知等待队列中 future 异常
            synchronized (this) {
                uninitializedCursors.remove(cursorName).completeExceptionally(exception);
            }
            // 执行失败
            callback.openCursorFailed(exception, ctx);
        }
    });
}

// 这里是初始化游标位置，决定消费者从哪个 position 读，哪个 position 标记删除，以及当前可消费消息计数
void initializeCursorPosition(Pair<PositionImpl, Long> lastPositionCounter) {
    readPosition = ledger.getNextValidPosition(lastPositionCounter.getLeft());
    markDeletePosition = lastPositionCounter.getLeft();

    // 初始化计数器，使得写在 ML 和 messagesConsumed 上的消息之间的差异为0，以确保初始积压计数为0。
    messagesConsumedCounter = lastPositionCounter.getRight();
}

// 游标实现类，构造方法，创建一个管理游标
ManagedCursorImpl(BookKeeper bookkeeper, ManagedLedgerConfig config, ManagedLedgerImpl ledger, String cursorName) {
    this.bookkeeper = bookkeeper;
    this.config = config;
    this.ledger = ledger;
    this.name = cursorName;
    this.digestType = BookKeeper.DigestType.fromApiDigestType(config.getDigestType());
    STATE_UPDATER.set(this, State.Uninitialized);
    PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.set(this, 0);
    PENDING_READ_OPS_UPDATER.set(this, 0);
    RESET_CURSOR_IN_PROGRESS_UPDATER.set(this, FALSE);
    WAITING_READ_OP_UPDATER.set(this, null);
    this.clock = config.getClock();
    this.lastActive = this.clock.millis();
    this.lastLedgerSwitchTimestamp = this.clock.millis();

    if (config.getThrottleMarkDelete() > 0.0) {
        markDeleteLimiter = RateLimiter.create(config.getThrottleMarkDelete());
    } else {
        // Disable mark-delete rate limiter
        markDeleteLimiter = null;
    }
}

// 游标初始化
void initialize(PositionImpl position, final VoidCallback callback) {
    recoveredCursor(position, Collections.emptyMap(), null);
    if (log.isDebugEnabled()) {
        log.debug("[{}] Consumer {} cursor initialized with counters: consumed {} mdPos {} rdPos {}",
                ledger.getName(), name, messagesConsumedCounter, markDeletePosition, readPosition);
    }

    createNewMetadataLedger(new VoidCallback() {
        @Override
        public void operationComplete() {
            STATE_UPDATER.set(ManagedCursorImpl.this, State.Open);
            callback.operationComplete();
        }

        @Override
        public void operationFailed(ManagedLedgerException exception) {
            callback.operationFailed(exception);
        }
    });
}

// 尝试恢复游标
private void recoveredCursor(PositionImpl position, Map<String, Long> properties,
                                 LedgerHandle recoveredFromCursorLedger) {
    
    // 如果 position 位于不存在的 ledger 中（因为如果以前是空的，它将被删除），需要转移到下一个已存在的 ledger。
    if (!ledger.ledgerExists(position.getLedgerId())) {
        // 获取下一个 ledger
        Long nextExistingLedger = ledger.getNextValidLedger(position.getLedgerId());
        // 下一个为空
        if (nextExistingLedger == null) {
            log.info("[{}] [{}] Couldn't find next next valid ledger for recovery {}", ledger.getName(), name,
                    position);
        }
        // 重置下 position
        position = nextExistingLedger != null ? PositionImpl.get(nextExistingLedger, -1) : position;
    }
    // 对比下 ledger 最新的 position,如果 position 大,则设置为 ledger 当前最新 position
    if (position.compareTo(ledger.getLastPosition()) > 0) {
        log.warn("[{}] [{}] Current position {} is ahead of last position {}", ledger.getName(), name, position,
                ledger.getLastPosition());
        position = PositionImpl.get(ledger.getLastPosition());
    }
    log.info("[{}] Cursor {} recovered to position {}", ledger.getName(), name, position);
    // 获取可消费消息计数
    messagesConsumedCounter = -getNumberOfEntries(Range.openClosed(position, ledger.getLastPosition()));
    // 标记删除 position 位置
    markDeletePosition = position;
    // 找到下一个可读消息位置
    readPosition = ledger.getNextValidPosition(position);
    // 最后标记删除消息
    lastMarkDeleteEntry = new MarkDeleteEntry(markDeletePosition, properties, null, null);
    // assign cursor-ledger so, it can be deleted when new ledger will be switched
    // 分配 curor-ledger ，可以在切换新 ledger 时删除它
    this.cursorLedger = recoveredFromCursorLedger;
    // 设置当前状态为没有 ledger
    STATE_UPDATER.set(this, State.NoLedger);
}

// 获取可消费消息个数
protected long getNumberOfEntries(Range<PositionImpl> range) {
    long allEntries = ledger.getNumberOfEntries(range);

    if (log.isDebugEnabled()) {
        log.debug("[{}] getNumberOfEntries. {} allEntries: {}", ledger.getName(), range, allEntries);
    }

    long deletedEntries = 0;

    lock.readLock().lock();
    try {
        //单个被删除的消息集合(简单的说就是ledger中形成了不能消费的空洞,要统计空洞的数量出来)
        for (Range<PositionImpl> r : individualDeletedMessages.asRanges()) {
            // r 是否在 range 之间
            if (r.isConnected(range)) {
                // 返回 r 与 range 的交集(即被删除的消息)
                Range<PositionImpl> commonEntries = r.intersection(range);
                // 统计交集的消息数
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

// 在指定范围内获取消息总数
long getNumberOfEntries(Range<PositionImpl> range) {
    PositionImpl fromPosition = range.lowerEndpoint();
    boolean fromIncluded = range.lowerBoundType() == BoundType.CLOSED;
    PositionImpl toPosition = range.upperEndpoint();
    boolean toIncluded = range.upperBoundType() == BoundType.CLOSED;

    if (fromPosition.getLedgerId() == toPosition.getLedgerId()) {
        // 如果2个 position 在同一 ledger 上 
        long count = toPosition.getEntryId() - fromPosition.getEntryId() - 1;
        count += fromIncluded ? 1 : 0;
        count += toIncluded ? 1 : 0;
        return count;
    } else {
        long count = 0;
        // 如果起始 position 与 截止 position 在不同的 ledger 上,则需要
        // 1. 对于截止 position , entryId 就是消息个数
        count += toPosition.getEntryId();
        // 如果包含截止 position ,则 + 1
        count += toIncluded ? 1 : 0;

        // 2. 对于起始 position 所在 ledger,则 每个 ledger 的消息个数减去起始 position 再减1
        // 如果包含起始 position ,则 + 1
        LedgerInfo li = ledgers.get(fromPosition.getLedgerId());
        if (li != null) {
            count += li.getEntries() - (fromPosition.getEntryId() + 1);
            count += fromIncluded ? 1 : 0;
        }

        // 3. 这里就简单了,直接给出 ledgers 在 fromPosition 的 ledgerId 到 toPosition 的 ledgerId 之间的子集,子集中各个 ledger 中消息总数相加
        for (LedgerInfo ls : ledgers.subMap(fromPosition.getLedgerId(), false, toPosition.getLedgerId(), false)
                .values()) {
            count += ls.getEntries();
        }

        return count;
    }
}

// 指定 position ,找出下一可用的 position
PositionImpl getNextValidPosition(final PositionImpl position) {
    PositionImpl nextPosition = position.getNext();
    while (!isValidPosition(nextPosition)) {
        // 返回大于等于的最小 nextPosition.ledgerId() + 1 ,如果没有返回 null
        Long nextLedgerId = ledgers.ceilingKey(nextPosition.getLedgerId() + 1);
        if (nextLedgerId == null) {
            return null;
        }
        nextPosition = PositionImpl.get(nextLedgerId.longValue(), 0);
    }
    return nextPosition;
}
// 校验 position 是否合法
boolean isValidPosition(PositionImpl position) {
    PositionImpl last = lastConfirmedEntry;
    if (log.isDebugEnabled()) {
        log.debug("IsValid position: {} -- last: {}", position, last);
    }
    // position 小于 0 ,不合法
    if (position.getEntryId() < 0) {
        return false;
    // 比最后确认消息都大,不合法
    } else if (position.getLedgerId() > last.getLedgerId()) {
        return false;
    } else if (position.getLedgerId() == last.getLedgerId()) {
        // 不能大于最后确认消息的下一消息
        return position.getEntryId() <= (last.getEntryId() + 1);
    } else {
        // 从 ledgers map中找出 ledger
        LedgerInfo ls = ledgers.get(position.getLedgerId());
        // 如果为空
        if (ls == null) {
            if (position.getLedgerId() < last.getLedgerId()) {
                // 指向比当前 ledger 旧的现有 ledger 无效
                return false;
            } else {
                // 指向不存在的 ledger 仅在 ledger 为空时才合法
                return position.getEntryId() == 0;
            }
        }
        // entryId 不能大于每个 ledger 的容量
        return position.getEntryId() < ls.getEntries();
    }
}

// 创建新的元数据 ledger,其实就是创建存储游标位置的 ledger
void createNewMetadataLedger(final VoidCallback callback) {
    ledger.mbean.startCursorLedgerCreateOp();
    //异步创建 ledger
    ledger.asyncCreateLedger(bookkeeper, config, digestType, (rc, lh, ctx) -> {
         // 判定是否超时,如果超时则异步删除ledger
        if (ledger.checkAndCompleteLedgerOpTask(rc, lh, ctx)) {
            return;
        }
        // 执行创建 ledger 后,开始处理结果
        ledger.getExecutor().execute(safeRun(() -> {
            ledger.mbean.endCursorLedgerCreateOp();
            // 创建错误.
            if (rc != BKException.Code.OK) {
                log.warn("[{}] Error creating ledger for cursor {}: {}", ledger.getName(), name,
                        BKException.getMessage(rc));
                callback.operationFailed(new ManagedLedgerException(BKException.getMessage(rc)));
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Created ledger {} for cursor {}", ledger.getName(), lh.getId(), name);
            }
            // 已成功创建 ledger ,现在开始写入最后 position 消息
            MarkDeleteEntry mdEntry = lastMarkDeleteEntry;
            // 保存 position 到 leger
            persistPositionToLedger(lh, mdEntry, new VoidCallback() {
                @Override
                public void operationComplete() {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Persisted position {} for cursor {}", ledger.getName(),
                                mdEntry.newPosition, name);
                    }
                    // 如果保存 position 成功，则切换新的 ledger
                    switchToNewLedger(lh, new VoidCallback() {
                        @Override
                        public void operationComplete() {
                            callback.operationComplete();
                        }

                        @Override
                        public void operationFailed(ManagedLedgerException exception) {
                            // 这意味着它无法切换新创建的 ledger，故删除以防止泄漏（无法完全切换，新建的 ledger 沦为垃圾要删除）
                            bookkeeper.asyncDeleteLedger(lh.getId(), (int rc, Object ctx) -> {
                                if (rc != BKException.Code.OK) {
                                    log.warn("[{}] Failed to delete orphan ledger {}", ledger.getName(),
                                            lh.getId());
                                }
                            }, null);
                            callback.operationFailed(exception);
                        }
                    });
                }

                @Override
                public void operationFailed(ManagedLedgerException exception) {
                    log.warn("[{}] Failed to persist position {} for cursor {}", ledger.getName(),
                            mdEntry.newPosition, name);

                    ledger.mbean.startCursorLedgerDeleteOp();
                    // 保存失败，删除 ledger
                    bookkeeper.asyncDeleteLedger(lh.getId(), new DeleteCallback() {
                        @Override
                        public void deleteComplete(int rc, Object ctx) {
                            ledger.mbean.endCursorLedgerDeleteOp();
                        }
                    }, null);
                    callback.operationFailed(exception);
                }
            });
        }));
    }, LedgerMetadataUtils.buildAdditionalMetadataForCursor(name));

}

// 这里检查和完成 ledger 的操作任务
protected boolean checkAndCompleteLedgerOpTask(int rc, LedgerHandle lh, Object ctx) {
    if (ctx != null && ctx instanceof AtomicBoolean) {
        // ledger-creation 已经超时,故回调已执行,删除 ledger 并返回.
        if (((AtomicBoolean) (ctx)).get()) {
            if (rc == BKException.Code.OK) {
                log.warn("[{}]-{} ledger creation timed-out, deleting ledger", this.name, lh.getId());
                asyncDeleteLedger(lh.getId(), DEFAULT_LEDGER_DELETE_RETRIES);
            }
            return true;
        }
        ((AtomicBoolean) ctx).set(true);
    }
    return false;
}

protected void asyncCreateLedger(BookKeeper bookKeeper, ManagedLedgerConfig config, DigestType digestType,
            CreateCallback cb, Map<String, byte[]> metadata) {
    AtomicBoolean ledgerCreated = new AtomicBoolean(false);
    Map<String, byte[]> finalMetadata = new HashMap<>();
    finalMetadata.putAll(ledgerMetadata);
    finalMetadata.putAll(metadata);
    if (log.isDebugEnabled()) {
        log.debug("creating ledger, metadata: "+finalMetadata);
    }
    // bookkeeper 异步创建 ledger
    bookKeeper.asyncCreateLedger(config.getEnsembleSize(), config.getWriteQuorumSize(), config.getAckQuorumSize(),
            digestType, config.getPassword(), cb, ledgerCreated, finalMetadata);
    // 规定时间(MetadataOperationsTimeoutSeconds)内,如果创建没完成,则置为超时
    scheduledExecutor.schedule(() -> {
        if (!ledgerCreated.get()) {
            ledgerCreated.set(true);
            cb.createComplete(BKException.Code.TimeoutException, null, null);
        }
    }, config.getMetadataOperationsTimeoutSeconds(), TimeUnit.SECONDS);
}

// 保存 Position 到 ledger 中
void persistPositionToLedger(final LedgerHandle lh, MarkDeleteEntry mdEntry, final VoidCallback callback) {
    PositionImpl position = mdEntry.newPosition;
    // 构建 Position 信息(就是游标信息,游标开销还是蛮低的)
    PositionInfo pi = PositionInfo.newBuilder().setLedgerId(position.getLedgerId())
            .setEntryId(position.getEntryId())
            .addAllIndividualDeletedMessages(buildIndividualDeletedMessageRanges())
            .addAllProperties(buildPropertiesMap(mdEntry.properties)).build();


    if (log.isDebugEnabled()) {
        log.debug("[{}] Cursor {} Appending to ledger={} position={}", ledger.getName(), name, lh.getId(),
                position);
    }

    checkNotNull(lh);
    lh.asyncAddEntry(pi.toByteArray(), (rc, lh1, entryId, ctx) -> {
        if (rc == BKException.Code.OK) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Updated cursor {} position {} in meta-ledger {}", ledger.getName(), name, position,
                        lh1.getId());
            }

            if (shouldCloseLedger(lh1)) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Need to create new metadata ledger for consumer {}", ledger.getName(), name);
                }
                // 创建新的元数据存储 ledger
                startCreatingNewMetadataLedger();
            }
            // 执行回调，表示已成功完成
            callback.operationComplete();
        } else {
            log.warn("[{}] Error updating cursor {} position {} in meta-ledger {}: {}", ledger.getName(), name,
                    position, lh1.getId(), BKException.getMessage(rc));
            // 如果有写错误，ledger 将自动关闭，需要创建一个新的 ledger，同时标记删除操作将排队。
            STATE_UPDATER.compareAndSet(ManagedCursorImpl.this, State.Open, State.NoLedger);

            // 放弃之前，尝试保存 position 在元数据存储中（即 Zk 中）
            persistPositionMetaStore(-1, position, mdEntry.properties, new MetaStoreCallback<Void>() {
                @Override
                public void operationComplete(Void result, Stat stat) {
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "[{}][{}] Updated cursor in meta store after previous failure in ledger at position {}",
                                ledger.getName(), name, position);
                    }
                    callback.operationComplete();
                }

                @Override
                public void operationFailed(MetaStoreException e) {
                    log.warn("[{}][{}] Failed to update cursor in meta store after previous failure in ledger: {}",
                            ledger.getName(), name, e.getMessage());
                    callback.operationFailed(createManagedLedgerException(rc));
                }
            }, true);
        }
    }, null);
}

// 是否应该关闭 ledger
boolean shouldCloseLedger(LedgerHandle lh) {
    long now = clock.millis();
    // 最后新增确认大于等于 每个ledger最大消息数或者最后 ledger 切换时间已过,并且当前游标状态不是已关闭或正关闭中
    if ((lh.getLastAddConfirmed() >= config.getMetadataMaxEntriesPerLedger()
            || lastLedgerSwitchTimestamp < (now - config.getLedgerRolloverTimeout() * 1000))
            && (STATE_UPDATER.get(this) != State.Closed && STATE_UPDATER.get(this) != State.Closing)) {
        // 修改时间戳是安全的，因为此方法只会从回调中调用，这意味着将在一个线程上串行化调用
        lastLedgerSwitchTimestamp = now;
        return true;
    } else {
        return false;
    }
}

void startCreatingNewMetadataLedger() {
    // 更改状态，以便新的标记删除操作将排队，而不是立即提交(执行)
    State oldState = STATE_UPDATER.getAndSet(this, State.SwitchingLedger);
    if (oldState == State.SwitchingLedger) {
        // 如果正在切换中..则立即返回
        return;
    }

    // 正标记删除提交计数,如果等于0，则立即创建新的 ledger
    if (PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.get(this) == 0) {
        createNewMetadataLedger();
    }
}

// 切换新的 ledger
void switchToNewLedger(final LedgerHandle lh, final VoidCallback callback) {
    if (log.isDebugEnabled()) {
        log.debug("[{}] Switching cursor {} to ledger {}", ledger.getName(), name, lh.getId());
    }
    // 保存 position 到元数据存储中 （zk中）
    persistPositionMetaStore(lh.getId(), lastMarkDeleteEntry.newPosition, lastMarkDeleteEntry.properties,
            new MetaStoreCallback<Void>() {
        @Override
        public void operationComplete(Void result, Stat stat) {
            log.info("[{}] Updated cursor {} with ledger id {} md-position={} rd-position={}", ledger.getName(),
                    name, lh.getId(), markDeletePosition, readPosition);
            // 切换
            final LedgerHandle oldLedger = cursorLedger;
            cursorLedger = lh;
            cursorLedgerStat = stat;

            // position 位置安全删除
            callback.operationComplete();
            // 异步删除老的 ledger（即老的游标信息将被删除）
            asyncDeleteLedger(oldLedger);
        }

        @Override
        public void operationFailed(MetaStoreException e) {
            log.warn("[{}] Failed to update consumer {}", ledger.getName(), name, e);
            callback.operationFailed(e);
        }
    }, false);
}

// 创建持久化订阅
private PersistentSubscription createPersistentSubscription(String subscriptionName, ManagedCursor cursor) {
    checkNotNull(compactedTopic);
    // 对比订阅名，判断是否压缩订阅
    if (subscriptionName.equals(Compactor.COMPACTION_SUBSCRIPTION)) {
        return new CompactorSubscription(this, compactedTopic, subscriptionName, cursor);
    } else {
        return new PersistentSubscription(this, subscriptionName, cursor);
    }
}

// 往持久化订阅增加消费者
public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
    // 更新游标最新访问时间（即最后活动时间）
    cursor.updateLastActive();
    // 状态判断，如果订阅已关闭，则抛异常
    if (IS_FENCED_UPDATER.get(this) == TRUE) {
        log.warn("Attempting to add consumer {} on a fenced subscription", consumer);
        throw new SubscriptionFencedException("Subscription is fenced");
    }
    // 如果分发器为空 或者 分发器暂无活动消费者
    if (dispatcher == null || !dispatcher.isConsumerConnected()) {
        switch (consumer.subType()) {
        case Exclusive:
            // 独占模式
            if (dispatcher == null || dispatcher.getType() != SubType.Exclusive) {
                dispatcher = new PersistentDispatcherSingleActiveConsumer(cursor, SubType.Exclusive, 0, topic);
            }
            break;
        case Shared:
            // 共享模式
            if (dispatcher == null || dispatcher.getType() != SubType.Shared) {
                dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursor);
            }
            break;
        case Failover:
            //故障转移模式
            int partitionIndex = TopicName.getPartitionIndex(topicName);
            if (partitionIndex < 0) {
                // For non partition topics, assume index 0 to pick a predictable consumer
                partitionIndex = 0;
            }

            if (dispatcher == null || dispatcher.getType() != SubType.Failover) {
                dispatcher = new PersistentDispatcherSingleActiveConsumer(cursor, SubType.Failover, partitionIndex,
                        topic);
            }
            break;
        default:
            throw new ServerMetadataException("Unsupported subscription type");
        }
    } else {
        if (consumer.subType() != dispatcher.getType()) {
            throw new SubscriptionBusyException("Subscription is of different type");
        }
    }

    dispatcher.addConsumer(consumer);
}

// AbstractDispatcherSingleActiveConsumer 构造方法
public AbstractDispatcherSingleActiveConsumer(SubType subscriptionType, int partitionIndex,
        String topicName) {
    this.topicName = topicName;
    this.consumers = new CopyOnWriteArrayList<>();
    this.partitionIndex = partitionIndex;
    this.subscriptionType = subscriptionType;
    ACTIVE_CONSUMER_UPDATER.set(this, null);
}

// 单活消费者实现
// 单活构造方法
public PersistentDispatcherSingleActiveConsumer(ManagedCursor cursor, SubType subscriptionType, int partitionIndex,
        PersistentTopic topic) {
    super(subscriptionType, partitionIndex, topic.getName());
    this.topic = topic;
    this.name = topic.getName() + " / " + (cursor.getName() != null ? Codec.decode(cursor.getName())
            : ""/* NonDurableCursor 没有名字*/);
    this.cursor = cursor;
    this.serviceConfig = topic.getBrokerService().pulsar().getConfiguration();
    this.readBatchSize = serviceConfig.getDispatcherMaxReadBatchSize();
    // 重发跟踪器是被禁用的
    this.redeliveryTracker = RedeliveryTrackerDisabled.REDELIVERY_TRACKER_DISABLED;
    this.initializeDispatchRateLimiterIfNeeded(Optional.empty());
}

public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
    // 状态检查
    if (IS_CLOSED_UPDATER.get(this) == TRUE) {
        log.warn("[{}] Dispatcher is already closed. Closing consumer ", this.topicName, consumer);
        consumer.disconnect();
    }
    // 独占模式下，已有消费者，则抛异常
    if (subscriptionType == SubType.Exclusive && !consumers.isEmpty()) {
        throw new ConsumerBusyException("Exclusive consumer is already connected");
    }
    // topic 对消费者数限制
    if (isConsumersExceededOnTopic()) {
        log.warn("[{}] Attempting to add consumer to topic which reached max consumers limit", this.topicName);
        throw new ConsumerBusyException("Topic reached max consumers limit");
    }
    // 每个订阅对消费者数限制
    if (subscriptionType == SubType.Failover && isConsumersExceededOnSubscription()) {
        log.warn("[{}] Attempting to add consumer to subscription which reached max consumers limit", this.topicName);
        throw new ConsumerBusyException("Subscription reached max consumers limit");
    }

    consumers.add(consumer);

    if (!pickAndScheduleActiveConsumer()) {
        // the active consumer is not changed
        Consumer currentActiveConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
        if (null == currentActiveConsumer) {
            if (log.isDebugEnabled()) {
                log.debug("Current active consumer disappears while adding consumer {}", consumer);
            }
        } else {
            consumer.notifyActiveConsumerChange(currentActiveConsumer);
        }
    }

}
// topic 消费者人数配额限制
protected boolean isConsumersExceededOnTopic() {
    Policies policies;
    try {
        policies = topic.getBrokerService().pulsar().getConfigurationCache().policiesCache()
                .get(AdminResource.path(POLICIES, TopicName.get(topicName).getNamespace()))
                .orElseGet(() -> new Policies());
    } catch (Exception e) {
        policies = new Policies();
    }
    final int maxConsumersPerTopic = policies.max_consumers_per_topic > 0 ?
            policies.max_consumers_per_topic :
            serviceConfig.getMaxConsumersPerTopic();
    if (maxConsumersPerTopic > 0 && maxConsumersPerTopic <= topic.getNumberOfConsumers()) {
        return true;
    }
    return false;
}

// 订阅中最大消费者配额限制
protected boolean isConsumersExceededOnSubscription() {
    Policies policies;
    try {
        policies = topic.getBrokerService().pulsar().getConfigurationCache().policiesCache()
                .get(AdminResource.path(POLICIES, TopicName.get(topicName).getNamespace()))
                .orElseGet(() -> new Policies());
    } catch (Exception e) {
        policies = new Policies();
    }
    final int maxConsumersPerSubscription = policies.max_consumers_per_subscription > 0 ?
            policies.max_consumers_per_subscription :
            serviceConfig.getMaxConsumersPerSubscription();
    if (maxConsumersPerSubscription > 0 && maxConsumersPerSubscription <= consumers.size()) {
        return true;
    }
    return false;
}

protected boolean pickAndScheduleActiveConsumer() {
    // 不能为空
    checkArgument(!consumers.isEmpty());
    // 根据消费者名字排序
    consumers.sort((c1, c2) -> c1.consumerName().compareTo(c2.consumerName()));
    // 确定消费者
    int index = partitionIndex % consumers.size();
    // 保存原消费者
    Consumer prevConsumer = ACTIVE_CONSUMER_UPDATER.getAndSet(this, consumers.get(index));
    // 获取设置的消费者
    Consumer activeConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
    if (prevConsumer == activeConsumer) {
        // 如果活的消费者没有变化，则返回 false
        return false;
    } else {
        // 如果活的消费者已经变革，则发送通知
        scheduleReadOnActiveConsumer();
        return true;
    }
}

protected void scheduleReadOnActiveConsumer() {
    // 取消正读请求
    if (havePendingRead && cursor.cancelPendingReadRequest()) {
        havePendingRead = false;
    }
    // 如果取消失败，则返回
    if (havePendingRead) {
        return;
    }

    // 当新的消费者被选中，从未确认消息中开始重发，如果有任何正读取操作，让它完成并且重置
    // 当订阅类型不是故障转移 或者 故障转移延迟时间小于等于 0,执行操作
    if (subscriptionType != SubType.Failover || serviceConfig.getActiveConsumerFailoverDelayTimeMillis() <= 0) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Rewind cursor and read more entries without delay", name);
        }
        cursor.rewind();

        Consumer activeConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
        // 通知活的消费者变更
        notifyActiveConsumerChanged(activeConsumer);
        // 读取更多消息
        readMoreEntries(activeConsumer);
        return;
    }

    // 如果订阅类型为故障转移，延迟配置时间间隔重置游标和读取消息，避免消息重复
    if (readOnActiveConsumerTask != null) {
        return;
    }

    readOnActiveConsumerTask = topic.getBrokerService().executor().schedule(() -> {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Rewind cursor and read more entries after {} ms delay", name,
                    serviceConfig.getActiveConsumerFailoverDelayTimeMillis());
        }
        cursor.rewind();

        Consumer activeConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
        notifyActiveConsumerChanged(activeConsumer);
        readMoreEntries(activeConsumer);
        readOnActiveConsumerTask = null;
    }, serviceConfig.getActiveConsumerFailoverDelayTimeMillis(), TimeUnit.MILLISECONDS);
}

// 多个消费者实现
// 多活构造方法
public PersistentDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor) {
    this.serviceConfig = topic.getBrokerService().pulsar().getConfiguration();
    this.cursor = cursor;
    this.name = topic.getName() + " / " + Codec.decode(cursor.getName());
    this.topic = topic;
    // 消息重放集合
    this.messagesToReplay = new ConcurrentLongPairSet(512, 2);
    // 根据配置来决定是否启用内存重发跟踪器
    this.redeliveryTracker = this.serviceConfig.isSubscriptionRedeliveryTrackerEnabled()
            ? new InMemoryRedeliveryTracker()
            : RedeliveryTrackerDisabled.REDELIVERY_TRACKER_DISABLED;
    this.readBatchSize = serviceConfig.getDispatcherMaxReadBatchSize();
    // 这里初始化最大未确认消息
    this.maxUnackedMessages = topic.getBrokerService().pulsar().getConfiguration()
            .getMaxUnackedMessagesPerSubscription();
    this.initializeDispatchRateLimiterIfNeeded(Optional.empty());
}


public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
    if (IS_CLOSED_UPDATER.get(this) == TRUE) {
        log.warn("[{}] Dispatcher is already closed. Closing consumer ", name, consumer);
        consumer.disconnect();
        return;
    }
    // 当前消费者列表为空
    if (consumerList.isEmpty()) {
        if (havePendingRead || havePendingReplayRead) {
            // 如果有正（回放）读取消息已经在运行，必须等到它们完成后才能转回游标，此标志为true
            shouldRewindBeforeReadingOrReplaying = true;
        } else {
            cursor.rewind();
            shouldRewindBeforeReadingOrReplaying = false;
        }
        // 这里清空消息回放池
        messagesToReplay.clear();
    }

    if (isConsumersExceededOnTopic()) {
        log.warn("[{}] Attempting to add consumer to topic which reached max consumers limit", name);
        throw new ConsumerBusyException("Topic reached max consumers limit");
    }

    if (isConsumersExceededOnSubscription()) {
        log.warn("[{}] Attempting to add consumer to subscription which reached max consumers limit", name);
        throw new ConsumerBusyException("Subscription reached max consumers limit");
    }

    consumerList.add(consumer);
    // 按照优先级排序
    consumerList.sort((c1, c2) -> c1.getPriorityLevel() - c2.getPriorityLevel());
    consumerSet.add(consumer);
}


// ============================ 非持久化订阅 ===================================

private CompletableFuture<? extends Subscription> getNonDurableSubscription(String subscriptionName, MessageId startMessageId) {
    CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
    log.info("[{}][{}] Creating non-durable subscription at msg id {}", topic, subscriptionName, startMessageId);

    // 仅当第一个消费者连接时，创建一个新的非持久化游标
    Subscription subscription = subscriptions.computeIfAbsent(subscriptionName, name -> {
        //起始消息ID不为空，则用起始消息ID，否则用最后的消息ID
        MessageIdImpl msgId = startMessageId != null ? (MessageIdImpl) startMessageId
                : (MessageIdImpl) MessageId.latest;

        long ledgerId = msgId.getLedgerId();
        long entryId = msgId.getEntryId();
        // 判定是否批量消息ID
        if (msgId instanceof BatchMessageIdImpl) {
            // 当起始消息ID是关联一个批量（消息ID）时，需要在（批量消息ID中的）第一个消息ID上取前一个消息ID，因为这批量（消息）可能还没被完全消费，客户端将丢弃在批量消息中的第一个消息
            if (((BatchMessageIdImpl) msgId).getBatchIndex() >= 0) {//第一个消息
                entryId = msgId.getEntryId() - 1;//第一个消息的上个消息
            }
        }
        // 确定起始 Position
        Position startPosition = new PositionImpl(ledgerId, entryId);
        // 创建非持久化游标
        ManagedCursor cursor = null;
        try {
            cursor = ledger.newNonDurableCursor(startPosition);
        } catch (ManagedLedgerException e) {
            subscriptionFuture.completeExceptionally(e);
        }
        // 这里跟持久化订阅一样，只不过被回调的方法是空实现，实际上转发给 NonDurableCursorImpl 类
        return new PersistentSubscription(this, subscriptionName, cursor);
    });

    if (!subscriptionFuture.isDone()) {
        subscriptionFuture.complete(subscription);
    } else {
        // 初始化游标失败，清理已创建的订阅
        subscriptions.remove(subscriptionName);
    }

    return subscriptionFuture;
}

// 创建非持久化游标
public ManagedCursor newNonDurableCursor(Position startCursorPosition) throws ManagedLedgerException {
    // 检查 ML 是否打开
    checkManagedLedgerIsOpen();
    // 检查是否已关闭或删除
    checkFenced();

    return new NonDurableCursorImpl(bookKeeper, config, this, null, (PositionImpl) startCursorPosition);
}

// 非持久化类
NonDurableCursorImpl(BookKeeper bookkeeper, ManagedLedgerConfig config, ManagedLedgerImpl ledger, String cursorName,
            PositionImpl startCursorPosition) {
    super(bookkeeper, config, ledger, cursorName);

    // 仅使用 ledgerId 与“最新” position 标记进行比较。 由于 C++ 客户端使用48位来存储 entryId，
    // 因此它无法将 Long.max() 作为 entryId 传递(因为 Long.max() 为2的63次方减1)。 在这种情况下，没有必要要求 ledgerId 和 entryId 都是 Long.max()
    if (startCursorPosition == null || startCursorPosition.getLedgerId() == PositionImpl.latest.getLedgerId()) {
        // 用最近的 position 初始化游标 position
        initializeCursorPosition(ledger.getLastPositionAndCounter());
    } else if (startCursorPosition.equals(PositionImpl.earliest)) {
        // 用第一个可用消息 position 作为起始订阅 position
        recoverCursor(ledger.getPreviousPosition(ledger.getFirstPosition()));
    } else {
        // 由于游标位于标记删除 position，需要从所需的读取 position 向后退一步（即前一个消息 position）
        recoverCursor(startCursorPosition);
    }

    log.info("[{}] Created non-durable cursor read-position={} mark-delete-position={}", ledger.getName(),
            readPosition, markDeletePosition);
}

// 恢复游标
private void recoverCursor(PositionImpl mdPosition) {
    // 获取新的 position 以及消息计数
    Pair<PositionImpl, Long> lastEntryAndCounter = ledger.getLastPositionAndCounter();
    // 找到下一可用 position 地址，作为读 position
    this.readPosition = ledger.getNextValidPosition(mdPosition);
    // 当前地址标识为删除
    markDeletePosition = mdPosition;

    // 初始化计数器，使得写在 managerLedger 和 messagesConsumed 上的消息之间的差异等于当前的积压（否定）。
    // 其实就是对比当前读游标与最新的 position 之间是否有可用消息，如果有则计算出来，否则为0
    long initialBacklog = readPosition.compareTo(lastEntryAndCounter.getLeft()) < 0
            ? ledger.getNumberOfEntries(Range.closed(readPosition, lastEntryAndCounter.getLeft())) : 0;
    messagesConsumedCounter = lastEntryAndCounter.getRight() - initialBacklog;
}

// 把消费者加入到非持久化订阅中
public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
    if (IS_FENCED_UPDATER.get(this) == TRUE) {
        log.warn("Attempting to add consumer {} on a fenced subscription", consumer);
        throw new SubscriptionFencedException("Subscription is fenced");
    }

    if (dispatcher == null || !dispatcher.isConsumerConnected()) {
        switch (consumer.subType()) {
        case Exclusive:
            if (dispatcher == null || dispatcher.getType() != SubType.Exclusive) {
                dispatcher = new NonPersistentDispatcherSingleActiveConsumer(SubType.Exclusive, 0, topic, this);
            }
            break;
        case Shared:
            if (dispatcher == null || dispatcher.getType() != SubType.Shared) {
                dispatcher = new NonPersistentDispatcherMultipleConsumers(topic, this);
            }
            break;
        case Failover:
            int partitionIndex = TopicName.getPartitionIndex(topicName);
            if (partitionIndex < 0) {
                partitionIndex = 0;
            }

            if (dispatcher == null || dispatcher.getType() != SubType.Failover) {
                dispatcher = new NonPersistentDispatcherSingleActiveConsumer(SubType.Failover, partitionIndex,
                        topic, this);
            }
            break;
        default:
            throw new ServerMetadataException("Unsupported subscription type");
        }
    } else {
        if (consumer.subType() != dispatcher.getType()) {
            throw new SubscriptionBusyException("Subscription is of different type");
        }
    }

    dispatcher.addConsumer(consumer);
}


// 单活消费者，与持久化订阅一样

// 多活消费者
// 与持久化订阅基本上类似
public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
    if (IS_CLOSED_UPDATER.get(this) == TRUE) {
        log.warn("[{}] Dispatcher is already closed. Closing consumer ", name, consumer);
        consumer.disconnect();
        return;
    }

    if (isConsumersExceededOnTopic()) {
        log.warn("[{}] Attempting to add consumer to topic which reached max consumers limit", name);
        throw new ConsumerBusyException("Topic reached max consumers limit");
    }

    if (isConsumersExceededOnSubscription()) {
        log.warn("[{}] Attempting to add consumer to subscription which reached max consumers limit", name);
        throw new ConsumerBusyException("Subscription reached max consumers limit");
    }

    consumerList.add(consumer);
    consumerSet.add(consumer);
}

```

自此，订阅接口已经完毕，下面，总结下流程：

1. 跟生产者一样,先进行权限校验(包括 Proxy 和 Client)
2. 元数据校验
3. 获取或创建 Topic
4. 对订阅的 schema 进行兼容性检查或者添加
5. 如果订阅类型为持久化订阅,则:
   
   1. 检查 Topic 是否为当前为 broker 所有
   2. 检查压缩订阅模式仅支持故障转移或独占订阅
   3. 检查订阅名不能为空
   4. 检查消费者不支持批量消息
   5. 检查订阅名不能为系统保留名
   6. 检查订阅是否限流
   7. 检查Topic的状态是否合法
   8. 打开游标(主要存储消费进度)
   9. 如果是订阅名为 `__compaction`,则创建压缩 Topic 订阅,否则,创建一般的持久化订阅

6. 否则为非持久化订阅:
   
   1. 创建一般的持久化订阅

7. 创建消费者，并把消费者新增到订阅
8. 尝试用新创建的消费者执行完成,如果成功，则发送订阅成功，否则关闭消费者。

#### 4. newFlow 实现

```java

 protected void handleFlow(CommandFlow flow) {
    checkArgument(state == State.Connected);
    if (log.isDebugEnabled()) {
        log.debug("[{}] Received flow from consumer {} permits: {}", remoteAddress, flow.getConsumerId(),
                flow.getMessagePermits());
    }
    // 通过消费者ID查找消费者信息
    CompletableFuture<Consumer> consumerFuture = consumers.get(flow.getConsumerId());

    if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
        Consumer consumer = consumerFuture.getNow(null);
        if (consumer != null) {
            // 流控核心方法
            consumer.flowPermits(flow.getMessagePermits());
        } else {
            log.info("[{}] Couldn't find consumer {}", remoteAddress, flow.getConsumerId());
        }
    }
}

void flowPermits(int additionalNumberOfMessages) {
    // 消费者给定的推送消息数一定要大于0
    checkArgument(additionalNumberOfMessages > 0);

    // 当未确认数达到最大未确认消息数限制时，阻塞共享的消费者
    if (shouldBlockConsumerOnUnackMsgs() && unackedMessages >= maxUnackedMessages) {
        blockedConsumerOnUnackedMsgs = true;
    }
    int oldPermits;
    if (!blockedConsumerOnUnackedMsgs) {
        // 如果没超过限制，则设置新的流控消息数
        oldPermits = MESSAGE_PERMITS_UPDATER.getAndAdd(this, additionalNumberOfMessages);
        subscription.consumerFlow(this, additionalNumberOfMessages);
    } else {
        // 否则暂停（到达这里，意味着不对客户端（消费端）进行响应，任由它超时）
        oldPermits = PERMITS_RECEIVED_WHILE_CONSUMER_BLOCKED_UPDATER.getAndAdd(this, additionalNumberOfMessages);
    }

    if (log.isDebugEnabled()) {
        log.debug("[{}-{}] Added more flow control message permits {} (old was: {}), blocked = {} ", topicName,
                subscription, additionalNumberOfMessages, oldPermits, blockedConsumerOnUnackedMsgs);
    }

}

//当未确认消息时是否应该阻塞消费者
//1. 消费者是共享订阅模式
//2. 最大未确认消息数大于0
private boolean shouldBlockConsumerOnUnackMsgs() {
    return SubType.Shared.equals(subType) && maxUnackedMessages > 0;
}

// ============================= 持久化订阅对应的实现 ===============================

public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
    dispatcher.consumerFlow(consumer, additionalNumberOfMessages);
}

// 多活消费者实现
public synchronized void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
    // 如果消费者不在消费者集合里面，则忽略
    if (!consumerSet.contains(consumer)) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Ignoring flow control from disconnected consumer {}", name, consumer);
        }
        return;
    }
    // 总可用许可（简单的说就是对应一个 Topic ，某时间段的可允许的推送给消费者的消息数）
    totalAvailablePermits += additionalNumberOfMessages;
    if (log.isDebugEnabled()) {
        log.debug("[{}-{}] Trigger new read after receiving flow control message with permits {}", name, consumer,
                totalAvailablePermits);
    }
    // 开始从 bookkeeper 中读取消息
    readMoreEntries();
}


public void readMoreEntries() {
    // 如果总可用许可数大于0 并且 当前至少有一个消费者存活
    if (totalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
        // 读批量大小与总可用许可数取最小值
        int messagesToRead = Math.min(totalAvailablePermits, readBatchSize);

        // 限制仅仅发生在以下2种情况：（1） 游标不是活动状态（或者 throttle-nonBacklogConsumer 标志被启用）那么活动游标读取
        // 消息是从缓存而不是 bookkeeper（2）从 bookkeeper 读取消息时，如果 topic 已达到消息速率阈值：则在 MESSAGE_RATE_BACKOFF_MS 之后定时读取
        if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
            // 判断 Topic 级别分发器限流是否启用
            if (topic.getDispatchRateLimiter().isPresent() && topic.getDispatchRateLimiter().get().isDispatchRateLimitingEnabled()) {
                DispatchRateLimiter topicRateLimiter = topic.getDispatchRateLimiter().get();
                // 判断 Topic 级别消息分发器是否有配置限流参数
                if (!topicRateLimiter.hasMessageDispatchPermit()) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] message-read exceeded topic message-rate {}/{}, schedule after a {}", name,
                                topicRateLimiter.getDispatchRateOnMsg(), topicRateLimiter.getDispatchRateOnByte(),
                                MESSAGE_RATE_BACKOFF_MS);
                    }
                    topic.getBrokerService().executor().schedule(() -> readMoreEntries(), MESSAGE_RATE_BACKOFF_MS,
                            TimeUnit.MILLISECONDS);
                    return;
                } else {
                    // 如果分发器限速启用，则根据可用许可数来给出当前消息读取数
                    long availablePermitsOnMsg = topicRateLimiter.getAvailableDispatchRateLimitOnMsg();
                    if (availablePermitsOnMsg > 0) {
                        messagesToRead = Math.min(messagesToRead, (int) availablePermitsOnMsg);
                    }
                }
            }
            // 订阅级别的限流器是否配置，这里基本上跟上类似，只是隔离级别不一样，一个是 Topic 级别，一个是订阅级别
            if (dispatchRateLimiter.isPresent() && dispatchRateLimiter.get().isDispatchRateLimitingEnabled()) {
                if (!dispatchRateLimiter.get().hasMessageDispatchPermit()) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] message-read exceeded subscription message-rate {}/{}, schedule after a {}", name,
                            dispatchRateLimiter.get().getDispatchRateOnMsg(), dispatchRateLimiter.get().getDispatchRateOnByte(),
                            MESSAGE_RATE_BACKOFF_MS);
                    }
                    topic.getBrokerService().executor().schedule(() -> readMoreEntries(), MESSAGE_RATE_BACKOFF_MS,
                        TimeUnit.MILLISECONDS);
                    return;
                } else {
                    // if dispatch-rate is in msg then read only msg according to available permit
                    long availablePermitsOnMsg = dispatchRateLimiter.get().getAvailableDispatchRateLimitOnMsg();
                    if (availablePermitsOnMsg > 0) {
                        messagesToRead = Math.min(messagesToRead, (int) availablePermitsOnMsg);
                    }
                }
            }

        }
        // 消息回放不为空
        if (!messagesToReplay.isEmpty()) {
            // 如果有真正读取的回放，则跳过本次
            if (havePendingReplayRead) {
                log.debug("[{}] Skipping replay while awaiting previous read to complete", name);
                return;
            }
            // 获取正回放消息的 Postition 信息
            Set<PositionImpl> messagesToReplayNow = messagesToReplay.items(messagesToRead).stream()
                    .map(pair -> new PositionImpl(pair.first, pair.second)).collect(toSet());

            if (log.isDebugEnabled()) {
                log.debug("[{}] Schedule replay of {} messages for {} consumers", name, messagesToReplayNow.size(),
                        consumerList.size());
            }
            // 设置正回放读标志
            havePendingReplayRead = true;
            Set<? extends Position> deletedMessages = cursor.asyncReplayEntries(messagesToReplayNow, this,
                    ReadType.Replay);
            // 从回放池中清除已确认的消息
            deletedMessages.forEach(position -> messagesToReplay.remove(((PositionImpl) position).getLedgerId(),
                    ((PositionImpl) position).getEntryId()));
            // 如果所有的消息都已确认，则从消息回放池中清除，尝试读取下一轮消息
            if ((messagesToReplayNow.size() - deletedMessages.size()) == 0) {
                havePendingReplayRead = false;
                readMoreEntries();
            }
        } else if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.get(this) == TRUE) {//分发器读阻塞，因为未确认消息数已达到最大值
            log.warn("[{}] Dispatcher read is blocked due to unackMessages {} reached to max {}", name,
                    totalUnackedMessages, maxUnackedMessages);
        } else if (!havePendingRead) {//暂没读取任务，则开始读取
            if (log.isDebugEnabled()) {
                log.debug("[{}] Schedule read of {} messages for {} consumers", name, messagesToRead,
                        consumerList.size());
            }
            havePendingRead = true;
            // 游标异步读取消息或等待，前面章节已分析过，这里不再赘述
            cursor.asyncReadEntriesOrWait(messagesToRead, this, ReadType.Normal);
        } else {
            // 等前一个读完成
            log.debug("[{}] Cannot schedule next read until previous one is done", name);
        }
    } else {
        // 消费者缓存满了，暂停读取
        if (log.isDebugEnabled()) {
            log.debug("[{}] Consumer buffer is full, pause reading", name);
        }
    }
}

public boolean hasMessageDispatchPermit() {
    return (dispatchRateLimiterOnMessage == null || dispatchRateLimiterOnMessage.getAvailablePermits() > 0)
            && (dispatchRateLimiterOnByte == null || dispatchRateLimiterOnByte.getAvailablePermits() > 0);
}

// 从 position 集合中获取已确认消息的 position
public Set<? extends Position> asyncReplayEntries(final Set<? extends Position> positions,
        ReadEntriesCallback callback, Object ctx) {

    List<Entry> entries = Lists.newArrayListWithExpectedSize(positions.size());
    // 如果 position 集合为空，则直接返回
    if (positions.isEmpty()) {
        callback.readEntriesComplete(entries, ctx);
    }

    // 过滤掉已经确认的消息
    Set<Position> alreadyAcknowledgedPositions = Sets.newHashSet();
    lock.readLock().lock();
    try {
        // 判定 position 如果位于单独删除消息集合或小于标记删除 position，则认为是已确认消息
        positions.stream()
                .filter(position -> individualDeletedMessages.contains((PositionImpl) position)
                        || ((PositionImpl) position).compareTo(markDeletePosition) < 0)
                .forEach(alreadyAcknowledgedPositions::add);
    } finally {
        lock.readLock().unlock();
    }
    // 总不可用 position 数
    final int totalValidPositions = positions.size() - alreadyAcknowledgedPositions.size();
    final AtomicReference<ManagedLedgerException> exception = new AtomicReference<>();
    ReadEntryCallback cb = new ReadEntryCallback() {
        int pendingCallbacks = totalValidPositions;

        @Override
        public synchronized void readEntryComplete(Entry entry, Object ctx) {
            if (exception.get() != null) {
                // 如果在一个不同的 position 有异常，则释放实体，并不要把它放入列表
                entry.release();
                if (--pendingCallbacks == 0) {
                    callback.readEntriesFailed(exception.get(), ctx);
                }
            } else {
                entries.add(entry);
                if (--pendingCallbacks == 0) {
                    callback.readEntriesComplete(entries, ctx);
                }
            }
        }

        @Override
        public synchronized void readEntryFailed(ManagedLedgerException mle, Object ctx) {
            log.warn("[{}][{}] Error while replaying entries", ledger.getName(), name, mle);
            if (exception.compareAndSet(null, mle)) {
                // 尝试设置异常，如果成功只要读取异常，则释放所有实体
                entries.forEach(Entry::release);
            }
            if (--pendingCallbacks == 0) {
                callback.readEntriesFailed(exception.get(), ctx);
            }
        }
    };
    // 这里异步读取 position 对应的消息
    positions.stream().filter(position -> !alreadyAcknowledgedPositions.contains(position))
            .forEach(p -> ledger.asyncReadEntry((PositionImpl) p, cb, ctx));

    return alreadyAcknowledgedPositions;
}

// 回放池异步读取消息成功执行
public synchronized void readEntriesComplete(List<Entry> entries, Object ctx) {
    ReadType readType = (ReadType) ctx;
    int start = 0;
    int entriesToDispatch = entries.size();
    // 读类型
    if (readType == ReadType.Normal) {
        havePendingRead = false;
    } else {
        havePendingReplayRead = false;
    }
    // 确定分发器批量读取消息大小
    if (readBatchSize < serviceConfig.getDispatcherMaxReadBatchSize()) {
        int newReadBatchSize = Math.min(readBatchSize * 2, serviceConfig.getDispatcherMaxReadBatchSize());
        if (log.isDebugEnabled()) {
            log.debug("[{}] Increasing read batch size from {} to {}", name, readBatchSize, newReadBatchSize);
        }

        readBatchSize = newReadBatchSize;
    }
    // 如果读消息失败，则减半
    readFailureBackoff.reduceToHalf();
    // 在读或重放之前应该把游标重置（即以标记删除 position 的下一个消息为新的读 position）
    if (shouldRewindBeforeReadingOrReplaying && readType == ReadType.Normal) {
        //当前所有消息释放
        entries.forEach(Entry::release);
        // 重置
        cursor.rewind();
        shouldRewindBeforeReadingOrReplaying = false;
        // 开始读消息
        readMoreEntries();
        return;
    }

    if (log.isDebugEnabled()) {
        log.debug("[{}] Distributing {} messages to {} consumers", name, entries.size(), consumerList.size());
    }

    long totalMessagesSent = 0;
    long totalBytesSent = 0;
    // 如果消息待分发数、总可用许可数大于0，并且至少有一个消费者可用（其连接通道可写）
    while (entriesToDispatch > 0 && totalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
        // 上文已经解析过获取消费者(消费者优先级，轮询)
        Consumer c = getNextConsumer();
        if (c == null) {
            // 当前没消费者，游标将重置（即当前标记删除的position的下一个消息为读 position）
            log.info("[{}] rewind because no available consumer found from total {}", name, consumerList.size());
            // 释放消息
            entries.subList(start, entries.size()).forEach(Entry::release);
            cursor.rewind();
            return;
        }

        // 消费者轮询批量分发
        int messagesForC = Math.min(
            Math.min(entriesToDispatch, c.getAvailablePermits()),
            serviceConfig.getDispatcherMaxRoundRobinBatchSize());
        // 如果分发数大于0
        if (messagesForC > 0) {

            // 如果读取类型为 ReadType.Replay （重放），则移除对应的回放池中消息
            if (readType == ReadType.Replay) {
                entries.subList(start, start + messagesForC).forEach(entry -> {
                    messagesToReplay.remove(entry.getLedgerId(), entry.getEntryId());
                });
            }
            // 组成消息包，发送给消费者
            SendMessageInfo sentMsgInfo = c.sendMessages(entries.subList(start, start + messagesForC));
            // 统计指标，准备下一轮发送
            long msgSent = sentMsgInfo.getTotalSentMessages();
            start += messagesForC;
            entriesToDispatch -= messagesForC;
            totalAvailablePermits -= msgSent;
            totalMessagesSent += sentMsgInfo.getTotalSentMessages();
            totalBytesSent += sentMsgInfo.getTotalSentMessageBytes();
        }
    }

    // 启用非积压消费者的分发限制（简单的说就是体现配额限制） 或者游标不活动了，尝试把这些数据放入限流器中
    if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
        if (topic.getDispatchRateLimiter().isPresent()) {
            topic.getDispatchRateLimiter().get().tryDispatchPermit(totalMessagesSent, totalBytesSent);
        }

        if (dispatchRateLimiter.isPresent()) {
            dispatchRateLimiter.get().tryDispatchPermit(totalMessagesSent, totalBytesSent);
        }
    }
    // 如果此时待分发数还大于0
    if (entriesToDispatch > 0) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] No consumers found with available permits, storing {} positions for later replay", name,
                    entries.size() - start);
        }
        // 把剩余的消息（未分发给消息者的）存到消息回放池
        entries.subList(start, entries.size()).forEach(entry -> {
            messagesToReplay.add(entry.getLedgerId(), entry.getEntryId());
            entry.release();
        });
    }
    // 继续读更多的消息
    readMoreEntries();
}


// 回放池读取消息失败
public synchronized void readEntriesFailed(ManagedLedgerException exception, Object ctx) {

    ReadType readType = (ReadType) ctx;
    // 获取读下一次等待时间
    long waitTimeMillis = readFailureBackoff.next();
    // 没有更多消息可读异常
    if (exception instanceof NoMoreEntriesToReadException) {
        // 如果挤压队列消息为0，则通知消费者集合已达到 Topic 的末尾（换句话说就是：此时刻，Topic 中的消息已全部消费）
        if (cursor.getNumberOfEntriesInBacklog() == 0) {
            // Topic 已被终止，并且没有更多消息可供读取且所有消息已被确认时，通知消费者
            consumerList.forEach(Consumer::reachedEndOfTopic);
        }
    } else if (!(exception instanceof TooManyRequestsException)) {
        // 读请求异常
        log.error("[{}] Error reading entries at {} : {}, Read Type {} - Retrying to read in {} seconds", name,
                cursor.getReadPosition(), exception.getMessage(), readType, waitTimeMillis / 1000.0);
    } else {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Error reading entries at {} : {}, Read Type {} - Retrying to read in {} seconds", name,
                    cursor.getReadPosition(), exception.getMessage(), readType, waitTimeMillis / 1000.0);
        }
    }
    //前面已说明
    if (shouldRewindBeforeReadingOrReplaying) {
        shouldRewindBeforeReadingOrReplaying = false;
        cursor.rewind();
    }
    // 如果读类型为正常类型，则设置 havePendingRead 为false
    if (readType == ReadType.Normal) {
        havePendingRead = false;
    } else {
        havePendingReplayRead = false;
        // 异常为不可用回放 position 异常，则移除那些已删除的消息ID
        if (exception instanceof ManagedLedgerException.InvalidReplayPositionException) {
            PositionImpl markDeletePosition = (PositionImpl) cursor.getMarkDeletedPosition();
            messagesToReplay.removeIf((ledgerId, entryId) -> {
                return ComparisonChain.start().compare(ledgerId, markDeletePosition.getLedgerId())
                        .compare(entryId, markDeletePosition.getEntryId()).result() <= 0;
            });
        }
    }

    readBatchSize = serviceConfig.getDispatcherMinReadBatchSize();
    // 重试读更多消息
    topic.getBrokerService().executor().schedule(() -> {
        synchronized (PersistentDispatcherMultipleConsumers.this) {
            if (!havePendingRead) {
                log.info("[{}] Retrying read operation", name);
                readMoreEntries();
            } else {
                log.info("[{}] Skipping read retry: havePendingRead {}", name, havePendingRead, exception);
            }
        }
    }, waitTimeMillis, TimeUnit.MILLISECONDS);

}

// 单活消费者实现

public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
    topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName, SafeRun.safeRun(() -> {
        internalConsumerFlow(consumer, additionalNumberOfMessages);
    }));
}

private synchronized void internalConsumerFlow(Consumer consumer, int additionalNumberOfMessages) {
    // 如果正在读，则忽略本次调用
    if (havePendingRead) {
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Ignoring flow control message since we already have a pending read req", name,
                    consumer);
        }
        // 如果原先消费者不是本次消费者，则忽略本次调用
    } else if (ACTIVE_CONSUMER_UPDATER.get(this) != consumer) {
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Ignoring flow control message since consumer is not active partition consumer", name,
                    consumer);
        }
        // 如果活消息读任务不为空，则忽略本次调用
    } else if (readOnActiveConsumerTask != null) {
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Ignoring flow control message since consumer is waiting for cursor to be rewinded",
                    name, consumer);
        }
    } else {
        // 触发本次读消息调用
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Trigger new read after receiving flow control message", name, consumer);
        }
        readMoreEntries(consumer);
    }
}

protected void readMoreEntries(Consumer consumer) {
    // 当所有的消费者与 broker 断开连接时，消费者为空，故如果当前没有存活消费者时，跳过本地读消息调用
    if (null == consumer) {
        return;
    }

    int availablePermits = consumer.getAvailablePermits();

    if (availablePermits > 0) {
        if (!consumer.isWritable()) {
            // 如果当前连接不可写，无论如何还是发读请求，但仅读取一个消息，这里的目的是继续使用这请求作为通知机制，同时避免读取和分派大量的消息，
            // 这些消息在写入套接字之前需要等待。
            availablePermits = 1;
        }

        int messagesToRead = Math.min(availablePermits, readBatchSize);
        // 这段跟上个实现基本一致
        if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
            if (topic.getDispatchRateLimiter().isPresent()
                    && topic.getDispatchRateLimiter().get().isDispatchRateLimitingEnabled()) {
                DispatchRateLimiter topicRateLimiter = topic.getDispatchRateLimiter().get();
                if (!topicRateLimiter.hasMessageDispatchPermit()) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] message-read exceeded topic message-rate {}/{}, schedule after a {}", name,
                            topicRateLimiter.getDispatchRateOnMsg(), topicRateLimiter.getDispatchRateOnByte(),
                            MESSAGE_RATE_BACKOFF_MS);
                    }
                    topic.getBrokerService().executor().schedule(() -> {
                        // 这里获取到消费者
                        Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
                        if (currentConsumer != null && !havePendingRead) {
                            readMoreEntries(currentConsumer);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Skipping read retry for topic: Current Consumer {}, havePendingRead {}",
                                    topic.getName(), currentConsumer, havePendingRead);
                            }
                        }
                    }, MESSAGE_RATE_BACKOFF_MS, TimeUnit.MILLISECONDS);
                    return;
                } else {
                    long availablePermitsOnMsg = topicRateLimiter.getAvailableDispatchRateLimitOnMsg();
                    if (availablePermitsOnMsg > 0) {
                        messagesToRead = Math.min(messagesToRead, (int) availablePermitsOnMsg);
                    }
                }
            }

            if (dispatchRateLimiter.isPresent() && dispatchRateLimiter.get().isDispatchRateLimitingEnabled()) {
                if (!dispatchRateLimiter.get().hasMessageDispatchPermit()) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] message-read exceeded subscription message-rate {}/{}, schedule after a {}", name,
                            dispatchRateLimiter.get().getDispatchRateOnMsg(), dispatchRateLimiter.get().getDispatchRateOnByte(),
                            MESSAGE_RATE_BACKOFF_MS);
                    }
                    topic.getBrokerService().executor().schedule(() -> {
                        Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
                        if (currentConsumer != null && !havePendingRead) {
                            readMoreEntries(currentConsumer);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Skipping read retry: Current Consumer {}, havePendingRead {}",
                                    topic.getName(), currentConsumer, havePendingRead);
                            }
                        }
                    }, MESSAGE_RATE_BACKOFF_MS, TimeUnit.MILLISECONDS);
                    return;
                } else {
                    long subPermitsOnMsg = dispatchRateLimiter.get().getAvailableDispatchRateLimitOnMsg();
                    if (subPermitsOnMsg > 0) {
                        messagesToRead = Math.min(messagesToRead, (int) subPermitsOnMsg);
                    }
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Schedule read of {} messages", name, consumer, messagesToRead);
        }
        havePendingRead = true;
        // 注意，这里区分了压缩读（最主要的区别之一）
        if (consumer.readCompacted()) {
            topic.compactedTopic.asyncReadEntriesOrWait(cursor, messagesToRead, this, consumer);
        } else {
            cursor.asyncReadEntriesOrWait(messagesToRead, this, consumer);
        }
    } else {
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Consumer buffer is full, pause reading", name, consumer);
        }
    }
}

// 消息读取成功，执行方法
public void readEntriesComplete(final List<Entry> entries, Object obj) {
    topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName, SafeRun.safeRun(() -> {
        internalReadEntriesComplete(entries, obj);
    }));
}

public synchronized void internalReadEntriesComplete(final List<Entry> entries, Object obj) {
    Consumer readConsumer = (Consumer) obj;
    if (log.isDebugEnabled()) {
        log.debug("[{}-{}] Got messages: {}", name, readConsumer, entries.size());
    }
    
    havePendingRead = false;

    if (readBatchSize < serviceConfig.getDispatcherMaxReadBatchSize()) {
        int newReadBatchSize = Math.min(readBatchSize * 2, serviceConfig.getDispatcherMaxReadBatchSize());
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Increasing read batch size from {} to {}", name, readConsumer, readBatchSize,
                    newReadBatchSize);
        }

        readBatchSize = newReadBatchSize;
    }

    readFailureBackoff.reduceToHalf();
    // 获取当前唯一活的消费者
    Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
    // 如果当前消费者为 null 或者 与读取消息之前设置的消费者不一样
    if (currentConsumer == null || readConsumer != currentConsumer) {
        // 自发出读取请求以来，活动消费者已发生变更。 此时，需要倒回游标并重新发出新消费者的读取请求
        if (log.isDebugEnabled()) {
            log.debug("[{}] rewind because no available consumer found", name);
        }
        entries.forEach(Entry::release);
        cursor.rewind();
        if (currentConsumer != null) {
            // 通知消费者已变更
            notifyActiveConsumerChanged(currentConsumer);
            readMoreEntries(currentConsumer);
        }
    } else {
        // 这里发送消息
        currentConsumer.sendMessages(entries, (future, sentMsgInfo) -> {
            if (future.isSuccess()) {
                // 与上文一样，不再说明
                if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
                    if (topic.getDispatchRateLimiter().isPresent()) {
                        topic.getDispatchRateLimiter().get().tryDispatchPermit(sentMsgInfo.getTotalSentMessages(),
                                sentMsgInfo.getTotalSentMessageBytes());
                    }

                    if (dispatchRateLimiter.isPresent()) {
                        dispatchRateLimiter.get().tryDispatchPermit(sentMsgInfo.getTotalSentMessages(),
                                sentMsgInfo.getTotalSentMessageBytes());
                    }
                }

                // 仅在将上一批次数据写入套接字后才进行新的批量消息读取操作
                topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName, SafeRun.safeRun(() -> {
                    synchronized (PersistentDispatcherSingleActiveConsumer.this) {
                        Consumer newConsumer = getActiveConsumer();
                        if (newConsumer != null && !havePendingRead) {
                            readMoreEntries(newConsumer);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "[{}-{}] Ignoring write future complete. consumerAvailable={} havePendingRead={}",
                                        name, newConsumer, newConsumer != null, havePendingRead);
                            }
                        }
                    }
                }));
            }
        });
    }
}

// 批量读取消息失败回调
public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
    topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName, SafeRun.safeRun(() -> {
        internalReadEntriesFailed(exception, ctx);
    }));
}

private synchronized void internalReadEntriesFailed(ManagedLedgerException exception, Object ctx) {
    havePendingRead = false;
    Consumer c = (Consumer) ctx;

    long waitTimeMillis = readFailureBackoff.next();

    if (exception instanceof NoMoreEntriesToReadException) {
        if (cursor.getNumberOfEntriesInBacklog() == 0) {
            // topic 已被终止，没有更多消息可读取，仅在已确认所有消息时才通知消费者
            consumers.forEach(Consumer::reachedEndOfTopic);
        }
    } else if (!(exception instanceof TooManyRequestsException)) {
        // 读取异常
        log.error("[{}-{}] Error reading entries at {} : {} - Retrying to read in {} seconds", name, c,
                cursor.getReadPosition(), exception.getMessage(), waitTimeMillis / 1000.0);
    } else {
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Got throttled by bookies while reading at {} : {} - Retrying to read in {} seconds",
                    name, c, cursor.getReadPosition(), exception.getMessage(), waitTimeMillis / 1000.0);
        }
    }
    // 消费者非空判断
    checkNotNull(c);

    readBatchSize = serviceConfig.getDispatcherMinReadBatchSize();

    topic.getBrokerService().executor().schedule(() -> {

        topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName, SafeRun.safeRun(() -> {
            synchronized (PersistentDispatcherSingleActiveConsumer.this) {
                Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
                // 如果有一个活动的消费者并且没有正读取消息操作，此时应该尝试读取消息
                if (currentConsumer != null && !havePendingRead) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}-{}] Retrying read operation", name, c);
                    }
                    if (currentConsumer != c) {
                        notifyActiveConsumerChanged(currentConsumer);
                    }
                    readMoreEntries(currentConsumer);
                } else {
                    log.info("[{}-{}] Skipping read retry: Current Consumer {}, havePendingRead {}", name, c,
                            currentConsumer, havePendingRead);
                }
            }
        }));
    }, waitTimeMillis, TimeUnit.MILLISECONDS);

}

// ============================= 非持久化订阅对应的实现==============================

//单活消费者非持久化分发器实现
public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
    // No-op
}

//多活消费者非持久化分发器实现
public synchronized void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
    // 查看当前消费者是否在消费者集中
    if (!consumerSet.contains(consumer)) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Ignoring flow control from disconnected consumer {}", name, consumer);
        }
        return;
    }
    // 增加最大可用许可
    TOTAL_AVAILABLE_PERMITS_UPDATER.addAndGet(this, additionalNumberOfMessages);
    if (log.isDebugEnabled()) {
        log.debug("[{}] Trigger new read after receiving flow control message", consumer);
    }
}

```

流控命令实现还是较为简单。为了兼容多种订阅模式，实现了4种组合：非持久化单活消费者，非持久化多活消费者，持久化单活消费者，持久化多活消费者。
下面就梳理下命令实现流程：

1. 未确认消息时，是否应该阻塞消费者
2. 如果非阻塞，则增加消息许可，并调用订阅流控方法，否则增加当消费者阻塞时，增加消息许可
3. 如果消费者是持久化类型订阅，且仅有单活消费者，则执行 PersistentDispatcherSingleActiveConsumer 类中 consumerFlow 方法：
   1. 判断是否是单活消费者，并忽略部分调用。
   2. 执行 Topic 、 订阅级别的限流
   3. 如果消费者是压缩读，则直接在 `compactedTopic` 中异步读或等待，否则，消费者直接从游标中异步读取消息或等待。
   4. 如果从游标中读取消息成功，则
       1. 调整批量读消息大小
       2. 调整读取消息失败时批量大小减半
       3. 如果当前消费者为空或者不等于消息读取之前的消费者，则重置游标，重新用当前消费者去读取消息，否则，发送（推送）消息给客户端的消费者。
       4. 尝试用本次发送消息数、发送字节数更新 Topic 、订阅级别的分发限制器
   5. 如果读取失败，则获取读取失败等待时间，异常处理，并延时发布一个读取消息任务。
4. 如果消费者是持久化订阅，但支持多活订阅，则执行 PersistentDispatcherMultipleConsumers 类中 consumerFlow 方法：
   1. 检查当前消费者是否是消费者集合中的
   2. 新增总可用许可数
   3. 如果总可用许可数大于0并至少有一个消费者可用，则执行 Topic 、 订阅级别的限流
   4. 如果消息回放池不为空，根据可用许可以及消息回放池确定要回放的消息（ID），把得出要回放的消息（ID）尝试从 ledger 读取回放的消息（此时会过滤已被确认的消息），此时已被确认的消息将从
   消息回放池移除，如果从 ledger 读取消息成功后，将执行回调： 1. 选出一个合适的消费者 2. 把消息推送选出的消费者 3. 尝试用本次发送消息数、发送字节数更新 Topic 、订阅级别的分发限制器 4. 如果
   消息还没有分发完，则重新放入消息回放池中。如果失败，1. 根据异常来处理不同的返回 2. 定时下一轮重试读取。
5. 如果消费者是非持久化订阅，且仅有单活消费者，则执行 NonPersistentDispatcherSingleActiveConsumer 类中 consumerFlow 方法：空实现。
6. 如果消费者是非持久化订阅，但支持多活订阅，则执行 NonPersistentDispatcherMultipleConsumers 类中 consumerFlow 方法：1. 如果不在消费者集中，直接返回。2. 增加可用许可数。


#### 5. handleAck 命令实现

```java

protected void handleAck(CommandAck ack) {
    // 状态校验
    checkArgument(state == State.Connected);
    CompletableFuture<Consumer> consumerFuture = consumers.get(ack.getConsumerId());
    // 如果创建消费者完成正常
    if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
        consumerFuture.getNow(null).messageAcked(ack);
    }
}

// 消息确认
void messageAcked(CommandAck ack) {
    Map<String,Long> properties = Collections.emptyMap();
    if (ack.getPropertiesCount() > 0) {
        properties = ack.getPropertiesList().stream()
            .collect(Collectors.toMap((e) -> e.getKey(),
                                      (e) -> e.getValue()));
    }
    // 确认类型为累计确认（简单的说就是，一个消息 Position 就可以确认之前所有的消息）
    if (ack.getAckType() == AckType.Cumulative) {
        if (ack.getMessageIdCount() != 1) {
            log.warn("[{}] [{}] Received multi-message ack at {} - Reason: {}", subscription, consumerId);
            return;
        }

        if (subType == SubType.Shared) {
            log.warn("[{}] [{}] Received cumulative ack on shared subscription, ignoring", subscription, consumerId);
            return;
        }

        MessageIdData msgId = ack.getMessageId(0);
        PositionImpl position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId());
        subscription.acknowledgeMessage(Collections.singletonList(position), AckType.Cumulative, properties);
    } else {
        // 确认类型为单个确认 （这里支持跨位确认，假设有1-10个消息，可以先确认第3个，再确认1-2）
        List<Position> positionsAcked = new ArrayList<>();
        for (int i = 0; i < ack.getMessageIdCount(); i++) {
            MessageIdData msgId = ack.getMessageId(i);
            PositionImpl position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId());
            positionsAcked.add(position);

            if (subType == SubType.Shared) {
                removePendingAcks(position);
            }

            if (ack.hasValidationError()) {
                log.error("[{}] [{}] Received ack for corrupted message at {} - Reason: {}", subscription,
                        consumerId, position, ack.getValidationError());
            }
        }
        subscription.acknowledgeMessage(positionsAcked, AckType.Individual, properties);
    }
}


// ==================== 持久化确认 ====================

public void acknowledgeMessage(List<Position> positions, AckType ackType, Map<String,Long> properties) {
    // 累计确认
    if (ackType == AckType.Cumulative) {
        if (positions.size() != 1) {
            log.warn("[{}][{}] Invalid cumulative ack received with multiple message ids", topicName, subName);
            return;
        }

        Position position = positions.get(0);
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Cumulative ack on {}", topicName, subName, position);
        }
        cursor.asyncMarkDelete(position, properties, markDeleteCallback, position);
    } else {
        // 单个确认
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Individual acks on {}", topicName, subName, positions);
        }
        // 异步删除消息
        cursor.asyncDelete(positions, deleteCallback, positions);
        // 这里是消息重传跟踪器，跟踪消息重传次数
        dispatcher.getRedeliveryTracker().removeBatch(positions);
    }
    // 如果 Topic 已经关闭并且积压队列中无消息，则通知消费者 Topic 已达到底部
    if (topic.getManagedLedger().isTerminated() && cursor.getNumberOfEntriesInBacklog() == 0) {
         dispatcher.getConsumers().forEach(Consumer::reachedEndOfTopic);
    }
}


public void asyncMarkDelete(final Position position, Map<String, Long> properties,
        final MarkDeleteCallback callback, final Object ctx) {
    checkNotNull(position);
    checkArgument(position instanceof PositionImpl);
    // 检查游标是否关闭
    if (isClosed()) {
        callback.markDeleteFailed(new ManagedLedgerException("Cursor was already closed"), ctx);
        return;
    }
    // 判断游标是否正在重置，如果在，则直接返回
    if (RESET_CURSOR_IN_PROGRESS_UPDATER.get(this) == TRUE) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] cursor reset in progress - ignoring mark delete on position [{}] for cursor [{}]",
                    ledger.getName(), position, name);
        }
        callback.markDeleteFailed(
                new ManagedLedgerException("Reset cursor in progress - unable to mark delete position "
                        + ((PositionImpl) position).toString()),
                ctx);
    }

    if (log.isDebugEnabled()) {
        log.debug("[{}] Mark delete cursor {} up to position: {}", ledger.getName(), name, position);
    }
    PositionImpl newPosition = (PositionImpl) position;
    // 如果最后确认消息 position 小于 当前 position，则直接返回，表示不合法 position
    if (((PositionImpl) ledger.getLastConfirmedEntry()).compareTo(newPosition) < 0) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "[{}] Failed mark delete due to invalid markDelete {} is ahead of last-confirmed-entry {} for cursor [{}]",
                    ledger.getName(), position, ledger.getLastConfirmedEntry(), name);
        }
        callback.markDeleteFailed(new ManagedLedgerException("Invalid mark deleted position"), ctx);
        return;
    }

    lock.writeLock().lock();
    try {
        // 根据当前新的 position，来确认标记删除 position，读 position
        newPosition = setAcknowledgedPosition(newPosition);
    } catch (IllegalArgumentException e) {
        callback.markDeleteFailed(getManagedLedgerException(e), ctx);
        return;
    } finally {
        lock.writeLock().unlock();
    }

    // 应用限制标记删除操作（如果没有拿到许可，重置最后标记删除实体，并直接执行回调并返回）
    if (markDeleteLimiter != null && !markDeleteLimiter.tryAcquire()) {
        lastMarkDeleteEntry = new MarkDeleteEntry(newPosition, properties, null, null);
        callback.markDeleteComplete(ctx);
        return;
    }
    internalAsyncMarkDelete(newPosition, properties, callback, ctx);
}

// 设置确认 position
PositionImpl setAcknowledgedPosition(PositionImpl newMarkDeletePosition) {
    // 如果新的标记删除 position 小于 原标记删除 position，则抛异常
    if (newMarkDeletePosition.compareTo(markDeletePosition) < 0) {
        throw new IllegalArgumentException("Mark deleting an already mark-deleted position");
    }
    
    PositionImpl oldMarkDeletePosition = markDeletePosition;
    // 这里不等于，实际上就是大于了
    if (!newMarkDeletePosition.equals(oldMarkDeletePosition)) {
        long skippedEntries = 0;
        // 判断是不是相邻的
        if (newMarkDeletePosition.getLedgerId() == oldMarkDeletePosition.getLedgerId()
                && newMarkDeletePosition.getEntryId() == oldMarkDeletePosition.getEntryId() + 1) {
            // 如果单个消息确认集合中有此消息，则跳过计数
            skippedEntries = individualDeletedMessages.contains(newMarkDeletePosition) ? 0 : 1;
        } else {
            // 计算跳过的消息个数，该方法签名章节已分析，这里跳过
            skippedEntries = getNumberOfEntries(Range.openClosed(oldMarkDeletePosition, newMarkDeletePosition));
        }
        // 找出新标记删除 position 后可用 position
        PositionImpl positionAfterNewMarkDelete = ledger.getNextValidPosition(newMarkDeletePosition);
        // 如果新的可用 position 已经在单个消息确认集合中
        if (individualDeletedMessages.contains(positionAfterNewMarkDelete)) {
            // 确定新的可用 position 在哪个范围内
            Range<PositionImpl> rangeToBeMarkDeleted = individualDeletedMessages
                    .rangeContaining(positionAfterNewMarkDelete);
            //返回范围内最大的 position 
            newMarkDeletePosition = rangeToBeMarkDeleted.uppeEndpoint();
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Moved ack position from: {} to: {} -- skipped: {}", ledger.getName(),
                    oldMarkDeletePosition, newMarkDeletePosition, skippedEntries);
        }
        // 统计已消费消息总数
        messagesConsumedCounter += skippedEntries;
    }

    // 标记删除 position ，清除那些已删除（已确认）的消息
    markDeletePosition = PositionImpl.get(newMarkDeletePosition);
    individualDeletedMessages.remove(Range.atMost(markDeletePosition));
    // 如果读 position 小于等于新的标记删除 position，意味着客户端已经跳过一些消息，则重新计算移动到新的读 position
    if (readPosition.compareTo(newMarkDeletePosition) <= 0) {
        PositionImpl oldReadPosition = readPosition;
        readPosition = ledger.getNextValidPosition(newMarkDeletePosition);

        if (log.isDebugEnabled()) {
            log.debug("[{}] Moved read position from: {} to: {}, and new mark-delete position {}", ledger.getName(),
                    oldReadPosition, readPosition, markDeletePosition);
        }
    }

    return newMarkDeletePosition;
}

// 内部异步标记删除操作
protected void internalAsyncMarkDelete(final PositionImpl newPosition, Map<String, Long> properties,
        final MarkDeleteCallback callback, final Object ctx) {
    ledger.mbean.addMarkDeleteOp();

    MarkDeleteEntry mdEntry = new MarkDeleteEntry(newPosition, properties, callback, ctx);

    // 无法在切换期间写入 ledger，需要等到新的元数据 ledger 可用
    synchronized (pendingMarkDeleteOps) {
        // 在等待队列互斥时，状态可能已经改变
        switch (STATE_UPDATER.get(this)) {
        case Closed:
            callback.markDeleteFailed(new ManagedLedgerException("Cursor was already closed"), ctx);
            return;

        case NoLedger:
            // 需要创建新 ledger 写入（这里前面章节已经分析过，不再重复）
            startCreatingNewMetadataLedger();
            // ledger 切换中，需要把当前操作放入等待队列
        case SwitchingLedger:
            pendingMarkDeleteOps.add(mdEntry);
            break;

        case Open:
            if (PENDING_READ_OPS_UPDATER.get(this) > 0) {
                // 等待直到没有读操作发生才进行下一个操作
                pendingMarkDeleteOps.add(mdEntry);
            } else {
                // 立即执行标记删除
                internalMarkDelete(mdEntry);
            }
            break;

        default:
            log.error("[{}][{}] Invalid cursor state: {}", ledger.getName(), name, state);
            callback.markDeleteFailed(new ManagedLedgerException("Cursor was in invalid state: " + state), ctx);
            break;
        }
    }
}

void internalMarkDelete(final MarkDeleteEntry mdEntry) {
    // 该计数器用于标记提交给BK且尚未完成的所有待处理标记删除请求。
    // 当有未完成的请求，无法关闭当前 ledger，故当计数器变为0时，切换到新 ledger 会被推迟。
    PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.incrementAndGet(this);

    lastMarkDeleteEntry = mdEntry;
    // 保存 position 信息到 ledger，前面章节有分析，这里不再赘述
    persistPositionToLedger(cursorLedger, mdEntry, new VoidCallback() {
        @Override
        public void operationComplete() {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Mark delete cursor {} to position {} succeeded", ledger.getName(), name,
                        mdEntry.newPosition);
            }

            // 这新的标记删除 position 之前所有对应的 position，都将在单个删除消息池中移除
            // （简单的说，只要保存成功新的 position，在单个删除消息池中所有小于这个 position 都将移除）
            lock.writeLock().lock();
            try {
                individualDeletedMessages.remove(Range.atMost(mdEntry.newPosition));
            } finally {
                lock.writeLock().unlock();
            }
            // 更新游标
            ledger.updateCursor(ManagedCursorImpl.this, mdEntry.newPosition);
            
            decrementPendingMarkDeleteCount();

            // 在触发 switchin-ledger 操作后才触发最终回调。 这将确保在下一个标记删除和切换操作之间不会发生竞态条件。
            if (mdEntry.callbackGroup != null) {
                // 触发组里每个请求的回调
                for (MarkDeleteEntry e : mdEntry.callbackGroup) {
                    e.callback.markDeleteComplete(e.ctx);
                }
            } else {
                // 仅触发当前请求的回调
                mdEntry.callback.markDeleteComplete(mdEntry.ctx);
            }
        }
        
        // 保存失败，执行一些标记删除失败操作。
        @Override
        public void operationFailed(ManagedLedgerException exception) {
            log.warn("[{}] Failed to mark delete position for cursor={} position={}", ledger.getName(),
                    ManagedCursorImpl.this, mdEntry.newPosition);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Consumer {} cursor mark delete failed with counters: consumed {} mdPos {} rdPos {}",
                        ledger.getName(), name, messagesConsumedCounter, markDeletePosition, readPosition);
            }

            decrementPendingMarkDeleteCount();

            if (mdEntry.callbackGroup != null) {
                for (MarkDeleteEntry e : mdEntry.callbackGroup) {
                    e.callback.markDeleteFailed(exception, e.ctx);
                }
            } else {
                mdEntry.callback.markDeleteFailed(exception, mdEntry.ctx);
            }
        }
    });
}

// 递减计数，当计数为0时，判定当前状态是否是正切换 ledger 状态，如果是，则创建新元数据 ledger
void decrementPendingMarkDeleteCount() {
    if (PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.decrementAndGet(this) == 0) {
        final State state = STATE_UPDATER.get(this);
        if (state == State.SwitchingLedger) {
            createNewMetadataLedger();
        }
    }
}

// 更新游标
void updateCursor(ManagedCursorImpl cursor, PositionImpl newPosition) {
    Pair<PositionImpl, PositionImpl> pair = cursors.cursorUpdated(cursor, newPosition);
    if (pair == null) {
        // 此时，游标已经被移除，则检查是否已有消费完的ledger，并且删除
        trimConsumedLedgersInBackground();
        return;
    }

    PositionImpl previousSlowestReader = pair.getLeft();
    PositionImpl currentSlowestReader = pair.getRight();
    
    // 最慢消费者没有变更，则直接返回
    if (previousSlowestReader.compareTo(currentSlowestReader) == 0) {
        return;
    }

    // 仅当 ledger 进行切换时，才进行垃圾回收 
    if (previousSlowestReader.getLedgerId() != newPosition.getLedgerId()) {
        trimConsumedLedgersInBackground();
    }
}

// 游标更新，并返回更新之前最慢消费者和更新之后最慢消费者
public Pair<PositionImpl, PositionImpl> cursorUpdated(ManagedCursor cursor, Position newPosition) {
    checkNotNull(cursor);

    long stamp = rwLock.writeLock();
    try {
        Item item = cursors.get(cursor.getName());
        if (item == null) {
            return null;
        }

        PositionImpl previousSlowestConsumer = heap.get(0).position;

        // 当游标向前移动时，需要将其推向树的底部，如果重置完成则将其向上推

        item.position = (PositionImpl) newPosition;
        if (item.idx == 0 || getParent(item).position.compareTo(item.position) <= 0) {
            siftDown(item);
        } else {
            siftUp(item);
        }

        PositionImpl newSlowestConsumer = heap.get(0).position;
        return Pair.of(previousSlowestConsumer, newSlowestConsumer);
    } finally {
        rwLock.unlockWrite(stamp);
    }
}

void startCreatingNewMetadataLedger() {
    // 尝试改变 ledger 状态，如果之前状态为正切换，则返回
    State oldState = STATE_UPDATER.getAndSet(this, State.SwitchingLedger);
    if (oldState == State.SwitchingLedger) {
         return;
    }

    // 如果正标记删除提交计数为0，则立即创建元数据 ledger
    if (PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.get(this) == 0) {
        createNewMetadataLedger();
    }
}

void createNewMetadataLedger() {
    createNewMetadataLedger(new VoidCallback() {
        @Override
        public void operationComplete() {
            // 当成功创建后，开始执行队列中的请求
            synchronized (pendingMarkDeleteOps) {
                flushPendingMarkDeletes();

                // 恢复正常状态
                STATE_UPDATER.set(ManagedCursorImpl.this, State.Open);
            }
        }

        @Override
        public void operationFailed(ManagedLedgerException exception) {
            log.error("[{}][{}] Metadata ledger creation failed", ledger.getName(), name, exception);

            synchronized (pendingMarkDeleteOps) {
                // 把等待队列的请求全部置为失败
                while (!pendingMarkDeleteOps.isEmpty()) {
                    MarkDeleteEntry entry = pendingMarkDeleteOps.poll();
                    entry.callback.markDeleteFailed(exception, entry.ctx);
                }

                // 创建失败，置为没有 ledger 状态
                STATE_UPDATER.set(ManagedCursorImpl.this, State.NoLedger);
            }
        }
    });
}

private void flushPendingMarkDeletes() {
    if (!pendingMarkDeleteOps.isEmpty()) {
        internalFlushPendingMarkDeletes();
    }
}

void internalFlushPendingMarkDeletes() {
    // 拉取最后一个标记删除操作，把剩下来的请求组成一个组
    MarkDeleteEntry lastEntry = pendingMarkDeleteOps.getLast();
    lastEntry.callbackGroup = Lists.newArrayList(pendingMarkDeleteOps);
    pendingMarkDeleteOps.clear();
    // 执行，前面已分析此方法
    internalMarkDelete(lastEntry);
}

// 异步删除
public void asyncDelete(Iterable<Position> positions, AsyncCallbacks.DeleteCallback callback, Object ctx) {
    if (isClosed()) {
        callback.deleteFailed(new ManagedLedgerException("Cursor was already closed"), ctx);
        return;
    }

    PositionImpl newMarkDeletePosition = null;

    lock.writeLock().lock();

    try {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Deleting individual messages at {}. Current status: {} - md-position: {}",
                    ledger.getName(), name, positions, individualDeletedMessages, markDeletePosition);
        }

        for (Position pos : positions) {
            PositionImpl position  = (PositionImpl) checkNotNull(pos);
            // position 不能大于 最后确认 position，否则直接返回
            if (((PositionImpl) ledger.getLastConfirmedEntry()).compareTo(position) < 0) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "[{}] Failed mark delete due to invalid markDelete {} is ahead of last-confirmed-entry {} for cursor [{}]",
                            ledger.getName(), position, ledger.getLastConfirmedEntry(), name);
                }
                callback.deleteFailed(new ManagedLedgerException("Invalid mark deleted position"), ctx);
                return;
            }
            // 如果已经包含在单个消息确认集合中或小于当前标记删除 position，则跳过
            if (individualDeletedMessages.contains(position) || position.compareTo(markDeletePosition) <= 0) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Position was already deleted {}", ledger.getName(), name, position);
                }
                continue;
            }

            // 取当前消息前一个消息，组成一个开闭集 (prev, pos]
            PositionImpl previousPosition = ledger.getPreviousPosition(position);
            // 把开闭集放入单个消息确认集合中
            individualDeletedMessages.add(Range.openClosed(previousPosition, position));
            // 增加已消费计数
            ++messagesConsumedCounter;

            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Individually deleted messages: {}", ledger.getName(), name,
                        individualDeletedMessages);
            }
        }
        // 如果单个消息确认集合为空（即无变更数据），则直接回调完成并返回
        if (individualDeletedMessages.isEmpty()) {
            callback.deleteComplete(ctx);
            return;
        }

        // 如果区间的下限是当前标记删除位置，那么我们可以触发新的标记删除到第一个区间段的上限（因为只有第一个区间删除消息是安全的）
        Range<PositionImpl> range = individualDeletedMessages.asRanges().iterator().next();

        // 如果区间的下限小于等于标记删除的 position 或者 标记删除的 position 与 区间的下限之间消息数为 0 
        if (range.lowerEndpoint().compareTo(markDeletePosition) <= 0 || ledger
                .getNumberOfEntries(Range.openClosed(markDeletePosition, range.lowerEndpoint())) <= 0) {

            if (log.isDebugEnabled()) {
                log.debug("[{}] Found a position range to mark delete for cursor {}: {} ", ledger.getName(),
                        name, range);
            }
            // 那么调整新的标记删除 position 为区间的上限
            newMarkDeletePosition = range.upperEndpoint();
        }
        // 重新设置
        if (newMarkDeletePosition != null) {
            newMarkDeletePosition = setAcknowledgedPosition(newMarkDeletePosition);
        } else {
            newMarkDeletePosition = markDeletePosition;
        }
    } catch (Exception e) {
        log.warn("[{}] [{}] Error while updating individualDeletedMessages [{}]", ledger.getName(), name,
                e.getMessage(), e);
        callback.deleteFailed(getManagedLedgerException(e), ctx);
        return;
    } finally {
        lock.writeLock().unlock();
    }

    // 应用标记删除限流
    if (markDeleteLimiter != null && !markDeleteLimiter.tryAcquire()) {
        lastMarkDeleteEntry = new MarkDeleteEntry(newMarkDeletePosition, Collections.emptyMap(), null, null);
        callback.deleteComplete(ctx);
        return;
    }

    try {
        // 异步调用标记删除
        internalAsyncMarkDelete(newMarkDeletePosition, Collections.emptyMap(), new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                callback.deleteComplete(ctx);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                callback.deleteFailed(exception, ctx);
            }

        }, ctx);

    } catch (Exception e) {
        log.warn("[{}] [{}] Error doing asyncDelete [{}]", ledger.getName(), name, e.getMessage(), e);
        if (log.isDebugEnabled()) {
            log.debug("[{}] Consumer {} cursor asyncDelete error, counters: consumed {} mdPos {} rdPos {}",
                    ledger.getName(), name, messagesConsumedCounter, markDeletePosition, readPosition);
        }
        callback.deleteFailed(new ManagedLedgerException(e), ctx);
    }
}

// ==================== 压缩持久化确认 ====================

// 跟压缩持久化订阅类似，压缩持久化
public void acknowledgeMessage(List<Position> positions, AckType ackType, Map<String,Long> properties) {
    // 必须累计确认
    checkArgument(ackType == AckType.Cumulative);
    // position 集合必须只能有一个
    checkArgument(positions.size() == 1);
    // 属性KEY必须包含 CompactedTopicLedger 标志
    checkArgument(properties.containsKey(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY));
    // 取出对应的压缩后的 ledger
    long compactedLedgerId = properties.get(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY);

    Position position = positions.get(0);

    if (log.isDebugEnabled()) {
        log.debug("[{}][{}] Cumulative ack on compactor subscription {}", topicName, subName, position);
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    // 异步确认
    cursor.asyncMarkDelete(position, properties, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Mark deleted messages until position on compactor subscription {}",
                              topicName, subName, position);
                }
                future.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Failed to mark delete for position on compactor subscription {}",
                              topicName, subName, ctx, exception);
                }
            }
        }, null);

    if (topic.getManagedLedger().isTerminated() && cursor.getNumberOfEntriesInBacklog() == 0) {
        dispatcher.getConsumers().forEach(Consumer::reachedEndOfTopic);
    }

    // 一旦属性被保存后，通知压缩 topic 用新的 ledger。
    future.thenAccept((v) -> compactedTopic.newCompactedLedger(position, compactedLedgerId));
}

public CompletableFuture<?> newCompactedLedger(Position p, long compactedLedgerId) {
    synchronized (this) {
        compactionHorizon = (PositionImpl)p;

        CompletableFuture<CompactedTopicContext> previousContext = compactedTopicContext;
        // 尝试打开 ledger
        compactedTopicContext = openCompactedLedger(bk, compactedLedgerId);

        // 一旦新的 ledger创建成功，则删除上下文中的老 ledger
        if (previousContext != null) {
            return compactedTopicContext.thenCompose((res) -> previousContext)
                .thenCompose((res) -> tryDeleteCompactedLedger(bk, res.ledger.getId()));
        } else {
            return compactedTopicContext;
        }
    }
}

private static CompletableFuture<CompactedTopicContext> openCompactedLedger(BookKeeper bk, long id) {
    CompletableFuture<LedgerHandle> promise = new CompletableFuture<>();
    bk.asyncOpenLedger(id,
                       Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                       Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD,
                       (rc, ledger, ctx) -> {
                           if (rc != BKException.Code.OK) {
                               promise.completeExceptionally(BKException.create(rc));
                           } else {
                               promise.complete(ledger);
                           }
                       }, null);
    return promise.thenApply((ledger) -> new CompactedTopicContext(
                                     ledger, createCache(ledger, DEFAULT_STARTPOINT_CACHE_SIZE)));
}

private static CompletableFuture<Void> tryDeleteCompactedLedger(BookKeeper bk, long id) {
    CompletableFuture<Void> promise = new CompletableFuture<>();
    bk.asyncDeleteLedger(id,
                         (rc, ctx) -> {
                             if (rc != BKException.Code.OK) {
                                 log.warn("Error deleting compacted topic ledger {}",
                                          id, BKException.create(rc));
                             } else {
                                 log.debug("Compacted topic ledger deleted successfully");
                             }
                             promise.complete(null); 
                         }, null);
    return promise;
}


// ==================== 非持久化确认 ====================

// 空实现
public void acknowledgeMessage(List<Position> position, AckType ackType, Map<String, Long> properties) {
    // No-op
}
```

消息确认是所有现代MQ或IM系统必备的功能，主要功能在于保证消费者在网络抖动、业务、应用异常、主机宕机情况下，
都能保证消费到，通知 broker 表示已成功消费，可以推送新消息。当然，在某些情况下，某条消息消费多次还是没有确认（即确认超时），
系统会自动把它放入死信 topic 中。目前流行的MQ，都只有类似于累积确认模式，并没有单个消息确认模式。

具体流程如下：

1. 累积确认
   1. 累积模式下，只能有一个确认消息
   2. 累积模式下，订阅类型不能用共享模式
   3. 持久化订阅时，普通的 Topic 订阅时，调用的 PersistentSubscription 类的 acknowledgeMessage 方法来进行消息确认：
   
      1. 游标状态判断
      2. 游标是否处于重置状态
      3. 判断当前消息确认 position 是否大于 ledger 对应的最近确认（分布式坏境应该叫提交）消息 position
      4. 根据当前消息确认 position，计算出标记删除 position 和读 position ，并返回新的标记删除 position
      5. 标记删除操作可能被限流
      6. 根据当前游标状态来操作： 1. 已关闭，则抛异常。 2. 无 ledger，则开始创建新的元数据 ledger。 3. 正切换 ledger，则把当前标记删除操作放入等待队列。4. 已打开，如果有正读操作，则放入等待队列，否则立即执行标记删除操作。
      7. 保存确认 position （包括单个消息确认集合和属性信息）到 cursorLedger，如果成功，则检查是否要关闭 cursorLedger，如果要关闭，则重新创建一个新的元数据 cursorLedger，并执行完成回调。如果失败，则尝试保存在元数据存储（zookkeeper中），此时如果成功，则执行完成回调，否则执行失败回调。
      8. 接步骤7，如果执行完成回调，则移除已确认消息之前所有的单个消息确认池中的 position，更新游标信息，尝试清理已消费完的 ledger，执行标记删除实例中的回调。如果执行失败回到，则执行标记删除实例中的回调为异常结果。
      9. 检查 ledger 是否已被删除或禁用以及游标积压队列积压数为0，此时通知所有的消费者 topic 已经达到末尾。

   4. 持久化订阅时，压缩 Topic 订阅，调用的 CompactorSubscription 类的 acknowledgeMessage 方法来进行消息确认：

      1. 检查确认中属性是否包含 `CompactedTopicLedger`,保存游标信息 ledger，此操作跟 PersistentSubscription 类的保存动作完全一致。
      2. 检查 ledger 是否已被删除或禁用以及游标积压队列积压数为0，此时通知所有的消费者 topic 已经达到末尾。
      3. 属性被保存后，尝试创建新的 ledger，并删除老的 ledger。
   5. 非持久化订阅时，无操作。

2. 单个消息确认

   1. 从请求参数中依次读取确认ID，如果订阅类型为共享模式，则尝试从当前消费者 pendingAcks 移除相关的确认 position，如果确认 position 没有出现当前消费者的 pendingAcks 中，则尝试从其他连接此订阅的消费者中删除（当客户端尝试通过同一订阅下的不同消费者确认消息时会发生这种情况）。如果删除成功，则判断当前消费者总未确认数是否小于等于最大未确认数的一半、未确认消息导致阻塞消费者为真以及是否应该阻塞当前消费者为真，如果为真，则设置消费者的阻塞标志为false，且发起流控操作。
   2. 持久化订阅时，普通的 Topic 订阅时，调用的 PersistentSubscription 类的 acknowledgeMessage 方法来进行消息确认：
      1. 异步执行游标删除操作，先确定新的标记删除 position ，然后执行标记删除限流操作，最后执行异步标记删除操作。这里跟累计确认异步标记删除过程一样。
      2. 分发器获取重发跟踪器，移除前面 position 集合。
   3. 非持久化订阅时，无操作。


从这里可以看出，累计确认和单个消息确认最大的区别在于，单个消息确认是创造性的支持交叉错位确认，其主要解决共享模式中多消费者的消息确认问题。



#### 6. handleRedeliverUnacknowledged 命令实现

```java


protected void handleRedeliverUnacknowledged(CommandRedeliverUnacknowledgedMessages redeliver) {
    checkArgument(state == State.Connected);
    if (log.isDebugEnabled()) {
        log.debug("[{}] Received Resend Command from consumer {} ", remoteAddress, redeliver.getConsumerId());
    }

    CompletableFuture<Consumer> consumerFuture = consumers.get(redeliver.getConsumerId());

    if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
        Consumer consumer = consumerFuture.getNow(null);
        if (redeliver.getMessageIdsCount() > 0 && consumer.subType() == SubType.Shared) {
            consumer.redeliverUnacknowledgedMessages(redeliver.getMessageIdsList());
        } else {
            consumer.redeliverUnacknowledgedMessages();
        }
    }
}

//  如果参数中消息ID总数大于0并且消费者子类型为共享类型，则执行如下：

public void redeliverUnacknowledgedMessages(List<MessageIdData> messageIds) {

    int totalRedeliveryMessages = 0;
    List<PositionImpl> pendingPositions = Lists.newArrayList();
    // 统计每个消息重发次数
    for (MessageIdData msg : messageIds) {
        PositionImpl position = PositionImpl.get(msg.getLedgerId(), msg.getEntryId());
        LongPair batchSize = pendingAcks.get(position.getLedgerId(), position.getEntryId());
        if (batchSize != null) {
            pendingAcks.remove(position.getLedgerId(), position.getEntryId());
            totalRedeliveryMessages += batchSize.first;
            pendingPositions.add(position);
        }
    }
    // 增加未确认消息数
    addAndGetUnAckedMsgs(this, -totalRedeliveryMessages);
    // 未确认消息阻塞消费者
    blockedConsumerOnUnackedMsgs = false;

    if (log.isDebugEnabled()) {
        log.debug("[{}-{}] consumer {} received {} msg-redelivery {}", topicName, subscription, consumerId,
                totalRedeliveryMessages, pendingPositions.size());
    }

    subscription.redeliverUnacknowledgedMessages(this, pendingPositions);
    // 记录多事件
    msgRedeliver.recordMultipleEvents(totalRedeliveryMessages, totalRedeliveryMessages);
    // 当消费者阻塞时，接收到的许可数，这里置为0（这里阻塞是因为子订阅为共享类型并且未确认消息数已达到最大未确认数时候，
    // 并不会立即推送消息给客户端，而是等符合条件才推送，具体参考 flowPermits 方法）
    int numberOfBlockedPermits = PERMITS_RECEIVED_WHILE_CONSUMER_BLOCKED_UPDATER.getAndSet(this, 0);

    // 如果许可计数大于0，则加上许可，并开始推送消息给客户端
    if (numberOfBlockedPermits > 0) {
        MESSAGE_PERMITS_UPDATER.getAndAdd(this, numberOfBlockedPermits);
        // 这里就是执行流控命令，上面章节详细分析
        subscription.consumerFlow(this, numberOfBlockedPermits);
    }
}

private int addAndGetUnAckedMsgs(Consumer consumer, int ackedMessages) {
    subscription.addUnAckedMessages(ackedMessages);
    return UNACKED_MESSAGES_UPDATER.addAndGet(consumer, ackedMessages);
}

public void addUnAckedMessages(int unAckMessages) {
    dispatcher.addUnAckedMessages(unAckMessages);
}

//============================= 持久化 - 未确认消息重发 ======

public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
    dispatcher.redeliverUnacknowledgedMessages(consumer, positions);
}

// 单活消费者处理

public void addUnAckedMessages(int unAckMessages) {
    // No-op
}

public void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
    // 无法保证单个消费者重发消息保持（原消息）顺序，这里是统计消息的重发次数
    positions.forEach(redeliveryTracker::incrementAndGetRedeliveryCount);
    redeliverUnacknowledgedMessages(consumer);
}

public void redeliverUnacknowledgedMessages(Consumer consumer) {
    topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName, SafeRun.safeRun(() -> {
        internalRedeliverUnacknowledgedMessages(consumer);
    }));
}

private synchronized void internalRedeliverUnacknowledgedMessages(Consumer consumer) {
    if (consumer != ACTIVE_CONSUMER_UPDATER.get(this)) {
        log.info("[{}-{}] Ignoring reDeliverUnAcknowledgedMessages: Only the active consumer can call resend",
                name, consumer);
        return;
    }

    if (readOnActiveConsumerTask != null) {
        log.info("[{}-{}] Ignoring reDeliverUnAcknowledgedMessages: consumer is waiting for cursor to be rewinded",
                name, consumer);
        return;
    }
    // 这里进行判断，如果正在读，但是游标已经取消正在读请求，则置为 false
    if (havePendingRead && cursor.cancelPendingReadRequest()) {
        havePendingRead = false;
    }
    // 如果没有正在读的请求，则发起读
    if (!havePendingRead) {
        cursor.rewind();
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Cursor rewinded, redelivering unacknowledged messages. ", name, consumer);
        }
        // 前面已解析，这里不再重复
        readMoreEntries(consumer);
    } else {
        log.info("[{}-{}] Ignoring reDeliverUnAcknowledgedMessages: cancelPendingRequest on cursor failed", name,
                consumer);
    }

}

// 多活消费者处理

// 增加未确认消息数
public void addUnAckedMessages(int numberOfMessages) {
    // 如果 最大未确认消息数（maxUnackedMessages） 等于 0，则直接返回
    if (maxUnackedMessages <= 0) {
        return;
    }
    // 当前未确认消息总数
    int unAckedMessages = TOTAL_UNACKED_MESSAGES_UPDATER.addAndGet(this, numberOfMessages);
    if (unAckedMessages >= maxUnackedMessages
            && BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, FALSE, TRUE)) {
        // 如果到达最大未确认消息限制，则阻塞分发器
        log.info("[{}] Dispatcher is blocked due to unackMessages {} reached to max {}", name,
                TOTAL_UNACKED_MESSAGES_UPDATER.get(this), maxUnackedMessages);
    } else if (topic.getBrokerService().isBrokerDispatchingBlocked()
            && blockedDispatcherOnUnackedMsgs == TRUE) {
        // 恢复分发器：如果分发器被阻塞是因为 broker 级别未确认消息数达到限制。如果已确认足够多的消息
        if (totalUnackedMessages < (topic.getBrokerService().maxUnackedMsgsPerDispatcher / 2)) {
            if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, TRUE, FALSE)) {
                // 这将分发器从阻塞列表移除，恢复分发器轮询读操作
                topic.getBrokerService().unblockDispatchersOnUnAckMessages(Lists.newArrayList(this));
            }
        }
    } else if (blockedDispatcherOnUnackedMsgs == TRUE && unAckedMessages < maxUnackedMessages / 2) {
        // 恢复（当前）分发器，如果已确认足够多的消息。此时开始读取消息
        if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, TRUE, FALSE)) {
            log.info("[{}] Dispatcher is unblocked", name);
            topic.getBrokerService().executor().execute(() -> readMoreEntries());
        }
    }
    // 增加 broker 级别总数
    topic.getBrokerService().addUnAckedMessages(this, numberOfMessages);
}

public void unblockDispatchersOnUnAckMessages(List<PersistentDispatcherMultipleConsumers> dispatcherList) {
    lock.writeLock().lock();
    try {
        // 把当前的分发器恢复，并开始读取消息，并从阻塞的分发器列表移除
        dispatcherList.forEach(dispatcher -> {
            dispatcher.unBlockDispatcherOnUnackedMsgs();
            executor().execute(() -> dispatcher.readMoreEntries());
            log.info("[{}] Dispatcher is unblocked", dispatcher.getName());
            blockedDispatchers.remove(dispatcher);
        });
    } finally {
        lock.writeLock().unlock();
    }
}

public void addUnAckedMessages(PersistentDispatcherMultipleConsumers dispatcher, int numberOfMessages) {
    // 如果 最大未确认消息数（maxUnackedMessages） 等于 0，则直接返回
    if (maxUnackedMessages > 0) {
        totalUnackedMessages.add(numberOfMessages);

        // 阻塞分发器：如果 broker 已经被阻塞和当 broker 被阻塞时，分发器达到最大分发限制
        if (blockedDispatcherOnHighUnackedMsgs.get() && !dispatcher.isBlockedDispatcherOnUnackedMsgs()
                && dispatcher.getTotalUnackedMessages() > maxUnackedMsgsPerDispatcher) {
            lock.readLock().lock();
            try {
                log.info("[{}] dispatcher reached to max unack msg limit on blocked-broker {}",
                        dispatcher.getName(), dispatcher.getTotalUnackedMessages());
                // 因为未确认消息阻塞分发器
                dispatcher.blockDispatcherOnUnackedMsgs();
                // 加入阻塞列表
                blockedDispatchers.add(dispatcher);
            } finally {
                lock.readLock().unlock();
            }
        }
    }
}

public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
    positions.forEach(position -> {
        // 记录需要重放的消息
        messagesToReplay.add(position.getLedgerId(), position.getEntryId());
        // 记录消息重放次数
        redeliveryTracker.incrementAndGetRedeliveryCount(position);
    });
    if (log.isDebugEnabled()) {
        log.debug("[{}-{}] Redelivering unacknowledged messages for consumer {}", name, consumer, positions);
    }
    // 读更多消息
    readMoreEntries();
}

//  非持久化实现为空
public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
    // No-op
}

// 这里为消费者没有指定消息ID和子类型不为共享类型（目前即为独占或故障转移类型）
// =========================================

public void redeliverUnacknowledgedMessages() {
    // 清除未确认消息桶，再一次重发未确认消息
    clearUnAckedMsgs(this);
    blockedConsumerOnUnackedMsgs = false;
    if (log.isDebugEnabled()) {
        log.debug("[{}-{}] consumer {} received redelivery", topicName, subscription, consumerId);
    }
    // 重发未确认的消息
    subscription.redeliverUnacknowledgedMessages(this);
    flowConsumerBlockedPermits(this);
    // 统计总重发次数，并记录事件，最后清空正确认队列
    if (pendingAcks != null) {
        AtomicInteger totalRedeliveryMessages = new AtomicInteger(0);
        pendingAcks.forEach(
                (ledgerId, entryId, batchSize, none) -> totalRedeliveryMessages.addAndGet((int) batchSize));
        msgRedeliver.recordMultipleEvents(totalRedeliveryMessages.get(), totalRedeliveryMessages.get());
        pendingAcks.clear();
    }
}

// 话说这个分支不是共享类型分支，这个方法有作用？
void flowConsumerBlockedPermits(Consumer consumer) {
    // 当消费者达到阻塞时，接收到的许可数，这里置为0
    int additionalNumberOfPermits = PERMITS_RECEIVED_WHILE_CONSUMER_BLOCKED_UPDATER.getAndSet(consumer, 0);
    // 增加新的流控许可数到实际的消费者消息许可
    MESSAGE_PERMITS_UPDATER.getAndAdd(consumer, additionalNumberOfPermits);
    // 分发正处理许可到流控更多消息：它将新增更多的许可到分发器和消费者
    subscription.consumerFlow(consumer, additionalNumberOfPermits);
}

private void clearUnAckedMsgs(Consumer consumer) {
    int unaAckedMsgs = UNACKED_MESSAGES_UPDATER.getAndSet(this, 0);
    subscription.addUnAckedMessages(-unaAckedMsgs);
}

public synchronized void redeliverUnacknowledgedMessages(Consumer consumer) {
    dispatcher.redeliverUnacknowledgedMessages(consumer);
}

// 多消费者实现

public synchronized void redeliverUnacknowledgedMessages(Consumer consumer) {
    // 这里注意与接口有消息ID集合的区别
    consumer.getPendingAcks().forEach((ledgerId, entryId, batchSize, none) -> {
        messagesToReplay.add(ledgerId, entryId);
    });
    if (log.isDebugEnabled()) {
        log.debug("[{}-{}] Redelivering unacknowledged messages for consumer {}", name, consumer, messagesToReplay);
    }
    readMoreEntries();
}

// 单消费者实现
public void redeliverUnacknowledgedMessages(Consumer consumer) {
    topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topicName, SafeRun.safeRun(() -> {
        internalRedeliverUnacknowledgedMessages(consumer);
    }));
}

private synchronized void internalRedeliverUnacknowledgedMessages(Consumer consumer) {
    if (consumer != ACTIVE_CONSUMER_UPDATER.get(this)) {
        log.info("[{}-{}] Ignoring reDeliverUnAcknowledgedMessages: Only the active consumer can call resend",
                name, consumer);
        return;
    }

    if (readOnActiveConsumerTask != null) {
        log.info("[{}-{}] Ignoring reDeliverUnAcknowledgedMessages: consumer is waiting for cursor to be rewinded",
                name, consumer);
        return;
    }

    if (havePendingRead && cursor.cancelPendingReadRequest()) {
        havePendingRead = false;
    }

    if (!havePendingRead) {
        cursor.rewind();
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Cursor rewinded, redelivering unacknowledged messages. ", name, consumer);
        }
        readMoreEntries(consumer);
    } else {
        log.info("[{}-{}] Ignoring reDeliverUnAcknowledgedMessages: cancelPendingRequest on cursor failed", name,
                consumer);
    }

}


```

对于未确认消息重发，它的主要作用在于“提醒”消费客户端对消息消费后进行确认，当消息达到一定的重发次数，
消费客户端会把消息投递到对应的死信 Topic 中，另外为了支持共享消费特性，还单独进行了处理，即消费确认
可以不连续（但最终会连续）。

1. 如果订阅类型为共享类型，并且消息数大于0：

   1.  

2. 否则：



#### 7. handleUnsubscribe 命令实现

```java

protected void handleUnsubscribe(CommandUnsubscribe unsubscribe) {
    checkArgument(state == State.Connected);

    CompletableFuture<Consumer> consumerFuture = consumers.get(unsubscribe.getConsumerId());

    if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
        consumerFuture.getNow(null).doUnsubscribe(unsubscribe.getRequestId());
    } else {
        ctx.writeAndFlush(
                Commands.newError(unsubscribe.getRequestId(), ServerError.MetadataError, "Consumer not found"));
    }
}

void doUnsubscribe(final long requestId) {
    final ChannelHandlerContext ctx = cnx.ctx();
    // 反订阅
    subscription.doUnsubscribe(this).thenAccept(v -> {
        // 执行成功的话，则从连接池移除消费者，并发成功消息给消费者
        log.info("Unsubscribed successfully from {}", subscription);
        cnx.removedConsumer(this);
        ctx.writeAndFlush(Commands.newSuccess(requestId));
    }).exceptionally(exception -> {
        log.warn("Unsubscribe failed for {}", subscription, exception);
        ctx.writeAndFlush(
                Commands.newError(requestId, BrokerServiceException.getClientErrorCode(exception.getCause()),
                        exception.getCause().getMessage()));
        return null;
    });
}

public void close() throws BrokerServiceException {
    subscription.removeConsumer(this);
    cnx.removedConsumer(this);
}

public void removedConsumer(Consumer consumer) {
    if (log.isDebugEnabled()) {
        log.debug("[{}] Removed consumer: {}", remoteAddress, consumer);
    }

    consumers.remove(consumer.consumerId());
}

// PersistentSubscription 类移除消费者
public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
    cursor.updateLastActive();
    if (dispatcher != null) {
        dispatcher.removeConsumer(consumer);
    }
    if (dispatcher.getConsumers().isEmpty()) {
        deactivateCursor();

        if (!cursor.isDurable()) {
            close();
            topic.getBrokerService().pulsar().getExecutor().submit(() ->{
                topic.removeSubscription(subName);
            });
        }
    }

    PersistentTopic.USAGE_COUNT_UPDATER.decrementAndGet(topic);
    if (log.isDebugEnabled()) {
        log.debug("[{}] [{}] [{}] Removed consumer -- count: {}", topic.getName(), subName, consumer.consumerName(),
                PersistentTopic.USAGE_COUNT_UPDATER.get(topic));
    }
}

// ======================== 持久化订阅 =======================

public CompletableFuture<Void> doUnsubscribe(Consumer consumer) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
        // 检查是否能反订阅
        if (dispatcher.canUnsubscribe(consumer)) {
            // broker 端消费者关闭
            consumer.close();
            return delete();
        }
        future.completeExceptionally(
                new ServerMetadataException("Unconnected or shared consumer attempting to unsubscribe"));
    } catch (BrokerServiceException e) {
        log.warn("Error removing consumer {}", consumer);
        future.completeExceptionally(e);
    }
    return future;
}

// 单活消费者
public synchronized boolean canUnsubscribe(Consumer consumer) {
    // 只有一个消费者，并且当前消费者与活的消费者是同一个
    return (consumers.size() == 1) && Objects.equals(consumer, ACTIVE_CONSUMER_UPDATER.get(this));
}

// AbstractDispatcherSingleActiveConsumer 移除消费者
public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
    log.info("Removing consumer {}", consumer);
    // 如果没有移除成功，则抛异常
    if (!consumers.remove(consumer)) {
        throw new ServerMetadataException("Consumer was not connected");
    }
    // 如果当前消费者为空，则置活动消费者为null
    if (consumers.isEmpty()) {
        ACTIVE_CONSUMER_UPDATER.set(this, null);
    }
    // 不是关闭以及消费者不为空，则尝试切换活动消费者
    if (closeFuture == null && !consumers.isEmpty()) {
        pickAndScheduleActiveConsumer();
        return;
    }
    //取消正读操作 
    cancelPendingRead();
    if (consumers.isEmpty() && closeFuture != null && !closeFuture.isDone()) {
        // 只有在创建closeFuture时，控制才会到达此处，意味着没有更多的已连接消费者了。
        closeFuture.complete(null);
    }
}

// 多活消费者
public synchronized boolean canUnsubscribe(Consumer consumer) {
    // 只有一个消费者，并且当前消费者在消费者集合中
    return consumerList.size() == 1 && consumerSet.contains(consumer);
}

// PersistentDispatcherMultipleConsumers 类移除消费者
public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
    // 从移除的消费者中减去未确认消息数
    addUnAckedMessages(-consumer.getUnackedMessages());
    if (consumerSet.removeAll(consumer) == 1) {
        consumerList.remove(consumer);
        log.info("Removed consumer {} with pending {} acks", consumer, consumer.getPendingAcks().size());
        // 已经无消费者
        if (consumerList.isEmpty()) {
            // 停止正常读的请求
            if (havePendingRead && cursor.cancelPendingReadRequest()) {
                havePendingRead = false;
            }
            // 清空消息回放池
            messagesToReplay.clear();
            if (closeFuture != null) {
                log.info("[{}] All consumers removed. Subscription is disconnected", name);
                closeFuture.complete(null);
            }
            totalAvailablePermits = 0;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Consumer are left, reading more entries", name);
            }
            // 把消费者未确认消息放进回放池
            consumer.getPendingAcks().forEach((ledgerId, entryId, batchSize, none) -> {
                messagesToReplay.add(ledgerId, entryId);
            });
            // 减去消费者可用许可
            totalAvailablePermits -= consumer.getAvailablePermits();
            readMoreEntries();
        }
    } else {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Trying to remove a non-connected consumer: {}", name, consumer);
        }
    }
}

// 移除消费者
public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
    // 更新游标最后访问时间
    cursor.updateLastActive();
    if (dispatcher != null) {
        dispatcher.removeConsumer(consumer);
    }
    if (dispatcher.getConsumers().isEmpty()) {
        // 如果分发器的消费者为空，则游标为非活动状态
        deactivateCursor();

        if (!cursor.isDurable()) {
            // 如果游标是非持久性的，也需要清理订阅信息，这里设置游标状态为已关闭
            close();
            // 当 topic 关闭时，并发订阅Map会在迭代过程中关闭每个订阅，又移除 topic 时，会试着访问相同的 map 这可能导致死锁，所以在不同的线程执行它
            topic.getBrokerService().pulsar().getExecutor().submit(() ->{
                topic.removeSubscription(subName);
            });
        }
    }
    // 使用计数递减
    PersistentTopic.USAGE_COUNT_UPDATER.decrementAndGet(topic);
    if (log.isDebugEnabled()) {
        log.debug("[{}] [{}] [{}] Removed consumer -- count: {}", topic.getName(), subName, consumer.consumerName(),
                PersistentTopic.USAGE_COUNT_UPDATER.get(topic));
    }
}

public CompletableFuture<Void> delete() {
    CompletableFuture<Void> deleteFuture = new CompletableFuture<>();

    log.info("[{}][{}] Unsubscribing", topicName, subName);

    // 游标关闭并取消确认操作
    this.close().thenCompose(v -> topic.unsubscribe(subName)).thenAccept(v -> deleteFuture.complete(null))
            .exceptionally(exception -> {
                IS_FENCED_UPDATER.set(this, FALSE);
                log.error("[{}][{}] Error deleting subscription", topicName, subName, exception);
                deleteFuture.completeExceptionally(exception);
                return null;
            });

    return deleteFuture;
}

// 尝试关闭
public CompletableFuture<Void> close() {
    synchronized (this) {
        if (dispatcher != null && dispatcher.isConsumerConnected()) {
            return FutureUtil.failedFuture(new SubscriptionBusyException("Subscription has active consumers"));
        }
        IS_FENCED_UPDATER.set(this, TRUE);
        log.info("[{}][{}] Successfully closed subscription [{}]", topicName, subName, cursor);
    }

    return CompletableFuture.completedFuture(null);
}

// 反订阅，从元数据存储库中删除游标信息
public CompletableFuture<Void> unsubscribe(String subscriptionName) {
    CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();

    ledger.asyncDeleteCursor(Codec.encode(subscriptionName), new DeleteCursorCallback() {
        @Override
        public void deleteCursorComplete(Object ctx) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Cursor deleted successfully", topic, subscriptionName);
            }
            // 删除成功
            subscriptions.remove(subscriptionName);
            unsubscribeFuture.complete(null);
            lastActive = System.nanoTime();
        }

        @Override
        public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Error deleting cursor for subscription", topic, subscriptionName, exception);
            }
            unsubscribeFuture.completeExceptionally(new PersistenceException(exception));
        }
    }, null);

    return unsubscribeFuture;
}

// ======================== 非持久化订阅 =====================

public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
    if (dispatcher != null) {
        dispatcher.removeConsumer(consumer);
    }

    // invalid consumer remove will throw an exception
    // decrement usage is triggered only for valid consumer close
    NonPersistentTopic.USAGE_COUNT_UPDATER.decrementAndGet(topic);
    if (log.isDebugEnabled()) {
        log.debug("[{}] [{}] [{}] Removed consumer -- count: {}", topic.getName(), subName, consumer.consumerName(),
                NonPersistentTopic.USAGE_COUNT_UPDATER.get(topic));
    }
}

public CompletableFuture<Void> doUnsubscribe(Consumer consumer) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
        // 这里判定是否当前消费者是最后一个，如果是的话，关闭消费者，删除
        if (dispatcher.canUnsubscribe(consumer)) {
            consumer.close();
            return delete();
        }
        future.completeExceptionally(
                new ServerMetadataException("Unconnected or shared consumer attempting to unsubscribe"));
    } catch (BrokerServiceException e) {
        log.warn("Error removing consumer {}", consumer);
        future.completeExceptionally(e);
    }
    return future;
}

// 关闭消费者两种情况：1.连接已经被关闭 2.连接是打开的（和平关闭）但是无消息确认
public void close() throws BrokerServiceException {
    // 从订阅中移除当前消费者
    subscription.removeConsumer(this);
    cnx.removedConsumer(this);
}

// 从订阅中移除消费者
public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
    if (dispatcher != null) {
        dispatcher.removeConsumer(consumer);
    }

    // 递减使用计数
    NonPersistentTopic.USAGE_COUNT_UPDATER.decrementAndGet(topic);
    if (log.isDebugEnabled()) {
        log.debug("[{}] [{}] [{}] Removed consumer -- count: {}", topic.getName(), subName, consumer.consumerName(),
                NonPersistentTopic.USAGE_COUNT_UPDATER.get(topic));
    }
}

// 多活消费者 NonPersistentDispatcherMultipleConsumers 类
public synchronized boolean canUnsubscribe(Consumer consumer) {
    return (consumers.size() == 1) && Objects.equals(consumer, ACTIVE_CONSUMER_UPDATER.get(this));
}

public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
    if (consumerSet.removeAll(consumer) == 1) {
        consumerList.remove(consumer);
        log.info("Removed consumer {}", consumer);
        // 当前订阅中消费者已经没有，订阅也该关闭
        if (consumerList.isEmpty()) {
            if (closeFuture != null) {
                log.info("[{}] All consumers removed. Subscription is disconnected", name);
                closeFuture.complete(null);
            }
            // 总可用许可数归0
            TOTAL_AVAILABLE_PERMITS_UPDATER.set(this, 0);
        }
    } else {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Trying to remove a non-connected consumer: {}", name, consumer);
        }
        // 尝试移除一个未连接的消费者，并且减少此消费者的可用许可
        TOTAL_AVAILABLE_PERMITS_UPDATER.addAndGet(this, -consumer.getAvailablePermits());
    }
}

// 单活消费者 AbstractDispatcherSingleActiveConsumer 类
public synchronized boolean canUnsubscribe(Consumer consumer) {
    return consumerList.size() == 1 && consumerSet.contains(consumer);
}
// 这里持久化订阅一样 也是 AbstractDispatcherSingleActiveConsumer 类
public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
    log.info("Removing consumer {}", consumer);
    if (!consumers.remove(consumer)) {
        throw new ServerMetadataException("Consumer was not connected");
    }

    if (consumers.isEmpty()) {
        ACTIVE_CONSUMER_UPDATER.set(this, null);
    }

    if (closeFuture == null && !consumers.isEmpty()) {
        pickAndScheduleActiveConsumer();
        return;
    }

    cancelPendingRead();

    if (consumers.isEmpty() && closeFuture != null && !closeFuture.isDone()) {
        closeFuture.complete(null);
    }
}

```

消费者取消订阅，意味着 broker 不再推送消息给消费者。

1. 持久化订阅取消

   1. 如果是单活消费者分发器判断是否能取消订阅：当前消费者为存活消费者，且消费者集合中只有一个消费者。多活消费者时判断：消费者集合包含当前消费者，且只有一个消费者。如果不能取消订阅，则抛异常。
   2. 从分发器中移除消费者（这里是订阅转发）：
      1. 如果是单活消费者： 
          1. 如果消费者集合为空，则置活动消费者为null。如果消费者集合不为空且不处于关闭消费者状态，则根据分区索引来切换活动消费者。
          2. 果新选出的消费者不等于当前消费者，则取消当前读操作。如果订阅类型不是故障转移或者活消费者故障转移延迟时间小于等于0，则回退游标，通知消费者变更，并且开始使用新的消费者读取消息，返回。
          3. 如果订阅类型为故障转移，则开启一个定时任务：回退游标，通知消费者变更，并且开始使用新的消费者读取消息。
          4. 如果消费者集合已经为空，则取消当前读请求。如果消费者为空，关闭future不为空且还没完成，则置关闭futrue为完成。
      2. 如果是多活消费者： 
          1. 根据当前移除消费者，减少消费者未确认消息数
          2. 如果从消费者集合中移除成功之后，如果消费者集合已为空，则取消读请求，清空消息回放池，设置关闭future为已完成，总可用许可置为0，否则把移除消费者未确认的消息全部放入消息回放池，并且减去移除消费者的可以许可数，继续读取更多消息。
   6. 如果此时分发器中消费者已经为空了， 则把当前游标置为非活动状态，递减持久化 Topic 使用计数。
   7. 如果分发器不为空并且分发器还有消费者连接，则返回异常。否则置订阅为已关闭。
   8. 尝试从元数据管理器删除游标信息，如果成功，重试3次，异步删除 ledger 保存的游标信息，从游标集合移除游标信息，并使消息缓存部分失效，后台执行已消费完的 ledger 清理，这里后标记执行成功。否则标记删除游标失败。
   9. 此时从订阅集合中移除此订阅，标记取消订阅成功。否则，取消订阅失败。
   10. 如果以上执行成功，从连接池移除消费者，并发送成功命令给消费者，否则把执行错误发给消费者。

2. 非持久化订阅取消

    1. 如果是单活消费者分发器判断是否能取消订阅：当前消费者为存活消费者，且消费者集合中只有一个消费者。多活消费者时判断：消费者集合包含当前消费者，且只有一个消费者，与持久化订阅一致。
    2. 从分发器中移除消费者（这里是订阅转发）：
       1. 如果是单活消费者：
          1. 如果消费者集合为空，则置活动消费者为null。如果消费者集合不为空且不处于关闭消费者状态，则根据分区索引来切换活动消费者。
          2. 切换活动消费者为空实现。
       2. 如果是多消费者：
          1. 如果移除消费者成功，则再次判定消费者集合是否为空，如果是，则置关闭futrue为完成，总可用许可置为0。否则，总可用许可减去移除消费者的可用许可。
    3. 递减非持久化 Topic 引用计数。
    4. 从连接池移除当前消费者。
    5. 置订阅为已关闭，执行完成后，置取消订阅为完成。
    6. 如果以上执行成功，从连接池移除消费者，并发送成功命令给消费者，否则把执行错误发给消费者。

#### 8. handleCloseConsumer 命令实现

```java

protected void handleCloseConsumer(CommandCloseConsumer closeConsumer) {
    checkArgument(state == State.Connected);
    log.info("[{}] Closing consumer: {}", remoteAddress, closeConsumer.getConsumerId());

    long requestId = closeConsumer.getRequestId();
    long consumerId = closeConsumer.getConsumerId();

    CompletableFuture<Consumer> consumerFuture = consumers.get(consumerId);
    if (consumerFuture == null) {
        log.warn("[{}] Consumer was not registered on the connection: {}", consumerId, remoteAddress);
        ctx.writeAndFlush(Commands.newError(requestId, ServerError.MetadataError, "Consumer not found"));
        return;
    }

    if (!consumerFuture.isDone() && consumerFuture
            .completeExceptionally(new IllegalStateException("Closed consumer before creation was complete"))) {
        // 在消费者实际完成之前已经收到了关闭消费者的请求，现在将消费者futrue标记为失败，可以告诉客户端关闭操作成功。 当实际创建操作完成时，新的消费者将被丢弃。
        log.info("[{}] Closed consumer {} before its creation was completed", remoteAddress, consumerId);
        ctx.writeAndFlush(Commands.newSuccess(requestId));
        return;
    }
    // 消费者本身创建失败，然后就不管，直接应答关闭成功
    if (consumerFuture.isCompletedExceptionally()) {
        log.info("[{}] Closed consumer {} that already failed to be created", remoteAddress, consumerId);
        ctx.writeAndFlush(Commands.newSuccess(requestId));
        return;
    }

    // 正常消费者关闭处理
    Consumer consumer = consumerFuture.getNow(null);
    try {
        consumer.close();
        consumers.remove(consumerId, consumerFuture);
        ctx.writeAndFlush(Commands.newSuccess(requestId));
        log.info("[{}] Closed consumer {}", remoteAddress, consumer);
    } catch (BrokerServiceException e) {
        log.warn("[{]] Error closing consumer: ", remoteAddress, consumer, e);
        ctx.writeAndFlush(
                Commands.newError(requestId, BrokerServiceException.getClientErrorCode(e), e.getMessage()));
    }
}
// 这里取消订阅逻辑中已经分析过了。
public void close() throws BrokerServiceException {
    subscription.removeConsumer(this);
    cnx.removedConsumer(this);
}


```

从这里可以看出，消费者关闭，有很大的一部分跟取消订阅类似，取消订阅多了一个删除游标动作，而关闭消费者则没有。

### 6. Broker admin 接口实现

## 4. Pulsar Proxy 解析

## 5. Pulsar Discovery 解析

## 6. Pulsar Functions 解析

## 7. Pulsar IO 解析