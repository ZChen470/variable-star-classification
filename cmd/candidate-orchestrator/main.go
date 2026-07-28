package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	kafkaadapter "github.com/ZChen470/variable-star-classification/internal/adapter/kafka"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if err := run(); err != nil {
		log.Printf("candidate orchestrator failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	// 加载配置，传入 os.LookupEnv 函数，将环境变量读取逻辑注入配置加载器
	config, err := loadCandidateOrchestratorConfig(
		os.LookupEnv,
	)
	if err != nil {
		return fmt.Errorf(
			"load candidate orchestrator config: %w",
			err,
		)
	}
	// 创建 Kafka 客户端
	client, err := kgo.NewClient(
		kgo.SeedBrokers(config.kafkaBrokers...),      // Kafka 集群地址
		kgo.ClientID(config.kafkaClientID),           // 客户端标识，便于监控和排查
		kgo.ConsumerGroup(config.kafkaConsumerGroup), // 消费者组，实现负载均衡
		kgo.ConsumeTopics(config.candidateTopic),     // 订阅的主题
		kgo.DisableAutoCommit(),                      // 手动提交 Offset，确保消息处理成功后再确认
		kgo.BlockRebalanceOnPoll(),                   // 在消息处理期间阻止分区再平衡，避免重复消费
	)
	if err != nil {
		return fmt.Errorf("create Kafka client: %w", err)
	}
	defer client.CloseAllowingRebalance()

	// 初始化业务组件
	policy, err := application.NewClassificationPolicyV1(
		config.modelBundleVersion,
		config.classificationPolicyVersion,
	)
	if err != nil {
		return fmt.Errorf(
			"create classification policy: %w",
			err,
		)
	}

	publisher := kafkaadapter.NewPublisher(client)

	handler, err := application.NewCandidateHandler(
		config.candidateTopic,
		config.classificationCommandTopic,
		config.candidateDLQTopic,
		policy,
		publisher,
	)
	if err != nil {
		return fmt.Errorf("create candidate handler: %w", err)
	}

	// 创建消费者运行器
	runner, err := kafkaadapter.NewConsumerRunner(
		client,
		handler,
	)
	if err != nil {
		return fmt.Errorf("create Kafka consumer runner: %w", err)
	}

	// 优雅关闭
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	log.Printf(
		"candidate orchestrator started: client_id=%q consumer_group=%q candidate_topic=%q command_topic=%q candidate_dlq_topic=%q",
		config.kafkaClientID,
		config.kafkaConsumerGroup,
		config.candidateTopic,
		config.classificationCommandTopic,
		config.candidateDLQTopic,
	)

	// 启动服务
	if err := runner.Run(ctx); err != nil {
		return fmt.Errorf("run candidate orchestrator: %w", err)
	}

	log.Printf("candidate orchestrator stopped")
	return nil
}
