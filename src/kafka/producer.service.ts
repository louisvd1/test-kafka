import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
  Logger,
  ConflictException,
} from '@nestjs/common';
import {
  Kafka,
  Producer,
  ProducerRecord,
  KafkaMessage,
  logLevel,
  Admin,
} from 'kafkajs';
import { CreateTopicDto } from 'src/utils/dto/create-topic.dto';

@Injectable()
export class ProducerService implements OnModuleInit, OnApplicationShutdown {
  private readonly kafka = new Kafka({
    clientId: 'nestjs-producer',
    brokers: ['localhost:9092'],
    logLevel: logLevel.ERROR,
  });

  private readonly producer: Producer = this.kafka.producer();
  private readonly admin: Admin = this.kafka.admin();
  private readonly logger = new Logger(ProducerService.name);

  async onModuleInit() {
    await this.producer.connect();
    await this.admin.connect();
    this.logger.log('Kafka producer and admin connected');
  }

  async onApplicationShutdown() {
    try {
      await this.producer.disconnect();
      await this.admin.disconnect();
      this.logger.log('Kafka producer and admin disconnected');
    } catch (error) {
      this.logger.error('Error during shutdown: ' + error.message);
    }
  }

  /**
   * Send a single message to a topic
   */
  async sendMessage(topic: string, value: any, key?: string) {
    try {
      await this.producer.send({
        topic,
        messages: [
          {
            key,
            value: JSON.stringify(value),
          },
        ],
      });
      this.logger.log(`Produced message to ${topic}: ${JSON.stringify(value)}`);
    } catch (error) {
      this.logger.error(`Failed to produce message: ${error.message}`);
    }
  }

  /**
   * Send batch of messages
   */
  async sendBatchMessages(topic: string, messages: any[]) {
    const kafkaMessages = messages.map((msg) => ({
      value: JSON.stringify(msg),
    }));

    try {
      await this.producer.send({
        topic,
        messages: kafkaMessages,
      });
      this.logger.log(`Produced ${messages.length} messages to ${topic}`);
    } catch (error) {
      this.logger.error(`Batch send failed: ${error.message}`);
    }
  }

  /**
   * Retry send with custom attempts
   */
  async sendWithRetry(
    topic: string,
    value: any,
    key: string = '',
    retries = 3,
  ): Promise<boolean> {
    for (let i = 1; i <= retries; i++) {
      try {
        await this.sendMessage(topic, value, key);
        return true;
      } catch (e) {
        this.logger.warn(`Retry ${i}/${retries} failed: ${e.message}`);
        await new Promise((res) => setTimeout(res, 500 * i)); // delay before retry
      }
    }
    return false;
  }

  /**
   * Schedule message with delay (not real Kafka delay, use setTimeout)
   */
  async sendWithDelay(topic: string, value: any, delayMs: number) {
    setTimeout(() => {
      this.sendMessage(topic, value);
    }, delayMs);
  }

  /**
   * Ensure topic exists (create if missing)
   */
  async ensureTopic(topic: string, numPartitions = 1) {
    const topics = await this.admin.listTopics();
    console.log('topics', topics);
    if (!topics.includes(topic)) {
      await this.admin.createTopics({
        topics: [{ topic, numPartitions }], // create topic if missing
      });
      this.logger.log(`Created topic: ${topic}`);
    }
  }

  // Refactor produceMessage to not reconnect every time
  async produceMessage(data: any) {
    try {
      await this.producer.send({
        topic: 'test-topic',
        messages: [{ value: JSON.stringify(data) }],
      });
      this.logger.log(`Produced message: ${JSON.stringify(data)}`);
    } catch (error) {
      this.logger.error(`Failed to produce message: ${error.message}`);
    }
  }

  async produce(record: ProducerRecord) {
    try {
      await this.producer.send(record);
      this.logger.log('Produced record: ' + JSON.stringify(record));
    } catch (error) {
      this.logger.error(`Failed to produce record: ${error.message}`);
    }
  }

  async createTopicIfNotExists(
    topic: string,
    numPartitions = 1,
    replicationFactor = 1,
  ): Promise<{ message: string; topic: string }> {
    try {
      const existingTopics = await this.admin.listTopics();

      if (existingTopics.includes(topic)) {
        this.logger.warn(`Topic '${topic}' already exists`);
        return { message: `Topic '${topic}' already exists`, topic };
      }

      const created = await this.admin.createTopics({
        topics: [
          {
            topic,
            numPartitions,
            replicationFactor,
            configEntries: [
              { name: 'cleanup.policy', value: 'delete' },
              { name: 'retention.ms', value: '86400000' }, // 1 day
            ],
          },
        ],
        waitForLeaders: true,
      });

      if (created) {
        this.logger.log(
          `Created topic '${topic}' with ${numPartitions} partitions`,
        );
        return { message: `Topic '${topic}' created successfully`, topic };
      } else {
        this.logger.error(`Failed to create topic '${topic}'`);
        return { message: `Failed to create topic '${topic}'`, topic };
      }
    } catch (err) {
      this.logger.error(`Error creating topic: ${err.message}`);
      throw err;
    }
  }

  async createAdvancedTopic(dto: CreateTopicDto) {
    const exists = (await this.admin.listTopics()).includes(dto.topic);
    if (exists) {
      throw new ConflictException(`Topic ${dto.topic} already exists`);
    }

    await this.admin.createTopics({
      topics: [
        {
          topic: dto.topic,
          numPartitions: dto.partitions,
          replicationFactor: dto.replication,
          configEntries: dto.configs || [],
        },
      ],
    });

    this.logger.log(`Created topic ${dto.topic}`);
    return { message: `Created topic ${dto.topic}` };
  }

  async listTopicsDetails() {
    const topics = await this.admin.listTopics();
    const metadata = await this.admin.fetchTopicMetadata({ topics });

    return metadata.topics.map((topic) => ({
      name: topic.name,
      partitions: topic.partitions.length,
      partitionInfo: topic.partitions,
    }));
  }

  async deleteTopic(topic: string) {
    await this.admin.deleteTopics({ topics: [topic] });
    this.logger.warn(`Deleted topic: ${topic}`);
    return { message: `Deleted topic: ${topic}` };
  }

  async sendMessageWithHeaders(
    topic: string,
    value: any,
    headers: Record<string, string>,
  ) {
    await this.producer.send({
      topic,
      messages: [
        {
          value: JSON.stringify(value),
          headers,
        },
      ],
    });

    this.logger.log(`Sent message to ${topic} with headers`);
    return { message: 'Sent with headers' };
  }

  async getClusterInfo() {
    const desc = await this.admin.describeCluster();
    return {
      clusterId: desc.clusterId,
      controller: desc.controller,
      brokers: desc.brokers.map((b) => `${b.host}:${b.port}`),
    };
  }
}
