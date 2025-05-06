import {
  Injectable,
  OnModuleInit,
  OnApplicationShutdown,
  Logger,
} from '@nestjs/common';
import { Client } from '@elastic/elasticsearch';
import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';

@Injectable()
export class ConsumerService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(ConsumerService.name);
  private readonly kafka = new Kafka({ brokers: ['localhost:9092'] });
  private readonly consumer: Consumer = this.kafka.consumer({
    groupId: 'consumer-group',
  });
  private readonly esClient = new Client({ node: 'http://localhost:9200' });

  async onModuleInit() {
    await this.connectConsumer();
    await this.subscribeTopics(['test-topic', 'log-topic']); // Đăng ký nhiều topic nếu cần
    await this.runConsumer();
  }

  private async connectConsumer() {
    try {
      await this.consumer.connect();
      this.logger.log('Kafka consumer connected.');
    } catch (error) {
      this.logger.error('Failed to connect consumer:', error);
    }
  }

  private async subscribeTopics(topics: string[]) {
    for (const topic of topics) {
      await this.consumer.subscribe({ topic, fromBeginning: true });
      this.logger.log(`Subscribed to topic: ${topic}`);
    }
  }

  private async runConsumer() {
    await this.consumer.run({
      eachMessage: async (payload: EachMessagePayload) => {
        try {
          const { topic, message } = payload;
          const value = message.value?.toString();
          if (!value) {
            this.logger.warn(`Message with null value from topic: ${topic}`);
            return;
          }

          this.logger.log(`Consumed from ${topic}: ${value}`);
          await this.handleMessage(topic, value);
        } catch (error) {
          this.logger.error('Error handling message:', error);
        }
      },
    });
  }

  private async handleMessage(topic: string, rawValue: string) {
    const data = JSON.parse(rawValue);

    switch (topic) {
      case 'test-topic':
        await this.esClient.index({
          index: 'kafka-data',
          document: data,
        });
        break;

      case 'log-topic':
        await this.esClient.index({
          index: 'log-data',
          document: {
            ...data,
            receivedAt: new Date().toISOString(),
          },
        });
        break;

      default:
        this.logger.warn(`No handler for topic: ${topic}`);
    }
  }

  async onApplicationShutdown() {
    await this.consumer.disconnect();
    this.logger.log('Kafka consumer disconnected.');
  }
}
