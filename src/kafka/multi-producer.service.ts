import {
  Injectable,
  OnModuleInit,
  OnApplicationShutdown,
  Logger,
} from '@nestjs/common';
import { Kafka, Producer, logLevel, Message } from 'kafkajs';

@Injectable()
export class MultiProducerService
  implements OnModuleInit, OnApplicationShutdown
{
  private readonly kafka = new Kafka({
    clientId: 'multi-producer',
    brokers: ['localhost:9092'],
    logLevel: logLevel.ERROR,
  });

  private readonly producer: Producer = this.kafka.producer();
  private readonly logger = new Logger(MultiProducerService.name);

  async onModuleInit() {
    await this.producer.connect();
    this.logger.log('MultiProducer connected');
  }

  async onApplicationShutdown() {
    await this.producer.disconnect();
    this.logger.log('MultiProducer disconnected');
  }

  // Gửi một message đơn giản
  async sendMessage(topic: string, value: any) {
    await this.producer.send({
      topic,
      messages: [{ value: JSON.stringify(value) }],
    });
    this.logger.log(`Sent message to ${topic} via MultiProducer`);
  }

  // Gửi nhiều messages cùng lúc
  async sendMessages(topic: string, values: any[]) {
    const messages: Message[] = values.map((value) => ({
      value: JSON.stringify(value),
    }));

    await this.producer.send({ topic, messages });
    this.logger.log(`Sent ${messages.length} messages to ${topic}`);
  }

  // Gửi message với key (hữu ích để đảm bảo message đi về cùng partition)
  async sendMessageWithKey(topic: string, key: string, value: any) {
    await this.producer.send({
      topic,
      messages: [{ key, value: JSON.stringify(value) }],
    });
    this.logger.log(`Sent message with key "${key}" to ${topic}`);
  }

  // Gửi message đến partition cụ thể
  async sendMessageToPartition(topic: string, partition: number, value: any) {
    await this.producer.send({
      topic,
      messages: [
        {
          value: JSON.stringify(value),
          partition,
        },
      ],
    });
    this.logger.log(`Sent message to partition ${partition} of topic ${topic}`);
  }

  // Gửi message với custom headers
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
    this.logger.log(`Sent message with headers to ${topic}`);
  }

  // Kiểm tra trạng thái producer (giả lập đơn giản)
  async isConnected(): Promise<boolean> {
    try {
      // dummy send to test connectivity
      await this.producer.send({
        topic: '__healthcheck',
        messages: [{ value: 'ping' }],
      });
      return true;
    } catch (error) {
      this.logger.error('Producer is not connected', error);
      return false;
    }
  }
}
