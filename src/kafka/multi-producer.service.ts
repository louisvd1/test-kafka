// multi-producer.service.ts
import {
  Injectable,
  OnModuleInit,
  OnApplicationShutdown,
  Logger,
} from '@nestjs/common';
import { Kafka, Producer, logLevel } from 'kafkajs';

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

  async sendMessage(topic: string, value: any) {
    await this.producer.send({
      topic,
      messages: [{ value: JSON.stringify(value) }],
    });
    this.logger.log(`Sent message to ${topic} via MultiProducer`);
  }

  async onApplicationShutdown() {
    await this.producer.disconnect();
    this.logger.log('MultiProducer disconnected');
  }
}
