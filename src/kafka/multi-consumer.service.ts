// multi-consumer.service.ts
import {
  Injectable,
  OnModuleInit,
  OnApplicationShutdown,
  Logger,
} from '@nestjs/common';
import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';

@Injectable()
export class MultiConsumerService
  implements OnModuleInit, OnApplicationShutdown
{
  private readonly kafka = new Kafka({
    clientId: 'multi-consumer',
    brokers: ['localhost:9092'],
  });

  private readonly consumer: Consumer = this.kafka.consumer({
    groupId: 'multi-group',
  });

  private readonly logger = new Logger(MultiConsumerService.name);

  async onModuleInit() {
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: 'multi-topic',
      fromBeginning: true,
    });

    await this.consumer.run({
      eachMessage: async (payload: EachMessagePayload) => {
        const { topic, message } = payload;
        const value = message.value?.toString();
        if (value) {
          this.logger.log(`MultiConsumer received from ${topic}: ${value}`);
          // Custom logic here
        }
      },
    });

    this.logger.log('MultiConsumer is listening...');
  }

  async onApplicationShutdown() {
    await this.consumer.disconnect();
    this.logger.log('MultiConsumer disconnected');
  }
}
