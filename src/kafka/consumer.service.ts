import { Injectable, OnModuleInit } from '@nestjs/common';
import { Client } from '@elastic/elasticsearch';
import { Kafka } from 'kafkajs';

@Injectable()
export class ConsumerService implements OnModuleInit {
  private kafka = new Kafka({
    brokers: ['localhost:9092'],
  });
  private consumer = this.kafka.consumer({ groupId: 'consumer-group' });
  private esClient = new Client({ node: 'http://localhost:9200' });

  async onModuleInit() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: 'test-topic', fromBeginning: true });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) {
          console.error('Received a message with null value');
          return;
        }
        const value = message.value.toString();
        console.log('Consumed:', value);
        await this.esClient.index({
          index: 'kafka-data',
          document: JSON.parse(value),
        });
      },
    });
  }
}
