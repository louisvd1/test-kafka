// kafka.controller.ts
import { Controller, Post } from '@nestjs/common';
import { ProducerService } from './producer.service';
import { faker } from '@faker-js/faker';

@Controller('kafka')
export class KafkaController {
  constructor(private readonly producerService: ProducerService) {}

  @Post('send')
  async sendFakeData() {
    const fakeData = {
      id: faker.string.uuid(),
      name: faker.person.fullName(),
      email: faker.internet.email(),
      timestamp: new Date().toISOString(),
    };
    await this.producerService.produceMessage(fakeData);
    return { message: 'Data sent to Kafka', data: fakeData };
  }
}
