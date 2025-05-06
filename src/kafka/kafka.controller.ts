import { Controller, Post, Body, Query } from '@nestjs/common';
import { ProducerService } from './producer.service';
import { faker } from '@faker-js/faker';

@Controller('kafka')
export class KafkaController {
  constructor(private readonly producerService: ProducerService) {}

  // Gửi dữ liệu giả lập mặc định
  @Post('send')
  async sendFakeData() {
    const fakeData = this.generateFakeData();
    await this.producerService.produceMessage(fakeData);
    return { message: 'Fake data sent to Kafka', data: fakeData };
  }

  // Gửi dữ liệu tùy chọn (body)
  @Post('send-custom')
  async sendCustomData(
    @Body() data: any,
    @Query('topic') topic = 'test-topic',
  ) {
    await this.producerService.produce({
      topic,
      messages: [{ value: JSON.stringify(data) }],
    });
    return { message: `Custom data sent to Kafka topic '${topic}'`, data };
  }

  private generateFakeData() {
    return {
      id: faker.string.uuid(),
      name: faker.person.fullName(),
      email: faker.internet.email(),
      address: faker.location.streetAddress(),
      phone: faker.phone.number(),
      company: faker.company.name(),
      timestamp: new Date().toISOString(),
    };
  }
}
