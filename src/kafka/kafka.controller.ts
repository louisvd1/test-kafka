import { Controller, Post, Body, Query, Get, Delete } from '@nestjs/common';
import { ProducerService } from './producer.service';
import { faker } from '@faker-js/faker';
import { CreateTopicDto } from 'src/utils/dto/create-topic.dto';

@Controller('kafka')
export class KafkaController {
  constructor(private readonly producerService: ProducerService) {}

  // Gửi dữ liệu giả lập mặc định đến test-topic
  @Post('send')
  async sendFakeData() {
    const fakeData = this.generateFakeData();
    await this.producerService.sendMessage('test-topic', fakeData);
    return { message: 'Fake data sent to Kafka', data: fakeData };
  }

  // Gửi dữ liệu tuỳ chỉnh đến topic
  @Post('send-custom')
  async sendCustomData(
    @Body() data: any,
    @Query('topic') topic = 'test-topic',
  ) {
    await this.producerService.sendMessage(topic, data);
    return { message: `Custom data sent to Kafka topic '${topic}'`, data };
  }

  // Gửi batch dữ liệu giả lập
  @Post('send-batch')
  async sendBatchFakeData(@Query('count') count = 5) {
    const batch = Array.from({ length: Number(count) }, () =>
      this.generateFakeData(),
    );
    await this.producerService.sendBatchMessages('test-topic', batch);
    return { message: `Batch of ${count} messages sent`, batch };
  }

  // Gửi dữ liệu với retry logic
  @Post('send-retry')
  async sendWithRetry(@Body() data: any, @Query('topic') topic = 'test-topic') {
    const success = await this.producerService.sendWithRetry(topic, data);
    return {
      message: success
        ? `Sent with retry success to topic '${topic}'`
        : `Retry failed for topic '${topic}'`,
      data,
    };
  }

  // Gửi dữ liệu sau delay
  @Post('send-delay')
  async sendWithDelay(
    @Body() data: any,
    @Query('delay') delay = 2000,
    @Query('topic') topic = 'test-topic',
  ) {
    await this.producerService.sendWithDelay(topic, data, Number(delay));
    return {
      message: `Message scheduled with ${delay}ms delay to topic '${topic}'`,
      data,
    };
  }

  // Tạo topic mới (nếu chưa tồn tại)
  @Post('create-topic')
  async createTopic(
    @Query('topic') topic: string,
    @Query('partitions') partitions = 1,
  ) {
    await this.producerService.ensureTopic(topic, Number(partitions));
    return {
      message: `Topic '${topic}' ensured (created if missing)`,
    };
  }

  @Post('create-topics')
  async createTopicIfNotExists(
    @Query('topic') topic: string,
    @Query('partitions') partitions = '1',
    @Query('replication') replication = '1',
  ) {
    const result = await this.producerService.createTopicIfNotExists(
      topic,
      parseInt(partitions),
      parseInt(replication),
    );
    return result;
  }

  @Post('create-topic-advanced')
  async createTopicAdvanced(@Body() dto: CreateTopicDto) {
    return this.producerService.createAdvancedTopic(dto);
  }

  @Get('topics')
  async listTopics() {
    return this.producerService.listTopicsDetails();
  }

  @Delete('topic')
  async deleteTopic(@Query('topic') topic: string) {
    return this.producerService.deleteTopic(topic);
  }

  @Post('send-with-headers')
  async sendWithHeaders(
    @Body() body: { topic: string; value: any; headers: any },
  ) {
    return this.producerService.sendMessageWithHeaders(
      body.topic,
      body.value,
      body.headers,
    );
  }

  @Get('cluster-info')
  async getClusterInfo() {
    return this.producerService.getClusterInfo();
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
