import { Module } from '@nestjs/common';
import { ProducerService } from './producer.service';
import { ConsumerService } from './consumer.service';
import { KafkaController } from './kafka.controller';
import { MultiProducerService } from './multi-producer.service';
import { MultiConsumerService } from './multi-consumer.service';

@Module({
  providers: [
    ProducerService,
    ConsumerService,
    MultiProducerService,
    MultiConsumerService,
  ],
  exports: [
    ProducerService,
    ConsumerService,
    MultiProducerService,
    MultiConsumerService,
  ],
  controllers: [KafkaController],
})
export class KafkaModule {}
