export class CreateTopicDto {
  topic: string;
  partitions: number;
  replication: number;
  configs?: { name: string; value: string }[];
}
