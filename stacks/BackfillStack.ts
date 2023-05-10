import { StackContext } from "sst/constructs";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import { Platform } from "aws-cdk-lib/aws-ecr-assets";
import { Duration } from 'aws-cdk-lib';
import { getCdkNames } from "./config.js";

export function BackfillStack({ stack }: StackContext) {
  const clusterName = getCdkNames('job-cluster', stack.stage)
  const cluster = new ecs.Cluster(stack, clusterName, {
    clusterName
  })

  cluster.addCapacity('DefaultAutoScalingGroupCapacity', {
    instanceType: new ec2.InstanceType("t2.xlarge"),
  })

  const taskDefinition = new ecs.Ec2TaskDefinition(stack, getCdkNames('task', stack.stage))

  const containerName = getCdkNames('job-container', stack.stage)
  taskDefinition.addContainer(containerName, {
    containerName,
    // TODO: see if maps
    image: ecs.ContainerImage.fromAsset(new URL('./packages/backfill', import.meta.url).pathname, {
      platform: Platform.LINUX_AMD64
    }),
    memoryLimitMiB: 4 * 1024, /* 8 GB RAM, min allowed with 4 vCPU */
    healthCheck: {
      command: ['CMD-SHELL', 'ps -ef | grep backfill || exit 1'],
      // the properties below are optional
      interval: Duration.seconds(20),
      retries: 3,
      // retries: 3,
      startPeriod: Duration.seconds(5),
      timeout: Duration.seconds(20)
    },
    logging: ecs.LogDrivers.awsLogs({
      streamPrefix: 'backfill',
      logRetention: 30
    }),
    environment: {
      // Main props
      DATA_URL: mustGetEnv('DATA_URL'),
      HEALTH_CHECK_PORT: mustGetEnv('HEALTH_CHECK_PORT'),
      BATCH_SIZE: mustGetEnv('BATCH_SIZE'),
      // Origin bucket
      ORIGIN_BUCKET_REGION: mustGetEnv('ORIGIN_BUCKET_REGION'),
      ORIGIN_BUCKET_NAME: mustGetEnv('ORIGIN_BUCKET_NAME'),
      ORIGIN_BUCKET_ENDPOINT: process.env['ORIGIN_BUCKET_ENDPOINT'] || '',
      ORIGIN_BUCKET_ACCESS_KEY_ID: mustGetEnv('ORIGIN_BUCKET_ACCESS_KEY_ID'),
      ORIGIN_BUCKET_SECRET_ACCESS_KEY: mustGetEnv('ORIGIN_BUCKET_SECRET_ACCESS_KEY'),
      // Destination bucket
      DESTINATION_BUCKET_REGION: mustGetEnv('DESTINATION_BUCKET_REGION'),
      DESTINATION_BUCKET_NAME: mustGetEnv('DESTINATION_BUCKET_NAME'),
      DESTINATION_BUCKET_ENDPOINT: process.env['DESTINATION_BUCKET_ENDPOINT'] || '',
      DESTINATION_BUCKET_ACCESS_KEY_ID: mustGetEnv('DESTINATION_BUCKET_ACCESS_KEY_ID'),
      DESTINATION_BUCKET_SECRET_ACCESS_KEY: mustGetEnv('DESTINATION_BUCKET_SECRET_ACCESS_KEY'),
      // Destination Bucket for side indexes
      DESTINATION_SIDE_INDEX_BUCKET_REGION: mustGetEnv('DESTINATION_SIDE_INDEX_BUCKET_REGION'),
      DESTINATION_SIDE_INDEX_BUCKET_NAME: mustGetEnv('DESTINATION_SIDE_INDEX_BUCKET_NAME'),
      DESTINATION_SIDE_INDEX_BUCKET_ENDPOINT: process.env['DESTINATION_SIDE_INDEX_BUCKET_ENDPOINT'] || '',
      DESTINATION_SIDE_INDEX_BUCKET_ACCESS_KEY_ID: mustGetEnv('DESTINATION_SIDE_INDEX_BUCKET_ACCESS_KEY_ID'),
      DESTINATION_SIDE_INDEX_BUCKET_SECRET_ACCESS_KEY: mustGetEnv('DESTINATION_SIDE_INDEX_BUCKET_SECRET_ACCESS_KEY'),
      // Destination bucket for root CID mapping
      DESTINATION_ROOT_INDEX_BUCKET_REGION: mustGetEnv('DESTINATION_ROOT_INDEX_BUCKET_REGION'),
      DESTINATION_ROOT_INDEX_BUCKET_NAME: mustGetEnv('DESTINATION_ROOT_INDEX_BUCKET_NAME'),
      DESTINATION_ROOT_INDEX_BUCKET_ENDPOINT: process.env['DESTINATION_ROOT_INDEX_BUCKET_ENDPOINT'] || '',
      DESTINATION_ROOT_INDEX_BUCKET_ACCESS_KEY_ID: mustGetEnv('DESTINATION_ROOT_INDEX_BUCKET_ACCESS_KEY_ID'),
      DESTINATION_ROOT_INDEX_BUCKET_SECRET_ACCESS_KEY: mustGetEnv('DESTINATION_ROOT_INDEX_BUCKET_SECRET_ACCESS_KEY'),
    }
  })

  // Instantiate an Amazon ECS Service
  const serviceName = getCdkNames('service', stack.stage)
  const ecsService = new ecs.Ec2Service(stack, serviceName, {
    serviceName,
    cluster,
    taskDefinition,
  })

  return ecsService
}

function mustGetEnv (name: string) {
  const value = process.env[name]
  if (!value) throw new Error(`missing ${name} environment variable`)
  return value
}