import { StackContext } from "sst/constructs";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import { Platform } from "aws-cdk-lib/aws-ecr-assets";
import { Duration } from 'aws-cdk-lib';
import { getCdkNames } from "./config.js";
import { list } from "./list.js"

export function BackfillStack({ stack }: StackContext) {
  const vpcName = getCdkNames('vpc-cluster', stack.stage)
  const vpc = new ec2.Vpc(stack, vpcName, {
    natGateways: 0,
    enableDnsHostnames: true,
    enableDnsSupport: true,
    subnetConfiguration: [
      {
        cidrMask: 28,
        name: `ecs-with-ec2-subnet-group-${stack.stage}`,
        subnetType: ec2.SubnetType.PUBLIC,
      },
    ],
  })

  const clusterName = getCdkNames('job-cluster', stack.stage)
  const cluster = new ecs.Cluster(stack, clusterName, {
    clusterName,
    vpc,
  })

  cluster.addCapacity('DefaultAutoScalingGroupCapacity', {
    instanceType: new ec2.InstanceType("t3.2xlarge"),
    maxCapacity: 20,
    desiredCapacity: 5
  })

  const tasks: ecs.Ec2TaskDefinition[] = []
  list.slice(0, 15)
    .map((dataUrl, index) => {
      const taskDefinitionJob = new ecs.Ec2TaskDefinition(stack, getCdkNames(`task-jobs-${index}`, stack.stage), {
        networkMode: ecs.NetworkMode.HOST
      })
      tasks.push(taskDefinitionJob)
      const containerName = getCdkNames(`job-container-${index}`, stack.stage)
      taskDefinitionJob.addContainer(containerName, {
        containerName,
        image: ecs.ContainerImage.fromAsset(new URL('./packages/backfill', import.meta.url).pathname, {
          platform: Platform.LINUX_AMD64
        }),
        memoryLimitMiB: 3 * 1024,
        healthCheck: {
          command: ['CMD-SHELL', 'ps -ef | grep backfill || exit 1'],
          // the properties below are optional
          interval: Duration.seconds(60),
          retries: 5,
          startPeriod: Duration.seconds(5),
          timeout: Duration.seconds(60)
        },
        logging: ecs.LogDrivers.awsLogs({
          streamPrefix: 'backfill',
          logRetention: 30
        }),
        environment: {
          // Main props
          DATA_URL: dataUrl,
          HEALTH_CHECK_PORT: String(Number(mustGetEnv('HEALTH_CHECK_PORT')) + index),
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
    })

  // const taskDefinitionList = new ecs.Ec2TaskDefinition(stack, getCdkNames('task-list', stack.stage))
  // const containerName = getCdkNames(`job-container-list`, stack.stage)
  // taskDefinitionList.addContainer(containerName, {
  //   containerName,
  //   // TODO: see if maps
  //   image: ecs.ContainerImage.fromAsset(new URL('./packages/bucket-diff', import.meta.url).pathname, {
  //     platform: Platform.LINUX_AMD64
  //   }),
  //   memoryLimitMiB: 8 * 1024,
  //   healthCheck: {
  //     command: ['CMD-SHELL', 'ps -ef | grep bucket-diff || exit 1'],
  //     // the properties below are optional
  //     interval: Duration.seconds(20),
  //     retries: 3,
  //     // retries: 3,
  //     startPeriod: Duration.seconds(5),
  //     timeout: Duration.seconds(20)
  //   },
  //   logging: ecs.LogDrivers.awsLogs({
  //     streamPrefix: 'bucket-diff',
  //     logRetention: 30
  //   }),
  //   environment: {
  //     // Main props
  //     DATA_URL: mustGetEnv('DATA_URL'),
  //     HEALTH_CHECK_PORT: mustGetEnv('HEALTH_CHECK_PORT'),
  //     BATCH_SIZE: mustGetEnv('BATCH_SIZE'),
  //     PREFIX: 'raw',
  //     READ_BATCH_SIZE: mustGetEnv('READ_BATCH_SIZE'),
  //     WRITE_BATCH_SIZE: mustGetEnv('WRITE_BATCH_SIZE'),
  //     WEB3_STORAGE_TOKEN: mustGetEnv('WEB3_STORAGE_TOKEN'),
  //     // Origin bucket
  //     ORIGIN_BUCKET_REGION: mustGetEnv('ORIGIN_BUCKET_REGION'),
  //     ORIGIN_BUCKET_NAME: mustGetEnv('ORIGIN_BUCKET_NAME'),
  //     ORIGIN_BUCKET_ENDPOINT: process.env['ORIGIN_BUCKET_ENDPOINT'] || '',
  //     ORIGIN_BUCKET_ACCESS_KEY_ID: mustGetEnv('ORIGIN_BUCKET_ACCESS_KEY_ID'),
  //     ORIGIN_BUCKET_SECRET_ACCESS_KEY: mustGetEnv('ORIGIN_BUCKET_SECRET_ACCESS_KEY'),
  //   }
  // })

  // Instantiate an Amazon ECS Service
  tasks.map((taskDefinition, index) => {
    const serviceNameJobs = getCdkNames(`service-jobs-${index}`, stack.stage)
    new ecs.Ec2Service(stack, serviceNameJobs, {
      serviceName: serviceNameJobs,
      cluster,
      taskDefinition,
    })
  })

  // const serviceNameList = getCdkNames('service-list', stack.stage)
  // const ecsServiceList = new ecs.Ec2Service(stack, serviceNameList, {
  //   serviceName: serviceNameList,
  //   cluster,
  //   taskDefinition: taskDefinitionList,
  // })
}

function mustGetEnv (name: string) {
  const value = process.env[name]
  if (!value) throw new Error(`missing ${name} environment variable`)
  return value
}
