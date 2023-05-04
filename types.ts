import { Readable } from 'stream'
import {
  ListObjectsV2CommandInput,
  PutObjectCommandInput
} from '@aws-sdk/client-s3'

export interface BucketProps {
  region: string
  name: string
  endpoint?: string
  accessKeyId: string
  secretAccessKey: string
}

export interface BackfillBucketProps {
  originBucket: BucketProps
  destinationBucket: BucketProps
  destinationSideIndexBucket: BucketProps
  // destinationRootIndexBucket: BucketProps
}

export interface BackfillProps extends BackfillBucketProps {
  prometheusNamespace: string
  prefix?: string
  port: number
  batchSize: number
}

export interface BackfillBucketClients {
  originBucket: BucketClient
  destinationBucket: BucketClient
  destinationSideIndexBucket: BucketClient
  // destinationRootIndexBucket: BucketClient
}

export type PutBody = string | Readable | ReadableStream<any> | Blob | Uint8Array | Buffer | undefined
export type GetBody = Readable | ReadableStream | Blob
export interface ListItem {
  Key?: string
  ETag?: string
}

export interface PutOptions extends Omit<PutObjectCommandInput, 'Key' | 'Bucket'> {}
export interface ListOptions extends Omit<ListObjectsV2CommandInput, 'Bucket'> {}

export interface BucketClient {
  has (key: string): Promise<boolean>
  put (key: string, body: PutBody, options?: PutOptions): Promise<void>
  get (key: string): Promise<GetBody | undefined>
  list (options?: ListOptions): AsyncGenerator<ListItem[], void>
}

export type CopyStatus = 'EXIST' | 'SUCCESS' | 'FAIL'
export interface CopyResult {
  key: string
  status: CopyStatus
}