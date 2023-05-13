import { Readable } from 'stream'
import {
  ListObjectsV2CommandInput,
  PutObjectCommandInput
} from '@aws-sdk/client-s3'

// Main API

export interface BucketDiffProps {
  originBucket: BucketProps
  prefix?: string
  healthcheckPort: number
  dataUrl: string
  web3StorageToken: string
}

export interface BucketDiffCreateListProps extends BucketDiffProps {
  readBatchSize: number
  writeBatchSize: number
  continuationToken?: string
}

export interface BucketDiffUpdateListProps extends BucketDiffProps {
  destinationBucket: BucketProps
}

export interface ListResult {
  inKey: string
  outKey: string
  size: number
}

export interface ListEntry {
  in: string
  out: string
  size: number
}

// Buckets

export interface BucketClients {
  originBucket: BucketClient
}

export interface BucketCreateListClients extends BucketClients {}

export interface BucketUpdateListClients extends BucketClients {
  destinationBucket: BucketClient
}

export interface BucketProps {
  region: string
  name: string
  endpoint?: string
  accessKeyId: string
  secretAccessKey: string
}

export interface BucketClient {
  has (key: string): Promise<boolean>
  put (key: string, body: PutBody, options?: PutOptions): Promise<void>
  get (key: string): Promise<GetResponse | undefined>
  list (options?: ListOptions): AsyncGenerator<ListItem[], void>
}

export interface PutOptions extends Omit<PutObjectCommandInput, 'Key' | 'Bucket'> {}
export interface ListOptions extends Omit<ListObjectsV2CommandInput, 'Bucket'> {}

export type PutBody = string | Readable | ReadableStream<any> | Blob | Uint8Array | Buffer | undefined
export interface GetResponse {
  body: GetBody
  etag: string
  contentLength: number
}
export type GetBody = Readable | ReadableStream | Blob
export interface ListItem {
  Key?: string
  ETag?: string
  Size?: number
}