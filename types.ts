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
  destinationRootIndexBucket: BucketProps
}

export interface BackfillProps extends BackfillBucketProps {
  healthcheckPort: number
  batchSize: number
  dataUrl: string
}

export interface BackfillListProps extends BackfillBucketProps {
  batchSize: number
  continuationToken?: string
  prefix: string
  web3StorageToken: string
  port: number
}

export interface BackfillBucketClients {
  originBucket: BucketClient
  destinationBucket: BucketClient
  destinationSideIndexBucket: BucketClient
  destinationRootIndexBucket: BucketClient
}

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

export interface ListResult {
  has: boolean
  inKey?: string
  outKey: string
  size?: number
}

export interface PutOptions extends Omit<PutObjectCommandInput, 'Key' | 'Bucket'> {}
export interface ListOptions extends Omit<ListObjectsV2CommandInput, 'Bucket'> {}

export interface BucketClient {
  has (key: string): Promise<boolean>
  put (key: string, body: PutBody, options?: PutOptions): Promise<void>
  get (key: string): Promise<GetResponse | undefined>
  list (options?: ListOptions): AsyncGenerator<ListItem[], void>
}

export type CopyStatus = 'EXIST' | 'SUCCESS' | 'FAIL'
export interface CopyResult {
  key: string
  status: CopyStatus
}
export interface ListEntry {
  in: string
  out: string
  size: number
}
