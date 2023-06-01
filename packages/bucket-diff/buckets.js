import {
  S3Client,
  HeadObjectCommand,
  PutObjectCommand,
  ListObjectsV2Command,
  GetObjectAttributesCommand,
  GetObjectCommand
} from '@aws-sdk/client-s3'
import pRetry from 'p-retry'
import * as API from './types.js'

/**
 * @param {API.BucketDiffCreateListProps} ctx
 * @returns {API.BucketCreateListClients}
 */
export function getCreateListBuckets (ctx) {
  return {
    originBucket: new BucketClient(ctx.originBucket)
  }
}

/**
 * @param {API.BucketDiffUpdateListProps} ctx
 * @returns {API.BucketUpdateListClients}
 */
export function getUpdateListBuckets (ctx) {
  return {
    originBucket: new BucketClient(ctx.originBucket),
    destinationBucket: new BucketClient(ctx.destinationBucket),
    destinationSideIndexBucket: new BucketClient(ctx.destinationSideIndexBucket),
    destinationRootIndexBucket: new BucketClient(ctx.destinationRootIndexBucket),
  }
}

/**
 * @implements {API.BucketClient}
 */
export class BucketClient {
  /**
   * 
   * @param {API.BucketProps} props 
   */
  constructor (props) {
    this.name = props.name

    this.client = new S3Client({
      region: props.region,
      endpoint: props.endpoint || undefined,
      credentials: {
        accessKeyId: props.accessKeyId,
        secretAccessKey: props.secretAccessKey
      }
    })
  }

  /**
   * @param {string} key
   */
  async has (key) {
    const cmd = new HeadObjectCommand({
      Key: key,
      Bucket: this.name,
    })
    return await pRetry(async () => {
      try {
        await this.client.send(cmd)
      } catch (cause) {
        if (cause?.$metadata?.httpStatusCode === 404) {
          return false
        }
        throw new Error(`Failed to check bucket ${this.name}`)
      }
      return true
    }, {
      retries: 3
    })
  }


  /**
   * @param {string} key
   * @param {API.PutBody} body
   * @param {API.PutOptions} [options]
   */
  async put (key, body, options) {
    const putCmd = new PutObjectCommand({
      ...options,
      Key: key,
      Bucket: this.name,
      Body: body
    })
    await pRetry(
      () => this.client.send(putCmd),
      { retries: 3 }
    )
  }

  /**
   * @param {string} key
   */
  async get (key) {
    const getCmd = new GetObjectCommand({
      Bucket: this.name,
      Key: key,
    })

    const res = await pRetry(
      () => this.client.send(getCmd),
      { retries: 3 }
    )

    if (!res.Body) {
      return undefined
    }

    return {
      body: res.Body,
      etag: res.ETag || '',
      contentLength: res.ContentLength || 0
    }
  }

  /**
   * @param {string} key
   */
  async getAttributes (key) {
    const getCmd = new GetObjectAttributesCommand({
      Bucket: this.name,
      Key: key,
      ObjectAttributes: ['Checksum', 'ObjectSize', ]
    })

    const res = await pRetry(
      () => this.client.send(getCmd),
      { retries: 3 }
    )

    return res
  }

  /**
   * @param {API.ListOptions} [options]
   */
  async * list (options) {
    let continuationToken
    let count = 0
    do {
      /** @type {import('@aws-sdk/client-s3').ListObjectsV2CommandOutput} */
      const response = await this.client.send(new ListObjectsV2Command({
        ...(options || {}),
        Bucket: this.name,
        ContinuationToken: continuationToken
      }))

      continuationToken = response.NextContinuationToken
      console.log(count, 'continuation token', continuationToken)

      if (response.Contents) {
        yield response.Contents
      }
      count++
    } while (continuationToken)
  }
}