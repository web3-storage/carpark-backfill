import Stream from 'stream'
import { toString } from 'uint8arrays/to-string'
import { fromString } from 'uint8arrays/from-string'

import { getSideIndex } from './car.js'

/**
 * @param {import('./types').BackfillBucketClients} buckets
 * @param {import('./types').ListItem} item
 */
export async function copyAndIndex (buckets, item) {
  if (!item.Key) {
    // This should never happen
    return {
      key: item.Key || 'unkown key',
      status: COPY_STATUS.FAIL
    }
  }
  const exists = await buckets.destinationBucket.has(item.Key)
  if (exists) {
    return {
      key: item.Key,
      status: COPY_STATUS.EXIST
    }
  }

  /** @type {Stream.Readable | undefined} */
  // @ts-expect-error Readable AWS types...
  const body = await buckets.originBucket.get(item.Key)
  if (!body) {
    return {
      key: item.Key,
      status: COPY_STATUS.FAIL
    }
  }

  // Get 2 streams passthrough from origin bucket readable stream
  const stream0 = body.pipe(new Stream.PassThrough())
  const stream1 = body.pipe(new Stream.PassThrough())

  const base16Md5 = (item.ETag?.replaceAll('"', '') || '').split('-')[0]
  const rawMd5 = fromString(base16Md5, 'base16')
  const base64Md5 = toString(rawMd5, 'base64pad')

  // TODO: get destination Key format instead of replicating

  await Promise.all([
    // copy
    buckets.destinationBucket.put(item.Key, stream0, {
      ContentMD5: base64Md5
    }),
    // create and write index
    buckets.destinationSideIndexBucket.put(
      `${item.Key}.idx`,
      await getSideIndex(stream1)
    )
  ])

  return {
    key: item.Key,
    status: COPY_STATUS.SUCCESS
  }
}

/**
 * @type {Record<string, import('./types').CopyStatus}
 */
const COPY_STATUS = {
  EXIST: 'EXIST',
  SUCCESS: 'SUCCESS',
  FAIL: 'FAIL'
}
