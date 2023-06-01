// import Stream from 'stream'
import crypto from 'crypto'

import { Multibases } from 'ipfs-core-utils/multibases'
import { bases } from 'multiformats/basics'
import { CID } from 'multiformats/cid'
import { toString } from 'uint8arrays/to-string'
import { fromString } from 'uint8arrays/from-string'

import { getSideIndex } from './car.js'
import { getSideIndexFromBytes } from './car.js'

/**
 * @param {import('./types').BackfillBucketClients} buckets
 * @param {import('./types').ListEntry} item
 * @param {import("./types").GetResponse} response
 */
async function copyAndIndexStream (buckets, item, response) {
  // Get 2 streams passthrough from origin bucket readable stream
  // @ts-expect-error aws types not match
  const stream0 = response.body.pipe(new Stream.PassThrough())
  // @ts-expect-error aws types not match
  const stream1 = response.body.pipe(new Stream.PassThrough())

  // @ts-ignore es version type update
  const base16Md5 = (response.etag.replaceAll('"', '') || '').split('-')[0]
  const rawMd5 = fromString(base16Md5, 'base16')
  const base64Md5 = toString(rawMd5, 'base64pad')

  let rootCid
  try {
    if (item.in.includes('raw/')) {
      rootCid = normalizeCid(item.in.split('raw/')[1].split('/')[0])
    }
  } catch {
    console.log('normalizecid-error', item.in)
  }

  await Promise.all([
    // copy
    buckets.destinationBucket.put(item.out, stream0, {
      ContentMD5: base64Md5,
      ContentLength: response.contentLength
    }),
    // create and write index
    buckets.destinationSideIndexBucket.put(
      `${item.out}.idx`,
      await getSideIndex(stream1)
    ),
    // write to dudewhere if we have root CID
    rootCid && buckets.destinationRootIndexBucket.put(
      `${rootCid}/${item.out.split('/')[0]}`,
      undefined,
      {
        ContentLength: 0
      }
    )
  ])

  return {
    key: item.out,
    status: COPY_STATUS.SUCCESS
  }
}

/**
 * @param {import('./types').BackfillBucketClients} buckets
 * @param {import('./types').ListEntry} item
 * @param {object} [options]
 * @param {boolean} [options.stream]
 */
export async function copyAndIndex (buckets, item, options = {}) {
  const response = await buckets.originBucket.get(item.in)
  if (!response) {
    return {
      key: item.out,
      status: COPY_STATUS.FAIL
    }
  }

  if (options.stream) {
    return copyAndIndexStream(buckets, item, response)
  }

  // @ts-ignore
  const bytes = await response.body?.transformToByteArray()

  const hash = crypto.createHash('md5').update(bytes).digest('base64')
  const rawMd5 = fromString(hash, 'base64')
  const base64Md5 = toString(rawMd5, 'base64pad')

  let rootCid
  try {
    if (item.in.includes('raw/')) {
      rootCid = normalizeCid(item.in.split('raw/')[1].split('/')[0])
    }
  } catch {}

  await Promise.all([
    // copy
    buckets.destinationBucket.put(item.out, bytes, {
      ContentMD5: base64Md5,
      ContentLength: response.contentLength
    }),
    // create and write index
    buckets.destinationSideIndexBucket.put(
      `${item.out}.idx`,
      // await getSideIndex(stream1)
      await getSideIndexFromBytes(bytes)
    ),
    // write to dudewhere if we have root CID
    rootCid && buckets.destinationRootIndexBucket.put(
      `${rootCid}/${item.out.split('/')[0]}`,
      undefined,
      {
        ContentLength: 0
      }
    )
  ])

  return {
    key: item.out,
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

/**
 * Parse CID and return normalized b32 v1.
 *
 * @param {string} cid
 */
export async function normalizeCid (cid) {
  const baseDecoder = await getMultibaseDecoder(cid)
  const c = CID.parse(cid, baseDecoder)
  return c.toV1().toString()
}

/**
 * Get multibase to decode CID
 *
 * @param {string} cid
 */
async function getMultibaseDecoder (cid) {
  const multibaseCodecs = Object.values(bases)
  const basicBases = new Multibases({
    bases: multibaseCodecs
  })

  const multibasePrefix = cid[0]
  const base = await basicBases.getBase(multibasePrefix)

  return base.decoder
}
