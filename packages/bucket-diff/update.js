import { Writable } from 'stream'
import { createReadStream, createWriteStream } from 'fs'

import { Multibases } from 'ipfs-core-utils/multibases'
import { bases } from 'multiformats/basics'
import { CID } from 'multiformats/cid'
import { pipe } from 'it-pipe'
import { transform } from 'streaming-iterables'
import { parse } from 'it-ndjson'

/**
 * @param {string|URL} url
 * @returns {AsyncIterable<import('./types.js').ListEntry>}
 */
export async function * fetchCar (url, log) {
  const data = await fetchDataFile(url, log)
  if (!data) {
    throw new Error('no data found')
  }
  yield * parse(data)
}

/**
 * @param {string|URL} dataUrl
 * @param {any} log
 */
export async function fetchDataFile (dataUrl, log) {
  log('fetching dataUrl %s', dataUrl)
  const fileName = 'data'
  for (let i = 0; i < 10; i++) {
    try {
      const res = await fetch(dataUrl)
      if (!res.ok || !res.body) {
        const errMessage = `${res.status} ${res.statusText} ${dataUrl}`
        throw new Error(errMessage)
      }
      await res.body.pipeTo(Writable.toWeb(createWriteStream(fileName)))
      const readStream = createReadStream(fileName)
      readStream.on('close', () => {
        console.log('stream close')
      })
      readStream.on('error', (err) => {
        console.log('stream error', err)
      })
      return readStream
    } catch (err) {
      log('Error fetchData: %o', err)
    }
  }
  log('fetchData: giving up. could no get %s', dataUrl)
}

/**
 * @param {import('./types.js').BucketUpdateListClients} buckets
 * @param {Set<string>} badCids
 * @param {any} log
 * @param {string} dataUrl
 */
export function filterAlreadyStoredOrBad (buckets, badCids, log, dataUrl) {
  return async function * (source) {
    let totalCount = 0

    yield * pipe(
      source,
      transform(40, async (/** @type {import('./types.js').ListEntry} */ item) => {
        totalCount++
        log(`try ${totalCount}`)
        if (badCids.has(item.out.split('/')[0])) {
          return {
            item,
            bad: true
          }
        }

        let rootCid
        try {
          rootCid = await normalizeCid(item.in.replace('raw/', '').split('/')[0])
        } catch {
          console.log('norootcid', item.in)
        }

        const [dest, destIndex, destRoot] = await Promise.all([
          buckets.destinationBucket.has(item.out),
          buckets.destinationSideIndexBucket.has(`${item.out}.idx`),
          rootCid && buckets.destinationRootIndexBucket.has(`${rootCid}/${item.out.split('/')[0]}`)
        ])

        return {
          item,
          dest,
          destIndex,
          destRoot
        }
      }),
      async function * (source) {
        for await (const entry of source) {
          if (entry?.item != null) {
            yield {
              inKey: entry.item.in,
              outKey: entry.item.out,
              size: entry.item.size,
              bad: false,
              dest: entry.dest || false,
              destIndex: entry.destIndex || false,
              destRoot: entry.destRoot || false,
            }
          }
        }
      }
    )
  }
}

/**
 * Parse CID and return normalized b32 v1.
 *
 * @param {string} cid
 */
export async function normalizeCid (cid) {
  try {
    const c = CID.parse(cid)
    return c.toV1().toString()
  } catch {
    const baseDecoder = await getMultibaseDecoder(cid)
    const c = CID.parse(cid, baseDecoder)
    return c.toV1().toString()
  }
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
  try {
    const base = await basicBases.getBase(multibasePrefix)
  
    return base.decoder
  } catch {
    return undefined
  }
}
