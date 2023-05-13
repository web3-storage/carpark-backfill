import { Writable } from 'stream'
import { createReadStream, createWriteStream } from 'fs'

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
 * @param {import('./types.js').BucketClient} destinationBucket
 * @param {Set<string>} badCids
 * @param {any} log
 * @param {string} dataUrl
 */
export function filterAlreadyStoredOrBad (destinationBucket, badCids, log, dataUrl) {
  return async function * (source) {
    let totalCount = 0

    yield * pipe(
      source,
      transform(40, async (/** @type {import('./types.js').ListEntry} */ item) => {
        log(`try ${totalCount}`)
        if (badCids.has(item.out.split('/')[0])) {
          // TODO: ignore
          return null
        }
        // TODO: Freeway check
        const has = await destinationBucket.has(item.out)
        if (has) {
          return null
        }

        return item
      }),
      async function * (source) {
        for await (const item of source) {
          if (item != null) {
            yield {
              inKey: item.in,
              outKey: item.out,
              size: item.size
            }
          }
        }
      }
    )
  }
}
