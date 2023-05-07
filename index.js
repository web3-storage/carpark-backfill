import debug from 'debug'
import http from 'http'
import { Writable } from 'stream'
import { createReadStream, createWriteStream } from 'fs'

import { parse } from 'it-ndjson'
import { pipe } from 'it-pipe'
import { transform } from 'streaming-iterables'
import ndjson from 'ndjson'
import { Web3Storage } from 'web3.storage'

import { createHealthCheckServer } from './health.js'
import { getBuckets } from './buckets.js'
import { copyAndIndex } from './copy.js'
import { getDestinationKey } from './utils.js'

const log = debug(`backfill`)

const REPORT_INTERVAL = 1000 * 60 // log download progress every minute

/**
 * @param {import('./types').BackfillProps} props
 */
export async function startBackfill (props) {
  // Get CSV File
  const sourceDataFile = props.dataUrl.substring(props.dataUrl.lastIndexOf('/') + 1)

  // Healh check
  const gracePeriodMs = REPORT_INTERVAL * 2
  const health = createHealthCheckServer({ sourceDataFile, gracePeriodMs })

  // Logging
  const logger = debug(`backfill:${sourceDataFile}`)
  const log = (...args) => {
    // @ts-ignore
    logger(...args)
    health.heartbeat()
  }
  health.srv.listen(props.healthcheckPort, '127.0.0.1', () => {
    log(`healthcheck server listening on ${props.healthcheckPort}`)
  })

  const buckets = getBuckets(props)

  await pipe(
    fetchCar(props.dataUrl, log),
    filterAlreadyStored(buckets.destinationBucket),
    transform(props.batchSize, async (/** @type {import('./types.js').ListEntry} */ item) => {
      log(`processing ${item.out}`)
      await copyAndIndex(buckets, item)
      return item
    }),
    async function (source) {
      for await (const item of source) {
        log(`processed ${item.out}`)
      }
    }
  )

  log('backfill complete 🎉')
  health.done()
}

/**
 * @param {string|URL} url
 * @returns {AsyncIterable<import('./types.js').ListEntry>}
 */
async function * fetchCar (url, log) {
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
      return createReadStream(fileName)
    } catch (err) {
      log('Error fetchData: %o', err)
    }
  }
  log('fetchData: giving up. could no get %s', dataUrl)
}

/**
 * @param {import('./types.js').BucketClient} destinationBucket
 */
function filterAlreadyStored (destinationBucket) {
  return async function * (source) {
    yield * pipe(
      source,
      transform(100, async (/** @type {import('./types.js').ListEntry} */ item) => {
        const has = await destinationBucket.has(item.out)
        if (has) {
          return null
        }
        return item
      }),
      async function * (source) {
        for await (const item of source) {
          if (item != null) yield item
        }
      }
    )
  }
}

/**
 * @param {import('./types').BackfillListProps} props
 */
export async function startBackfillList (props) {
  log('creating HTTP server...')
  const server = http.createServer(async (req, res) => {
    // @ts-expect-error request url
    const url = new URL(req.url, `http://${req.headers.host}`)
    if (url.pathname === '/metrics') {
      res.write({})
    } else {
      res.statusCode = 404
      res.write('not found')
    }
    res.end()
  })
  server.listen(
    props.port,
    () => log(`server listening on: http://localhost:${props.port}`)
  )

  await pipe(
    backfillList(props),
    logListResult(props)
  )

  log('closing HTTP server...')
  server.close()
}

/**
 * @param {import('./types').BackfillListProps} props
 */
async function * backfillList (props) {
  const buckets = getBuckets(props)

  for await (const contents of buckets.originBucket.list({
    MaxKeys: props.batchSize,
    Prefix: props.prefix,
    ContinuationToken: props.continuationToken
  })) {
    const results = await Promise.all(
      contents
        // We can't filter by suffix within List object command
        .filter(c => c.Key?.endsWith('.car'))
        .map(async c => {
          const key = await getDestinationKey(c.Key || '')
          return {
            has: await buckets.destinationBucket.has(key),
            inKey: c.Key,
            outKey: key,
            size: c.Size || 0
          }
        })
    )

    // Multiple users might have same uploads, let's remove unique keys
    const uniqueOutResults = results.filter((r, index, array) => {
      return array.findIndex(a => a.outKey === r.outKey) === index
    })

    for (const result of uniqueOutResults) {
      if (!result.has) {
        yield result
      }
    }
  }
}

/**
 * @param {import('./types').BackfillListProps} props
 */
function logListResult (props) {
  const outputFile = `${props.originBucket.name}-${props.prefix}.ndjson`
  const transformStream = ndjson.stringify()
  const outputStream = transformStream.pipe(createWriteStream(outputFile))

  /**
   * @param {AsyncIterable<import('./types').ListResult>} source
   */
  return async function (source) {
    for await (const result of source) {
      log(`[${result.has}] ${result.outKey}`)

      transformStream.write({
        in: result.inKey || '',
        out: result.outKey,
        size: result.size || 0
      })
    }
  
    transformStream.end()
    await new Promise((resolve) => {
      outputStream.on('finish', () => resolve(true))
    })
    console.log('output file', outputFile)

    // Store data to web3.storage
  const w3Client = new Web3Storage({
    token: props.web3StorageToken
  })

  const files = [{
    name: outputFile,
    stream: () => createReadStream(outputFile)
  }]

  const cid = await w3Client.put(files, {
    name: outputFile,
    wrapWithDirectory: false
  })
  console.log('cid', cid)
  }
}
