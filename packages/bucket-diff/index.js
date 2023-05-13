import debug from 'debug'
import { createWriteStream, createReadStream, promises as fsProm } from 'fs'

import { pipe } from 'it-pipe'
import ndjson from 'ndjson'
import { Web3Storage, getFilesFromPath } from 'web3.storage'

import { createHealthCheckServer } from './health.js'
import { getUpdateListBuckets } from './buckets.js'
import { getList } from './create.js'
import { fetchCar, filterAlreadyStoredOrBad } from './update.js'

const log = debug(`carpark-bucket-diff`)

const REPORT_INTERVAL = 1000 * 60 // log download progress every minute

/**
 * @param {import('./types').BucketDiffCreateListProps} props
 */
export async function startCreateList (props) {
  const sourceDataFile = props.dataUrl?.substring(props.dataUrl.lastIndexOf('/') + 1)

  log('creating HTTP server...')
  // Healh check
  const gracePeriodMs = REPORT_INTERVAL * 2
  const health = createHealthCheckServer({ sourceDataFile, gracePeriodMs })

  health.srv.listen(props.healthcheckPort, '0.0.0.0', () => {
    log(`healthcheck server listening on ${props.healthcheckPort}`)
  })

  const i = setInterval(() => {
    health.heartbeat()
  }, 2000)

  log('starting create list...')
  await pipe(
    getList(props),
    logListResult(props)
  )
  log('ending create list...')

  // Store data to web3.storage
  await storeList(props)

  log('closing HTTP server...')
  clearInterval(i)
}

/**
 * @param {import('./types').BucketDiffUpdateListProps} props
 */
export async function startUpdateList (props) {
  const sourceDataFile = props.dataUrl?.substring(props.dataUrl.lastIndexOf('/') + 1)

  log('creating HTTP server...')
  // Healh check
  const gracePeriodMs = REPORT_INTERVAL * 2
  const health = createHealthCheckServer({ sourceDataFile, gracePeriodMs })

  health.srv.listen(props.healthcheckPort, '0.0.0.0', () => {
    log(`healthcheck server listening on ${props.healthcheckPort}`)
  })

  const i = setInterval(() => {
    health.heartbeat()
  }, 2000)

  log('starting update list...')

  const buckets = getUpdateListBuckets(props)
  // TODO: bad CIDs
  const badCids = new Set()

  await pipe(
    fetchCar(props.dataUrl, log),
    filterAlreadyStoredOrBad(buckets.destinationBucket, badCids, log, props.dataUrl),
    logListResult(props),
  )

  // Store data to web3.storage
  await storeList(props)

  log('closing HTTP server...')
  clearInterval(i)
}

/**
 * @param {import('./types').BucketDiffProps} props
 */
async function storeList (props) {
  const w3Client = new Web3Storage({
    token: props.web3StorageToken
  })
  const files = await getFilesFromPath(`${props.originBucket.name}-${props.prefix}`)
  const cid = await w3Client.put(files)

  console.log('Content added with CID:', cid)
}

/**
 * @param {import('./types').BucketDiffProps} props
 */
function logListResult (props) {
  let counter = 0

  return async function (source) {
    const outDir = `${props.originBucket.name}-${props.prefix}`
    // Allow failure if exists
    try {
      await fsProm.mkdir(outDir)
    } catch {}

    for await (const page of source) {
      const outputFile = `${outDir}/${counter}.ndjson`
      const transformStream = ndjson.stringify()
      const outputStream = transformStream.pipe(createWriteStream(outputFile))

      for (const result of page) {
        transformStream.write({
          in: result.inKey || '',
          out: result.outKey,
          size: result.size || 0
        })
      }

      counter++

      console.log(counter, 'wrote', outputFile)
      transformStream.end()
      await new Promise((resolve) => {
        outputStream.on('finish', () => resolve(true))
      })

      const w3Client = new Web3Storage({
        token: props.web3StorageToken
      })
      const finalFiles = [{
        name: outputFile,
        stream: () => createReadStream(outputFile)
      }]
      const cid = await w3Client.put(finalFiles)
      console.log('cid', cid)
      await fsProm.rm(outputFile)
    }
  }
}
