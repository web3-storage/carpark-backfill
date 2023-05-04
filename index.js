import http from 'http'
import { pipe } from 'it-pipe'
import debug from 'debug'

import { getBuckets } from './buckets.js'
import { copyAndIndex } from './copy.js'
import { createRegistry, recordMetrics } from './prom.js'

const log = debug('backfill:index')

/**
 * @param {import('./types').BackfillProps} props
 */
export async function startBackfill (props) {
  log('creating Prometheus metrics registry...')
  const { metrics, registry } = createRegistry(props.prometheusNamespace)

  log('creating HTTP server...')
  const server = http.createServer(async (req, res) => {
    // @ts-expect-error request url
    const url = new URL(req.url, `http://${req.headers.host}`)
    if (url.pathname === '/metrics') {
      res.write(await registry.metrics())
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

  log('starting backfill')
  try {
    await pipe(
      backfill(props),
      recordMetrics(metrics),
      logResult
    )
  } finally {
    log('closing HTTP server...')
    server.close()
  }
}

/**
 * @param {import('./types').BackfillProps} props
 */
async function * backfill (props) {
  const buckets = getBuckets(props)

  for await (const contents of buckets.originBucket.list({
    MaxKeys: props.batchSize,
    Prefix: props.prefix
  })) {
    const results = await Promise.all(
      contents
        // We can't filter by suffix within List object command
        .filter(c => c.Key?.endsWith('.car'))
        .map(c => copyAndIndex(buckets, c))
    )

    for (const result of results) {
      yield result
    }
  }
}

/**
 * @param {AsyncIterable<import('./types').CopyResult>} source
 */
async function logResult (source) {
  for await (const result of source) {
    log(`[${result.status}] ${result.key}`)
  }
}