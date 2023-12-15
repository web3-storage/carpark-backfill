import { pipe } from 'it-pipe'
import { transform } from 'streaming-iterables'

import { getDestinationKey } from './utils.js'
import { getCreateListBuckets } from './buckets.js'
import { normalizeCid } from './update.js'

/**
 * @param {import('./types').BucketDiffCreateListProps} props
 */
export async function * getList (props) {
  const buckets = getCreateListBuckets(props)
  let outList = []

  for await (const contents of buckets.originBucket.list({
    MaxKeys: props.readBatchSize,
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
            inKey: c.Key,
            outKey: key,
            size: c.Size || 0
          }
        })
    )

    outList = [
      ...outList,
      // remove duplicates from new
      ...(results.filter((r, index, array) => {
        return array.findIndex(a => a.outKey === r.outKey) === index
      }))
    ]

    if (outList.length >= props.writeBatchSize) {
      for (const item of outList.splice(0, props.writeBatchSize)) {
        yield item
      }
      // yield outList.splice(0, props.writeBatchSize)
    }
  }

  yield outList
}

/**
 * @param {import('./types.js').BucketUpdateListClients} buckets
 * @param {Set<string>} badCids
 * @param {any} log
 */
export function filterAlreadyStoredOrBadOnCreate (buckets, badCids, log) {
  return async function * (source) {
    let totalCount = 0

    yield * pipe(
      source,
      transform(40, async (/** @type {import('./types.js').ListResult} */ item) => {
        totalCount++
        log(`try ${totalCount}`)
        console.log('item', item)
        if (badCids.has(item.outKey.split('/')[0])) {
          return {
            item,
            bad: true
          }
        }

        let rootCid
        try {
          rootCid = await normalizeCid(item.inKey.replace('raw/', '').split('/')[0])
        } catch {
          console.log('norootcid', item.inKey)
        }

        const [dest, destRoot] = await Promise.all([
          buckets.destinationBucket.has(item.outKey),
          // buckets.destinationSideIndexBucket.has(`${item.outKey}.idx`),
          rootCid && buckets.destinationRootIndexBucket.has(`${rootCid}/${item.outKey.split('/')[0]}`)
        ])

        console.log('cool', item.outKey, dest, destRoot)

        return {
          item,
          dest,
          destIndex: true,
          destRoot
        }
      }),
      async function * (source) {
        for await (const entry of source) {
          if (entry?.item != null) {
            yield {
              inKey: entry.item.inKey,
              outKey: entry.item.outKey,
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