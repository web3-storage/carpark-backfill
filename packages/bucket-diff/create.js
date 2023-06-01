import { getDestinationKey } from './utils.js'
import { getCreateListBuckets } from './buckets.js'

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
      yield outList.splice(0, props.writeBatchSize)
    }
  }

  yield outList
}