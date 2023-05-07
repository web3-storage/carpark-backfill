import { MultihashIndexSortedWriter } from 'cardex'
import { CarIndexer } from '@ipld/car/indexer'
import { concat as uint8arraysConcat } from 'uint8arrays'

/**
 * Build the side index.
 *
 * @param {AsyncIterable<Uint8Array>} stream
 * @returns
 */
export async function getSideIndex(stream) {
  const { writer, out } = MultihashIndexSortedWriter.create()
  /** @type {Error?} */
  let failError

  const fillWriterWithIndexBlocks = async () => {
    try {
      const indexer = await CarIndexer.fromIterable(stream)
      for await (const blockIndexData of indexer) {
        // @ts-ignore CID versions incompatible
        await writer.put(blockIndexData)
      }
    } catch (/** @type {any} */ error) {
      failError = error
    } finally {
      await writer.close()
    }
  }

  // call here, but don't await so it does this async
  fillWriterWithIndexBlocks()

  const chunks = []
  for await (const chunk of out) {
    chunks.push(chunk)
  }

  // @ts-ignore ts being ts
  if (failError) {
    throw failError
  }


  return uint8arraysConcat(chunks)
}
