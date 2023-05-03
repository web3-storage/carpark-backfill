import { CID } from 'multiformats/cid'
import { fromString } from "uint8arrays/from-string"
import { sha256 } from 'multiformats/hashes/sha2'

export const CAR_CODE = 0x202

/**
 * We have been iterating on keys over time in our multiple buckets.
 * - `dotstorage-prod-0` has keys in format:
 *   - `raw/${rootCid.toString()}/${userId}/${toString(dataHash.bytes, 'base32')}.car`
 *   - `complete/${rootCid.toString()}`
 *   - examples:
 *     - complete/bafybeifejmdbliebpx2cecepy26peiesiqgozt7k52kwnixa4zhecuccma.car
 *     - raw/bafkreia223gzz3t46ajnosijo3mgajipbyjyikwbhbkabmsqdal6o4k6uu/315318734258473247/ciqi26nuu3dnsi2dirisvxmz3jlamyocdpmfpdpxniktfjsffmcodnq.car
 *     - aw/QmW5yQT7FqcXF6RiqccxS4Uk6XKZq125SghJhEQmi5uFzW/315318734258473158/ciqdtkwlqivr34apk4lf5blyzxhhesdvvqa4drsmuoaa24j3wvstbny.car
 */

/**
 * Get key in carpark format `${carCid}/${carCid}.car`.
 *
 * @param {string} key 
 */
export async function getDestinationKey (key) {
  if (key.includes('complete/')) {
    // TODO
    throw new Error('not valid yet')
  } else if (key.includes('raw/')) {
    const paths = key.split('/')
    const b32in = paths[paths.length - 1].split('.car')[0]
    const bytesIn = fromString(b32in)
    const digest = await sha256.digest(bytesIn)
    const carCid = CID.createV1(CAR_CODE, digest)
    return `${carCid.toString()}/${carCid.toString()}.car`
  }

  throw new Error('not valid yet')
}

/**
 * Parse CID and return normalized b32 v1
 *
 * @param {string} cid
 */
export function normalizeCid (cid) {
  const c = CID.parse(cid)
  return c.toV1().toString()
}