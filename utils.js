import { CID } from 'multiformats/cid'
import { fromString } from "uint8arrays/from-string"
import { sha256 } from 'multiformats/hashes/sha2'
import { Digest } from 'multiformats/hashes/digest'

export function getEnvContext () {
  return {
    originBucket: {
      region: mustGetEnv('ORIGIN_BUCKET_REGION'),
      name: mustGetEnv('ORIGIN_BUCKET_NAME'),
      endpoint: process.env['ORIGIN_BUCKET_ENDPOINT'],
      accessKeyId: mustGetEnv('ORIGIN_BUCKET_ACCESS_KEY_ID'),
      secretAccessKey: mustGetEnv('ORIGIN_BUCKET_SECRET_ACCESS_KEY')
    },
    destinationBucket: {
      region: mustGetEnv('DESTINATION_BUCKET_REGION'),
      name: mustGetEnv('DESTINATION_BUCKET_NAME'),
      endpoint: process.env['DESTINATION_BUCKET_ENDPOINT'],
      accessKeyId: mustGetEnv('DESTINATION_BUCKET_ACCESS_KEY_ID'),
      secretAccessKey: mustGetEnv('DESTINATION_BUCKET_SECRET_ACCESS_KEY')
    },
    // destinationSideIndexBucket: {
    //   region: mustGetEnv('DESTINATION_SIDE_INDEX_BUCKET_REGION'),
    //   name: mustGetEnv('DESTINATION_SIDE_INDEX_BUCKET_NAME'),
    //   endpoint: process.env['DESTINATION_SIDE_INDEX_BUCKET_ENDPOINT'],
    //   accessKeyId: mustGetEnv('DESTINATION_SIDE_INDEX_BUCKET_ACCESS_KEY_ID'),
    //   secretAccessKey: mustGetEnv('DESTINATION_SIDE_INDEX_BUCKET_SECRET_ACCESS_KEY')
    // },
    // destinationRootIndexBucket: {
    //   region: mustGetEnv('DESTINATION_ROOT_INDEX_BUCKET_REGION'),
    //   name: mustGetEnv('DESTINATION_ROOT_INDEX_BUCKET_NAME'),
    //   endpoint: process.env['DESTINATION_ROOT_INDEX_BUCKET_ENDPOINT'],
    //   accessKeyId: mustGetEnv('DESTINATION_ROOT_INDEX_BUCKET_ACCESS_KEY_ID'),
    //   secretAccessKey: mustGetEnv('DESTINATION_ROOT_INDEX_BUCKET_SECRET_ACCESS_KEY')
    // },
    batchSize: process.env['BATCH_SIZE'] ? Number(process.env['BATCH_SIZE']) : 10,
    // dataUrl: mustGetEnv('DATA_URL'),
    healthcheckPort: process.env['HEALTH_CHECK_PORT'] ? Number(process.env['HEALTH_CHECK_PORT']) : 3000,
    port: process.env['PORT'] ? Number(process.env['PORT']) : 3000,
    prefix: process.env['PREFIX'],
    continuationToken: process.env['CONTINUATION_TOKEN'],
    web3StorageToken: mustGetEnv('WEB3_STORAGE_TOKEN')
  }
}

/**
 * @param {string} name
 */
export function mustGetEnv (name) {
  const value = process.env[name]
  if (!value) throw new Error(`missing ${name} environment variable`)
  return value
}

export const CAR_CODE = 0x202

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
    const bytesIn = fromString(b32in, 'base32')
    const digest = new Digest(sha256.code, 32, bytesIn, bytesIn)
    const carCid = CID.createV1(CAR_CODE, digest)
    return `${carCid.toString()}/${carCid.toString()}.car`
  }

  throw new Error('not valid yet')
}
