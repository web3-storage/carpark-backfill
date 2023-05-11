import { CID } from 'multiformats/cid'
import { fromString } from "uint8arrays/from-string"
import { sha256 } from 'multiformats/hashes/sha2'
import { Digest } from 'multiformats/hashes/digest'

export function getUpdateListEnvContext () {
  return {
    ...(getCommonEnvContext()),
    destinationBucket: {
      region: mustGetEnv('DESTINATION_BUCKET_REGION'),
      name: mustGetEnv('DESTINATION_BUCKET_NAME'),
      endpoint: process.env['DESTINATION_BUCKET_ENDPOINT'],
      accessKeyId: mustGetEnv('DESTINATION_BUCKET_ACCESS_KEY_ID'),
      secretAccessKey: mustGetEnv('DESTINATION_BUCKET_SECRET_ACCESS_KEY')
    },
  }
}

export function getCreateListEnvContext () {
  return {
    ...(getCommonEnvContext()),
    readBatchSize: Number(mustGetEnv('READ_BATCH_SIZE')),
    writeBatchSize: Number(mustGetEnv('WRITE_BATCH_SIZE')),
  }
}

export function getCommonEnvContext () {
  return {
    originBucket: {
      region: mustGetEnv('ORIGIN_BUCKET_REGION'),
      name: mustGetEnv('ORIGIN_BUCKET_NAME'),
      endpoint: process.env['ORIGIN_BUCKET_ENDPOINT'],
      accessKeyId: mustGetEnv('ORIGIN_BUCKET_ACCESS_KEY_ID'),
      secretAccessKey: mustGetEnv('ORIGIN_BUCKET_SECRET_ACCESS_KEY')
    },
    prefix: process.env['PREFIX'],
    web3StorageToken: mustGetEnv('WEB3_STORAGE_TOKEN'),
    dataUrl: mustGetEnv('DATA_URL'),
    healthcheckPort: process.env['HEALTH_CHECK_PORT'] ? Number(process.env['HEALTH_CHECK_PORT']) : 3000,
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
