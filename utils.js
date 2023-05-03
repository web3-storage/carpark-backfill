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
    destinationSideIndexBucket: {
      region: mustGetEnv('DESTINATION_SIDE_INDEX_BUCKET_REGION'),
      name: mustGetEnv('DESTINATION_SIDE_INDEX_BUCKET_NAME'),
      endpoint: process.env['DESTINATION_SIDE_INDEX_BUCKET_ENDPOINT'],
      accessKeyId: mustGetEnv('DESTINATION_SIDE_INDEX_BUCKET_ACCESS_KEY_ID'),
      secretAccessKey: mustGetEnv('DESTINATION_SIDE_INDEX_BUCKET_SECRET_ACCESS_KEY')
    },
    // destinationRootIndexBucket: {
    //   region: mustGetEnv('DESTINATION_ROOT_INDEX_BUCKET_REGION'),
    //   name: mustGetEnv('DESTINATION_ROOT_INDEX_BUCKET_NAME'),
    //   endpoint: process.env['DESTINATION_ROOT_INDEX_BUCKET_ENDPOINT'],
    //   accessKeyId: mustGetEnv('DESTINATION_ROOT_INDEX_BUCKET_ACCESS_KEY_ID'),
    //   secretAccessKey: mustGetEnv('DESTINATION_ROOT_INDEX_BUCKET_SECRET_ACCESS_KEY')
    // },
    prometheusNamespace: process.env['PROM_NAMESPACE'] || 'backfill',
    batchSize: process.env['BATCH_SIZE'] ? Number(process.env['BATCH_SIZE']) : 10,
    prefix: process.env['PREFIX'],
    port: process.env['PORT'] ? Number(process.env['PORT']) : 3000
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
