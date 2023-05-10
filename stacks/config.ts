/**
 * Get nicer CDK resources name
 */
export function getCdkNames (name: string, stage: string, version = 0) {
  // e.g `prod-backfill-task-0`
  return `${stage}-backfill-${name}-${version}`
}
