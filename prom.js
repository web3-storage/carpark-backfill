import * as Prom from 'prom-client'

/**
 * @typedef {{
*   copyTotal: Prom.Counter<any>
*   existingTotal: Prom.Counter<any>
*   errorTotal: Prom.Counter<any>
* }} Metrics
*/

/**
 * @param {string} ns
 */
export function createRegistry (ns) {
  const registry = new Prom.Registry()
  return {
    registry,
    metrics: {
      copyTotal: new Prom.Counter({
        name: `${ns}_copy_total`,
        help: 'Number of successfull CARs copied.',
        registers: [registry]
      }),
      existingTotal: new Prom.Counter({
        name: `${ns}_existing_total`,
        help: 'Number of already existing CARs.',
        registers: [registry]
      }),
      errorTotal: new Prom.Counter({
        name: `${ns}_error_total`,
        help: 'Number of already existing CARs.',
        registers: [registry]
      })
    }
  }
}

/**
 * @param {Metrics} metrics
 */
export function recordMetrics (metrics) {
  /**
   * @param {AsyncIterable<import('./types').CopyResult>} source
   */
  return async function * (source) {
    for await (const result of source) {
      if (result.status === 'EXIST') {
        metrics.existingTotal.inc(1)
      } else if (result.status === 'FAIL') {
        metrics.errorTotal.inc(1)
      } else {
        metrics.copyTotal.inc(1)
      }
      yield result
    }
  }
}
