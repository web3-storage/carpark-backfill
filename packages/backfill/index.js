import debug from 'debug'
import { Writable } from 'stream'
import { createReadStream, createWriteStream, promises as fsProm } from 'fs'

import { parse } from 'it-ndjson'
import { pipe } from 'it-pipe'
import { transform } from 'streaming-iterables'
import ndjson from 'ndjson'

import { createHealthCheckServer } from './health.js'
import { getBuckets } from './buckets.js'
import { copyAndIndex } from './copy.js'
import { getDestinationKey } from './utils.js'

const REPORT_INTERVAL = 1000 * 60 // log download progress every minute

/**
 * @param {import('./types').BackfillProps} props
 */
export async function startBackfill (props) {
  // Get Data File
  const sourceDataFile = props.dataUrl?.substring(props.dataUrl.lastIndexOf('/') + 1) || 'djdjd'

  // Logging
  const logger = debug(`backfill:${sourceDataFile}`)
  const log = (...args) => {
    // @ts-ignore
    logger(...args)
    health.heartbeat()
  }

  // Healh check
  const gracePeriodMs = REPORT_INTERVAL * 2
  const health = createHealthCheckServer({ sourceDataFile, gracePeriodMs })

  health.srv.listen(props.healthcheckPort, '0.0.0.0', () => {
    log(`healthcheck server listening on ${props.healthcheckPort}`)
  })

  const buckets = getBuckets(props)
  let counter = 0

  const badCids = await getBadCids([
    'nft_cids_uploads_blocked_users.json',
    'w3_cids_uploads_blocked_users.json'
  ])

  await pipe(
    fetchCar(props.dataUrl, log),
    filterAlreadyStoredOrBad(buckets.destinationBucket, badCids, log, props.dataUrl),
    transform(props.batchSize, async (/** @type {import('./types.js').ListEntry} */ item) => {
      log(`processing ${item.out}`)
      await copyAndIndex(buckets, item)
      return item
    }),
    async function (source) {
      for await (const item of source) {
        counter++
        log(`copy processed (${counter}) ${item.out.split('/')[0]}`)
      }
    }
  )

  log('backfill complete 🎉')

  // health.done()
  // process.exit(0)
}

/**
 * @param {string[]} files 
 */
async function getBadCids (files) {
  const badCids = new Set()
  for (const file of files) {
    const data = JSON.parse(await fsProm.readFile(file, 'utf-8'))

    for (const entry of data) {
      entry.contentCid && entry.contentCid[0] && badCids.add(entry.contentCid[0])
    }
  }

  return badCids
}

/**
 * @param {string|URL} url
 * @returns {AsyncIterable<import('./types.js').ListEntry>}
 */
async function * fetchCar (url, log) {
  const data = await fetchDataFile(url, log)
  if (!data) {
    throw new Error('no data found')
  }
  yield * parse(data)
}

/**
 * @param {string|URL} dataUrl
 * @param {any} log
 */
export async function fetchDataFile (dataUrl, log) {
  log('fetching dataUrl %s', dataUrl)
  const fileName = 'data'
  for (let i = 0; i < 10; i++) {
    try {
      const res = await fetch(dataUrl)
      if (!res.ok || !res.body) {
        const errMessage = `${res.status} ${res.statusText} ${dataUrl}`
        throw new Error(errMessage)
      }
      await res.body.pipeTo(Writable.toWeb(createWriteStream(fileName)))
      return createReadStream(fileName)
    } catch (err) {
      log('Error fetchData: %o', err)
    }
  }
  log('fetchData: giving up. could no get %s', dataUrl)
}

/**
 * @param {import('./types.js').BucketClient} destinationBucket
 * @param {Set<string>} badCids
 * @param {any} log
 * @param {string} dataUrl
 */
function filterAlreadyStoredOrBad (destinationBucket, badCids, log, dataUrl) {
  return async function * (source) {
    let totalCount = 0
    let badCidsFilteredCount = 0
    let existingCidsFilteredCount = 0
    yield * pipe(
      source,
      transform(30, async (/** @type {import('./types.js').ListEntry} */ item) => {
        totalCount++
        if (current[dataUrl] && current[dataUrl] > totalCount) {
          log(`skipped ${totalCount}`)
          return null
        }
        log(`try ${totalCount}`)
        if (badCids.has(item.out.split('/')[0])) {
          badCidsFilteredCount++
          log(`bad (${badCidsFilteredCount}) ${item.out}`)
          return null
        }
        const has = await destinationBucket.has(item.out)
        if (has) {
          existingCidsFilteredCount++
          log(`existing (${existingCidsFilteredCount}) ${item.out}`)
          return null
        }
        return item
      }),
      async function * (source) {
        for await (const item of source) {
          if (item != null) yield item
        }
      }
    )

    log(`filter finished with ${badCidsFilteredCount} bad CIDs filtered and ${existingCidsFilteredCount} already existing filtered`)
  }
}

/**
 * @param {import('./types').BackfillListProps} props
 */
export async function startBackfillList (props) {
  // Logging
  const logger = debug(`backfill-list`)
  const log = (...args) => {
    // @ts-ignore
    logger(...args)
  }

  await pipe(
    backfillList(props),
    logListResult(props, log)
  )
}

/**
 * @param {import('./types').BackfillListProps} props
 */
async function * backfillList (props) {
  const buckets = getBuckets(props)

  for await (const contents of buckets.originBucket.list({
    MaxKeys: props.batchSize,
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
            has: await buckets.destinationBucket.has(key),
            inKey: c.Key,
            outKey: key,
            size: c.Size || 0
          }
        })
    )

    // Multiple users might have same uploads, let's remove unique keys
    const uniqueOutResults = results.filter((r, index, array) => {
      return array.findIndex(a => a.outKey === r.outKey) === index
    })

    for (const result of uniqueOutResults) {
      if (!result.has) {
        yield result
      }
    }
  }
}

/**
 * @param {import('./types').BackfillListProps} props
 */
function logListResult (props, log) {
  const outputFile = `${props.originBucket.name}-${props.prefix}.ndjson`
  const transformStream = ndjson.stringify()
  const outputStream = transformStream.pipe(createWriteStream(outputFile))

  /**
   * @param {AsyncIterable<import('./types').ListResult>} source
   */
  return async function (source) {
    for await (const result of source) {
      log(`[${result.has}] ${result.outKey}`)

      transformStream.write({
        in: result.inKey || '',
        out: result.outKey,
        size: result.size || 0
      })
    }
  
    transformStream.end()
    await new Promise((resolve) => {
      outputStream.on('finish', () => resolve(true))
    })
    console.log('output file', outputFile)
  }
}

const current = {
  'https://bafybeigps4o62ukdoyw3vmx2x63r67kfmbx6djsl7cy6kd6fmcssntlixe.ipfs.w3s.link/dotstorage-prod-0-raw/0.ndjson': 1000000,
  'https://bafybeieeegaxtams7ijm7ttmngjqgec7cfnkj3unm2h4wyp7xyrcbyvlum.ipfs.w3s.link/dotstorage-prod-0-raw/1.ndjson': 1000000,
  'https://bafybeichwr6qd7fnzio6bmctv3dtds6gy27tzcf5h7fjgjkld4q6owo2iy.ipfs.w3s.link/dotstorage-prod-0-raw/2.ndjson': 1000000,
  'https://bafybeifwctghp2anpq4usfekvoqozgaonvbvybl7a2pzdipk7d477ouwbu.ipfs.w3s.link/dotstorage-prod-0-raw/3.ndjson': 1000000,
  'https://bafybeihn6w5v2u74mywbkcwq4u7duflip6ut7ks3u77vompqfmrh3f52mm.ipfs.w3s.link/dotstorage-prod-0-raw/4.ndjson': 1000000,
  'https://bafybeihssizcrn236umssnk3ytbg73aospkxct6igi6biy4vdqc5es4zhy.ipfs.w3s.link/dotstorage-prod-0-raw/5.ndjson': 1000000,
  'https://bafybeifnyjq36k7gtrtjp5zwalulfljqgdkrhiqqyyazm6vorb4hm6vcei.ipfs.w3s.link/dotstorage-prod-0-raw/6.ndjson': 1000000,
  'https://bafybeidgh7wl7awcqeixo63yfhmt4ynjj5ebajo6hellexptsliqvks5rq.ipfs.w3s.link/dotstorage-prod-0-raw/7.ndjson': 1000000,
  'https://bafybeicdiop5crgjnbfxbgtjn35v6wvuu5wy7gc47qnnduvwtnmdileagu.ipfs.w3s.link/dotstorage-prod-0-raw/8.ndjson': 1000000,
  'https://bafybeias7ivwmnsibxkhafa6hjff2eahf4bwwxs7ogyihod7tizt36syfi.ipfs.w3s.link/dotstorage-prod-0-raw/9.ndjson': 1000000,
  'https://bafybeifbqgafzu6p623lpfaxraxmj3ds63o7tfmguqmyw67w6omqaeebii.ipfs.w3s.link/dotstorage-prod-0-raw/10.ndjson': 1000000,
  'https://bafybeia7lcvnoe7sdo3yikqbrnkk7ebpww3kx5cz5lefmq7osp65bligr4.ipfs.w3s.link/dotstorage-prod-0-raw/11.ndjson': 1000000,
  'https://bafybeicao54hbahic6honneqfpczmig4gcos7a6gupvlbj4fvk5ghy5zma.ipfs.w3s.link/dotstorage-prod-0-raw/12.ndjson': 1000000,
  'https://bafybeifiirvfb2dkt2vpb4zwak2uhmlsyfwkfv7huucmdxyj74i4h6zs4a.ipfs.w3s.link/dotstorage-prod-0-raw/13.ndjson': 1000000,
  'https://bafybeigz3wnwz25bvbwqgs4f6jku56wjfpe4uimvd42zvvn6p73jh7wvkm.ipfs.w3s.link/dotstorage-prod-0-raw/14.ndjson': 1000000,
  'https://bafybeiho5mza2ijf5gnxamjborpy4pkh6uebajvwzf6oanidlqdfga5zbi.ipfs.w3s.link/dotstorage-prod-0-raw/15.ndjson': 1000000,
  'https://bafybeidvs5cuehudkzth2g7thunps6isv2uoayxzbe3muj7eeohetmf2sy.ipfs.w3s.link/dotstorage-prod-0-raw/16.ndjson': 1000000,
  'https://bafybeiepsqfmwjyday636t4mypud4amsoyredpmyy4oibxtvrkvaums6ea.ipfs.w3s.link/dotstorage-prod-0-raw/17.ndjson': 1000000,
  'https://bafybeiahvmr2o7ibddmdpcqgwvq7vbyr4dbeomrsoko6lyslzukueyhknq.ipfs.w3s.link/dotstorage-prod-0-raw/18.ndjson': 1000000,
  'https://bafybeicmcnkyynecsklknfotubr3ks7johezwyzxlwqs6nbrnrxagrfjdq.ipfs.w3s.link/dotstorage-prod-0-raw/19.ndjson': 1000000,

  'https://bafybeian6tbhczmnd5ksrzgnxbs32glzxscrssho7r5lzgnnw2sdq3jrvm.ipfs.w3s.link/dotstorage-prod-0-raw/20.ndjson': 1000000,
  'https://bafybeigum2tpggwinsqtezx4galf65wdr4qgyalohg6qnm57t7tidb6qsq.ipfs.w3s.link/dotstorage-prod-0-raw/21.ndjson': 1000000,
  'https://bafybeiavs55l7yjiukbipu2s4lzcuqlmnuqea6re4twnrlp4euuxtx27ky.ipfs.w3s.link/dotstorage-prod-0-raw/22.ndjson': 1000000,
  'https://bafybeif62i6ambehhjkregisgwa4rxkmml4wc6wpwbz7qwpfajsvai7mbq.ipfs.w3s.link/dotstorage-prod-0-raw/23.ndjson': 1000000,
  'https://bafybeiamsph7mnf2w5fk4gd6ivg63t4zqgwetghascwsljw4mveylubyzu.ipfs.w3s.link/dotstorage-prod-0-raw/24.ndjson': 1000000,
  'https://bafybeiahsrdehbmh2benc3vom65d4atl32ekqmomdtvsehaserveu6uxhm.ipfs.w3s.link/dotstorage-prod-0-raw/25.ndjson': 1000000,
  'https://bafybeig4qzeuxb3i5qbk7chfjbvlzvqqyffaor3mqqhlx63j2yymfoqjdm.ipfs.w3s.link/dotstorage-prod-0-raw/26.ndjson': 1000000,
  'https://bafybeidr5fqepo7buoc352rsynra4qgwpagdsxsogvdy2fndogznkknkjq.ipfs.w3s.link/dotstorage-prod-0-raw/27.ndjson': 1000000,
  'https://bafybeiflvziam6xgbf5jt6tc5kc4ubhacr4oqcgvqmvx4hguzeivjmhlxq.ipfs.w3s.link/dotstorage-prod-0-raw/28.ndjson': 1000000,
  'https://bafybeibqbvnjnwzli5vsxvf23ulubwgg346samfkwi3lyouqswgv2rqpxi.ipfs.w3s.link/dotstorage-prod-0-raw/29.ndjson': 1000000,
  'https://bafybeibquzfkognqzkt6h7ghur3x5mgxeh7isg2lkl2ssp7dvv65lt2lza.ipfs.w3s.link/dotstorage-prod-0-raw/30.ndjson': 1000000,
  'https://bafybeicc4wmkq3rsax5ckgxbvthxhspvarwzifcipds4t3xkzkf22pvyn4.ipfs.w3s.link/dotstorage-prod-0-raw/31.ndjson': 1000000,
  'https://bafybeihj3ptcqapgzdcba5n2llpwghi2666oaa3ppsvelrtxtdoztyh5lq.ipfs.w3s.link/dotstorage-prod-0-raw/32.ndjson': 1000000,
  'https://bafybeic2y5y4v25jxflao247dvjrua524vxf5mr6l7afd76n5xwj6t3sdm.ipfs.w3s.link/dotstorage-prod-0-raw/33.ndjson': 1000000,
  'https://bafybeiebmrwbgr34cp5blqu3kwwfhqq3fxfyabk5uqbbx6skdbv42a4hgm.ipfs.w3s.link/dotstorage-prod-0-raw/34.ndjson': 1000000,
  'https://bafybeibv3xvyfqaizpeiaitkmcljuz77zb42nb2h36ggsqlsdeop3b5g7m.ipfs.w3s.link/dotstorage-prod-0-raw/35.ndjson': 1000000,
  'https://bafybeidtvan4efo34mfaeydbxiauchfa3rr5hq6o3n5g4bicd3tayrbjc4.ipfs.w3s.link/dotstorage-prod-0-raw/36.ndjson': 1000000,
  'https://bafybeidpbplwylivbwjyjveoe4mw7bdfqf6tf4uth4kkloivtr2wyrufzq.ipfs.w3s.link/dotstorage-prod-0-raw/37.ndjson': 1000000,
  'https://bafybeidvnv2rw6iho6ihw6s2racwlizjownzkao2ehkszd6ofrhrov247y.ipfs.w3s.link/dotstorage-prod-0-raw/38.ndjson': 1000000,
  'https://bafybeigcqdergd6ja6ax57tgttc5r5r6ndoowll3tbfbxm4uweldrk6l6a.ipfs.w3s.link/dotstorage-prod-0-raw/39.ndjson': 1000000,

  'https://bafybeiex7r42i7zidf65enylpolg4cgqebxlflbv2cfjpny2owvtownd3u.ipfs.w3s.link/dotstorage-prod-0-raw/40.ndjson': 1000000,
  'https://bafybeibz4tqvdlnzchu62gn74zpnlgdchvavoqxvpaqbarrppmyttyymsm.ipfs.w3s.link/dotstorage-prod-0-raw/41.ndjson': 1000000,
  'https://bafybeig5trsh5iebreobgovw7f7uli2f4qzxygksqiw7wjumfyoxf4gwyq.ipfs.w3s.link/dotstorage-prod-0-raw/42.ndjson': 1000000,
  'https://bafybeie2tgg4qpvmleqwheokz7ehqdv7d4r3b7o3r3dwqxfqeudnsrzd4i.ipfs.w3s.link/dotstorage-prod-0-raw/43.ndjson': 1000000,
  'https://bafybeicslomwbayjc3zvwuioiatdjmgvyh5eyqagjwpwnaadgwxmdxrsta.ipfs.w3s.link/dotstorage-prod-0-raw/44.ndjson': 1000000,
  'https://bafybeie3ersikq3rzwvag57yug2iexbr52yz54e4hgjr7ewum335lnwisy.ipfs.w3s.link/dotstorage-prod-0-raw/45.ndjson': 1000000,
  'https://bafybeiexdalfyhfrohvyyibt3bkdklh2ywocc66pmshr5d6s2tjox6gili.ipfs.w3s.link/dotstorage-prod-0-raw/46.ndjson': 1000000,
  'https://bafybeihfd7jablkx7toffhcfujuuwtuaym43lyud3gn6z7mboyvyyey56q.ipfs.w3s.link/dotstorage-prod-0-raw/47.ndjson': 1000000,
  'https://bafybeidhb2wcvi5jpgoqyofcv6fadof6zw5ibkwrc264dkjxnp5j72pz3i.ipfs.w3s.link/dotstorage-prod-0-raw/48.ndjson': 1000000,
  'https://bafybeidduayd3aixrqgexxa4zq7h3qybcbvt2pedjmhc3nsluutrrhv6ey.ipfs.w3s.link/dotstorage-prod-0-raw/49.ndjson': 1000000,
  'https://bafybeidh6eydsql4jjuwq7ubzwjklrdyqeut4wcgrzx3t35s6dogasuuqm.ipfs.w3s.link/dotstorage-prod-0-raw/50.ndjson': 1000000,
  'https://bafybeifctpiibfny5d5eb3bmlhyiy3sywopr3qwceb4li4rb5a677ptrsu.ipfs.w3s.link/dotstorage-prod-0-raw/51.ndjson': 1000000,
  'https://bafybeiat6qzwebnruvttd3qnjhytkppuwnkwwfblpxfmln664b46cyhdsi.ipfs.w3s.link/dotstorage-prod-0-raw/52.ndjson': 1000000,
  'https://bafybeictjyppx2hkizvwwqwlnipng7hstkdmgnt5kej43id2rvl4p33djm.ipfs.w3s.link/dotstorage-prod-0-raw/53.ndjson': 1000000,
  'https://bafybeih256lmnaa66ntfmx5lneoyji2nirtljowf3acdus3wo66xnya4kq.ipfs.w3s.link/dotstorage-prod-0-raw/54.ndjson': 1000000,
  'https://bafybeidgwp4bj55hrju4ak7cwpptwuizx2e47yinhhq6noaphzdrwmdgje.ipfs.w3s.link/dotstorage-prod-0-raw/55.ndjson': 1000000,
  'https://bafybeie22q3svxz7taawipt2mxswbtzt6fxrjbvnzx5x5td37yvjs2mjxa.ipfs.w3s.link/dotstorage-prod-0-raw/56.ndjson': 1000000,
  'https://bafybeidc6nuycvwhapwomto34s4og3bbgzjn7b5bv3dq4duhaqu3imokqy.ipfs.w3s.link/dotstorage-prod-0-raw/57.ndjson': 1000000,
  'https://bafybeied2sch7e3hnqybqdxswy55jrb3bbxjnwrmjowf5evmmcpn7422nm.ipfs.w3s.link/dotstorage-prod-0-raw/58.ndjson': 991381,
  'https://bafybeicamhhpev3ph25v6ityd2wxqalu245tubhaxgxyalnwykst2f5w2q.ipfs.w3s.link/dotstorage-prod-0-raw/59.ndjson': 1000000,

  'https://bafybeidsub6anjgrwukhos6vjthqjtet6ybduwlh3k344c7pe42vovmvce.ipfs.w3s.link/dotstorage-prod-0-raw/60.ndjson': 986180,
  'https://bafybeicuxbdxldodkwwcexcwsc4ju4ngehyn2on3yenitlrgphhgc2bfee.ipfs.w3s.link/dotstorage-prod-0-raw/61.ndjson': 1000000,
  'https://bafybeicbrvv5jiuhnijwvxcumqtf5y4mmrn54mkcvsko5v5vk24kbomfua.ipfs.w3s.link/dotstorage-prod-0-raw/62.ndjson': 1000000,
  'https://bafybeiavnfsfrpwictwje42rrlybu26krb5tzd7ouavhkmz6jnjzd4vd6i.ipfs.w3s.link/dotstorage-prod-0-raw/63.ndjson': 1000000,
  'https://bafybeiadn66ptihi5srbgk6qggyykrxg4espdllvvdbl3spqcqi7kwvlt4.ipfs.w3s.link/dotstorage-prod-0-raw/64.ndjson': 1000000,
  'https://bafybeicmgew42w4xktqg226medqcygecmaelcsr2znc635quggb43krr3u.ipfs.w3s.link/dotstorage-prod-0-raw/65.ndjson': 1000000,
  'https://bafybeidstwtrj6gaddz7byoer7ingsdn6sdupqe2juykosohwslgxg4vbq.ipfs.w3s.link/dotstorage-prod-0-raw/66.ndjson': 979120,
  'https://bafybeidgqirv63iqstzt57cb2diuaeco7x6s44nq4vmsxjbf22ob4vs2fq.ipfs.w3s.link/dotstorage-prod-0-raw/67.ndjson': 985471,
  'https://bafybeidjm5mj2wrgq33bkfnuny4264ic2uttyxfj2arjxe2pbxttnju4la.ipfs.w3s.link/dotstorage-prod-0-raw/68.ndjson': 675625,
  'https://bafybeih24pwcmbp3dpes24h3hcprizfi5boedxs4g4shn7ci5s76xaxczu.ipfs.w3s.link/dotstorage-prod-0-raw/69.ndjson': 355694,
  'https://bafybeietig46rgghllytlt6nudsgb7mzl56s6rpmu7lifrrx6yx3glukue.ipfs.w3s.link/dotstorage-prod-0-raw/70.ndjson': 144834,
  'https://bafybeig56se7nl6w3i5qouv2i6ney24kqgrkovq6dws3jpocfcxp3dawtu.ipfs.w3s.link/dotstorage-prod-0-raw/71.ndjson': 132694,
  'https://bafybeiasrqbvm4rxpvuhcpqwemmeuq3uo3rvewwayhs4nhnaa7zdaoo3sq.ipfs.w3s.link/dotstorage-prod-0-raw/72.ndjson': 133512,
  'https://bafybeienh4fwdnvq3u3tbjewjr4n5i7cm737orwbmt6zpwnz7iwula2n4u.ipfs.w3s.link/dotstorage-prod-0-raw/73.ndjson': 29701,
  'https://bafybeif3jitwh5xfy4mnxcost4n4wca77uckn3nvdn6k72m4r5x4iq77fm.ipfs.w3s.link/dotstorage-prod-0-raw/74.ndjson': 41672,
  'https://bafybeihidy5q6vepi6i2scf7yvvkfjeynt57qdntoymqqxroddzipxg6ly.ipfs.w3s.link/dotstorage-prod-0-raw/75.ndjson': 34629,
  'https://bafybeie6buzlclwbnmkljabw6urdl3fmb5ymotiq6pmvsd76oo3wcstg5u.ipfs.w3s.link/dotstorage-prod-0-raw/76.ndjson': 35890,
  'https://bafybeienmywh4j3uf7pqdefvocphkgb3mvuwrispduwez55ji44ao3am6e.ipfs.w3s.link/dotstorage-prod-0-raw/77.ndjson': 0,
  'https://bafybeiaxuam6crnqgwujmhmnn2wkjf3fdxg5j3ajevf3t2q5db53a27pxy.ipfs.w3s.link/dotstorage-prod-0-raw/78.ndjson': 0,
  'https://bafybeiawcqbauqjjmc77qbrdoav5mcw6uq6ca2p4ea3zn7cpzdzmj73bl4.ipfs.w3s.link/dotstorage-prod-0-raw/79.ndjson': 0,
  'https://bafybeiecxcmzhu62l4t3ci62j67aauzfxoj7x5auegnnxpdmo7gqehcrsa.ipfs.w3s.link/dotstorage-prod-0-raw/80.ndjson': 0,
  'https://bafybeifmkwdbmtlmxz6qsfglntzantw6lsavmhdnph63nxcw35bewz2tie.ipfs.w3s.link/dotstorage-prod-0-raw/81.ndjson': 0,
  'https://bafybeibwufu4irzzmrzevibyexut4wk2eavf4dwy7mwtasykbtdvn2s4h4.ipfs.w3s.link/dotstorage-prod-0-raw/82.ndjson': 0,
  'https://bafybeibp2q3zgkh6no26sxuiuclzptcp3555327z4xmwk2dmi4aay6l2qi.ipfs.w3s.link/dotstorage-prod-0-raw/83.ndjson': 0,
  'https://bafybeih4uq3mkbi3hzs52qjvmicx3kubsvhnjxfpphcxhutkcc2viopmva.ipfs.w3s.link/dotstorage-prod-0-raw/84.ndjson': 0,
  'https://bafybeia4ddw7unqus5uyreeaxna4frkj63hyympv2xhjbka6rwvg3dckni.ipfs.w3s.link/dotstorage-prod-0-raw/85.ndjson': 0,
  'https://bafybeielfsk2abfb5zgn2wz7yohmmdtkvdpocpzzfdvttmv4btolyxloky.ipfs.w3s.link/dotstorage-prod-0-raw/86.ndjson': 0,
  'https://bafybeigpggtnk7v7xcm5nhppv7gmyjygq57y34ww2t6a4a6437nmtyjixm.ipfs.w3s.link/dotstorage-prod-0-raw/87.ndjson': 0,
  'https://bafybeib3zbwqzjobczhykp5jm7si5tkeectz4la7rranw37g47wsxf74fa.ipfs.w3s.link/dotstorage-prod-0-raw/88.ndjson': 0,
  'https://bafybeihtj4wpzeamipy4aetkiivh7umldes2c3d7mh7usgacz5stv6umry.ipfs.w3s.link/dotstorage-prod-0-raw/89.ndjson': 0,
  'https://bafybeihmianmmdwgrdjfarxkog53w7ylu7n5w3twb33yc36krnulhh6zxu.ipfs.w3s.link/dotstorage-prod-0-raw/90.ndjson': 0
}
