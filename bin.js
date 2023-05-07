#!/usr/bin/env node

import dotenv from 'dotenv'
import { startBackfill, startBackfillList } from './index.js'
import { getEnvContext } from './utils.js'

import sade from 'sade'

dotenv.config()

const prog = sade('carpark-backfill')

prog
  .command('backfill')
  .describe('Backfill destination bucket')
  .action(() => startBackfill(getEnvContext()))
  .command('backfill-list')
  .describe('Run job to get backfill list')
  .action(() => startBackfillList(getEnvContext()))

prog.parse(process.argv)

