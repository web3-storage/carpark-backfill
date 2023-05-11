#!/usr/bin/env node

import dotenv from 'dotenv'
import { startCreateList, startUpdateList } from './index.js'
import { getCreateListEnvContext, getUpdateListEnvContext } from './utils.js'

import sade from 'sade'

dotenv.config()

const prog = sade('carpark-bucket-diff')

prog
  .command('create-list')
  .describe('create list of files in origin bucket but not in destination')
  .action(async () => startCreateList(getCreateListEnvContext()))
  .command('update-list')
  .describe('update list of files that are in origin bucket but not in destination')
  .action(() => startUpdateList(getUpdateListEnvContext()))
// TODO: Split list

prog.parse(process.argv)

