#!/usr/bin/env node

import dotenv from 'dotenv'
import { startBackfill } from './index.js'
import { getEnvContext } from './utils.js'

dotenv.config()

startBackfill(getEnvContext())
