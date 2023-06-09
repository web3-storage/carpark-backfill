import { SSTConfig } from "sst";
import { BackfillStack } from "./stacks/BackfillStack";

export default {
  config(_input) {
    return {
      name: "carpark-backfill",
      region: "us-east-2",
    };
  },
  stacks(app) {
    app.stack(BackfillStack);
  }
} satisfies SSTConfig;
