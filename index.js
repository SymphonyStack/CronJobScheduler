import * as dotenv from "dotenv";
dotenv.config();

import express from "express";
import { createClient } from "@supabase/supabase-js";
import cors from "cors";
const app = express();
const port = process.env.PORT || 8081;
const SUPABASE_URL = process.env.SUPABASE_URL ?? "";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY ?? "";

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(
  cors({
    origin: "*",
  }),
);

app.get("/health", (_, res) => {
  res.status(200).send("OK");
});

app.use((err, _, res, __) => {
  console.error(err);
  res.status(err.status || 500).end(err.message);
});

export const createDbClient = () => {
  return createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
    auth: {
      persistSession: false,
    },
  });
};

const runCron = async () => {
  while (true) {
    const clientInstance = await createDbClient();
    const flow_response = await clientInstance
      .from("Flow")
      .select()
      .match({ trigger_type: "time" });
    if (flow_response.error) {
      continue;
    }
    for (let flow of flow_response.data) {
      console.log("FLOW: ",flow);
      const job_status_response = await clientInstance
        .from("JobStatus")
        .select()
        .match({ flow_id: flow.id, status: "SUCCESS" })
        .order("created_at", { ascending: false });
      if (job_status_response.data) {
        if (
          !job_status_response.data ||
          job_status_response.data.length == 0 ||
          Date.now() - new Date(job_status_response.data[0].created_at) >=
            flow.trigger_condition
        ) {
          const JobStatus = {
            flow_id: flow.id,
            status: "SUBMITTED",
          };
          const response = await clientInstance
            .from("JobStatus")
            .insert(JobStatus)
            .select();
          if (response.error) {
            return { status: response.status, data: response.error.message };
          }
          return response;
        }
      }
    }
    await new Promise(r => setTimeout(r, 5 * 60 * 1000));
  }
};

try {
  app.listen(port, () => {
    console.log("App listening on port " + port);
    runCron();
  });
} catch (error) {
  console.log(error);
  console.error("Error occurred while starting the server:", error);
}
