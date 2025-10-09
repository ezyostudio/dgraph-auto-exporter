import 'dotenv/config'
import { CronJob } from 'cron';
import { createApp, defineEventHandler } from 'h3';

const config = {
  cronSchedule: process.env.CRON_SCHEDULE || '0 0 * * *',
  dgraphUrl: process.env.DGRAPH_URL,
  dgraphAdminToken: process.env.DGRAPH_ADMIN_TOKEN,
  dgraphExportFormat: process.env.DGRAPH_EXPORT_FORMAT || 'rdf',
  awsAccessKeyId: process.env.AWS_ACCESS_KEY_ID,
  awsSecretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  awsDestination: process.env.AWS_DESTINATION,
}

if (!config.dgraphUrl) {
  throw new Error('Missing env variable "DGRAPH_URL"');
}

try {
  new URL('/graphql', config.dgraphUrl);
} catch (e: any) {
  if (e.message === 'Invalid URL') {
    throw new Error(`Invalid env variable "DGRAPH_URL": use format protocol://hostname `)
  }

  throw e
}

if (!config.dgraphAdminToken) {
  throw new Error('Missing env variable "DGRAPH_ADMIN_TOKEN"');
}

if (!config.dgraphUrl) {
  throw new Error('Missing env variable "DGRAPH_URL"');
}

try {
  new URL('/graphql', config.dgraphUrl);
} catch (e: any) {
  if (e.message === 'Invalid URL') {
    throw new Error(`Invalid env variable "DGRAPH_URL": use format protocol://hostname `)
  }

  throw e
}

if (!config.awsAccessKeyId) {
  throw new Error('Missing env variable "AWS_ACCESS_KEY_ID"');
}

if (!config.awsSecretAccessKey) {
  throw new Error('Missing env variable "AWS_SECRET_ACCESS_KEY"');
}


if (!config.awsDestination) {
  throw new Error('Missing env variable "AWS_DESTINATION"');
}

try {
  new URL('/', config.awsDestination);
} catch (e: any) {
  if (e.message === 'Invalid URL') {
    throw new Error(`Invalid env variable "AWS_DESTINATION": use format s3://s3.<region>.amazonaws.com/<bucket-name>/<optional-folder> `)
  }

  throw e
}


const latestRun = { date: null as Date | null, response: null as any };

const performExport = async () => {
  latestRun.date = new Date();
  console.log(`[${latestRun.date.toISOString()}] Starting export...`);

  const mutation = /*GraphQL*/`
    mutation CreateExport($destination: String!, $accessKey: String!, $secretKey: String!, $format: String) {
      export(
        input: {
          destination: $destination
          accessKey: $accessKey
          secretKey: $secretKey
          format: $format
        }
      ) {
        response {
          message
          code
        }
      }
    }
  `;

  const response = await fetch(new URL('/admin', config.dgraphUrl).toString(), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Dgraph-AuthToken': config.dgraphAdminToken!,
    },
    body: JSON.stringify({
      query: mutation,
      variables: {
        destination: config.awsDestination,
        accessKey: config.awsAccessKeyId,
        secretKey: config.awsSecretAccessKey,
        format: config.dgraphExportFormat,
      }
    }),
  }).then(res => res.json()).then(data => data.data.export.response);

  latestRun.response = response

  console.log(`[${latestRun.date.toISOString()}] Export queued:`, response);
}

const job = CronJob.from({
  cronTime: config.cronSchedule,
  onTick: performExport,
  start: true,
  runOnInit: true,
});

export const app = createApp();

app.use(defineEventHandler((event) => {
  return {
    currentTime: new Date(),
    nextRun: job.nextDate().toJSDate(),
    isRunning: job.isCallbackRunning,
    latestRun,
  };
}));
