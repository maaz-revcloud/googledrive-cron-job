const cron = require("node-cron");
const AWS = require("aws-sdk");
const { Pool } = require("pg");

AWS.config.update({
  region: "us-west-2",
  signatureVersion: "v4",
});

const sqs = new AWS.SQS({ apiVersion: "2012-11-05" });

let init = false;
let pool = null;

const parseSecrets = async (secretString) => {
  const parsedSecrets = JSON.parse(secretString);
  for (const key in parsedSecrets) {
    if (parsedSecrets.hasOwnProperty(key)) {
      const innerString = parsedSecrets[key];
      parsedSecrets[key] = JSON.parse(innerString);
    }
  }
  return parsedSecrets;
};

const fetchSecrets = async () => {
  try {
    const secretsManager = new AWS.SecretsManager();
    const DEV_SECRET_NAME = "dev/envs";

    const data = await secretsManager
      .getSecretValue({ SecretId: DEV_SECRET_NAME })
      .promise();
    const secretString = data.SecretString;
    if (secretString) {
      const parsedSecrets = await parseSecrets(secretString);
      return parsedSecrets;
    } else {
      throw new Error("SecretString is empty");
    }
  } catch (error) {
    console.error("Error retrieving secret:", error);
    throw error;
  }
};

const getDatabasePool = async (event) => {
  try {
    if (!init) {
      const { DB_CREDENTIALS } = await fetchSecrets();

      pool = new Pool({
        user: DB_CREDENTIALS.user,
        host: DB_CREDENTIALS.host,
        database: DB_CREDENTIALS.database,
        password: DB_CREDENTIALS.password,
        port: DB_CREDENTIALS.port,
        idleTimeoutMillis: 10000000,
        connectionTimeoutMillis: 1000000,
      });
      init = true;
    }

    if (!pool) {
      throw new Error("Env not initialized.");
    }
    return pool;
  } catch (error) {
    console.error("Error", error);
  }
};

const syncGDriveConnections = async () => {
  try {
    const pool = await getDatabasePool();

    const getGDriveConnectionsQuery = `select * from api_connectors.connections where connector_id = 6 and status ='Active'`;
    const getGDriveConnections = await pool.query(getGDriveConnectionsQuery);

    getGDriveConnections.rows.forEach(connection => {
      const sendMessageToSQS = {
        type: "SYNC",
        body: {
          userId: connection.userId,
          folderId: connection.name,
          connectionId: connection.id,
        },
      };
  
      const jsonString = JSON.stringify(sendMessageToSQS);
  
      const sqsParams = {
        MessageBody: jsonString,
        QueueUrl:
          "https://sqs.us-west-2.amazonaws.com/221490242148/gdrive-files",
      };
  
      sqs.sendMessage(sqsParams, (err, data) => {
        if (err) {
          console.error("Error sending message to SQS:", err);
        } else {
          console.log("Message sent successfully:", data.MessageId);
        }
      });
    });
  } catch (error) {
    console.log(error)
  }
}


const job = cron.schedule(
  "0 0 * * *",
  async () => {
    console.log("Cron job started.")

    await syncGDriveConnections();

    console.log("Cron job executed.");
  },
  {
    scheduled: true,
    timezone: "America/New_York",
  }
);

job.start();
