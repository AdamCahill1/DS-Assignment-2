/* eslint-disable import/extensions, import/no-absolute-path */
import { SQSHandler } from "aws-lambda";
import {
  GetObjectCommand,
  PutObjectCommandInput,
  GetObjectCommandInput,
  S3Client,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import {
  isImageCorrect
} from "./utils";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";

const s3 = new S3Client();
const ddbDocClient = createDDbDocClient();

export const handler: SQSHandler = async (event) => {
  console.log("Event ", JSON.stringify(event));
  try {
    for (const record of event.Records) {
      const recordBody = JSON.parse(record.body);        // Parse SQS message
      const snsMessage = JSON.parse(recordBody.Message); // Parse SNS message
      
      if (snsMessage.Records) {
        console.log("Record body ", JSON.stringify(snsMessage));
        for (const messageRecord of snsMessage.Records) {
          const s3e = messageRecord.s3;
          const srcBucket = s3e.bucket.name;
          const srcKey = decodeURIComponent(s3e.object.key.replace(/\+/g, " "));


          const supportedImage = isImageCorrect(srcKey);
          if (!supportedImage) {
            console.log(`Unsupported file detected: ${srcKey}`);
            throw new Error(`Unsupported image type: ${srcKey}`);
          }

          try {
            const commandOutput = await ddbDocClient.send(
              new PutCommand({
                TableName: process.env.TABLE_NAME,
                Item: {
                  imageName: srcKey,
                },
              })
            );
            console.log("DynamoDB write successful:", commandOutput);
          } catch (error) {
            console.log("Error writing to DynamoDB:", error);
          }
        }
      }
    }
  } catch (error) {
    console.log("Error processing record:", error);
    throw error;
  }
};


function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}