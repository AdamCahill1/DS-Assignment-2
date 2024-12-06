import { SNSHandler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, DeleteCommand, GetCommand } from "@aws-sdk/lib-dynamodb";


const ddbDocClient = createDDbDocClient();

export const handler: SNSHandler = async (event) => {
  console.log("Received event:", JSON.stringify(event));

  for (const record of event.Records) {
    try {
        const snsMessage = JSON.parse(record.Sns.Message); 
        if (snsMessage.Records) { 

            for (const messageRecord of snsMessage.Records) {     
                const s3e = messageRecord.s3;
                const srcBucket = s3e.bucket.name;
                const srcKey = decodeURIComponent(s3e.object.key.replace(/\+/g, " "));


                const commandOutput = await ddbDocClient.send(
                    new GetCommand({
                      TableName: process.env.TABLE_NAME,
                      Key: { imageName: srcKey },
                    })
                );

                if (!commandOutput.Item) {
                    console.error("Invalid image Id does not exist in database: ", srcKey);
                    continue;
                }

                try {
                    const commandOutput = await ddbDocClient.send(
                      new DeleteCommand({
                        TableName: process.env.TABLE_NAME,
                        Key: { imageName: srcKey },
                      })
                    );
                    console.log("DynamoDB delete successful:", commandOutput);
                  } catch (error) {
                    console.log("Error deleting to DynamoDB:", error);
                  }
            }
        }

    } catch (error) {
      console.error("Error processing record:", error);
    }
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