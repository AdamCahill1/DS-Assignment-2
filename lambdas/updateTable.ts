import { SNSHandler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand, GetCommand } from "@aws-sdk/lib-dynamodb";


const ddbDocClient = createDDbDocClient();

export const handler: SNSHandler = async (event) => {
  console.log("Received event: ", JSON.stringify(event));

  for (const record of event.Records) {
    try {
      const snsMessage = JSON.parse(record.Sns.Message); 
      const attributes = record.Sns.MessageAttributes; 

      const imageId = snsMessage.id;
      const metadataValue = snsMessage.value;
      const metadataType = attributes?.metadata_type?.Value;

      if (!imageId || !metadataValue || !metadataType) {
        console.error("Invalid message format", { snsMessage, attributes });
        continue;
      }

      const commandOutput = await ddbDocClient.send(
        new GetCommand({
          TableName: process.env.TABLE_NAME,
          Key: { imageName: imageId },
        })
      );

      if (!commandOutput.Item) {
        console.error("Invalid image Id does not exist in database: ", imageId);
        continue;
      }

      try {
        const command = await ddbDocClient.send( 
            new UpdateCommand(
            {
                TableName: process.env.TABLE_NAME,
                Key: { imageName: imageId },
                UpdateExpression: `SET #attr = :value`,
                ExpressionAttributeNames: {
                  "#attr": metadataType,
                },
                ExpressionAttributeValues: {
                  ":value": metadataValue,
                },
            })
        );
        console.log("DynamoDB Update Result: ", command);
      } catch (error) {
        console.error("Error updating DynamoDB: ", error);
        throw error;
      }

      console.log(`Successfully updated metadata for image: ${imageId}`);
    } catch (error) {
      console.error("Error processing message: ", error);
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
