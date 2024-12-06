import * as cdk from "aws-cdk-lib";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as iam from "aws-cdk-lib/aws-iam";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";

import { StreamViewType } from "aws-cdk-lib/aws-dynamodb";
import {  DynamoEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { StartingPosition } from "aws-cdk-lib/aws-lambda";

import { Construct } from "constructs";
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class EDAAppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const imageTable = new dynamodb.Table(this, "ImageTable", {
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      partitionKey: { name: "imageName", type: dynamodb.AttributeType.STRING },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      tableName: "Images",
      stream: dynamodb.StreamViewType.NEW_IMAGE,
    });

    const imagesBucket = new s3.Bucket(this, "images", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      publicReadAccess: false,
    });

    // Integration infrastructure
    const imageRejectionQueue = new sqs.Queue(this, "rejectionDLQ", {
      queueName: "rejectionDLQ",
    });

    const imageProcessQueue = new sqs.Queue(this, "img-created-queue", {
      receiveMessageWaitTime: cdk.Duration.seconds(10),
      deadLetterQueue: {
        queue: imageRejectionQueue,
        maxReceiveCount: 1
  
      },
    });


    // Topic
    const newImageTopic = new sns.Topic(this, "NewImageTopic", {
      displayName: "New Image topic",
    }); 


    // Lambda functions

    const processImageFn = new lambdanode.NodejsFunction(this, "ProcessImageFn", {
        runtime: lambda.Runtime.NODEJS_18_X,
        entry: `${__dirname}/../lambdas/processImage.ts`,
        timeout: cdk.Duration.seconds(15),
        memorySize: 128,
        environment: {
          TABLE_NAME: imageTable.tableName,
          REGION: cdk.Aws.REGION,
        },
      }
    );

    const confirmationMailerFn = new lambdanode.NodejsFunction(this, "confirmationMailerFn", {
      runtime: lambda.Runtime.NODEJS_16_X,
      memorySize: 1024,
      timeout: cdk.Duration.seconds(5),
      entry: `${__dirname}/../lambdas/mailer.ts`,
    });


    const rejectionMailerFn = new lambdanode.NodejsFunction(this, "rejection-mailer-function", {
      runtime: lambda.Runtime.NODEJS_16_X,
      memorySize: 1024,
      timeout: cdk.Duration.seconds(5),
      entry: `${__dirname}/../lambdas/rejectionMailer.ts`,
    });

    const updateTableFn = new lambdanode.NodejsFunction(this, "UpdateTableFn", {
      runtime: lambda.Runtime.NODEJS_18_X,
      entry: `${__dirname}/../lambdas/updateTable.ts`,
      timeout: cdk.Duration.seconds(10),
      memorySize: 128,
      environment: {
        TABLE_NAME: imageTable.tableName,
        REGION: cdk.Aws.REGION,
      },
    });

    const deleteImageFn = new lambdanode.NodejsFunction(this, "DeleteImageFn", {
      runtime: lambda.Runtime.NODEJS_18_X,
      entry: `${__dirname}/../lambdas/deleteImageHandler.ts`,
      timeout: cdk.Duration.seconds(10),
      memorySize: 128,
      environment: {
        TABLE_NAME: imageTable.tableName,
        REGION: cdk.Aws.REGION,
      },
    });

    // S3 --> SQS
    imagesBucket.addEventNotification(
        s3.EventType.OBJECT_CREATED,
        new s3n.SnsDestination(newImageTopic)  // Changed
    );
    
    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_REMOVED,
      new s3n.SnsDestination(newImageTopic)
    );
    
        //Subscribed straight to the topic
    // newImageTopic.addSubscription(
    //   new subs.LambdaSubscription(confirmationMailerFn, {
    //     filterPolicyWithMessageBody: {  
    //       Records: sns.FilterOrPolicy.policy({
    //         eventName: sns.FilterOrPolicy.filter(
    //           sns.SubscriptionFilter.stringFilter({
    //             allowlist: ["ObjectCreated:Put"], 
    //           })
    //         ),
    //       }),
    //     },
    //   })
    // );
    

    newImageTopic.addSubscription(
      new subs.SqsSubscription(imageProcessQueue, {
        filterPolicyWithMessageBody: {  
          Records: sns.FilterOrPolicy.policy({
            eventName: sns.FilterOrPolicy.filter(
              sns.SubscriptionFilter.stringFilter({
                allowlist: ["ObjectCreated:Put"], 
              })
            ),
          }), 
        },
      })
    );   

    newImageTopic.addSubscription(
      new subs.LambdaSubscription(deleteImageFn, {
        filterPolicyWithMessageBody: {
          Records: sns.FilterOrPolicy.policy({
            eventName: sns.FilterOrPolicy.filter(
              sns.SubscriptionFilter.stringFilter({
                allowlist: ["ObjectRemoved:Delete"],
              })
            ),
          }),
        },
      })
    );
    

    newImageTopic.addSubscription(
      new subs.LambdaSubscription(updateTableFn, {
        filterPolicy: {
          metadata_type: sns.SubscriptionFilter.stringFilter({
            allowlist: ["Caption", "Date", "Photographer"],
          }),
        },
      })
    );


    // SQS --> Lambda
    const newImageEventSource = new events.SqsEventSource(imageProcessQueue, {
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(10),
    });

    const newImageRejectionEventSource = new events.SqsEventSource(imageRejectionQueue, {
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(10),
    }); 

    processImageFn.addEventSource(newImageEventSource);
    rejectionMailerFn.addEventSource(newImageRejectionEventSource);

    confirmationMailerFn.addEventSource(
      new DynamoEventSource(imageTable, {
        startingPosition: lambda.StartingPosition.LATEST,
        batchSize: 5, 
      })
    );

    // Permissions

    imagesBucket.grantReadWrite(processImageFn);
    imagesBucket.grantDelete(rejectionMailerFn);
    imageTable.grantReadWriteData(processImageFn)
    imageTable.grantReadWriteData(updateTableFn);
    imageTable.grantReadWriteData(deleteImageFn);


    confirmationMailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );


    rejectionMailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );
    
    new cdk.CfnOutput(this, "bucketName", {
      value: imagesBucket.bucketName,
    });

    new cdk.CfnOutput(this, "topicARN", {
      value: newImageTopic.topicArn,
    });
  }
} 
