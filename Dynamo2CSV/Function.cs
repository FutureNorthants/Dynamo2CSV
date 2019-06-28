using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace Dynamo2CSV
{
    public class Function
    {
        private static IAmazonS3 s3Client;
        public string FunctionHandler(string tableName, ILambdaContext context)
        {
            context.Logger.LogLine("Process Started");
            String functionResult = "";           
            AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
            clientConfig.RegionEndpoint = RegionEndpoint.EUWest2;
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(clientConfig);
            s3Client = new AmazonS3Client(RegionEndpoint.EUWest2);
            DateTime tomorrow = DateTime.Now.AddDays(1);
            String selectionDate = tomorrow.ToString("yyyy-MM-dd");
            String fileDate = tomorrow.ToString("yyyyMMdd");
            context.Logger.LogLine("Extracting data for " + selectionDate);
            ScanRequest request = new ScanRequest
            {
                TableName = tableName,
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                   {":val", new AttributeValue { S = selectionDate }}
                },
                FilterExpression = "CollectionDate = :val"
            };
            Task<ScanResponse> response = client.ScanAsync(request);
            var result = response.Result.Count;
            String fileContents = "";
            foreach (Dictionary<string, AttributeValue> item in response.Result.Items)
            {
                functionResult += item;
                context.Logger.LogLine("Case " + item["Reference"].S.ToString());
                fileContents += item["Reference"].S.ToString() + "," + "\r";
            }
            Write2S3Async(context, "cxm2veoliafiles", tableName + "-" + fileDate + ".csv", fileContents).Wait();
            context.Logger.LogLine("Process Finished");
            return result.ToString();
        }

        static async Task Write2S3Async(ILambdaContext context, String bucketName, String keyName, String fileContents)
        {
            try
            {
                // 1. Put object-specify only key name for the new object.
                var putRequest1 = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = keyName,
                    ContentBody = fileContents
                };

                PutObjectResponse response1 = await s3Client.PutObjectAsync(putRequest1);
            }
            catch (AmazonS3Exception e)
            {
                context.Logger.LogLine(
                        "Error encountered when writing an object"
                       );
            }
            catch (Exception e)
            {
                context.Logger.LogLine(
                    "Unknown encountered on server when writing an object"
                    );
            }
        }
    }
}
