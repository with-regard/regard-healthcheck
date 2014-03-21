using System;
using System.Configuration;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace Regard.HealthCheck
{
    class Program
    {
        // CopyPasta from the consumer (supplying the signature will produce some health check trace on the server and will also ensure that our event ends up
        // where we expect it)
        /// <summary>
        /// Generates a signature given some data and a shared secret
        /// </summary>
        public static string Signature(string data, string sharedSecret)
        {
            // We can't handle the empty string, so that generates no signature
            if (string.IsNullOrEmpty(data) || string.IsNullOrEmpty(sharedSecret))
            {
                return "";
            }

            // Hash the data and the secret
            var hash = SHA256.Create();
            var hashBytes = hash.ComputeHash(Encoding.UTF8.GetBytes(sharedSecret + "--" + data));

            // Generate a result string
            StringBuilder result = new StringBuilder();
            foreach (var byt in hashBytes)
            {
                result.Append(byt.ToString("x2", CultureInfo.InvariantCulture));
            }

            return result.ToString();
        }

        static void Main(string[] args)
        {
            try
            {
                // Get the configuration
                var appSettings = ConfigurationManager.AppSettings;

                string endpointUrl = appSettings["EndPointUrl"];
                string storageTableName = appSettings["StorageTableName"];
                string postPath = appSettings["PostPath"];
                string partitionKey = appSettings["PartitionKey"];
                string storageConnectionString = appSettings["StorageConnectionString"];
                string healthCheckSharedSecret = appSettings["HealthCheckSharedSecret"];

                // Create a web request
                var request = WebRequest.Create(new Uri(new Uri(endpointUrl), postPath));

                // Open the storage
                Console.WriteLine("HealthCheck: attaching to the storage account");
                var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
                var tableClient = storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(storageTableName);
                table.CreateIfNotExists();

                // Generate the row key
                // Don't use any characters that can get sanitised so this key is exactly what gets inserted in the table
                var rowkey = "healthcheckX" + DateTime.Now.Ticks + "X";

                Console.WriteLine("HealthCheck: found {0} matches initially", CountMatches(rowkey, table, partitionKey));

                // Generate the payload request
                var payload = JsonConvert.SerializeObject(new
                                                          {
                                                              rowkey = rowkey,
                                                              rowkeysignature =
                                                                  Signature(rowkey, healthCheckSharedSecret)
                                                          });

                var payloadBytes = Encoding.UTF8.GetBytes(payload);

                // Generate a JSON request
                request.Method = "POST";
                request.ContentType = "application/json";
                request.ContentLength = payloadBytes.Length;

                // Write the payload
                var stream = request.GetRequestStream();
                stream.Write(payloadBytes, 0, payloadBytes.Length);

                // Perform the request
                Console.WriteLine("HealthCheck: Will generate rowkey {0}", rowkey);
                Console.WriteLine("HealthCheck: Sending request");
                var response = (HttpWebResponse) request.GetResponse();

                Console.WriteLine("HealthCheck: Response {0} {1}", (int) response.StatusCode, response.StatusDescription);

                // Wait for the event to appear in the database
                for (;;)
                {
                    // Search for the event
                    Console.WriteLine("HealthCheck: querying for event");

                    var queryCount = CountMatches(rowkey, table, partitionKey);
                    Console.WriteLine("HealthCheck: Found {0} results", queryCount);

                    // Stop once we get a result
                    if (queryCount >= 1)
                    {
                        break;
                    }

                    // Wait a second before trying again
                    Thread.Sleep(1000);
                }

                Console.WriteLine("HealthCheck: request was processed");
            }
            catch (Exception e)
            {
                Console.WriteLine("HealthCheck: failed with exception");
                Console.WriteLine("HealthCheck: {0}", e);
            }
        }

        /// <summary>
        /// Counts the number of matches of a particular key in the Azure table storage
        /// </summary>
        private static int CountMatches(string rowkey, CloudTable table, string partitionKey)
        {
            TableQuery<TableEntity> eventQuery = new TableQuery<TableEntity>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowkey)));

            var queryResult = table.ExecuteQuery(eventQuery);
            var queryCount = queryResult.Count();
            return queryCount;
        }
    }
}
