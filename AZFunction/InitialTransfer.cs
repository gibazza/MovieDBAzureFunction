using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace MovieDBconnection
{
    public static class InitialTransfer
    {
        private static RESTHandler _peopleDiscovery;
        private static List<dynamic> _personDiscoveryList;
        private const string PARTKEY = "name";
        private const string ROWKEY = "id";

        private static CloudStorageAccount storageAccount;
        private static CloudTableClient tableClient;
        private static CloudTable table;

        private static TableBatchOperation batch;

        [FunctionName("GetMovieDBPopularPersons")]
        public static void Run([TimerTrigger("%timerSchedule%")] TimerInfo myTimer, ILogger log)
        {
            var cpBaseUri = System.Environment.GetEnvironmentVariable("BaseURL", EnvironmentVariableTarget.Process);
            var cpAPIKey = System.Environment.GetEnvironmentVariable("MovieDBAPIKey", EnvironmentVariableTarget.Process);
            var cpLang = System.Environment.GetEnvironmentVariable("Lang", EnvironmentVariableTarget.Process);
            var cpLIMIT = System.Environment.GetEnvironmentVariable("LimitRtnPages", EnvironmentVariableTarget.Process);
            var connString = System.Environment.GetEnvironmentVariable("ASConnString", EnvironmentVariableTarget.Process);
            var batches = new Dictionary<string, TableBatchOperation>();
            var batchPartition = string.Empty;

            storageAccount = CloudStorageAccount.Parse(connString);
            tableClient = storageAccount.CreateCloudTableClient();
            table = tableClient.GetTableReference("tblPeople");
            table.CreateIfNotExistsAsync();

            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            _peopleDiscovery = new RESTHandler(cpAPIKey, cpBaseUri, cpLang, cpLIMIT);
            _personDiscoveryList = _peopleDiscovery.ReadObjects();

            foreach (JObject jPerson in _personDiscoveryList)
            {
                var person = new PersonDiscoveryJsonTypes.Person();
                batch = new TableBatchOperation();
                foreach (KeyValuePair<string, JToken> item in jPerson)
                {
                    if (item.Key.ToLower().Equals(ROWKEY))
                    {
                        person.RowKey = item.Value.ToString();
                    }
                    else if (item.Key.ToLower().Equals(PARTKEY))
                    {
                        batchPartition = item.Value.ToString().Substring(0, 1);
                        person.PartitionKey = batchPartition;
                        person[item.Key] = item.Value.ToString();
                    }
                    else if (item.Key.ToLower().Equals("known_for")) { }
                    else person[item.Key] = item.Value.ToString();
                }

                if (batches.ContainsKey(batchPartition))
                {
                    batch = batches[batchPartition];
                    batch.InsertOrMerge(person);
                }
                else
                {
                    batch.InsertOrMerge(person);
                    batches.Add(batchPartition, batch);
                }
            }
            foreach (KeyValuePair<string, TableBatchOperation> b in batches)
            {
                table.ExecuteBatchAsync(b.Value);
            }
        }
    }
}
