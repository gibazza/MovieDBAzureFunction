using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using MovieDBconnection.PersonDiscoveryJsonTypes;
using System.Threading.Tasks;

namespace MovieDBconnection
{
    public static class DataTransfer
    {
        private static AZTableHandler _tableHandler;
        private static RESTHandler _peopleDiscovery;
        private static List<dynamic> _personDiscoveryList;
        private const string PARTKEY = "name";
        private const string ROWKEY = "id";

        private static CloudStorageAccount storageAccount;
        private static CloudTableClient tableClient;
        private static CloudTable table;

        private static TableBatchOperation batch;
        private static bool userInTable;


        //TODO: need to determine from call to popular people if it is a:
        //TODO: new person - insert
        //TODO: update to existing person - update
        //TODO: person is no longer in pop people list but is in AS table - delete

        [FunctionName("GetMovieDBPopularPersons")]
        public static async Task RunAsync([TimerTrigger("%timerSchedule%")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

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
            await table.CreateIfNotExistsAsync();

            _tableHandler = new AZTableHandler(table);
            _peopleDiscovery = new RESTHandler(cpAPIKey, cpBaseUri, cpLang, cpLIMIT);
            _personDiscoveryList = _peopleDiscovery.ReadObjects();

            foreach (JObject jPerson in _personDiscoveryList)
            {
                userInTable = false;
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
                    else
                    {
                        person.ETag = "*";
                        person[item.Key] = item.Value.ToString();
                    }
                }

                if (await _tableHandler.ExistInTableAsync(person.PartitionKey, person.RowKey)) { userInTable = true; }

                if (batches.ContainsKey(batchPartition))
                {
                    batch = batches[batchPartition];
                    await InsertOrUpdateAsync(userInTable, person);
                }
                else
                {
                    await InsertOrUpdateAsync(userInTable, person);
                    batches.Add(batchPartition, batch);
                }
            }
            foreach (KeyValuePair<string, TableBatchOperation> b in batches)
            {
                if (b.Value.Count > 0) { 
                await table.ExecuteBatchAsync(b.Value);
            }
        }
    }

    private static async Task InsertOrUpdateAsync(bool presentInTable, Person person)
    {
        if (presentInTable)
        {
            if (await AZTableHandler.hasPersonUpdated(person))
                batch.Merge(person);
        }
        else { batch.Insert(person); }
    }
}
}
