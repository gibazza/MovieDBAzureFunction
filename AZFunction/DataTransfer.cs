using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using MovieDBconnection.PersonDiscoveryJsonTypes;
using System.Threading.Tasks;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using Newtonsoft.Json;

namespace MovieDBconnection
{
    public static class DataTransfer
    {
        private static AZTableHandler _tableHandler;
        private static RESTHandler _peopleDiscovery;
        private static List<dynamic> _personDiscoveryList;
        private const string PARTKEY = "name";
        private const string ROWKEY = "id";
        private const string ACTIVESTATUS = "A";
        private const string INACTIVESTATUS = "D";

        private static CloudStorageAccount storageAccount;
        private static CloudTableClient tableClient;
        private static CloudTable table;

        private static TableBatchOperation batch;
        private static bool personInTable;


        //TODO: need to determine from api call to popular people if it is a:
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

            //Mark as deleted people from the table if not popular person list
            foreach (Person person in await _tableHandler.AllRows())
            {
                List<Person> moviePeople = _personDiscoveryList.ConvertAll<Person>(x => new Person(x));
                if (!moviePeople.Exists(x => x.RowKey == person.RowKey))
                {
                    _tableHandler.UpdateRow(person.PartitionKey, person.RowKey, INACTIVESTATUS);
                }
            }

            //Insert or Update people in the popular person list
            foreach (JObject jPerson in _personDiscoveryList)
            {
                personInTable = false;
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
                        person.status = ACTIVESTATUS;
                    }
                }

                if (await _tableHandler.ExistInTableAsync(person.PartitionKey, person.RowKey)) { personInTable = true; }

                if (batches.ContainsKey(batchPartition))
                {
                    batch = batches[batchPartition];
                    await InsertOrUpdateAsync(personInTable, person);
                }
                else
                {
                    await InsertOrUpdateAsync(personInTable, person);
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
