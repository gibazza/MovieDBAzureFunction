using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using MovieDBconnection.PersonDiscoveryJsonTypes;
using System.Threading.Tasks;
using System.Globalization;

//TODO: move anything using Microsoft.WindowsAzure.Storage to AZTableHandler class

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
        private const string INACTIVESTATUS = "I";

        private static CloudStorageAccount storageAccount;
        private static CloudTableClient tableClient;
        private static CloudTable table;

        private static TableBatchOperation batch;
        private static bool personInTable;

        private static ILogger _log;

        private static string connMovieDBString;
        private static string connLogicAppString;
        private static string operationStartDateTime;

        [FunctionName("GetMovieDBPopularPersons")]
        public static async Task RunAsync([TimerTrigger("%timerSchedule%")] TimerInfo myTimer, ILogger log)
        {
            operationStartDateTime = DateTime.Now.ToString();
            _log = log;
            _log.LogInformation(string.Format("{0}: C# Timer trigger function executed", operationStartDateTime));
            connMovieDBString = System.Environment.GetEnvironmentVariable("ASConnString", EnvironmentVariableTarget.Process);
            connLogicAppString = System.Environment.GetEnvironmentVariable("LogicAppTriggerURL", EnvironmentVariableTarget.Process);
            var cpBaseUri = System.Environment.GetEnvironmentVariable("BaseURL", EnvironmentVariableTarget.Process);
            var cpAPIKey = System.Environment.GetEnvironmentVariable("MovieDBAPIKey", EnvironmentVariableTarget.Process);
            var cpLang = System.Environment.GetEnvironmentVariable("Lang", EnvironmentVariableTarget.Process);
            var cpLIMIT = System.Environment.GetEnvironmentVariable("LimitRtnPages", EnvironmentVariableTarget.Process);
            
            var batches = new Dictionary<string, TableBatchOperation>();
            var batchPartition = string.Empty;

            storageAccount = CloudStorageAccount.Parse(connMovieDBString);
            tableClient = storageAccount.CreateCloudTableClient();
            table = tableClient.GetTableReference("tblPeople");
            await table.CreateIfNotExistsAsync();

            _tableHandler = new AZTableHandler(table);
            _peopleDiscovery = new RESTHandler(cpBaseUri, cpAPIKey, cpLang, cpLIMIT);
            _personDiscoveryList = _peopleDiscovery.ReadObjects();

            _log.LogInformation(string.Format("{0}: {1} people read from popular people MovieDB list",
                DateTime.Now, _personDiscoveryList.Count));

            //Mark as deleted people from the table if not popular person list
            //TODO: Put these updates in a batch to be processed with all other changes
            foreach (Person person in await _tableHandler.FilterByStatus(ACTIVESTATUS, -1))
            {
                List<Person> moviePeople = _personDiscoveryList.ConvertAll<Person>(x => new Person(x));
                if (!moviePeople.Exists(x => x.RowKey == person.RowKey))
                {
                    _tableHandler.UpdateRow(person.PartitionKey, person.RowKey, INACTIVESTATUS);
                    _log.LogInformation(string.Format("{0}: Row ID: {1} {2} is now inactive",
                        DateTime.Now, person.RowKey, person.name));
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
                if (b.Value.Count > 0)
                {
                    IList<TableResult> batchresult = await table.ExecuteBatchAsync(b.Value);
                    foreach (var operation in b.Value)
                    {
                        string operationType;
                        switch (operation.OperationType)
                        {
                            case TableOperationType.Insert:
                                operationType = "Insert";
                                break;
                            case TableOperationType.Merge:
                                Person thisPerson = (Person)operation.Entity;
                                if (string.Compare(thisPerson.status, ACTIVESTATUS, true, CultureInfo.InvariantCulture) == 1)
                                    operationType = "Update";
                                else
                                    operationType = "Delete";
                                break;
                            default:
                                throw new Exception(string.Format("Unhandled operation type {0}", operation.OperationType.ToString()));
                        }

                        _log.LogInformation(string.Format("{0}: {1} workflow has been initiated for Row ID: {2}",DateTime.Now, operationType, operation.Entity.RowKey));
                        RESTHandler.PostData(connLogicAppString,
                                    operationStartDateTime,
                                    operationType,
                                    operation.Entity.PartitionKey,
                                    operation.Entity.RowKey);
                    }
                }
            }
        }

        private static async Task InsertOrUpdateAsync(bool presentInTable, Person person)
        {
            if (presentInTable)
            {
                if (await AZTableHandler.hasPersonUpdated(person))
                {
                    batch.Merge(person);
                    _log.LogInformation(string.Format("{0}: Row ID: {1} {2} is updated",
                        DateTime.Now, person.RowKey, person.name));
                }
            }
            else { 
                batch.Insert(person);
                _log.LogInformation(string.Format("{0}: Row ID: {1} {2} is inserted",
                        DateTime.Now, person.RowKey, person.name));
            }
        }
    }
}
