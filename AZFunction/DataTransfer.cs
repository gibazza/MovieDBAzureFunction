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
        private static List<string> _arrtibutesToSkip = new List<string>() { "known_for", "profile_path", "adult", "popularity", ROWKEY };
        private const string PARTKEY = "name";
        private const string ROWKEY = "id";
        private const string ACTIVESTATUS = "A";
        private const string INACTIVESTATUS = "I";

        private static CloudStorageAccount storageAccount;
        private static CloudTableClient tableClient;
        private static CloudTable table;

        private static TableBatchOperation batch;
        private static Dictionary<string, TableBatchOperation> batches;

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
            connMovieDBString = Environment.GetEnvironmentVariable("ASConnString", EnvironmentVariableTarget.Process);
            connLogicAppString = Environment.GetEnvironmentVariable("LogicAppTriggerURL", EnvironmentVariableTarget.Process);
            var cpBaseUri = Environment.GetEnvironmentVariable("BaseURL", EnvironmentVariableTarget.Process);
            var cpAPIKey = Environment.GetEnvironmentVariable("MovieDBAPIKey", EnvironmentVariableTarget.Process);
            var cpLang = Environment.GetEnvironmentVariable("Lang", EnvironmentVariableTarget.Process);
            var cpLIMIT = Environment.GetEnvironmentVariable("LimitRtnPages", EnvironmentVariableTarget.Process);

            batches = new Dictionary<string, TableBatchOperation>();

            storageAccount = CloudStorageAccount.Parse(connMovieDBString);
            tableClient = storageAccount.CreateCloudTableClient();
            table = tableClient.GetTableReference("tblPeople");
            await table.CreateIfNotExistsAsync();

            _tableHandler = new AZTableHandler(table);
            _peopleDiscovery = new RESTHandler(cpBaseUri, cpAPIKey, cpLang, cpLIMIT);
            _personDiscoveryList = _peopleDiscovery.ReadObjects();

            _log.LogInformation(string.Format("{0}: {1} people read from popular people MovieDB list",
                DateTime.Now, _personDiscoveryList.Count));

            await FindActivePeopleToInactivate();

            await InsertOrUpdatePeople();

            await ProcessBatches();
        }

        private static async Task FindActivePeopleToInactivate()
        {
            foreach (Person person in await _tableHandler.FilterByStatus(ACTIVESTATUS, -1))
            {
                List<Person> moviePeople = _personDiscoveryList.ConvertAll<Person>(x => new Person(x));
                if (!moviePeople.Exists(x => x.RowKey == person.RowKey))
                {
                    person.status = INACTIVESTATUS;
                    AddtoBatchesAsync(person,true);
                    _log.LogInformation(string.Format("{0}: Row ID: {1} {2} is now inactive",
                        DateTime.Now, person.RowKey, person.name));
                }
            }
        }

        private static async Task InsertOrUpdatePeople()
        {
            Person person;
            bool personInTable;
            bool personUpdated;
            //Insert or Update people in the popular person list
            foreach (JObject jPerson in _personDiscoveryList)
            {
                string RowKey = jPerson.Property(ROWKEY).Value.ToString();
                string PartitionKey = jPerson.Property("name").Value.ToString().Substring(0, 1);
                personUpdated = false;
                personInTable = false;

                //Is this a new or exisitng person in the table
                if (await _tableHandler.ExistInTableAsync(PartitionKey, RowKey))
                {
                    personInTable = true;
                    //Get the persons entry from the table
                    person = await _tableHandler.GetPerson(PartitionKey, RowKey);

                    //It is potentially an update. Check if anything has actually changed form what is held in the table
                    foreach (KeyValuePair<string, JToken> item in jPerson)
                    {
                         if (!_arrtibutesToSkip.Contains(item.Key))
                        {
                            //TODO: Need to figure out how to handle inherited properties in the get properties operator in Person class
                            if (string.Compare(person.GetType().GetProperty(item.Key).GetValue(person).ToString(), item.Value.ToString(), true, CultureInfo.InvariantCulture) != 0)
                            {
                                personUpdated = true;
                                person[item.Key] = item.Value.ToString();
                            }
                        }
                    }
                    if (person.status == INACTIVESTATUS)
                    {
                        personUpdated = true;
                    }
                }
                else
                {
                    //A new person being added
                    person = new Person();
                    personUpdated = true;
                    foreach (KeyValuePair<string, JToken> item in jPerson)
                    {
                        if (item.Key.ToLower().Equals(ROWKEY))
                        {
                            person.RowKey = item.Value.ToString();
                        }
                        else if (item.Key.ToLower().Equals(PARTKEY))
                        {
                            //Adds partion key dirved from name and name to table
                            person.PartitionKey = item.Value.ToString().Substring(0, 1);
                            person[item.Key] = item.Value.ToString();
                        }
                        else if (item.Key.ToLower().Equals("known_for"))
                        {
                            //skips known_for
                        }
                        else
                        {
                            person[item.Key] = item.Value.ToString();
                        }
                    }
                }
                person.status = ACTIVESTATUS;
                person.ETag = "*";
                if (personUpdated) { AddtoBatchesAsync(person, personInTable); }
            }
        }

        private static void AddtoBatchesAsync(Person person, bool personInTable)
        {
            string batchPartition = person.PartitionKey;
            batch = new TableBatchOperation();

            //Is this a new or exisitng person in the table
            //if (await _tableHandler.ExistInTableAsync(person.PartitionKey, person.RowKey)) { personInTable = true; }

            //if the batch with partition key already exists in the batchs dictionary add this as another job to that batch with the same partionion key
            if (batches.ContainsKey(batchPartition))
            {
                batch = batches[batchPartition];
                InsertOrUpdate(personInTable, person);
            }
            //else the batch with partition key doesn't exist in the batchs dictionary add this as the first job to that batch with partionion key
            else
            {
                InsertOrUpdate(personInTable, person);
                batches.Add(batchPartition, batch);
            }
        }

        private static void InsertOrUpdate(bool presentInTable, Person person)
        {
            if (presentInTable)
            {
                //if (await AZTableHandler.hasPersonUpdated(person))
                //{
                batch.Merge(person);
                _log.LogInformation(string.Format("{0}: Row ID: {1} {2} is updated",
                    DateTime.Now, person.RowKey, person.name));
                //}
            }
            else
            {
                batch.Insert(person);
                _log.LogInformation(string.Format("{0}: Row ID: {1} {2} is inserted",
                        DateTime.Now, person.RowKey, person.name));
            }
        }

        private static async Task ProcessBatches()
        {
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
                                if (string.Compare(thisPerson.status, ACTIVESTATUS, true, CultureInfo.InvariantCulture) == 0)
                                    operationType = "Update";
                                else
                                    operationType = "Delete";
                                break;
                            default:
                                throw new Exception(string.Format("Unhandled operation type {0}", operation.OperationType.ToString()));
                        }

                        _log.LogInformation(string.Format("{0}: {1} workflow has been initiated for Row ID: {2}", DateTime.Now, operationType, operation.Entity.RowKey));
                        RESTHandler.PostData(connLogicAppString,
                                    operationStartDateTime,
                                    operationType,
                                    operation.Entity.PartitionKey,
                                    operation.Entity.RowKey);
                    }
                }
            }
        }
    }
}
