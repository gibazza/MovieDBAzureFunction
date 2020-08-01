using Microsoft.WindowsAzure.Storage.Table;
using MovieDBconnection.PersonDiscoveryJsonTypes;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace MovieDBconnection
{
    public class AZTableHandler
    {
        private static CloudTable _tableToQuery;
        public CloudTable tableToQuery
        {
            get => _tableToQuery;
            set => _tableToQuery = value;
        }

        public AZTableHandler()
        {
        }

        public AZTableHandler(CloudTable tableToQuery)
        {
            _tableToQuery = tableToQuery;
        }

        public async Task<bool> ExistInTableAsync(string partitionKey, string rowKey)
        {
            bool rtnValue = false;
            IList<Person> resultList = await FilterByPartRowKey(partitionKey, rowKey, 1);
            int resultCount = resultList.Count;
            if (resultCount == 1) { rtnValue = true; }
            return rtnValue;
        }

        public static async Task<bool> hasPersonUpdated(Person AZTablePerson)
        {
            bool rtnValue = false;

            IList<Person> resultList = await FilterByPartRowKey(AZTablePerson.PartitionKey, AZTablePerson.RowKey, 1);
            foreach (KeyValuePair<string, dynamic> attribute in AZTablePerson)
            {
                if (attribute.Key.ToLower() == "timestamp" ||
                   attribute.Key.ToLower() == "etag") { }
                else if (resultList[0].GetType().GetProperty(attribute.Key) != null)
                {
                    if (string.Compare((string)attribute.Value, resultList[0].GetType().GetProperty(attribute.Key).GetValue(resultList[0]).ToString(), true, CultureInfo.InvariantCulture) != 0)
                    {
                        return true;
                    }
                }
                else if (attribute.Value != resultList[0].GetType().GetProperty(attribute.Key).ToString())
                {
                    return true;
                }
            }
            return rtnValue;
        }

        public async void UpdateRow(string partitionKey, string rowKey, string newMessage)
        {
            TableOperation retrieve = TableOperation.Retrieve<Person>(partitionKey, rowKey);

            TableResult result = await _tableToQuery.ExecuteAsync(retrieve);

            Person person = (Person)result.Result;

            person.ETag = "*";
            person.status = newMessage;

            if (result != null)
            {
                TableOperation update = TableOperation.Replace(person);

                await _tableToQuery.ExecuteAsync(update);
            }

        }

        public async Task<Person> GetPerson(string partitionKey, string rowKey)
        {
            TableOperation retrieve = TableOperation.Retrieve<Person>(partitionKey, rowKey);
            TableResult result = await _tableToQuery.ExecuteAsync(retrieve);
            return (Person)result.Result;
        }

        public async Task<IList<Person>> AllRows()
        {
            return await RunQuery(new TableQuery<Person>());
        }

        public async Task<IList<Person>> FilterByStatus(string status, int top)
        {
            TableQuery<Person> query = new TableQuery<Person>().Where(
                    TableQuery.GenerateFilterCondition("status", QueryComparisons.Equal, status)
                );
            if (top > 0)
            {
                query.Take(top);
            }

            return await RunQuery(query);
        }

        public static async Task<IList<Person>> FilterByPartRowKey(string partitionKey, string rowKey, int top)
        {
            TableQuery<Person> query = new TableQuery<Person>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                        TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey)
                ));
            if (top > 0)
            {
                query.Take(top);
            }


            return await RunQuery(query);
        }

        private static async Task<IList<Person>> RunQuery(TableQuery<Person> query)
        {
            List<Person> rntValue;
            TableQuerySegment<Person> querySegment = null;
            do
            {
                querySegment = await _tableToQuery.ExecuteQuerySegmentedAsync(query, querySegment?.ContinuationToken);
                rntValue = querySegment.Results.ToList();
            } while (querySegment.ContinuationToken != null);
            return rntValue;
        }
    }
}