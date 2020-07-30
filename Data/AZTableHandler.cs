using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using MovieDBconnection.PersonDiscoveryJsonTypes;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using System.ComponentModel.DataAnnotations.Schema;
using System.Xml;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using System.Globalization;

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
            IList<DynamicTableEntity> resultList = await RunQuery(partitionKey, rowKey, 1);
            int resultCount = resultList.Count;
            if (resultCount == 1) { rtnValue = true; }
            return rtnValue;
        }

        public static async Task<bool> hasPersonUpdated(Person AZTablePerson)
        {
            bool rtnValue = false;
            IList<DynamicTableEntity> resultList = await RunQuery(AZTablePerson.PartitionKey, AZTablePerson.RowKey, 1);
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
                else if (attribute.Value != resultList[0].Properties[attribute.Key].StringValue)
                {
                    return true;
                }
            }
            return rtnValue;
        }

        private static async Task<IList<DynamicTableEntity>> RunQuery(string partitionKey, string rowKey, int top)
        {
            IList<DynamicTableEntity> rntValue;
            TableQuerySegment querySegment = null;

            TableQuery query = new TableQuery().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                        TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey)
                ));
            query.Take(top);

            do
            {
                querySegment = await _tableToQuery.ExecuteQuerySegmentedAsync(query, querySegment?.ContinuationToken);
                rntValue = querySegment.Results;
            } while (querySegment.ContinuationToken != null);
            return rntValue;
        }
    }
}