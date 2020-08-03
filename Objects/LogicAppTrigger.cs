using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using MovieDBconnection.PersonDiscoveryJsonTypes;

namespace LogicApp
{

    internal class Trigger
    {

        [JsonProperty("executeDate")]
        public string executeDate { get; set; }

        [JsonProperty("executeType")]
        public string executeType { get; set; }

        [JsonProperty("partitionKey")]
        public string partitionKey { get; set; }

        [JsonProperty("rowKey")]
        public string rowKey { get; set; }
    }

}