﻿// Generated by Xamasoft JSON Class Generator
// http://www.xamasoft.com/json-class-generator

using System;
using Newtonsoft.Json;
using System.Collections;
using System.Reflection;
using Newtonsoft.Json.Linq;

namespace MovieDBconnection.MovieDiscoveryJsonTypes
{

    internal class Result : IEnumerable
    {

        [JsonProperty("vote_count")]
        public Int64 vote_count { get; set; }

        [JsonProperty("id")]
        public int id { get; set; }

        [JsonProperty("video")]
        public bool video { get; set; }

        [JsonProperty("vote_average")]
        public double vote_average { get; set; }

        [JsonProperty("title")]
        public string title { get; set; }

        [JsonProperty("popularity")]
        public double popularity { get; set; }

        [JsonProperty("poster_path")]
        public string poster_path { get; set; }

        [JsonProperty("original_language")]
        public string original_language { get; set; }

        [JsonProperty("original_title")]
        public string original_title { get; set; }

        [JsonProperty("genre_ids")]
        public int[] genre_ids { get; set; }

        [JsonProperty("backdrop_path")]
        public string backdrop_path { get; set; }

        [JsonProperty("adult")]
        public bool adult { get; set; }

        [JsonProperty("overview")]
        public string overview { get; set; }

        [JsonProperty("release_date")]
        public string release_date { get; set; }

        [JsonProperty("media_type")]
        public string media_type { get; set; }

        [JsonProperty("original_name")]
        public string original_name { get; set; }

        [JsonProperty("name")]
        public string name { get; set; }

        [JsonProperty("first_air_date")]
        public string first_air_date { get; set; }

        public object this[string propertyName]
        {
            get
            {
                Type myType = typeof(Result);
                PropertyInfo myPropInfo = myType.GetProperty(propertyName);
                return myPropInfo.GetValue(this, null);
            }
            set
            {
                Type myType = typeof(Result);
                PropertyInfo myPropInfo = myType.GetProperty(propertyName);
                myPropInfo.SetValue(this, value);
            }
        }

        public IEnumerator GetEnumerator()
        {
            Type myType = typeof(Result);
            PropertyInfo[] myProps = myType.GetProperties();
            return myProps.GetEnumerator();
        }
    }

}
