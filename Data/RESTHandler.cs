using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace MovieDBconnection
{
    public class RESTHandler
    {
        private string _baseURL;
        private string _uriPopularPersonObject = "person/popular?";
        private string _uriApiKey = "api_key={0}";
        private string _uriLanguage = "language={0}";
        private string _uriPage = "page={0}";
        private int _noOfPages = 0;

        /*private string _uriMovieDiscovery = "discover/movie?";
        private string _uriMovieObject = "movie/{0}?";
        private string _uriMovieCreditsObject = "movie/{0}/credits?";
        private string _uriPersonObject = "person/{0}?";*/

        public RESTHandler(string apiKey, string restBaseURI, string lang, string noPages = null, string proxyServer = null)
        {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            if (!string.IsNullOrEmpty(noPages))
            {
                _noOfPages = Convert.ToInt32(noPages);
            }
            if (!String.IsNullOrEmpty(proxyServer) && proxyServer.Contains(":"))
            {
                string[] proxyParts = proxyServer.Split(':');
                if (proxyParts.Length > 2)
                {
                    WebRequest.DefaultWebProxy = new WebProxy(proxyParts[0] + ":" + proxyParts[1], Convert.ToInt32(proxyParts[2]));
                }
                else
                {
                    WebRequest.DefaultWebProxy = new WebProxy(proxyParts[0], Convert.ToInt32(proxyParts[1]));
                }
            }
            _uriApiKey = string.Format(_uriApiKey, apiKey);
            _uriLanguage = string.Format(_uriLanguage, lang);
            _baseURL = restBaseURI;
        }
        public List<dynamic> ReadObjects()
        {
            string jsonRequestBody = string.Empty;
            Uri PopularPersonUri;
            Dictionary<string, string> attributes = new Dictionary<string, string>();

            PopularPersonUri = new Uri(string.Format("{0}{1}&{2}&{3}", _baseURL, _uriPopularPersonObject, _uriLanguage, _uriApiKey));

            return _GetDiscoveryResults(PopularPersonUri);
        }
        private List<dynamic> _GetDiscoveryResults(Uri discoveryUri)
        {
            String nextPageUri = string.Empty;
            int totalResults = 0;
            int totalPages = 2;
            string jsonResponseBody = string.Empty;
            KeyValuePair<HttpStatusCode, string> discoveryResponse;
            string errMessage = string.Empty;
            JObject deSerializedResults;
            List<dynamic> rtnValue = new List<dynamic>();
            List<dynamic> pageResults = new List<dynamic>();

            for (int currentPage = 1; currentPage < totalPages; currentPage++)
            {
                nextPageUri = discoveryUri.AbsoluteUri + "&" + String.Format(_uriPage, currentPage.ToString());
                discoveryResponse = RESTReadObject(nextPageUri, "application/json", out jsonResponseBody, "get");

                switch ((int)discoveryResponse.Key)
                {
                    case 200:
                        deSerializedResults = JsonConvert.DeserializeObject<JObject>(jsonResponseBody);
                        if ((Convert.ToInt32(deSerializedResults["total_pages"].ToString()) > _noOfPages))
                        { totalPages = _noOfPages + 1; }
                        totalResults = Convert.ToInt32(deSerializedResults["total_results"].ToString());
                        pageResults = deSerializedResults["results"].ToList<dynamic>();
                        rtnValue.AddRange(pageResults);
                        break;
                    default:
                        errMessage = String.Format("{0} {1} failed with HTTP status code: {2}. Status message: {3}. Body: {4}", "Popular People", "Discovery",
                            (int)discoveryResponse.Key, discoveryResponse.Value.ToString(), string.IsNullOrEmpty(jsonResponseBody) ? "" : jsonResponseBody);
                        throw new Exception(errMessage);
                }
            }
            return rtnValue;
        }
        public static KeyValuePair<HttpStatusCode, string> RESTReadObject(string URL, string jsonContentType, out string jsonResponse, string method)
        {
            KeyValuePair<HttpStatusCode, string> httpReturn;
            JToken responses;
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(URL);

            request.Method = method;
            request.Accept = jsonContentType;
            request.ContentType = jsonContentType;
            httpReturn = _ParseResponse(request, out responses);

            if (responses != null)
            {
                jsonResponse = responses.ToString();
            }
            else
            {
                jsonResponse = "";
            }
            return httpReturn;
        }
        private static KeyValuePair<HttpStatusCode, string> _ParseResponse(HttpWebRequest request, out JToken token)
        {
            KeyValuePair<HttpStatusCode, string> rtnValue;
            try
            {
                HttpWebResponse httpResponse = (HttpWebResponse)request.GetResponse();
                Stream responseStream = httpResponse.GetResponseStream();
                rtnValue = new KeyValuePair<HttpStatusCode, string>((HttpStatusCode)httpResponse.StatusCode, httpResponse.StatusDescription);
                StreamReader reader = new StreamReader(responseStream, Encoding.GetEncoding("utf-8"));
                string respString = reader.ReadToEnd();
                if (!String.IsNullOrEmpty(respString.Trim()))
                {
                    token = JValue.Parse(respString);
                }
                else
                {
                    token = null;
                }
                reader.Close();
                reader.Dispose();
                responseStream.Close();
                responseStream.Dispose();
                httpResponse.Close();

            }
            catch (WebException ex)
            {
                if (ex.Response != null)
                {
                    var resp = new StreamReader(ex.Response.GetResponseStream()).ReadToEnd();
                    token = JValue.Parse(resp);
                    string statusCode = string.Empty;//((HttpWebResponse)ex.Response).StatusCode.ToString();
                    string message = string.Empty;
                    StringBuilder TokenContents = new StringBuilder(string.Empty);
                    foreach (var tokenValue in token)
                    {
                        TokenContents.Append(tokenValue.ToString() + " ");
                    }
                    rtnValue = new KeyValuePair<HttpStatusCode, string>(((HttpWebResponse)ex.Response).StatusCode, ((HttpWebResponse)ex.Response).StatusDescription);
                }
                else
                    throw ex;
            }
            return rtnValue;
        }
    }
}
