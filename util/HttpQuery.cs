﻿using System.Net;

namespace cc.net
{
    public class HttpQuery
    {
        private HttpClient httpClient = null!;

        public HttpQuery()
        {
            HttpClientHandler handler = new HttpClientHandler()
            {
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
            };
            this.httpClient = new HttpClient(handler);
        }

        public HttpQuery(string uriBaseAddress)
        {
            HttpClientHandler handler = new HttpClientHandler()
            {
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
            };
            this.httpClient = new HttpClient(handler)
            {
                BaseAddress = new Uri(uriBaseAddress),
            };
        }

        public string BaseAddress 
        { 
            get
            {
                return this.httpClient.BaseAddress?.ToString() ?? string.Empty;
            }

            set 
            {
                this.httpClient.BaseAddress = new Uri(value.Trim());
            }
        }

        public async Task<string> GetAsync(string queryString)
        {
            using HttpResponseMessage response = await this.httpClient.GetAsync($"{queryString.Trim()}");
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync();
        }

        public async Task<string> GetStringAsync(string queryString)
        {
            return await this.httpClient.GetStringAsync($"{queryString.Trim()}");
        }

        public async Task<string> GetValueAsync(string queryString, string startLabel, string endLabel)
        {
            try
            {
                var bodyResponse = await this.GetStringAsync(queryString);
                
                int startTagIndex = bodyResponse.IndexOf(startLabel);
                if (startTagIndex == -1)
                {
                    return string.Empty;
                }

                int startIndex = startTagIndex + startLabel.Length;
                int endIndex = bodyResponse.IndexOf(endLabel, startIndex);

                if (endIndex == -1)
                {
                    return string.Empty;
                }

                return bodyResponse.Substring(startIndex, endIndex - startIndex).Trim();   
            }
            catch (HttpRequestException)
            {
                return string.Empty;
            }
        }
    } /* end class HttpQuery */
} /* end cc.net namespace */