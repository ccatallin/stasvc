﻿using System.Net;
using System.Text.Json;

namespace cc.net
{
    public class HttpQuery
    {
        private const string userAgentHeader = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36";
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
            this.httpClient.DefaultRequestHeaders.Clear();
            this.httpClient.DefaultRequestHeaders.Add("User-Agent", HttpQuery.userAgentHeader);

            using HttpResponseMessage response = await this.httpClient.GetAsync($"{queryString.Trim()}");
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync();
        }

        public async Task<string> GetStringAsync(string queryString)
        {
            this.httpClient.DefaultRequestHeaders.Clear();
            this.httpClient.DefaultRequestHeaders.Add("User-Agent", HttpQuery.userAgentHeader);
            
            return await this.httpClient.GetStringAsync($"{queryString.Trim()}");
        }

        public async Task<JsonDocument> GetJSONAsync(string queryString)
        {
            this.httpClient.DefaultRequestHeaders.Clear();
            this.httpClient.DefaultRequestHeaders.Add("User-Agent", HttpQuery.userAgentHeader);
            
            var stream = await this.httpClient.GetStreamAsync($"{queryString.Trim()}");
            return await JsonDocument.ParseAsync(stream);
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
                // Console.WriteLine($"Exception: {exception.Message}");
                return string.Empty;
            }
        }

        public async Task<string> GetValueAsync(string queryString, string valueLabel)
        {
            try
            {
                using var bodyResponse = await this.GetJSONAsync(queryString);
                
                // Navigate Yahoo Finance JSON structure: chart -> result -> [0] -> meta -> valueLabel
                if (bodyResponse.RootElement.TryGetProperty("chart", out JsonElement chart) &&
                    chart.TryGetProperty("result", out JsonElement result) &&
                    result.ValueKind == JsonValueKind.Array &&
                    result.GetArrayLength() > 0)
                {
                    if (result[0].TryGetProperty("meta", out JsonElement meta) &&
                        meta.TryGetProperty(valueLabel, out JsonElement value))
                    {
                        return value.ToString();
                    }
                }
                return string.Empty;
            }
            catch (Exception exception)
            {
                Console.WriteLine($"Exception: {exception.Message}");
                return string.Empty;
            }
        }
    } /* end class HttpQuery */
} /* end cc.net namespace */