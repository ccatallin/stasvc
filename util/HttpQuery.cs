using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace cc.net
{

public class HttpQuery
{
	private HttpClient httpClient = null;

    public HttpQuery(string uriBaseAddress)
    {
        this.httpClient = new()
        {
            BaseAddress = new Uri(uriBaseAddress),
        };
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

}       /* end class HttpQuery */

}       /* end cc.net namespace */