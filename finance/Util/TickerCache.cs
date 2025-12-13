using System;
using System.Collections.Generic;

namespace FalxGroup.Finance.Util
{

public class TickerCache
{
    private readonly object _lock = new object();
    private Dictionary<string, (DateTime Timestamp, string Value)> cache;

    public TickerCache(int timeout /* minutes */ = 15)
    {
        this.cache = new Dictionary<string, (DateTime, string)>();
        this.Timeout = timeout; 
    }

    public int Timeout { get; set; }
    
    private bool IsTimeout(string key) 
    {
        // Use TotalMinutes. 'Minutes' only returns the minute component (0-59).
        // If diff is 65 minutes, .Minutes is 5, which is < Timeout (15), causing a bug.
        return this.Timeout < (DateTime.UtcNow - this.cache[key].Timestamp).TotalMinutes;
    }

    public bool IsExpired(string key)
    {
        lock (_lock)
        {
            if (this.cache.ContainsKey(key))
            {
                return this.IsTimeout(key);
            }
            return true; // Consider expired/invalid if not present
        }
    }

    public bool ContainsKey(string key)
    {
        lock (_lock)
        {
            return this.cache.ContainsKey(key);
        }
    }

    public (DateTime, string)? GetValueOrDefault(string key)
    {
        lock (_lock)
        {
            if (this.cache.ContainsKey(key) && !this.IsTimeout(key))
            {
                return this.cache[key];
            }
            return null;
        }
    }

    public void Add(string key, string value)
    {
        Update(key, value);
    }

    public void Update(string key, string value)
    {
        lock (_lock)
        {
            // Indexer [] handles both Add and Update
            this.cache[key] = (DateTime.UtcNow, value);
        }
    }

} /* end class TickerCache */

} /* end FalxGroup.Finance.Util namespace */