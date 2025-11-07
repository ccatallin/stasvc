using System;
using System.Collections.Generic;
using System.Threading;

namespace FalxGroup.Finance.Util
{

public class TickerCache
{
    private Mutex cacheAccessControl;        
    private Dictionary<string, Tuple<DateTime, string>> cache;

    public TickerCache(int timeout /* minutes */ = 15)
    {
        this.cache = new Dictionary<string, Tuple<DateTime, string>>();
        this.cacheAccessControl = new System.Threading.Mutex();
        this.Timeout = timeout; 
    }

    public int Timeout { get; set; }
    private bool IsTimeout(string key) => this.Timeout < DateTime.Now.Subtract(this.cache[key].Item1).Minutes;    

    public bool IsExpired(string key)
    {
        bool expired = false;

        try
        {
            cacheAccessControl.WaitOne();

            if (this.IsTimeout(key))
            {
                expired = true;
            }
        }
        catch (Exception)
        {
            // not interested in error informations here
        }
        finally
        {
            cacheAccessControl.ReleaseMutex();
        }

        return expired;        
    }

    public bool ContainsKey(string key)
    {
        bool exists = false;

        try
        {
            cacheAccessControl.WaitOne();
            exists = this.cache.ContainsKey(key);
        }
        catch (Exception)
        {
            // not interested in error informations here
        }
        finally
        {
            cacheAccessControl.ReleaseMutex();
        }

        return exists;
    }

    public Tuple<DateTime, string>? GetValueOrDefault(string key)
    {
        try
        {
            cacheAccessControl.WaitOne();

            if (!this.IsTimeout(key))
            {
                if (this.cache.TryGetValue(key, out var value))
                {
                    return value;
                }
            }
        }
        catch (Exception)
        {
            // not interested in error informations here
        }
        finally
        {
            cacheAccessControl.ReleaseMutex();
        }

        return null;
    }

    public void Add(string key, string value)
    {
        try
        {
            cacheAccessControl.WaitOne();
            this.cache.Add(key, new Tuple<DateTime, string>(DateTime.Now, value));
        }
        catch (Exception)
        {
            // not interested in informations for errors here
        }
        finally
        {
            cacheAccessControl.ReleaseMutex();
        }
    }

    public void Update(string key, string value)
    {
        try
        {
            cacheAccessControl.WaitOne();
            this.cache[key] = new Tuple<DateTime, string>(DateTime.Now, value);
        }
        catch (Exception)
        {
            // not interested in informations for errors here
        }
        finally
        {
            cacheAccessControl.ReleaseMutex();
        }
    }

} /* end class TickerCache */

} /* end FalxGroup.Finance.Util namespace */