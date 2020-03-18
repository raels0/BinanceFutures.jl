module BinanceFutures

import HTTP, SHA, JSON, Dates
using DataFrames, Distributed, Match

# futures base URLs
BINANCE_API_REST = "https://fapi.binance.com/"
BINANCE_API_TICKER = string(BINANCE_API_REST, "fapi/v1/ticker/")
BINANCE_API_KLINES = string(BINANCE_API_REST, "fapi/v1/klines")

function apiKeys()
    apiKey = get(ENV, "BINANCE_APIKEY", "")
    apiSecret = get(ENV, "BINANCE_SECRET", "")

    @assert apiKey != "" || apiSecret != "" "BINANCE_APIKEY/BINANCE_APISECRET should be present in the environment dictionary ENV"

    apiKey, apiSecret
end

# get time in Binance form (unix ms)
timestampNow() = Int64(floor(Dates.datetime2unix(Dates.now(Dates.UTC)) * 1000))

# Simple test if binance API is online
ping() = HTTP.request("GET", string(BINANCE_API_REST, "fapi/v1/ping")).status

# convenience wrapper for parsing response body
r2j = r -> JSON.parse(String(r))

# Binance servertime
function serverTime()
    r = HTTP.request("GET", string(BINANCE_API_REST, "fapi/v1/time"))
    Dates.unix2datetime(r2j(r.body)["serverTime"] / 1000)
end

function getSymbols()
    r = HTTP.request("GET",string(BINANCE_API_REST, "fapi/v1/exchangeInfo"))
    [x["symbol"] for x in r2j(r.body)["symbols"]]
end

function getLimit(type="REQUEST_WEIGHT")
    r = HTTP.request("GET", string(BINANCE_API_REST, "fapi/v1/exchangeInfo"))
    filter(x -> x["rateLimitType"] == type, r2j(r.body)["rateLimits"])[1]
end

# Get kline and process to df
function genQueries(symbol, startDateTime, endDateTime, interval, limit)
    query = string("?symbol=", symbol, "&interval=", interval, "&limit=", limit)

    startTime = Dates.datetime2unix(startDateTime)
    endTime = Dates.datetime2unix(endDateTime)

    # convert unit to number of seconds (for unix time)
    unit = @match interval[lastindex(interval)] begin
        'm' => parse(Int, interval[1:lastindex(interval)-1])*60
        'h' => parse(Int, interval[1:lastindex(interval)-1])*3600
        'd' => parse(Int, interval[1])*3600*24
        'w' => parse(Int, interval[1])*3600*24*7
        _ => throw(ArgumentError("Invalid interval specified, cannot get unit."))
    end

    # breakup query to meet limit
    timespan = collect(startTime:(unit*limit):endTime)
    if timespan[length(timespan)] != endTime
        timespan = append!(timespan, endTime)
    end
    queries = Array{String}(undef, length(timespan)-1)
    for i = 1:length(queries)
        queries[i] = query*"&startTime=" *
                        string(convert(Int64, timespan[i]*1000))*"&endTime=" * 
                        string(convert(Int64, timespan[i+1]*1000))
        timespan[i+1] += unit  # avoid overlap
    end
    return queries
end

function getKlines(symbol; startDateTime=Dates.now()-Day(1), endDateTime=startDateTime+Day(1),
    interval="1m", limit=1500)
    if (limit > 1500) throw(ErrorException("Maximum API limit is 1500.")) end
    if (endDateTime <= startDateTime)
        throw(ArgumentError("End time is before or equal to start time."))
    end

    queries = genQueries(symbol, startDateTime, endDateTime, interval, limit)
    # TODO: check if requests need to be spaced out to prevent rate limit
    data = pmap(x -> r2j(HTTP.request("GET", BINANCE_API_KLINES*x).body), queries)
    
    df = DataFrame()
    for frame in data
        # data returned as string, convert to float, hcat then transpose
        result = hcat(map(z -> map(x -> typeof(x) == String ?
                          parse(Float64, x) : convert(Float64, x), z), frame)...)'
        size(result,2) == 0 && return nothing

        symbolColumnData = map(x -> symbol, collect(1:size(result, 1)))
        df = vcat(df, DataFrame([symbolColumnData, Dates.unix2datetime.(result[:,1]/1000),
            result[:,2], result[:,3], result[:,4], result[:,5], result[:,6], result[:,8],
            Dates.unix2datetime.(result[:,7] / 1000), result[:,9], result[:,10], result[:,11]],
            [:symbol, :startDate, :open, :high, :low, :close, :volume, :quoteAVolume,
            :endDate, :trades, :tbBaseAVolume, :tbQuoteAVolume]))
    end
    return df
end

end # module
