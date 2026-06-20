using System.Collections.Concurrent;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://localhost:5090");
builder.Logging.AddConsole();

var app = builder.Build();
var accountStore = new AccountStore(app.Logger);

// ─── Health ────────────────────────────────────────────────────────────────
app.MapGet("/api/status", () =>
{
    var statuses = accountStore.GetAllStatus();
    return Results.Ok(new { status = "ok", accounts = statuses });
});



// ─── Account CRUD ──────────────────────────────────────────────────────────




app.MapPost("/api/accounts", (AccountConfig cfg) =>
{
    if (string.IsNullOrWhiteSpace(cfg.Id))
        return Results.BadRequest("id is required");
    var ok = accountStore.AddAccount(cfg);
    return ok ? Results.Ok(new { added = cfg.Id }) : Results.Conflict($"Account {cfg.Id} already exists");
});

app.MapDelete("/api/accounts/{id}", (string id) =>
{
    var ok = accountStore.RemoveAccount(id);
    return ok ? Results.Ok(new { removed = id }) : Results.NotFound();
});

// ─── Connect / Disconnect ──────────────────────────────────────────────────
app.MapPost("/api/accounts/{id}/connect", async (string id) =>
{
    var (ok, err) = await accountStore.ConnectAsync(id);
    return ok ? Results.Ok(new { connected = id }) : Results.BadRequest(new { error = err });
});

app.MapPost("/api/accounts/{id}/disconnect", (string id) =>
{
    var ok = accountStore.Disconnect(id);
    return ok ? Results.Ok(new { disconnected = id }) : Results.NotFound();
});

// ─── Account Info ──────────────────────────────────────────────────────────
app.MapGet("/api/accounts/{id}/info", (string id) =>
{
    var info = accountStore.GetAccountInfo(id);
    return info != null ? Results.Ok(info) : Results.NotFound();
});

app.MapGet("/api/accounts/{id}/positions", (string id) =>
{
    var positions = accountStore.GetPositions(id);
    return positions != null ? Results.Ok(positions) : Results.NotFound();
});

app.MapGet("/api/accounts/{id}/import", (string id, string? pair, string? comment) =>
{
    var positions = accountStore.GetPositionsForImport(id, pair ?? "", comment ?? "");
    return positions != null ? Results.Ok(positions) : Results.NotFound();
});

// ─── Quotes ────────────────────────────────────────────────────────────────
app.MapGet("/api/accounts/{id}/quote/{symbol}", (string id, string symbol) =>
{
    var quote = accountStore.GetQuote(id, symbol);
    return quote != null ? Results.Ok(quote) : Results.NotFound();
});

app.MapGet("/api/accounts/{id}/swaps", (string id, string? symbols) =>
{
    var syms = (symbols ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries);
    var result = accountStore.GetSwapRates(id, syms);
    return result != null ? Results.Ok(result) : Results.NotFound();
});

// ─── Trade Execution ───────────────────────────────────────────────────────
app.MapPost("/api/accounts/{id}/order", (string id, OrderRequest req) =>
{
    var result = accountStore.SendMarketOrder(id, req);
    return result != null ? Results.Ok(result) : Results.NotFound();
});

app.MapPost("/api/accounts/{id}/close", (string id, CloseRequest req) =>
{
    var result = accountStore.ClosePosition(id, req);
    return result != null ? Results.Ok(result) : Results.NotFound();
});

// ─── Deal History ──────────────────────────────────────────────────────────
app.MapGet("/api/accounts/{id}/history", (string id, long from, long to, bool? exclude_balance, string? fee_keywords) =>
{
    var keywords = string.IsNullOrEmpty(fee_keywords)
        ? Array.Empty<string>()
        : fee_keywords.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    var result = accountStore.GetDealHistory(id, from, to, exclude_balance ?? true, keywords);
    return result != null ? Results.Ok(result) : Results.NotFound();
});

// ─── Config Persistence ────────────────────────────────────────────────────
app.MapPost("/api/config/load", (ConfigLoadRequest? req) =>
{
    var path = req?.Path ?? "mt_direct_accounts.json";
    var count = accountStore.LoadConfig(path);
    return Results.Ok(new { loaded = count, path });
});

app.MapPost("/api/config/save", (ConfigSaveRequest? req) =>
{
    var path = req?.Path ?? "mt_direct_accounts.json";
    accountStore.SaveConfig(path);
    return Results.Ok(new { saved = path });
});

// ─── Update Account Config ────────────────────────────────────────────────
app.MapPut("/api/accounts/{id}", (string id, AccountConfig cfg) =>
{
    var ok = accountStore.UpdateAccount(id, cfg);
    return ok ? Results.Ok(new { updated = id }) : Results.NotFound();
});

app.Logger.LogInformation("MtBridgeService starting on http://localhost:5090");
app.Run();

// ═══════════════════════════════════════════════════════════════════════════
// Request/Response Models
// ═══════════════════════════════════════════════════════════════════════════

public record AccountConfig
{
    public string Id { get; init; } = "";
    public string Platform { get; init; } = "mt5"; // "mt4" or "mt5"
    public long Login { get; init; }
    public string Password { get; init; } = "";
    public string Server { get; init; } = "";
    public int Port { get; init; } = 443;
    public bool AutoConnect { get; init; } = true;
    public string? Label { get; init; }
    public double LotMultiplier { get; init; } = 100000;
    public JsonElement? Extra { get; init; } // pass-through for any extra config
}

public record OrderRequest
{
    public string Symbol { get; init; } = "";
    public string Side { get; init; } = "buy";
    public double Lots { get; init; }
    public string? SessionId { get; init; }
    public string? Comment { get; init; }
}

public record CloseRequest
{
    public long Ticket { get; init; }
    public string Symbol { get; init; } = "";
    public string Side { get; init; } = "buy";
    public double Lots { get; init; }
    public string? SessionId { get; init; }
    public string? Comment { get; init; }
}

public record ConfigLoadRequest { public string? Path { get; init; } }
public record ConfigSaveRequest { public string? Path { get; init; } }

// ═══════════════════════════════════════════════════════════════════════════
// Account Store — manages all MT4/MT5 connections
// ═══════════════════════════════════════════════════════════════════════════

public class AccountStore
{
    private readonly ConcurrentDictionary<string, MtAccount> _accounts = new();
    private readonly SemaphoreSlim _connectLock = new(1, 1);
    private readonly ILogger _logger;

    public AccountStore(ILogger logger)
    {
        _logger = logger;
    }

    public bool AddAccount(AccountConfig cfg)
    {
        var acct = new MtAccount(cfg, _logger, _connectLock);
        return _accounts.TryAdd(cfg.Id, acct);
    }

    public bool RemoveAccount(string id)
    {
        if (_accounts.TryRemove(id, out var acct))
        {
            acct.Stop();
            return true;
        }
        return false;
    }

    public bool UpdateAccount(string id, AccountConfig cfg)
    {
        if (!_accounts.TryGetValue(id, out var acct)) return false;
        acct.UpdateConfig(cfg);
        return true;
    }

    public async Task<(bool, string?)> ConnectAsync(string id)
    {
        if (!_accounts.TryGetValue(id, out var acct))
            return (false, "Account not found");
        return await acct.ConnectAsync();
    }

    public bool Disconnect(string id)
    {
        if (!_accounts.TryGetValue(id, out var acct)) return false;
        acct.Stop();
        return true;
    }

    public object? GetAccountInfo(string id) =>
        _accounts.TryGetValue(id, out var a) ? a.GetInfo() : null;

    public object? GetPositions(string id) =>
        _accounts.TryGetValue(id, out var a) ? a.GetPositions() : null;

    public object? GetPositionsForImport(string id, string pair, string comment) =>
        _accounts.TryGetValue(id, out var a) ? a.GetPositionsForImport(pair, comment) : null;

    public object? GetQuote(string id, string symbol) =>
        _accounts.TryGetValue(id, out var a) ? a.GetQuote(symbol) : null;

    public object? GetSwapRates(string id, string[] symbols) =>
        _accounts.TryGetValue(id, out var a) ? a.GetSwapRates(symbols) : null;

    public object? SendMarketOrder(string id, OrderRequest req) =>
        _accounts.TryGetValue(id, out var a) ? a.SendMarketOrder(req) : null;

    public object? ClosePosition(string id, CloseRequest req) =>
        _accounts.TryGetValue(id, out var a) ? a.ClosePosition(req) : null;

    public object? GetDealHistory(string id, long fromTs, long toTs, bool excludeBalance = true, string[]? feeKeywords = null) =>
        _accounts.TryGetValue(id, out var a) ? a.GetDealHistory(fromTs, toTs, excludeBalance, feeKeywords) : null;

    public Dictionary<string, object> GetAllStatus()
    {
        var result = new Dictionary<string, object>();
        foreach (var (id, acct) in _accounts)
            result[id] = acct.GetStatus();
        return result;
    }

    public int LoadConfig(string path)
    {
        if (!File.Exists(path))
        {
            _logger.LogWarning("Config file not found: {Path}", path);
            return 0;
        }
        var json = File.ReadAllText(path);
        var configs = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json);
        if (configs == null) return 0;

        int count = 0;
        int connectIndex = 0;
        foreach (var (id, elem) in configs)
        {
            var cfg = ParseConfigFromDashboard(id, elem);
            bool isNew = AddAccount(cfg);
            if (!isNew)
            {
                // Account already exists (bridge was already running) — update config
                UpdateAccount(id, cfg);
            }
            count++;

            // Auto-connect if requested and not already connected
            if (cfg.AutoConnect && _accounts.TryGetValue(id, out var acct) && !acct.Connected)
            {
                var delay = connectIndex * 2000;
                connectIndex++;
                _ = Task.Run(async () =>
                {
                    await Task.Delay(delay); // stagger connects
                    var (ok, err) = await ConnectAsync(id);
                    if (!ok) _logger.LogWarning("[{Id}] Auto-connect failed: {Error}", id, err);
                });
            }
        }
        _logger.LogInformation("Loaded {Count} accounts from {Path}", count, path);
        return count;
    }

    public void SaveConfig(string path)
    {
        var configs = new Dictionary<string, Dictionary<string, object>>();
        foreach (var (id, acct) in _accounts)
        {
            var dict = new Dictionary<string, object>();
            
            // 1. Copy any original keys from Extra to preserve extra settings (like slippage, reminders, cycle mode settings, etc.)
            if (acct.Config.Extra is JsonElement extra && extra.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in extra.EnumerateObject())
                {
                    dict[prop.Name] = prop.Value;
                }
            }

            // 2. Remove any keys that could conflict with case-insensitivity or standard fields
            dict.Remove("Extra");
            dict.Remove("extra");
            dict.Remove("Platform");
            dict.Remove("platform");
            dict.Remove("type");
            dict.Remove("Id");
            dict.Remove("id");
            dict.Remove("Login");
            dict.Remove("login");
            dict.Remove("Password");
            dict.Remove("password");
            dict.Remove("Server");
            dict.Remove("server");
            dict.Remove("Port");
            dict.Remove("port");
            dict.Remove("AutoConnect");
            dict.Remove("auto_connect_start");
            dict.Remove("Label");
            dict.Remove("label");
            dict.Remove("LotMultiplier");
            dict.Remove("lot_multiplier");

            // 3. Inject standard fields in exact lowercase/snake_case format expected by Python and the dashboard
            dict["type"] = acct.Config.Platform;
            dict["login"] = acct.Config.Login.ToString();
            dict["password"] = acct.Config.Password;
            dict["server"] = acct.Config.Server;
            dict["port"] = acct.Config.Port;
            dict["auto_connect_start"] = acct.Config.AutoConnect;
            dict["label"] = acct.Config.Label ?? acct.Config.Id;
            dict["lot_multiplier"] = acct.Config.LotMultiplier;

            configs[id] = dict;
        }
        var json = JsonSerializer.Serialize(configs, new JsonSerializerOptions { WriteIndented = true });

        // Atomic write via temp file
        var tempPath = path + ".tmp";
        try
        {
            File.WriteAllText(tempPath, json);
            File.Move(tempPath, path, overwrite: true);
            _logger.LogInformation("Saved {Count} accounts atomically to {Path}", configs.Count, path);
        }
        catch (Exception ex)
        {
            _logger.LogError("Failed to save config atomically to {Path}: {Error}", path, ex.Message);
            if (File.Exists(tempPath))
                File.Delete(tempPath);
            // Fallback to direct write if move failed
            File.WriteAllText(path, json);
        }
    }

    private AccountConfig ParseConfigFromDashboard(string id, JsonElement elem)
    {
        // Platform
        var platform = "mt5";
        if (elem.TryGetProperty("type", out var t))
            platform = t.GetString() ?? "mt5";
        else if (elem.TryGetProperty("platform", out var p))
            platform = p.GetString() ?? "mt5";
        else if (elem.TryGetProperty("Platform", out var pPascal))
            platform = pPascal.GetString() ?? "mt5";

        // Login can be string or number in the JSON
        long login = 0;
        if (elem.TryGetProperty("login", out var l))
        {
            if (l.ValueKind == JsonValueKind.Number)
                login = l.GetInt64();
            else if (l.ValueKind == JsonValueKind.String && long.TryParse(l.GetString(), out var parsed))
                login = parsed;
        }
        else if (elem.TryGetProperty("Login", out var lPascal))
        {
            if (lPascal.ValueKind == JsonValueKind.Number)
                login = lPascal.GetInt64();
            else if (lPascal.ValueKind == JsonValueKind.String && long.TryParse(lPascal.GetString(), out var parsed))
                login = parsed;
        }

        // Password
        var password = "";
        if (elem.TryGetProperty("password", out var pw))
            password = pw.GetString() ?? "";
        else if (elem.TryGetProperty("Password", out var pwPascal))
            password = pwPascal.GetString() ?? "";

        // Server
        var server = "";
        if (elem.TryGetProperty("server", out var s))
            server = s.GetString() ?? "";
        else if (elem.TryGetProperty("Server", out var sPascal))
            server = sPascal.GetString() ?? "";
        
        // Port can be string or number
        int port = 443;
        if (elem.TryGetProperty("port", out var pt))
        {
            if (pt.ValueKind == JsonValueKind.Number)
                port = pt.GetInt32();
            else if (pt.ValueKind == JsonValueKind.String && int.TryParse(pt.GetString(), out var pp))
                port = pp;
        }
        else if (elem.TryGetProperty("Port", out var ptPascal))
        {
            if (ptPascal.ValueKind == JsonValueKind.Number)
                port = ptPascal.GetInt32();
            else if (ptPascal.ValueKind == JsonValueKind.String && int.TryParse(ptPascal.GetString(), out var pp))
                port = pp;
        }

        // AutoConnect
        var autoConnect = true;
        if (elem.TryGetProperty("auto_connect_start", out var ac))
            autoConnect = ac.GetBoolean();
        else if (elem.TryGetProperty("AutoConnect", out var acPascal))
            autoConnect = acPascal.GetBoolean();

        // Label
        var label = elem.TryGetProperty("label", out var lb) ? lb.GetString() : null;
        if (label == null && elem.TryGetProperty("Label", out var lbPascal))
            label = lbPascal.GetString();

        // LotMultiplier
        double lotMultiplier = 100000;
        if (elem.TryGetProperty("lot_multiplier", out var lm))
        {
            if (lm.ValueKind == JsonValueKind.Number)
                lotMultiplier = lm.GetDouble();
            else if (lm.ValueKind == JsonValueKind.String && double.TryParse(lm.GetString(), out var lmp))
                lotMultiplier = lmp;
        }
        else if (elem.TryGetProperty("LotMultiplier", out var lmPascal))
        {
            if (lmPascal.ValueKind == JsonValueKind.Number)
                lotMultiplier = lmPascal.GetDouble();
            else if (lmPascal.ValueKind == JsonValueKind.String && double.TryParse(lmPascal.GetString(), out var lmp))
                lotMultiplier = lmp;
        }

        return new AccountConfig
        {
            Id = id,
            Platform = platform,
            Login = login,
            Password = password,
            Server = server,
            Port = port,
            AutoConnect = autoConnect,
            Label = label,
            LotMultiplier = lotMultiplier,
            Extra = elem
        };
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// MtAccount — wraps a single MT4 or MT5 connection
// ═══════════════════════════════════════════════════════════════════════════

public class MtAccount
{
    public AccountConfig Config { get; private set; }
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _connectLock;

    // MT5
    private mtapi.mt5.MT5API? _mt5;

    // MT4
    private TradingAPI.MT4Server.QuoteClient? _mt4;
    private TradingAPI.MT4Server.OrderClient? _mt4Order;

    // State
    private volatile bool _connected;
    private volatile bool _running;
    private Thread? _heartbeatThread;
    private string? _lastError;

    // Cached data (thread-safe reads)
    private readonly ConcurrentDictionary<string, QuoteData> _quotes = new();
    private readonly ConcurrentDictionary<long, PositionData> _positions = new();
    private double _balance, _equity, _margin, _freeMargin, _profit;
    private int _leverage;
    private readonly object _infoLock = new();

    // Reconnect
    private int _reconnectAttempt;
    private double _reconnectDelay = 5;
    private const double RECONNECT_MAX_DELAY = 120;

    public MtAccount(AccountConfig config, ILogger logger, SemaphoreSlim connectLock)
    {
        Config = config;
        _logger = logger;
        _connectLock = connectLock;
    }

    public void UpdateConfig(AccountConfig cfg)
    {
        bool connCriticalChanged = 
            !Config.Platform.Equals(cfg.Platform, StringComparison.OrdinalIgnoreCase) ||
            Config.Login != cfg.Login ||
            Config.Password != cfg.Password ||
            Config.Server != cfg.Server ||
            Config.Port != cfg.Port;

        Config = cfg with { Id = Config.Id }; // preserve ID

        if (connCriticalChanged)
        {
            _positions.Clear();
            _connected = false;
        }
    }

    public bool Connected => _connected;
    public bool IsMt5 => Config.Platform.Equals("mt5", StringComparison.OrdinalIgnoreCase);

    // ─── Connect ───────────────────────────────────────────────────────────
    public async Task<(bool, string?)> ConnectAsync()
    {
        // Serialize all .NET Connect() calls
        if (!await _connectLock.WaitAsync(TimeSpan.FromSeconds(30)))
            return (false, "Connect lock timeout — another account is connecting");

        try
        {
            _lastError = null;
            // Clear any stale position data before attempting to connect so that
            // old session positions are never returned while the new handshake is
            // in progress.  _push_positions() will populate fresh data on success.
            _positions.Clear();
            if (IsMt5)
                return ConnectMt5();
            else
                return ConnectMt4();
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            _logger.LogError("[{Id}] Connect error: {Error}", Config.Id, ex.Message);
            return (false, ex.Message);
        }
        finally
        {
            _connectLock.Release();
        }
    }

    private (bool, string?) ConnectMt5()
    {
        _logger.LogInformation("[{Id}] Connecting to MT5 {Server}:{Port}...", Config.Id, Config.Server, Config.Port);

        // Cleanup old instance
        if (_mt5 != null)
        {
            try { _mt5.Disconnect(); } catch { }
            _mt5 = null;
        }

        _mt5 = new mtapi.mt5.MT5API((ulong)Config.Login, Config.Password, Config.Server, Config.Port);

        try { _mt5.ProcessServerMessagesInThread = true; }
        catch (Exception ex) { _logger.LogWarning("[{Id}] Could not set ProcessServerMessagesInThread: {Err}", Config.Id, ex.Message); }

        try { _mt5.ExecutionTimeout = 120000; }
        catch (Exception ex) { _logger.LogWarning("[{Id}] Could not set ExecutionTimeout: {Err}", Config.Id, ex.Message); }

        // Connect with timeout
        Exception? connectErr = null;
        var connectThread = new Thread(() =>
        {
            try { _mt5.Connect(); }
            catch (Exception ex) { connectErr = ex; }
        }) { IsBackground = true };

        connectThread.Start();
        if (!connectThread.Join(TimeSpan.FromSeconds(15)))
        {
            _lastError = "Connect() timed out after 15s";
            _logger.LogWarning("[{Id}] {Error}", Config.Id, _lastError);
            try { _mt5.Disconnect(); } catch { }
            _mt5 = null;
            return (false, _lastError);
        }

        if (connectErr != null)
        {
            _lastError = connectErr.Message;
            _logger.LogError("[{Id}] Connect() raised: {Error}", Config.Id, connectErr.Message);
            _mt5 = null;
            return (false, _lastError);
        }

        // Wait for account data to sync (up to 5 seconds)
        var syncDeadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < syncDeadline)
        {
            try
            {
                var bal = _mt5.Account?.Balance ?? 0;
                var eq = _mt5.AccountEquity;
                var openOrders = _mt5.GetOpenedOrders();
                var posCount = openOrders != null ? openOrders.Length : 0;

                if (bal != 0 && eq != 0)
                {
                    if (posCount > 0 && eq == bal)
                    {
                        // Stale equity (equals balance but has open positions) — keep waiting
                    }
                    else
                    {
                        break;
                    }
                }
            }
            catch { }
            Thread.Sleep(100);
        }

        _connected = true;
        _logger.LogInformation("[{Id}] MT5 Connected!", Config.Id);

        // Subscribe to events
        _mt5.OnQuote += OnMt5Quote;
        _mt5.OnOrderUpdate += OnMt5OrderUpdate;

        // Start heartbeat
        StartHeartbeat();

        // Initial data push
        PushPositions();
        PushAccountInfo();

        return (true, null);
    }

    private (bool, string?) ConnectMt4()
    {
        _logger.LogInformation("[{Id}] Connecting to MT4 {Server}:{Port}...", Config.Id, Config.Server, Config.Port);

        if (_mt4 != null)
        {
            try { _mt4.Disconnect(); } catch { }
            _mt4 = null;
            _mt4Order = null;
        }

        _mt4 = new TradingAPI.MT4Server.QuoteClient((int)Config.Login, Config.Password, Config.Server, Config.Port);
        _mt4.Connect();

        if (!_mt4.Connected)
        {
            _lastError = "MT4 Connect failed";
            _mt4 = null;
            return (false, _lastError);
        }

        _mt4Order = _mt4.OrderClient;
        if (_mt4Order == null)
        {
            try
            {
                _mt4Order = new TradingAPI.MT4Server.OrderClient(_mt4);
                _logger.LogInformation("[{Id}] Created OrderClient via explicit constructor", Config.Id);
            }
            catch (Exception ex)
            {
                _logger.LogWarning("[{Id}] Failed to construct OrderClient explicitly: {Err}", Config.Id, ex.Message);
            }
        }
        _connected = true;
        _logger.LogInformation("[{Id}] MT4 Connected!", Config.Id);

        // Subscribe to events
        _mt4.OnQuote += OnMt4Quote;
        _mt4.OnOrderUpdate += OnMt4OrderUpdate;
        _mt4.OnDisconnect += OnMt4Disconnect;

        StartHeartbeat();
        PushPositions();
        PushAccountInfo();

        return (true, null);
    }

    // ─── Stop ──────────────────────────────────────────────────────────────
    public void Stop()
    {
        _running = false;
        _connected = false;
        try
        {
            if (_mt5 != null) { _mt5.Disconnect(); _mt5 = null; }
            if (_mt4 != null) { _mt4.Disconnect(); _mt4 = null; _mt4Order = null; }
        }
        catch (Exception ex) { _logger.LogWarning("[{Id}] Stop error: {Err}", Config.Id, ex.Message); }
        _logger.LogInformation("[{Id}] Stopped", Config.Id);
    }

    // ─── Heartbeat ─────────────────────────────────────────────────────────
    private void StartHeartbeat()
    {
        _running = true;
        if (_heartbeatThread?.IsAlive == true) return;
        _heartbeatThread = new Thread(HeartbeatLoop) { IsBackground = true, Name = $"HB-{Config.Id}" };
        _heartbeatThread.Start();
    }

    private void HeartbeatLoop()
    {
        while (_running)
        {
            try
            {
                // Update actual connection state first
                bool stillConnected = false;
                if (IsMt5)
                {
                    stillConnected = _mt5 != null && _mt5.Connected;
                }
                else
                {
                    stillConnected = _mt4 != null && _mt4.Connected;
                }

                if (_connected && !stillConnected)
                {
                    _logger.LogWarning("[{Id}] Heartbeat detected connection loss", Config.Id);
                    _connected = false;
                }

                if (_connected)
                {
                    PushPositions();
                    PushAccountInfo();
                    _reconnectAttempt = 0;
                    _reconnectDelay = 5;
                }
                else if (_running)
                {
                    // Auto-reconnect
                    _reconnectAttempt++;
                    _logger.LogInformation("[{Id}] Attempting reconnect #{Attempt}...", Config.Id, _reconnectAttempt);
                    var (ok, err) = ConnectAsync().GetAwaiter().GetResult();
                    if (ok)
                    {
                        _logger.LogInformation("[{Id}] Reconnected after {Attempts} attempt(s)", Config.Id, _reconnectAttempt);
                    }
                    else
                    {
                        _logger.LogWarning("[{Id}] Reconnect failed: {Error} — retrying in {Delay}s", Config.Id, err, _reconnectDelay);
                        Thread.Sleep((int)(_reconnectDelay * 1000));
                        _reconnectDelay = Math.Min(_reconnectDelay * 2, RECONNECT_MAX_DELAY);
                        continue; // skip the normal 30s sleep
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("[{Id}] Heartbeat error: {Error}", Config.Id, ex.Message);
            }
            Thread.Sleep(30_000); // 30s heartbeat interval
        }
    }

    // ─── Event Handlers ────────────────────────────────────────────────────
    private void OnMt5Quote(mtapi.mt5.MT5API api, mtapi.mt5.Quote quote)
    {
        try
        {
            var sym = quote.Symbol ?? "";
            var bid = quote.Bid;
            var ask = quote.Ask;
            var pipMult = sym.Contains("JPY", StringComparison.OrdinalIgnoreCase) ? 1000.0 : 100000.0;
            var spread = bid > 0 && ask > 0 ? Math.Round((ask - bid) * pipMult, 1) : 0;
            _quotes[sym] = new QuoteData(bid, ask, spread);
        }
        catch { }
    }

    private void OnMt5OrderUpdate(mtapi.mt5.MT5API api, mtapi.mt5.OrderUpdate update)
    {
        try { PushPositions(); } catch { }
    }

    private void OnMt4Quote(object sender, TradingAPI.MT4Server.QuoteEventArgs args)
    {
        try
        {
            var sym = args.Symbol ?? "";
            var bid = (double)args.Bid;
            var ask = (double)args.Ask;
            var pipMult = sym.Contains("JPY", StringComparison.OrdinalIgnoreCase) ? 1000.0 : 100000.0;
            var spread = bid > 0 && ask > 0 ? Math.Round((ask - bid) * pipMult, 1) : 0;
            _quotes[sym] = new QuoteData(bid, ask, spread);
        }
        catch { }
    }

    private void OnMt4OrderUpdate(object sender, TradingAPI.MT4Server.OrderUpdateEventArgs args)
    {
        try { PushPositions(); } catch { }
    }

    private void OnMt4Disconnect(object sender, TradingAPI.MT4Server.DisconnectEventArgs args)
    {
        _connected = false;
        _logger.LogWarning("[{Id}] MT4 disconnected", Config.Id);
    }

    // ─── Data Push ─────────────────────────────────────────────────────────
    private void PushAccountInfo()
    {
        lock (_infoLock)
        {
            try
            {
                if (IsMt5 && _mt5 != null)
                {
                    var acct = _mt5.Account;
                    if (acct != null)
                    {
                        _balance = acct.Balance;
                        _equity = _mt5.AccountEquity;
                        _margin = _mt5.AccountMargin;
                        _freeMargin = _mt5.AccountFreeMargin;
                        _profit = _mt5.AccountProfit;
                        _leverage = (int)acct.Leverage;

                        // Check if direct properties are stale (async update delay after connection)
                        if (_profit == 0.0 && _equity == _balance && _positions.Count > 0)
                        {
                            double calcProfit = 0;
                            foreach (var pos in _positions.Values)
                            {
                                calcProfit += pos.Profit + pos.Swap;
                            }
                            if (calcProfit != 0.0)
                            {
                                _profit = calcProfit;
                                _equity = _balance + calcProfit;
                                _freeMargin = _equity - _margin;
                            }
                        }
                    }
                }
                else if (_mt4 != null)
                {
                    _balance = _mt4.AccountBalance;
                    _equity = _mt4.AccountEquity;
                    _margin = _mt4.AccountMargin;
                    _freeMargin = _mt4.AccountFreeMargin;
                    _profit = _mt4.AccountProfit;
                    _leverage = _mt4.AccountLeverage;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("[{Id}] PushAccountInfo error: {Err}", Config.Id, ex.Message);
            }
        }
    }

    private void PushPositions()
    {
        try
        {
            var newPositions = new Dictionary<long, PositionData>();
            if (IsMt5 && _mt5 != null)
            {
                foreach (var order in _mt5.GetOpenedOrders())
                {
                    newPositions[order.Ticket] = new PositionData
                    {
                        Ticket = order.Ticket,
                        Symbol = order.Symbol,
                        Side = order.OrderType.ToString().Contains("Buy") ? "buy" : "sell",
                        Lots = order.Lots,
                        OpenPrice = order.OpenPrice,
                        OpenTime = order.OpenTime.ToString("o"),
                        Profit = order.Profit,
                        Swap = order.Swap,
                        Comment = order.Comment ?? ""
                    };
                }
            }
            else if (_mt4 != null)
            {
                foreach (var order in _mt4.GetOpenedOrders())
                {
                    var isBuy = order.Type == TradingAPI.MT4Server.Op.Buy;
                    var isSell = order.Type == TradingAPI.MT4Server.Op.Sell;
                    if (!isBuy && !isSell) continue; // skip pending orders
                    newPositions[order.Ticket] = new PositionData
                    {
                        Ticket = order.Ticket,
                        Symbol = order.Symbol,
                        Side = isBuy ? "buy" : "sell",
                        Lots = order.Lots,
                        OpenPrice = order.OpenPrice,
                        OpenTime = order.OpenTime.ToString("o"),
                        Profit = order.Profit,
                        Swap = order.Swap,
                        Comment = order.Comment ?? ""
                    };
                }
            }

            // Atomic swap
            _positions.Clear();
            foreach (var (ticket, pos) in newPositions)
                _positions[ticket] = pos;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[{Id}] PushPositions error: {Err}", Config.Id, ex.Message);
        }
    }

    // ─── Query Methods ─────────────────────────────────────────────────────
    public object GetInfo()
    {
        lock (_infoLock)
        {
            return new
            {
                connected = _connected,
                platform = Config.Platform,
                balance = _balance,
                equity = _equity,
                margin = _margin,
                free_margin = _freeMargin,
                profit = _profit,
                leverage = _leverage,
                positions = _positions.Count,
                last_error = _lastError
            };
        }
    }

    public object GetStatus() => new
    {
        connected = _connected,
        platform = Config.Platform,
        label = Config.Label,
        positions = _positions.Count,
        last_error = _lastError
    };

    public object GetPositions() => _positions.Values.ToList();

    public object GetPositionsForImport(string pairFilter, string commentFilter)
    {
        var results = new List<object>();
        foreach (var pos in _positions.Values)
        {
            if (!string.IsNullOrEmpty(pairFilter) &&
                !pos.Symbol.Contains(pairFilter, StringComparison.OrdinalIgnoreCase))
                continue;
            if (!string.IsNullOrEmpty(commentFilter) &&
                !(pos.Comment?.Contains(commentFilter, StringComparison.OrdinalIgnoreCase) ?? false))
                continue;
            results.Add(new
            {
                ticket = pos.Ticket,
                symbol = pos.Symbol,
                side = pos.Side,
                lots = pos.Lots,
                open_price = pos.OpenPrice,
                open_time = pos.OpenTime,
                comment = pos.Comment
            });
        }
        return results;
    }

    public object? GetQuote(string symbol)
    {
        // Try exact match first
        if (_quotes.TryGetValue(symbol, out var q))
            return new { bid = q.Bid, ask = q.Ask, spread = q.Spread };

        // Case-insensitive fallback
        foreach (var (key, val) in _quotes)
        {
            if (key.Equals(symbol, StringComparison.OrdinalIgnoreCase))
                return new { bid = val.Bid, ask = val.Ask, spread = val.Spread };
        }

        // Try GetQuote from API
        try
        {
            if (IsMt5 && _mt5 != null)
            {
                var mq = _mt5.GetQuote(symbol);
                if (mq != null)
                {
                    var pipMult = symbol.Contains("JPY", StringComparison.OrdinalIgnoreCase) ? 1000.0 : 100000.0;
                    var spread = Math.Round((mq.Ask - mq.Bid) * pipMult, 1);
                    return new { bid = mq.Bid, ask = mq.Ask, spread };
                }
            }
            else if (_mt4 != null)
            {
                var mq = _mt4.GetQuote(symbol);
                if (mq != null)
                {
                    var pipMult = symbol.Contains("JPY", StringComparison.OrdinalIgnoreCase) ? 1000.0 : 100000.0;
                    var bid = (double)mq.Bid;
                    var ask = (double)mq.Ask;
                    var spread = Math.Round((ask - bid) * pipMult, 1);
                    return new { bid, ask, spread };
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[{Id}] GetQuote({Symbol}) error: {Err}", Config.Id, symbol, ex.Message);
        }

        return null;
    }

    public object GetSwapRates(string[] symbols)
    {
        var result = new Dictionary<string, object>();
        try
        {
            if (IsMt5 && _mt5 != null)
            {
                foreach (var sym in symbols)
                {
                    try
                    {
                        var group = _mt5.Symbols.GetGroup(sym);
                        result[sym] = new { swap_long = group.SwapLong, swap_short = group.SwapShort };
                    }
                    catch { }
                }
            }
            else if (_mt4 != null)
            {
                foreach (var sym in symbols)
                {
                    try
                    {
                        var info = _mt4.GetSymbolInfo(sym);
                        result[sym] = new { swap_long = info.Ex.swap_long, swap_short = info.Ex.swap_short };
                    }
                    catch { }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[{Id}] GetSwapRates error: {Err}", Config.Id, ex.Message);
        }
        return result;
    }

    // ─── Trade Execution ───────────────────────────────────────────────────
    public object SendMarketOrder(OrderRequest req)
    {
        try
        {
            if (IsMt5 && _mt5 != null)
            {
                var orderType = req.Side.Equals("buy", StringComparison.OrdinalIgnoreCase)
                    ? mtapi.mt5.OrderType.Buy : mtapi.mt5.OrderType.Sell;

                var quote = _mt5.GetQuote(req.Symbol);
                double price = orderType == mtapi.mt5.OrderType.Buy ? (quote?.Ask ?? 0) : (quote?.Bid ?? 0);

                mtapi.mt5.Order? order = null;
                var policies = new[] {
                    mtapi.mt5.FillPolicy.ImmediateOrCancel,
                    mtapi.mt5.FillPolicy.FillOrKill,
                    mtapi.mt5.FillPolicy.FlashFill,
                    mtapi.mt5.FillPolicy.Any
                };
                Exception? lastEx = null;
                foreach (var policy in policies)
                {
                    try
                    {
                        _logger.LogInformation("[{Id}] Attempting OrderSend with policy: {Policy}", Config.Id, policy);
                        order = _mt5.OrderSend(req.Symbol, req.Lots, price, orderType, 0, 0, 1000,
                            req.Comment, 0, policy);
                        break;
                    }
                    catch (Exception ex)
                    {
                        lastEx = ex;
                        _logger.LogWarning("[{Id}] OrderSend failed with policy {Policy}: {Err}", Config.Id, policy, ex.Message);
                        continue;
                    }
                }
                if (order == null && lastEx != null)
                {
                    throw lastEx;
                }

                PushPositions();
                return new
                {
                    success = true,
                    ticket = order.Ticket,
                    open_price = order.OpenPrice,
                    symbol = order.Symbol,
                    lots = order.Lots
                };
            }
            else if (_mt4 != null && _mt4Order != null)
            {
                var op = req.Side.Equals("buy", StringComparison.OrdinalIgnoreCase)
                    ? TradingAPI.MT4Server.Op.Buy : TradingAPI.MT4Server.Op.Sell;

                var quote = _mt4.GetQuote(req.Symbol);
                double price = op == TradingAPI.MT4Server.Op.Buy ? (double)(quote?.Ask ?? 0) : (double)(quote?.Bid ?? 0);

                var order = _mt4Order.OrderSend(req.Symbol, op, req.Lots, price, 100, 0, 0,
                    req.Comment, 0, DateTime.MinValue);

                PushPositions();
                return new
                {
                    success = true,
                    ticket = order.Ticket,
                    open_price = order.OpenPrice,
                    symbol = order.Symbol,
                    lots = order.Lots
                };
            }
            return new { success = false, error = "Not connected" };
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Id}] SendMarketOrder error: {Err}", Config.Id, ex.Message);
            return new { success = false, error = ex.Message };
        }
    }

    public object ClosePosition(CloseRequest req)
    {
        try
        {
            if (IsMt5 && _mt5 != null)
            {
                // Check if position still exists
                var openedOrders = _mt5.GetOpenedOrders();
                bool exists = false;
                string actualSymbol = req.Symbol;
                mtapi.mt5.OrderType actualType = req.Side.Equals("buy", StringComparison.OrdinalIgnoreCase)
                    ? mtapi.mt5.OrderType.Buy : mtapi.mt5.OrderType.Sell;

                if (openedOrders != null)
                {
                    foreach (var o in openedOrders)
                    {
                        if (o.Ticket == req.Ticket)
                        {
                            exists = true;
                            actualSymbol = o.Symbol;
                            actualType = o.OrderType;
                            break;
                        }
                    }
                }
                if (!exists)
                {
                    _logger.LogInformation("[{Id}] ClosePosition: ticket {Ticket} not found in opened positions, assuming already closed.", Config.Id, req.Ticket);
                    return new
                    {
                        success = true,
                        ticket = req.Ticket,
                        close_price = 0.0
                    };
                }

                // If actual type is Buy (meaning it's currently a buy position), posType is Buy, closeType is Sell
                var posType = actualType;
                var closeType = posType == mtapi.mt5.OrderType.Buy ? mtapi.mt5.OrderType.Sell : mtapi.mt5.OrderType.Buy;

                var quote = _mt5.GetQuote(actualSymbol);
                double pricePos = posType == mtapi.mt5.OrderType.Buy ? (quote?.Bid ?? 0) : (quote?.Ask ?? 0);
                double priceClose = closeType == mtapi.mt5.OrderType.Buy ? (quote?.Bid ?? 0) : (quote?.Ask ?? 0);

                mtapi.mt5.Order? order = null;
                var policies = new[] {
                    mtapi.mt5.FillPolicy.Any,
                    mtapi.mt5.FillPolicy.ImmediateOrCancel,
                    mtapi.mt5.FillPolicy.FillOrKill,
                    mtapi.mt5.FillPolicy.FlashFill
                };
                Exception? lastEx = null;
                foreach (var policy in policies)
                {
                    // Combo 1: posType, pricePos
                    try
                    {
                        _logger.LogInformation("[{Id}] Attempting OrderClose (posType, price): policy={Policy} price={Price} type={Type} symbol={Symbol}", Config.Id, policy, pricePos, posType, actualSymbol);
                        order = _mt5.OrderClose(req.Ticket, actualSymbol, pricePos, req.Lots, posType,
                            3, policy, 0, "", 0, mtapi.mt5.PlacedType.Manually);
                        break;
                    }
                    catch (Exception ex)
                    {
                        lastEx = ex;
                        _logger.LogWarning("[{Id}] OrderClose failed (posType, price) policy={Policy}: {Err}", Config.Id, policy, ex.Message);
                    }

                    // Combo 2: posType, price = 0
                    try
                    {
                        _logger.LogInformation("[{Id}] Attempting OrderClose (posType, price=0): policy={Policy} type={Type} symbol={Symbol}", Config.Id, policy, posType, actualSymbol);
                        order = _mt5.OrderClose(req.Ticket, actualSymbol, 0.0, req.Lots, posType,
                            3, policy, 0, "", 0, mtapi.mt5.PlacedType.Manually);
                        break;
                    }
                    catch (Exception ex)
                    {
                        lastEx = ex;
                        _logger.LogWarning("[{Id}] OrderClose failed (posType, price=0) policy={Policy}: {Err}", Config.Id, policy, ex.Message);
                    }

                    // Combo 3: closeType, priceClose
                    try
                    {
                        _logger.LogInformation("[{Id}] Attempting OrderClose (closeType, price): policy={Policy} price={Price} type={Type} symbol={Symbol}", Config.Id, policy, priceClose, closeType, actualSymbol);
                        order = _mt5.OrderClose(req.Ticket, actualSymbol, priceClose, req.Lots, closeType,
                            3, policy, 0, "", 0, mtapi.mt5.PlacedType.Manually);
                        break;
                    }
                    catch (Exception ex)
                    {
                        lastEx = ex;
                        _logger.LogWarning("[{Id}] OrderClose failed (closeType, price) policy={Policy}: {Err}", Config.Id, policy, ex.Message);
                    }

                    // Combo 4: closeType, price = 0
                    try
                    {
                        _logger.LogInformation("[{Id}] Attempting OrderClose (closeType, price=0): policy={Policy} type={Type} symbol={Symbol}", Config.Id, policy, closeType, actualSymbol);
                        order = _mt5.OrderClose(req.Ticket, actualSymbol, 0.0, req.Lots, closeType,
                            3, policy, 0, "", 0, mtapi.mt5.PlacedType.Manually);
                        break;
                    }
                    catch (Exception ex)
                    {
                        lastEx = ex;
                        _logger.LogWarning("[{Id}] OrderClose failed (closeType, price=0) policy={Policy}: {Err}", Config.Id, policy, ex.Message);
                    }
                }
                if (order == null && lastEx != null)
                {
                    throw lastEx;
                }

                PushPositions();
                return new
                {
                    success = true,
                    ticket = order.Ticket,
                    close_price = order.ClosePrice
                };
            }
            else if (_mt4 != null && _mt4Order != null)
            {
                // Check if position still exists
                var openedOrders = _mt4.GetOpenedOrders();
                bool exists = false;
                string actualSymbol = req.Symbol;
                if (openedOrders != null)
                {
                    foreach (var o in openedOrders)
                    {
                        if (o.Ticket == req.Ticket)
                        {
                            exists = true;
                            actualSymbol = o.Symbol;
                            break;
                        }
                    }
                }
                if (!exists)
                {
                    _logger.LogInformation("[{Id}] ClosePosition: ticket {Ticket} not found in opened MT4 positions, assuming already closed.", Config.Id, req.Ticket);
                    return new
                    {
                        success = true,
                        ticket = req.Ticket,
                        close_price = 0.0
                    };
                }
                var quote = _mt4.GetQuote(actualSymbol);
                double price = req.Side.Equals("buy", StringComparison.OrdinalIgnoreCase)
                    ? (double)(quote?.Bid ?? 0)   // close buy = sell at bid
                    : (double)(quote?.Ask ?? 0);   // close sell = buy at ask

                var order = _mt4Order.OrderClose(actualSymbol, (int)req.Ticket, req.Lots, price, 100);

                PushPositions();
                return new
                {
                    success = true,
                    ticket = order.Ticket,
                    close_price = order.ClosePrice
                };
            }
            return new { success = false, error = "Not connected" };
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Id}] ClosePosition error: {Err}", Config.Id, ex.Message);
            return new { success = false, error = ex.Message };
        }
    }

    // ─── Deal History ──────────────────────────────────────────────────────
    public object? GetDealHistory(long fromTs, long toTs, bool excludeBalance = true, string[]? feeKeywords = null)
    {
        try
        {
            var fromUtc = DateTimeOffset.FromUnixTimeSeconds(fromTs).UtcDateTime;
            var toUtc   = DateTimeOffset.FromUnixTimeSeconds(toTs).UtcDateTime;
            var from = fromUtc;
            var to   = toUtc;
            bool hasFeeKeywords = feeKeywords != null && feeKeywords.Length > 0;
            _logger.LogInformation("[{Id}] GetDealHistory: from={From:yyyy-MM-dd} to={To:yyyy-MM-dd} exclude_balance={Excl} fee_keywords=[{Kw}]",
                Config.Id, from, to, excludeBalance, string.Join(",", feeKeywords ?? Array.Empty<string>()));

            if (IsMt5 && _mt5 != null)
            {
                var hist = _mt5.DownloadOrderHistory(from, to);
                double pnl = 0, swap = 0, fees = 0;
                int count = 0;
                int skippedBalance = 0;
                var bySymbol = new Dictionary<string, double[]>(); // [pnl, swap, fees, count, lots]
                if (hist?.InternalDeals != null)
                {
                    foreach (var deal in hist.InternalDeals)
                    {
                        var dealTypeStr = deal.Type.ToString();
                        bool isTradeType = dealTypeStr.Contains("Buy", StringComparison.OrdinalIgnoreCase)
                                        || dealTypeStr.Contains("Sell", StringComparison.OrdinalIgnoreCase);
                        bool isBalanceType = dealTypeStr.Contains("Balance", StringComparison.OrdinalIgnoreCase)
                                          || dealTypeStr.Contains("Credit", StringComparison.OrdinalIgnoreCase);

                        if (isTradeType)
                        {
                            // Always include trade deals
                            var dealSymbol = deal.Symbol ?? "UNKNOWN";
                            var dealFees = deal.Commission + deal.Fee;
                            pnl  += deal.Profit;
                            swap += deal.Swap;
                            fees += dealFees;
                            count++;
                            if (!bySymbol.ContainsKey(dealSymbol))
                                bySymbol[dealSymbol] = new double[5];
                            bySymbol[dealSymbol][0] += deal.Profit;
                            bySymbol[dealSymbol][1] += deal.Swap;
                            bySymbol[dealSymbol][2] += dealFees;
                            bySymbol[dealSymbol][3] += 1;
                            bySymbol[dealSymbol][4] += deal.Lots;
                        }
                        else
                        {
                            // Non-trade deal (Balance, Credit, Charge, etc.)
                            var dealComment = deal.Comment ?? "";
                            if (hasFeeKeywords)
                            {
                                // Fee-keyword mode: include only entries whose comment matches a keyword
                                bool matches = feeKeywords!.Any(kw => dealComment.Contains(kw, StringComparison.OrdinalIgnoreCase));
                                if (!matches) { skippedBalance++; continue; }
                            }
                            else if (excludeBalance && isBalanceType)
                            {
                                // Default mode: exclude Balance/Credit fund transfers
                                skippedBalance++;
                                continue;
                            }
                            // Include as fee (storage fee, charge, etc.)
                            var feeSymbol = string.IsNullOrEmpty(deal.Symbol) ? "FEES" : deal.Symbol;
                            fees += deal.Profit + deal.Commission + deal.Fee;
                            count++;
                            if (!bySymbol.ContainsKey(feeSymbol))
                                bySymbol[feeSymbol] = new double[5];
                            bySymbol[feeSymbol][2] += deal.Profit + deal.Commission + deal.Fee;
                            bySymbol[feeSymbol][3] += 1;
                        }
                    }
                }
                if (skippedBalance > 0)
                    _logger.LogInformation("[{Id}] GetDealHistory: skipped {Count} balance/non-fee operation(s)", Config.Id, skippedBalance);

                var bySymbolResult = new Dictionary<string, object>();
                foreach (var (sym, vals) in bySymbol)
                {
                    bySymbolResult[sym] = new {
                        pnl = Math.Round(vals[0], 2),
                        swap = Math.Round(vals[1], 2),
                        fees = Math.Round(vals[2], 2),
                        count = (int)vals[3],
                        lots = Math.Round(vals[4], 2)
                    };
                }
                return new { pnl = Math.Round(pnl, 2), swap = Math.Round(swap, 2), fees = Math.Round(fees, 2), deal_count = count, by_symbol = bySymbolResult };
            }
            else if (_mt4 != null)
            {
                var hist = _mt4.DownloadOrderHistory(from, to);
                double pnl = 0, swap = 0, fees = 0;
                int count = 0;
                var bySymbol = new Dictionary<string, double[]>(); // [pnl, swap, fees, count, lots]
                foreach (var order in hist)
                {
                    if (order.Type == TradingAPI.MT4Server.Op.Buy || order.Type == TradingAPI.MT4Server.Op.Sell)
                    {
                        // Trade order — always include
                        var dealSymbol = order.Symbol ?? "UNKNOWN";
                        pnl  += order.Profit;
                        swap += order.Swap;
                        fees += order.Commission;
                        count++;
                        if (!bySymbol.ContainsKey(dealSymbol))
                            bySymbol[dealSymbol] = new double[5];
                        bySymbol[dealSymbol][0] += order.Profit;
                        bySymbol[dealSymbol][1] += order.Swap;
                        bySymbol[dealSymbol][2] += order.Commission;
                        bySymbol[dealSymbol][3] += 1;
                        bySymbol[dealSymbol][4] += order.Lots;
                    }
                    else
                    {
                        // Non-trade order (Balance=6, Credit=7, or broker-specific Charge type)
                        var opStr = order.Type.ToString();
                        var comment = order.Comment ?? "";
                        if (!string.IsNullOrEmpty(order.Symbol)) {
                            _logger.LogInformation("[{Id}] Non-trade order with symbol: Type={Type} Symbol={Sym} Comment={Cmt} Profit={P}", Config.Id, opStr, order.Symbol, comment, order.Profit);
                        }
                        bool isBalanceOrCredit = opStr.Equals("Balance", StringComparison.OrdinalIgnoreCase)
                                               || opStr.Equals("Credit", StringComparison.OrdinalIgnoreCase);

                        if (hasFeeKeywords)
                        {
                            // Fee-keyword mode: include only entries whose comment matches a keyword
                            bool matches = feeKeywords!.Any(kw => comment.Contains(kw, StringComparison.OrdinalIgnoreCase));
                            if (!matches) continue;
                        }
                        else if (excludeBalance && isBalanceOrCredit)
                        {
                            // Default mode: exclude Balance/Credit fund transfers; include other types (Charge etc.)
                            continue;
                        }

                        // Include as fee (storage fee, charge, etc.) — NOT in pnl
                        var feeSymbol = string.IsNullOrEmpty(order.Symbol) ? "FEES" : order.Symbol;
                        fees += order.Profit;
                        count++;
                        if (!bySymbol.ContainsKey(feeSymbol))
                            bySymbol[feeSymbol] = new double[5];
                        bySymbol[feeSymbol][2] += order.Profit;
                        bySymbol[feeSymbol][3] += 1;
                    }
                }
                var bySymbolResult = new Dictionary<string, object>();
                foreach (var (sym, vals) in bySymbol)
                {
                    bySymbolResult[sym] = new {
                        pnl = Math.Round(vals[0], 2),
                        swap = Math.Round(vals[1], 2),
                        fees = Math.Round(vals[2], 2),
                        count = (int)vals[3],
                        lots = Math.Round(vals[4], 2)
                    };
                }
                return new { pnl = Math.Round(pnl, 2), swap = Math.Round(swap, 2), fees = Math.Round(fees, 2), deal_count = count, by_symbol = bySymbolResult };
            }
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Id}] GetDealHistory error: {Err}", Config.Id, ex.Message);
            return new { error = ex.Message };
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Data Models
// ═══════════════════════════════════════════════════════════════════════════

public record QuoteData(double Bid, double Ask, double Spread);

public class PositionData
{
    public long Ticket { get; init; }
    public string Symbol { get; init; } = "";
    public string Side { get; init; } = "";
    public double Lots { get; init; }
    public double OpenPrice { get; init; }
    public string OpenTime { get; init; } = "";
    public double Profit { get; init; }
    public double Swap { get; init; }
    public string Comment { get; init; } = "";
}
