import sys, json, time
sys.path.insert(0, 'd:/Documents/dev/TradeDashboard/TradeDashboardPY')
import trade_dashboard
import trading_logic

with open('configs/trade_sessions.json', 'r') as f:
    sessions = json.load(f)
with open('configs/trade_strategies.json', 'r') as f:
    strategies = json.load(f).get('strategies', {})

session = sessions.get('907e9db9-7b3f-4743-921c-1c95b653eefc')
strat = strategies.get('3e569fe6-607a-4b79-9f17-6d7dc1c72e80')
ctx = {
    'sessions': sessions,
    'strategies': strategies,
    'ea_account_info': {
        'DEMO-MT4-1218954455': {'symbol': 'EURUSD', 'bid': 1.10, 'ask': 1.1001, 'conn_type': 'mt4_direct', 'last_update': time.time(), 'positions': 0, 'open_tickets': []},
        'DUKA-DEMO-DEMO2uotdK': {'symbol': 'EURUSD', 'bid': 1.10, 'ask': 1.1001, 'conn_type': 'fix', 'last_update': time.time(), 'positions': 0, 'open_tickets': []}
    },
    'lock': None,
    'in_flight_commands': {},
    'save_sessions': lambda: None,
    'log_event': lambda *args: None,
    'is_news_blackout': lambda x: (False, '')
}
trading_logic.init(ctx)

print('Time window:', trading_logic._is_within_time_window(session))
print('Should issue:', trading_logic._should_issue_command(session, 'DEMO-MT4-1218954455'))
