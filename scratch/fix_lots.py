import json
import os

path = r'd:\Documents\dev\TradeDashboard\TradeDashboardPY\configs\trade_sessions.json'
with open(path, 'r') as f:
    data = json.load(f)

for sid, session in data.items():
    if session.get('match_mode') == 'lots':
        for account in session.get('sides', {}):
            fl = 0.0
            for fill in session.get('fills', []):
                if fill.get('account') == account:
                    lots = fill.get('lots')
                    if lots is None or lots <= 0:
                        lots = session.get('sides', {}).get(account, {}).get('lot_size')
                        if lots is None or lots <= 0:
                            lots = session.get('lot_size', 0)
                        fill['lots'] = lots
                    fl += float(fill.get('lots', 0))
            
            cl = 0.0
            for cfill in session.get('close_fills', []):
                if cfill.get('account') == account:
                    lots = cfill.get('lots')
                    if lots is None or lots <= 0:
                        # Try to find corresponding fill
                        ticket = cfill.get('ticket')
                        for fill in session.get('fills', []):
                            if fill.get('account') == account and str(fill.get('ticket')) == str(ticket):
                                lots = fill.get('lots', 0)
                                break
                    if lots is None or lots <= 0:
                        lots = session.get('sides', {}).get(account, {}).get('lot_size')
                        if lots is None or lots <= 0:
                            lots = session.get('lot_size', 0)
                    cfill['lots'] = lots
                    cl += float(cfill.get('lots', 0))

            if 'filled_lots' not in session:
                session['filled_lots'] = {}
            session['filled_lots'][account] = round(fl, 4)
            
            if 'closed_lots' not in session:
                session['closed_lots'] = {}
            session['closed_lots'][account] = round(cl, 4)

with open(path, 'w') as f:
    json.dump(data, f, indent=2)
print('Fixed lots in trade_sessions.json')
