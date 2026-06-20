import os

with open('trade_dashboard.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Line 4357 has: DASHBOARD_HTML = r"""<!DOCTYPE html>
# Line 9384 has: </html>"""
start = 4357 - 1  # 0-indexed
html_lines = []

for i in range(start, len(lines)):
    line = lines[i]
    if i == start:
        idx = line.find('<!DOCTYPE')
        if idx >= 0:
            html_lines.append(line[idx:])
        continue
    # Check for closing triple-quote
    stripped = line.rstrip()
    if stripped.endswith('"""'):
        cleaned = stripped[:-3]
        if cleaned:
            html_lines.append(cleaned + '\n')
        break
    html_lines.append(line)

outdir = os.path.join('TradeDashboard', 'TradeDashboard.Web', 'wwwroot')
os.makedirs(outdir, exist_ok=True)
outpath = os.path.join(outdir, 'index.html')
with open(outpath, 'w', encoding='utf-8') as f:
    f.writelines(html_lines)
print(f'Extracted {len(html_lines)} lines to {outpath}')
