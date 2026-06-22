import os, json

tree_lines = []
base = '_lakehouse'
for root, dirs, files in os.walk(base):
    dirs.sort()
    level = root.replace(base, '').count(os.sep)
    indent = '  ' * level
    tree_lines.append(f'{indent}{os.path.basename(root)}/')
    if '_delta_log' in root:
        for f in sorted(files):
            tree_lines.append(f'{indent}  {f}')
    elif level <= 2 and files:
        pq = [f for f in files if f.endswith('.parquet')]
        if pq:
            tree_lines.append(f'{indent}  {pq[0]} (+{len(pq)-1} more .parquet)')
tree_text = '\n'.join(tree_lines)

with open('_lakehouse/bronze/llm_calls_raw/_delta_log/00000000000000000000.json') as f:
    raw_json = f.read()
parsed_lines = []
for line in raw_json.strip().splitlines():
    parsed_lines.append(json.dumps(json.loads(line), indent=2))
json_text = '\n---\n'.join(parsed_lines)

html_parts = [
    '<!DOCTYPE html><html><head><meta charset="UTF-8">',
    '<style>',
    'body{font-family:monospace;background:#1e1e1e;color:#d4d4d4;padding:20px;font-size:13px}',
    'h2{color:#4ec9b0;border-bottom:1px solid #444;padding-bottom:6px;margin-top:30px}',
    'pre{background:#252526;border:1px solid #444;padding:16px;border-radius:4px;white-space:pre;line-height:1.6}',
    '</style></head><body>',
    '<h2>Criterion 1a — _lakehouse/ tree (Delta tables on local FS)</h2>',
    '<pre>' + tree_text.replace('<','&lt;').replace('>','&gt;') + '</pre>',
    '<h2>Criterion 1a — Bronze _delta_log/00000000000000000000.json content</h2>',
    '<pre>' + json_text.replace('<','&lt;').replace('>','&gt;') + '</pre>',
    '</body></html>',
]
html = '\n'.join(html_parts)

with open('submission/screenshots/evidence_delta_log.html', 'w', encoding='utf-8') as f:
    f.write(html)
print('done')
