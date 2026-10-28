"""Offline metadata reports. No Telegram or database access."""
from __future__ import annotations

import html
import base64
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

CATEGORIES = ('Private Channels', 'Private Groups', 'Private Chats', 'Public Channels',
              'Public Groups', 'Bots', 'Unclassified')
NOTE = ('Metadata only; no message contents. Username confirms public discoverability; '
        'missing usernames do not prove privacy. Unknown fields may be unavailable or unsupported. '
        'Private user chats describe the dialog type, not profile visibility. '
        'Topic lists are accessible metadata and may be incomplete; an empty list is not proof of no topics. '
        'XLSX omits unsupported control characters; overlong Excel cells cause an explicit format failure, never silent truncation.')


def clean(value):
    return str(value).encode('utf-8', 'replace').decode('utf-8') if value is not None else ''


@dataclass(frozen=True)
class Record:
    name: str
    chat_id: str
    username: str
    chat_type: str
    privacy: str
    source: str
    forum: str
    topics: str
    metadata: str
    category: str


@dataclass(frozen=True)
class Snapshot:
    records: Tuple[Record, ...]
    timestamp: str
    discovered: int
    note: str
    target_user_id: Optional[int] = None

    @property
    def counts(self) -> Dict[str, int]:
        counts = dict.fromkeys(CATEGORIES, 0)
        for record in self.records:
            counts[record.category] += 1
        return counts


def normalize(job) -> Snapshot:
    records, seen = [], set()
    for rec in job.records:
        key = (rec.chat_id, rec.chat_type)
        if key in seen:
            continue
        seen.add(key)
        kind = rec.category
        privacy = 'UNKNOWN'
        source = 'Unverified / unavailable'
        if kind == 'private':
            privacy, source = 'PRIVATE', 'Telegram private dialog type'
        elif rec.username and kind in ('channel', 'group'):
            privacy, source = 'PUBLIC', 'Telegram active username'
        elif kind == 'group' and rec.chat_type in ('ChatType.GROUP', 'group'):
            privacy, source = 'PRIVATE', 'Telegram basic group type'
        category = 'Unclassified'
        if kind == 'bot':
            category = 'Bots'
        elif kind == 'private':
            category = 'Private Chats'
        elif privacy != 'UNKNOWN' and kind in ('channel', 'group'):
            category = ('Public' if privacy == 'PUBLIC' else 'Private') + (' Channels' if kind == 'channel' else ' Groups')
        seen_topics = set()
        topic_lines = []
        for t in rec.topics:
            if t.topic_id not in seen_topics:
                seen_topics.add(t.topic_id)
                topic_lines.append(f'{t.topic_id}: {clean(t.title)} (closed: {t.closed}, hidden: {t.hidden})')
        topic_status = getattr(rec, 'topics_status', 'Unavailable / not enumerated')
        topic_count = getattr(rec, 'topic_count', None)
        topics = clean(topic_status) + ('; total unknown' if topic_count is None else f'; API reported count: {topic_count}')
        if topic_lines:
            topics += '\n' + '\n'.join(topic_lines)
        metadata = '\n'.join(f'{label}: {clean(value)}' for label, value in (
            ('Description', rec.description), ('Member count', rec.member_count),
            ('Inspection', rec.inspection_status)) if value is not None)
        records.append(Record(clean(rec.title) or '(name unavailable)', str(rec.chat_id), clean(rec.username),
                              clean(rec.chat_type), privacy, source,
                              getattr(rec, 'forum_status', 'Unknown / unverified') if rec.is_forum is None else ('Yes' if rec.is_forum else 'No'),
                              topics, metadata or 'Unavailable', category))
    records.sort(key=lambda r: CATEGORIES.index(r.category))
    partial = sum(r.inspection_status != 'COMPLETE' for r in job.records)
    limitations = f' Collected {len(records)} of {job.dialogs_discovered} discovered chats. Operation failures: {len(job.failures)}. Partial inspections: {partial}.'
    if any(f.get('stage') == 'scan' for f in job.failures):
        limitations += ' Dialog scan interrupted or configured chat limit reached; discovered count is a lower bound.'
    return Snapshot(tuple(records), datetime.now(timezone.utc).isoformat(), job.dialogs_discovered,
                    NOTE + limitations, job.target_user_id)


def render_html(snapshot: Snapshot, path: Union[str, Path]) -> None:
    esc = lambda v: html.escape(clean(v), quote=True)
    cards = []
    for n, r in enumerate(snapshot.records, 1):
        link = ''
        if re.fullmatch(r'[A-Za-z0-9_]{1,64}', r.username):
            link = f'<a href="https://t.me/{esc(r.username)}" rel="noreferrer noopener" target="_blank">@{esc(r.username)}</a>'
        cards.append(f'''<article data-category="{esc(r.category)}" data-privacy="{esc(r.privacy)}" data-search="{esc(r.name+' '+r.chat_id+' '+r.username)}">
<details><summary><small>#{n} · {esc(r.category)}</small><h2>{esc(r.name)}</h2><code>{esc(r.chat_id)}</code> · {esc(r.privacy)}</summary>
<p>{link}</p><dl><dt>Full name</dt><dd>{esc(r.name)}</dd><dt>Type</dt><dd>{esc(r.chat_type)}</dd><dt>Privacy evidence</dt><dd>{esc(r.source)}</dd><dt>Forum enabled</dt><dd>{esc(r.forum)}</dd></dl>
<details><summary>Topic metadata</summary><pre>{esc(r.topics)}</pre></details><pre>{esc(r.metadata)}</pre></details>
<button class="copy" data-id="{esc(r.chat_id)}">Copy ID</button></article>''')
    options = ''.join(f'<option>{esc(c)}</option>' for c in CATEGORIES)
    stats = ''.join(f'<button class="stat" data-filter="{esc(cat)}"><strong>{count}</strong>{esc(cat)}</button>' for cat, count in snapshot.counts.items())
    content = '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src SCRIPT_POLICY; style-src 'unsafe-inline'; connect-src 'none'; img-src 'none'; base-uri 'none'; form-action 'none'; object-src 'none'">
<title>Userdata inventory</title><style>
*{box-sizing:border-box}body{margin:0;background:#0c1220;color:#edf3ff;font:16px system-ui,sans-serif}main{max-width:1200px;margin:auto;padding:16px}h1{font-size:clamp(24px,5vw,38px)}h2{font-size:18px;margin:10px 0;overflow-wrap:anywhere}small,dt{color:#a8bad4}header,p{line-height:1.6}.stats,.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,270px),1fr));gap:12px}.stats div,article{background:#172238;padding:16px;border:1px solid #304362;border-radius:14px;min-width:0}.stats strong{display:block;font-size:26px;color:#69e0cc}nav{position:sticky;top:0;background:#0c1220;padding:12px 0;display:flex;gap:8px;flex-wrap:wrap}input,select,button{font:inherit;background:#23324d;color:#fff;border:1px solid #526584;border-radius:8px;padding:10px;max-width:100%}input{flex:1;min-width:160px}button,summary{cursor:pointer}button{margin-top:8px}summary{overflow-wrap:anywhere}code,pre,dd{overflow-wrap:anywhere;white-space:pre-wrap;margin-left:0}a{color:#79ceff}[hidden]{display:none!important}input:focus,button:focus,select:focus,summary:focus{outline:2px solid #69e0cc}
</style><main><header><small>TELEGRAM INVENTORY · OFFLINE REPORT</small><h1>Telegram inventory</h1>'''
    content += '''<style>h2{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}article:has(>details[open]) h2{display:block}.stat{text-align:left;margin:0;min-height:100px}.stat strong{display:block;font-size:28px;color:#69e0cc}.stat:nth-child(-n+2){border-color:#69e0cc;background:#163437}article[data-category="Private Channels"],article[data-category="Private Groups"]{border-left:3px solid #69e0cc}.grid{align-items:start}.grid>article{content-visibility:auto;contain-intrinsic-size:auto 200px}nav{z-index:1}nav input{min-width:min(100%,160px)}@media(max-width:480px){main{padding:12px}.stats{grid-template-columns:1fr 1fr}.stat{font-size:13px;padding:12px}nav select{flex:1;min-width:0}nav input{flex-basis:100%}}@media(prefers-reduced-motion:reduce){*{scroll-behavior:auto}}</style>'''
    account = f'<p>Account: {esc(snapshot.target_user_id)}</p>' if snapshot.target_user_id is not None else ''
    content += f'{account}<p>{esc(snapshot.timestamp)} · UTC<br>{len(snapshot.records)} chats · {snapshot.discovered} discovered</p><p>{esc(snapshot.note)}</p></header><section class="stats" aria-label="Category totals">{stats}</section>'
    content += f'''<nav aria-label="Report filters"><input id="search" aria-label="Search name, ID or username" placeholder="Search name, ID, username"><select id="category" aria-label="Category"><option value="">All categories</option>{options}</select><select id="privacy" aria-label="Privacy"><option value="">All privacy states</option><option>PRIVATE</option><option>PUBLIC</option><option>UNKNOWN</option></select><button id="clear">Clear</button></nav><p id="count" aria-live="polite"></p><p id="empty" hidden>No matching chats.</p><section class="grid">{''.join(cards)}</section></main>'''
    content += '''<script>
const cards=Array.from(document.querySelectorAll('article')),search=document.getElementById('search'),category=document.getElementById('category'),privacy=document.getElementById('privacy');
const fold=s=>s.normalize('NFKC').toLowerCase().replace(/ß/g,'ss').replace(/ς/g,'σ');
const index=cards.map(c=>fold(c.dataset.search));
function filter(){const q=fold(search.value.trim());let n=0;cards.forEach((c,i)=>{const show=index[i].includes(q)&&(!category.value||c.dataset.category===category.value)&&(!privacy.value||c.dataset.privacy===privacy.value);c.hidden=!show;if(show)n++;});document.getElementById('count').textContent=n+' of '+cards.length+' chats';document.getElementById('empty').hidden=n!==0;}
search.addEventListener('input',filter);category.addEventListener('change',filter);privacy.addEventListener('change',filter);document.getElementById('clear').onclick=()=>{search.value=category.value=privacy.value='';filter();};
document.querySelectorAll('.stat').forEach(b=>b.onclick=()=>{category.value=b.dataset.filter;filter();});
document.querySelectorAll('.copy').forEach(b=>b.onclick=async()=>{try{await navigator.clipboard.writeText(b.dataset.id);b.textContent='Copied';}catch(e){const t=document.createElement('textarea');t.value=b.dataset.id;document.body.appendChild(t);t.select();b.textContent=document.execCommand('copy')?'Copied':'Select ID above to copy';t.remove();}});filter();</script></html>'''
    script = content.rsplit('<script>', 1)[1].split('</script>', 1)[0]
    digest = base64.b64encode(hashlib.sha256(script.encode('utf-8')).digest()).decode('ascii')
    content = content.replace('script-src SCRIPT_POLICY', f"script-src 'sha256-{digest}'", 1)
    Path(path).write_text(content, encoding='utf-8')


def render_xlsx(snapshot: Snapshot, path: Union[str, Path]) -> None:
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    def append(sheet, values):
        # Validate before openpyxl's cell binder silently truncates long strings.
        safe = []
        for value in values:
            if isinstance(value, str):
                value = ILLEGAL_CHARACTERS_RE.sub('', clean(value))
                if len(value) > 32767:
                    raise ValueError('Excel cell limit exceeded; report not truncated')
            safe.append(value)
        sheet.append(safe)
    book = Workbook()
    summary = book.active
    summary.title = 'Summary'
    append(summary, ['Telegram metadata inventory', snapshot.timestamp])
    summary.append(['Total chats', len(snapshot.records)])
    summary.append(['Discovered chats', snapshot.discovered])
    append(summary, ['Limitations', snapshot.note])
    for cat, count in snapshot.counts.items():
        summary.append([cat, count])
        summary.cell(summary.max_row, 1).hyperlink = f"#'{cat}'!A1"
    if snapshot.target_user_id is not None:
        append(summary, ['Account ID', str(snapshot.target_user_id)])
    headers = ['Number', 'Chat Name', 'Chat ID', 'Username', 'Chat Type', 'Privacy Status', 'Forum Enabled', 'Topic Information', 'Additional Available Metadata']
    for cat in CATEGORIES:
        sheet = book.create_sheet(cat)
        sheet.append(headers)
        for n, r in enumerate((r for r in snapshot.records if r.category == cat), 1):
            append(sheet, [n, r.name, r.chat_id, r.username, r.chat_type, r.privacy+' — '+r.source, r.forum, r.topics, r.metadata])
        sheet.auto_filter.ref = sheet.dimensions
        sheet.freeze_panes = 'A2'
        for col, width in zip('ABCDEFGHI', (10, 32, 25, 24, 24, 40, 26, 50, 50)):
            sheet.column_dimensions[col].width = width
        for cell in sheet['C'][1:]:
            cell.number_format = '@'
    summary.column_dimensions['A'].width = 36
    summary.column_dimensions['B'].width = 85
    for sheet in book:
        sheet.freeze_panes = 'A2'
        for row in sheet:
            sheet.row_dimensions[row[0].row].height = 48 if row[0].row > 1 else 30
            for cell in row:
                if isinstance(cell.value, str):
                    cell.value = ILLEGAL_CHARACTERS_RE.sub('', clean(cell.value))
                    if len(cell.value) > 32767:
                        raise ValueError('Excel cell limit exceeded; report not truncated')
                    cell.data_type = 's'  # Explicit strings prevent formulas, including = + - @.
                cell.alignment = Alignment(vertical='top', wrap_text=True)
                cell.font = Font(name='Calibri', size=11, color='FFFFFF' if cell.row == 1 else '182C45', bold=cell.row == 1)
                cell.fill = PatternFill('solid', fgColor='203A5C' if cell.row == 1 else ('EDF3FA' if cell.row % 2 == 0 else 'FFFFFF'))
    summary.row_dimensions[4].height = 120
    book.save(path)
    check = load_workbook(path, read_only=True, data_only=False)
    try:
        if check.sheetnames != ['Summary', *CATEGORIES]:
            raise ValueError('Workbook validation failed')
    finally:
        check.close()
