import os,sys,json,re,sqlite3,secrets,hashlib,hmac,base64,datetime,threading,urllib.parse,html
from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
DB='/app/huddle.db'; PORT=int(sys.argv[1]) if len(sys.argv)>1 else 8000; NODE=PORT-8000
lock=threading.RLock()
def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00','Z')
def db():
 c=sqlite3.connect(DB,check_same_thread=False); c.row_factory=sqlite3.Row
 c.executescript('''CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY,username TEXT UNIQUE,password TEXT,display_name TEXT,timezone TEXT DEFAULT 'UTC',avatar_url TEXT DEFAULT '',status_text TEXT DEFAULT '',status_emoji TEXT DEFAULT '');
 CREATE TABLE IF NOT EXISTS tokens(token TEXT PRIMARY KEY,user_id INTEGER);
 CREATE TABLE IF NOT EXISTS workspaces(id INTEGER PRIMARY KEY,slug TEXT UNIQUE,name TEXT,owner_id INTEGER,join_mode TEXT DEFAULT 'open');
 CREATE TABLE IF NOT EXISTS members(workspace_id INTEGER,user_id INTEGER,role TEXT,PRIMARY KEY(workspace_id,user_id));
 CREATE TABLE IF NOT EXISTS channels(id INTEGER PRIMARY KEY,workspace_id INTEGER,name TEXT,is_private INTEGER DEFAULT 0,is_dm INTEGER DEFAULT 0,topic TEXT DEFAULT '',is_archived INTEGER DEFAULT 0,UNIQUE(workspace_id,name));
 CREATE TABLE IF NOT EXISTS cmembers(channel_id INTEGER,user_id INTEGER,PRIMARY KEY(channel_id,user_id));
 CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY,channel_id INTEGER,author_id INTEGER,body TEXT,parent_id INTEGER,created_at TEXT,edited_at TEXT,deleted INTEGER DEFAULT 0,event_id INTEGER);
 CREATE TABLE IF NOT EXISTS reactions(message_id INTEGER,user_id INTEGER,emoji TEXT,PRIMARY KEY(message_id,user_id,emoji));
 CREATE TABLE IF NOT EXISTS pins(message_id INTEGER PRIMARY KEY,user_id INTEGER,pinned_at TEXT);
 CREATE TABLE IF NOT EXISTS reads(channel_id INTEGER,user_id INTEGER,last_read_event_id INTEGER DEFAULT 0);
 CREATE TABLE IF NOT EXISTS events(channel_id INTEGER,event_id INTEGER,kind TEXT,message_id INTEGER,created_at TEXT,PRIMARY KEY(channel_id,event_id));
 CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY,uploader_id INTEGER,filename TEXT,content_type TEXT,size INTEGER,body BLOB,created_at TEXT);
 CREATE TABLE IF NOT EXISTS dms(a INTEGER,b INTEGER,channel_id INTEGER,PRIMARY KEY(a,b));
 CREATE TABLE IF NOT EXISTS invitations(code TEXT PRIMARY KEY,workspace_id INTEGER,invited_user_id INTEGER,expires_at TEXT,max_uses INTEGER DEFAULT 1,uses INTEGER DEFAULT 0,created_at TEXT);
 CREATE TABLE IF NOT EXISTS groups(id INTEGER PRIMARY KEY,workspace_id INTEGER,handle TEXT,name TEXT,creator_id INTEGER,user_ids TEXT,UNIQUE(workspace_id,handle));'''); return c
C=db()
def q(sql,args=(),one=False):
 with lock:
  c=db(); x=c.execute(sql,args); c.commit()
  if sql.lstrip().upper().startswith(('INSERT','UPDATE','DELETE','REPLACE')):
   c.close(); return x
  r=x.fetchone() if one else x.fetchall(); c.close(); return r
def user(r):
 return dict(r) if r else None
def tok(uid):
 t=secrets.token_urlsafe(24); q('INSERT INTO tokens VALUES(?,?)',(t,uid)); return t
def auth(h):
 v=h.get('Authorization','')
 if v.startswith('Bearer '): return q('SELECT u.* FROM users u JOIN tokens t ON t.user_id=u.id WHERE t.token=?',(v[7:],),True)
 return None
def j(o): return json.dumps(o,separators=(',',':')).encode()
def iso_user(u):
 d=dict(u); d.pop('password',None); return d
def channel(r): return dict(r) if r else None
def msg(r):
 if not r:return None
 d=dict(r); d['author']=iso_user(q('SELECT * FROM users WHERE id=?',(d['author_id'],),True)); d['parent_id']=d.get('parent_id')
 d['files']=[]; d['mentions']=[x for x in re.findall(r'(?<!\w)@([A-Za-z0-9_]+)',d['body'] or '') if x!='channel']
 rr=q('SELECT emoji,COUNT(*) n,GROUP_CONCAT(user_id) ids FROM reactions WHERE message_id=? GROUP BY emoji',(d['id'],))
 d['reactions']=[{'emoji':x['emoji'],'count':x['n'],'user_ids':[int(z) for z in x['ids'].split(',')]} for x in rr]
 d['reply_count']=q('SELECT COUNT(*) n FROM messages WHERE parent_id=? AND deleted=0',(d['id'],),True)['n']; return d
def emit(cid,kind,mid):
 n=q('SELECT COALESCE(MAX(event_id),0)+1 n FROM events WHERE channel_id=?',(cid,),True)['n']; q('INSERT INTO events VALUES(?,?,?,?,?)',(cid,n,kind,mid,now())); return n

class H(BaseHTTPRequestHandler):
 protocol_version='HTTP/1.1'
 def log_message(self,*a): pass
 def send(self,status,obj=None,ctype='application/json',extra=None):
  b=obj if isinstance(obj,bytes) else j(obj) if obj is not None else b''
  self.send_response(status); self.send_header('Content-Type',ctype); self.send_header('Content-Length',str(len(b))); self.send_header('Access-Control-Allow-Origin','*')
  if extra:
   for k,v in extra.items(): self.send_header(k,v)
  self.end_headers(); self.wfile.write(b)
 def body(self):
  n=int(self.headers.get('Content-Length','0')); return json.loads(self.rfile.read(n) or b'{}')
 def route(self):
  p=urllib.parse.urlparse(self.path); return p.path,p.query
 def do_OPTIONS(self): self.send(204)
 def do_GET(self):
  p,qs=self.route()
  if p=='/': return self.send(200,SPA.encode(),'text/html')
  if p=='/api/health': return self.send(200,{'status':'ok','node_id':NODE})
  if p=='/api/ws':
   qt=urllib.parse.parse_qs(qs).get('token',[''])[0]
   u=auth(self.headers) or q('SELECT u.* FROM users u JOIN tokens t ON t.user_id=u.id WHERE t.token=?',(qt,),True)
   if not u:return self.send(401,{'error':'invalid token'})
   if self.headers.get('Upgrade','').lower()!='websocket':return self.send(426,{'error':'websocket required'})
   key=self.headers.get('Sec-WebSocket-Key','')
   if not key:return self.send(400,{'error':'missing websocket key'})
   accept=base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
   self.send_response(101,'Switching Protocols'); self.send_header('Upgrade','WebSocket'); self.send_header('Connection','Upgrade'); self.send_header('Sec-WebSocket-Accept',accept); self.end_headers()
   def wf(obj):
    b=json.dumps(obj,separators=(',',':')).encode(); n=len(b)
    if n<126:h=bytes([129,n])
    elif n<65536:h=bytes([129,126])+n.to_bytes(2,'big')
    else:h=bytes([129,127])+n.to_bytes(8,'big')
    return h+b
   def rf():
    h=self.rfile.read(2)
    if len(h)<2:return None
    ln=h[1]&127; masked=h[1]&128
    if ln==126:ln=int.from_bytes(self.rfile.read(2),'big')
    elif ln==127:ln=int.from_bytes(self.rfile.read(8),'big')
    mask=self.rfile.read(4) if masked else b''; data=self.rfile.read(ln)
    if masked:data=bytes(data[i]^mask[i%4] for i in range(ln))
    return json.loads(data.decode()) if data else None
   try:
    while True:
     z=rf()
     if not z:break
     cid=int(z.get('channel_id',0)); head=q('SELECT COALESCE(MAX(event_id),0) n FROM events WHERE channel_id=?',(cid,),True)['n']
     if z.get('type')=='subscribe':self.request.sendall(wf({'type':'subscribed','channel_id':cid,'head_event_id':head}))
     elif z.get('type')=='resume':
      since=int(z.get('since_event_id',0)); rows=q('SELECT * FROM events WHERE channel_id=? AND event_id>? ORDER BY event_id',(cid,since))
      for e in rows:
       self.request.sendall(wf({'type':e['kind'],'event_id':e['event_id'],'channel_id':cid,'message':msg(q('SELECT * FROM messages WHERE id=?',(e['message_id'],),True)) if e['message_id'] else None}))
      self.request.sendall(wf({'type':'resumed','channel_id':cid,'head_event_id':head}))
   except Exception: pass
   return
  u=auth(self.headers)
  if p=='/api/auth/me':
   return self.send(200,{'user':iso_user(u)}) if u else self.send(401,{'error':'unauthorized'})
  if not u:return self.send(401,{'error':'unauthorized'})
  if p=='/api/workspaces':
   ws=q('SELECT * FROM workspaces w JOIN members m ON m.workspace_id=w.id WHERE m.user_id=?',(u['id'],))
   return self.send(200,{'workspaces':[dict(x) for x in ws]})
  m=re.match(r'^/api/channels/(\d+)/read$',p)
  if m:
   x=q('SELECT * FROM reads WHERE channel_id=? AND user_id=?',(m.group(1),u['id']),True)
   last=x['last_read_event_id'] if x else 0
   return self.send(200,{'read_state':{'channel_id':int(m.group(1)),'last_read_event_id':last,'unread_count':0,'mention_count':0}})
  m=re.match(r'^/api/files/(\d+)/download$',p)
  if m:
   x=q('SELECT * FROM files WHERE id=?',(m.group(1),),True)
   if not x:return self.send(404,{'error':'not found'})
   return self.send(200,bytes(x['body'] or b''),x['content_type'] or 'application/octet-stream',{'Content-Disposition':'attachment; filename="'+x['filename']+'"'})
  m=re.match(r'^/api/files/(\\d+)$',p)
  if m:
   x=q('SELECT id,uploader_id,filename,content_type,size,created_at FROM files WHERE id=?',(m.group(1),),True)
   return self.send(200,{'file':dict(x)}) if x else self.send(404,{'error':'not found'})
  m=re.match(r'^/api/users/(\\d+)$',p)
  if m:
   x=q('SELECT * FROM users WHERE id=?',(m.group(1),),True); return self.send(200,{'user':iso_user(x)}) if x else self.send(404,{'error':'not found'})
  m=re.match(r'^/api/workspaces/([^/]+)/members$',p)
  if m:
   w=q('SELECT * FROM workspaces WHERE slug=?',(m.group(1),),True)
   if not w:return self.send(404,{'error':'not found'})
   rows=q('SELECT u.id user_id,u.username,u.display_name,m.role FROM users u JOIN members m ON m.user_id=u.id WHERE m.workspace_id=?',(w['id'],))
   return self.send(200,{'members':[dict(x) for x in rows]})
  m=re.match(r'^/api/workspaces/([^/]+)/invitations$',p)
  if m:
   w=q('SELECT * FROM workspaces WHERE slug=?',(m.group(1),),True)
   if not w:return self.send(404,{'error':'not found'})
   rows=q('SELECT * FROM invitations WHERE workspace_id=?',(w['id'],))
   return self.send(200,{'invitations':[dict(x) for x in rows]})
  m=re.match(r'^/api/workspaces/([^/]+)/groups$',p)
  if m:
   w=q('SELECT * FROM workspaces WHERE slug=?',(m.group(1),),True)
   if not w:return self.send(404,{'error':'not found'})
   out=[]
   for x in q('SELECT * FROM groups WHERE workspace_id=?',(w['id'],)):
    d=dict(x); d['member_user_ids']=[int(z) for z in (d.pop('user_ids') or '').split(',') if z]; out.append(d)
   return self.send(200,{'groups':out})
  m=re.match(r'^/api/workspaces/([^/]+)$',p)
  if m:
   w=q('SELECT * FROM workspaces WHERE slug=?',(m.group(1),),True)
   if not w:return self.send(404,{'error':'not found'})
   ch=q('SELECT * FROM channels WHERE workspace_id=? AND (is_archived=0 OR is_archived IS NULL)',(w['id'],))
   return self.send(200,{'workspace':dict(w),'channels':[dict(x) for x in ch],'read_state':[]})
  m=re.match(r'^/api/channels/(\d+)/messages$',p)
  if m:
   rows=q('SELECT * FROM messages WHERE channel_id=? AND deleted=0 ORDER BY id DESC LIMIT 200',(m.group(1),))
   return self.send(200,{'messages':[msg(x) for x in rows],'next_cursor':None})
  m=re.match(r'^/api/channels/(\d+)$',p)
  if m:
   x=q('SELECT * FROM channels WHERE id=?',(m.group(1),),True)
   return self.send(200,{'channel':dict(x)}) if x else self.send(404,{'error':'not found'})
  m=re.match(r'^/api/channels/(\d+)/members$',p)
  if m:
   rows=q('SELECT u.* FROM users u JOIN cmembers c ON c.user_id=u.id WHERE c.channel_id=?',(m.group(1),))
   return self.send(200,{'members':[iso_user(x) for x in rows]})
  m=re.match(r'^/api/channels/(\d+)/pins$',p)
  if m:
   rows=q('SELECT p.*,m.* FROM pins p JOIN messages m ON m.id=p.message_id WHERE m.channel_id=? ORDER BY p.pinned_at DESC',(m.group(1),))
   return self.send(200,{'pins':[{'message':msg(x),'pinned_by':x['user_id'],'pinned_at':x['pinned_at']} for x in rows],'next_cursor':None})
  m=re.match(r'^/api/messages/(\d+)/replies$',p)
  if m:
   rows=q('SELECT * FROM messages WHERE parent_id=? AND deleted=0 ORDER BY id',(m.group(1),)); return self.send(200,{'replies':[msg(x) for x in rows],'next_cursor':None})
  if p=='/api/search':
   term=urllib.parse.parse_qs(qs).get('q',[''])[0]; rows=q('SELECT * FROM messages WHERE body LIKE ? AND deleted=0 ORDER BY id DESC',(f'%{term}%',)); return self.send(200,{'results':[msg(x) for x in rows],'next_cursor':None})
  return self.send(404,{'error':'not found'})
 def do_POST(self):
  p,_=self.route()
  if p=='/api/auth/register':
   try:
    b=self.body(); un=b.get('username',''); pw=b.get('password','')
    if not re.fullmatch(r'[A-Za-z0-9_]+',un) or len(pw)<8:return self.send(400,{'error':'invalid registration'})
    uid=q('INSERT INTO users(username,password,display_name) VALUES(?,?,?)',(un,hashlib.sha256(pw.encode()).hexdigest(),b.get('display_name') or un))
    uid=uid.lastrowid; t=tok(uid); return self.send(201,{'user':iso_user(q('SELECT * FROM users WHERE id=?',(uid,),True)),'token':t})
   except sqlite3.IntegrityError:return self.send(409,{'error':'duplicate username'})
  if p=='/api/auth/login':
   b=self.body(); x=q('SELECT * FROM users WHERE username=? AND password=?',(b.get('username'),hashlib.sha256(b.get('password','').encode()).hexdigest()),True)
   return self.send(200,{'user':iso_user(x),'token':tok(x['id'])}) if x else self.send(401,{'error':'wrong password'})
  u=auth(self.headers)
  if not u:return self.send(401,{'error':'unauthorized'})
  if p=='/api/files':
   ct=self.headers.get('Content-Type','')
   if 'multipart/form-data' not in ct:return self.send(400,{'error':'multipart required'})
   raw=self.rfile.read(int(self.headers.get('Content-Length','0'))); boundary=ct.split('boundary=',1)[-1].strip().strip('"').encode(); part=raw.split(b'--'+boundary)
   data=next((x for x in part if b'filename=' in x),b''); bits=data.split(b'\r\n\r\n',1); head=bits[0]; body=bits[1] if len(bits)>1 else b''; body=body.split(b'\r\n--',1)[0]; body=body[:-2] if body.endswith(b'\r\n') else body
   mm=re.search(br'filename="([^"]*)"',head); cm=re.search(br'Content-Type:\s*([^\r\n]+)',head,re.I); fn=(mm.group(1).decode(errors='replace') if mm else 'upload'); typ=(cm.group(1).decode().strip() if cm else 'application/octet-stream')
   if len(body)>10*1024*1024:return self.send(413,{'error':'file too large'})
   x=q('INSERT INTO files(uploader_id,filename,content_type,size,body,created_at) VALUES(?,?,?,?,?,?)',(u['id'],fn,typ,len(body),body,now())); fid=x.lastrowid
   return self.send(201,{'file':dict(q('SELECT id,uploader_id,filename,content_type,size,created_at FROM files WHERE id=?',(fid,),True))})
  m=re.match(r'^/api/invitations/([^/]+)/accept$',p)
  if m:
   x=q('SELECT * FROM invitations WHERE code=?',(m.group(1),),True)
   if not x or x['uses']>=x['max_uses'] or (x['expires_at'] and x['expires_at']<now()):return self.send(404,{'error':'invalid invitation'})
   if x['invited_user_id'] and x['invited_user_id']!=u['id']:return self.send(403,{'error':'targeted invitation'})
   q('INSERT OR REPLACE INTO members VALUES(?,?,?)',(x['workspace_id'],u['id'],'member')); q('UPDATE invitations SET uses=uses+1 WHERE code=?',(m.group(1),))
   return self.send(200,{'workspace':dict(q('SELECT * FROM workspaces WHERE id=?',(x['workspace_id'],),True))})
  m=re.match(r'^/api/workspaces/([^/]+)/invitations$',p)
  if m:
   w=q('SELECT * FROM workspaces WHERE slug=?',(m.group(1),),True); b=self.body()
   if not w:return self.send(404,{'error':'not found'})
   code=secrets.token_urlsafe(12); target=b.get('invited_user_id'); exp=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=int(b.get('expires_in_seconds',604800)))).isoformat().replace('+00:00','Z')
   q('INSERT INTO invitations VALUES(?,?,?,?,?,?,?)',(code,w['id'],target,exp,int(b.get('max_uses',1)),0,now()))
   return self.send(201,{'invitation':dict(q('SELECT * FROM invitations WHERE code=?',(code,),True))})
  m=re.match(r'^/api/workspaces/([^/]+)/groups$',p)
  if m:
   w=q('SELECT * FROM workspaces WHERE slug=?',(m.group(1),),True); b=self.body()
   if not w:return self.send(404,{'error':'not found'})
   try:
    ids=','.join(str(int(x)) for x in b.get('user_ids',[])); x=q('INSERT INTO groups(workspace_id,handle,name,creator_id,user_ids) VALUES(?,?,?,?,?)',(w['id'],b['handle'],b.get('name',b['handle']),u['id'],ids)); d=dict(q('SELECT * FROM groups WHERE id=?',(x.lastrowid,),True)); d['member_user_ids']=[int(z) for z in ids.split(',') if z]; d.pop('user_ids',None); return self.send(201,{'group':d})
   except sqlite3.IntegrityError:return self.send(409,{'error':'duplicate group'})
  if p=='/api/dms':
   b=self.body(); other=int(b.get('user_id',0)); a,bid=sorted((u['id'],other))
   x=q('SELECT channel_id FROM dms WHERE a=? AND b=?',(a,bid),True)
   if x:return self.send(200,{'channel_id':x['channel_id']})
   c=q('INSERT INTO channels(workspace_id,name,is_dm) VALUES(?,?,1)',(0,'dm-'+str(a)+'-'+str(bid))); cid=c.lastrowid
   q('INSERT INTO dms VALUES(?,?,?)',(a,bid,cid)); q('INSERT INTO cmembers VALUES(?,?)',(cid,a)); q('INSERT INTO cmembers VALUES(?,?)',(cid,bid))
   return self.send(200,{'channel_id':cid})
  m=re.match(r'^/api/channels/(\d+)/read$',p)
  if m:
   b=self.body(); eid=int(b.get('last_read_event_id',0)); old=q('SELECT last_read_event_id FROM reads WHERE channel_id=? AND user_id=?',(m.group(1),u['id']),True)
   if old and old['last_read_event_id']>eid:eid=old['last_read_event_id']
   q('INSERT OR REPLACE INTO reads VALUES(?,?,?)',(m.group(1),u['id'],eid))
   return self.send(200,{'read_state':{'channel_id':int(m.group(1)),'last_read_event_id':eid,'unread_count':0,'mention_count':0}})
  if p=='/api/workspaces':
   b=self.body()
   if not re.fullmatch(r'[a-z0-9-]{2,32}',b.get('slug','')):return self.send(400,{'error':'invalid slug'})
   try:
    w=q('INSERT INTO workspaces(slug,name,owner_id) VALUES(?,?,?)',(b['slug'],b.get('name',b['slug']),u['id'])); wid=w.lastrowid
    q('INSERT INTO members VALUES(?,?,?)',(wid,u['id'],'owner')); c=q('INSERT INTO channels(workspace_id,name) VALUES(?,?)',(wid,'general')); cid=c.lastrowid; q('INSERT INTO cmembers VALUES(?,?)',(cid,u['id']))
    return self.send(201,{'workspace':dict(q('SELECT * FROM workspaces WHERE id=?',(wid,),True)),'general_channel':dict(q('SELECT * FROM channels WHERE id=?',(cid,),True))})
   except sqlite3.IntegrityError:return self.send(409,{'error':'duplicate slug'})
  m=re.match(r'^/api/workspaces/([^/]+)/channels$',p)
  if m:
   w=q('SELECT * FROM workspaces WHERE slug=?',(m.group(1),),True); b=self.body()
   if not w:return self.send(404,{'error':'not found'})
   if not re.fullmatch(r'[a-z0-9-]{1,32}',b.get('name','')):return self.send(400,{'error':'invalid name'})
   try:
    x=q('INSERT INTO channels(workspace_id,name,is_private,topic) VALUES(?,?,?,?)',(w['id'],b['name'],int(bool(b.get('is_private'))),b.get('topic',''))); cid=x.lastrowid; q('INSERT INTO cmembers VALUES(?,?)',(cid,u['id'])); return self.send(201,{'channel':dict(q('SELECT * FROM channels WHERE id=?',(cid,),True))})
   except sqlite3.IntegrityError:return self.send(409,{'error':'duplicate channel'})
  m=re.match(r'^/api/channels/(\d+)/(leave|join)$',p)
  if m:
   cid=int(m.group(1)); c=q('SELECT * FROM channels WHERE id=?',(cid,),True)
   if not c:return self.send(404,{'error':'not found'})
   if m.group(2)=='leave':q('DELETE FROM cmembers WHERE channel_id=? AND user_id=?',(cid,u['id']))
   else:q('INSERT OR IGNORE INTO cmembers VALUES(?,?)',(cid,u['id']))
   return self.send(200,{'channel':dict(c)})
  m=re.match(r'^/api/channels/(\d+)/join$',p)
  if m:
   cid=int(m.group(1)); c=q('SELECT * FROM channels WHERE id=?',(cid,),True)
   if not c:return self.send(404,{'error':'not found'})
   q('INSERT OR IGNORE INTO cmembers VALUES(?,?)',(cid,u['id'])); q('INSERT OR IGNORE INTO members VALUES(?,?,?)',(c['workspace_id'],u['id'],'member')); return self.send(200,{'channel':dict(c)})
  m=re.match(r'^/api/messages/(\d+)/reactions$',p)
  if m:
   mid=int(m.group(1)); b=self.body(); emoji=b.get('emoji','')
   if not emoji:return self.send(400,{'error':'emoji required'})
   q('INSERT OR IGNORE INTO reactions(message_id,user_id,emoji) VALUES(?,?,?)',(mid,u['id'],emoji))
   rr=q('SELECT emoji,COUNT(*) n,GROUP_CONCAT(user_id) ids FROM reactions WHERE message_id=? GROUP BY emoji',(mid,))
   return self.send(200,{'reactions':[{'emoji':x['emoji'],'count':x['n'],'user_ids':[int(z) for z in x['ids'].split(',')]} for x in rr]})
  m=re.match(r'^/api/messages/(\d+)/pin$',p)
  if m:
   mid=int(m.group(1)); q('INSERT OR IGNORE INTO pins VALUES(?,?,?)',(mid,u['id'],now()))
   x=q('SELECT * FROM pins WHERE message_id=?',(mid,),True); return self.send(200,{'pin':{'message_id':x['message_id'],'pinned_by':x['user_id'],'pinned_at':x['pinned_at']}})
  m=re.match(r'^/api/channels/(\d+)/(archive|unarchive)$',p)
  if m:
   archived=1 if m.group(2)=='archive' else 0; q('UPDATE channels SET is_archived=? WHERE id=?',(archived,m.group(1)))
   return self.send(200,{'channel':dict(q('SELECT * FROM channels WHERE id=?',(m.group(1),),True))})
  m=re.match(r'^/api/channels/(\d+)/messages$',p)
  if m:
   cid=int(m.group(1)); b=self.body(); body=b.get('body','')
   if not body:return self.send(400,{'error':'empty message'})
   if body.startswith('/shrug'): body=body[6:].strip()+' ¯\\\\_(ツ)_/¯'
   x=q('INSERT INTO messages(channel_id,author_id,body,parent_id,created_at,event_id) VALUES(?,?,?,?,?,?)',(cid,u['id'],body,b.get('parent_id'),now(),0)); mid=x.lastrowid; eid=emit(cid,'message.reply' if b.get('parent_id') else 'message.created',mid); q('UPDATE messages SET event_id=? WHERE id=?',(eid,mid)); return self.send(201,{'message':msg(q('SELECT * FROM messages WHERE id=?',(mid,),True))})
  return self.send(404,{'error':'not found'})
 def do_PATCH(self):
  p,_=self.route(); u=auth(self.headers)
  if not u:return self.send(401,{'error':'unauthorized'})
  m=re.match(r'^/api/messages/(\d+)$',p)
  if m:
   b=self.body(); x=q('SELECT * FROM messages WHERE id=?',(m.group(1),),True)
   if not x:return self.send(404,{'error':'not found'})
   if x['author_id']!=u['id']:return self.send(403,{'error':'forbidden'})
   q('UPDATE messages SET body=?,edited_at=? WHERE id=?',(b.get('body',''),now(),m.group(1))); return self.send(200,{'message':msg(q('SELECT * FROM messages WHERE id=?',(m.group(1),),True))})
  m=re.match(r'^/api/users/me$',p)
  if m:
   b=self.body(); allowed=['display_name','timezone','avatar_url','status_text','status_emoji']; sets=[k+'=?' for k in allowed if k in b]
   if sets:q('UPDATE users SET '+','.join(sets)+' WHERE id=?',tuple(b[k] for k in allowed if k in b)+(u['id'],))
   return self.send(200,{'user':iso_user(q('SELECT * FROM users WHERE id=?',(u['id'],),True))})
  m=re.match(r'^/api/workspaces/([^/]+)$',p)
  if m:
   w=q('SELECT * FROM workspaces WHERE slug=?',(m.group(1),),True); b=self.body()
   if not w:return self.send(404,{'error':'not found'})
   if 'name' in b:q('UPDATE workspaces SET name=? WHERE id=?',(b['name'],w['id']))
   if 'join_mode' in b and b['join_mode'] in ('open','invite_only'):q('UPDATE workspaces SET join_mode=? WHERE id=?',(b['join_mode'],w['id']))
   return self.send(200,{'workspace':dict(q('SELECT * FROM workspaces WHERE id=?',(w['id'],),True))})
  m=re.match(r'^/api/channels/(\d+)$',p)
  if m:
   b=self.body()
   if 'topic' in b and len(b['topic'])>250:return self.send(400,{'error':'topic too long'})
   if 'topic' in b:q('UPDATE channels SET topic=? WHERE id=?',(b['topic'],m.group(1)))
   return self.send(200,{'channel':dict(q('SELECT * FROM channels WHERE id=?',(m.group(1),),True))})
  return self.send(404,{'error':'not found'})
 def do_DELETE(self):
  u=auth(self.headers)
  if not u:return self.send(401,{'error':'unauthorized'})
  m=re.match(r'^/api/messages/(\d+)$',self.path)
  if m:q('UPDATE messages SET deleted=1 WHERE id=?',(m.group(1),)); return self.send(200,{'deleted':True})
  m=re.match(r'^/api/messages/(\d+)/reactions/(.+)$',self.path)
  if m:q('DELETE FROM reactions WHERE message_id=? AND user_id=? AND emoji=?',(m.group(1),u['id'],urllib.parse.unquote(m.group(2)))); return self.send(200,{'reactions':[]})
  m=re.match(r'^/api/messages/(\d+)/pin$',self.path)
  if m:q('DELETE FROM pins WHERE message_id=?',(m.group(1),)); return self.send(200,{'unpinned':True})
  return self.send(404,{'error':'not found'})
SPA='''<!doctype html><html><head><meta charset="utf-8"><title>Huddle</title><style>body{margin:0;font:14px Arial;color:#222}button{border:0;padding:9px 14px;border-radius:4px;color:#fff}button[data-button-role=primary]{background:#007a5a}button[data-button-role=danger]{background:#e01e5a}button[data-button-role=secondary]{background:#1264a3}.layout{display:flex;height:100vh}.side{width:250px;background:#3f0e40;color:white;padding:18px}.main{flex:1;padding:25px}.msg{padding:12px;border-bottom:1px solid #ddd}.modal{padding:30px;max-width:400px}input,textarea{padding:10px;margin:5px;width:90%}</style></head><body><div id="app"></div><script>
const A='/api';let token=localStorage.getItem('huddle.token'),me=null,ws=null,channels=[],cur=null;
const el=(x,a={})=>{let e=document.createElement(x);for(let k in a)e.setAttribute(k,a[k]);return e};
async function api(p,o={}){o.headers={...(o.headers||{}),...(token?{Authorization:'Bearer '+token}:{'Content-Type':'application/json'})};if(o.body&&typeof o.body!='string')o.body=JSON.stringify(o.body);let r=await fetch(A+p,o);let x=await r.json().catch(()=>({}));if(!r.ok)throw Error(x.error||'Request failed');return x}
function auth(){let d=document.createElement('div');d.className='modal';d.innerHTML='<h1>Huddle</h1><form data-testid="auth-form"><input name="username" placeholder="username"><input name="password" type="password" placeholder="password"><input name="display_name" placeholder="display name"><button data-testid="auth-submit" data-button-role="primary">Sign in</button><button type="button" data-testid="auth-toggle">Register</button><p id="err"></p></form>';document.body.innerHTML='';document.body.append(d);let reg=false;d.querySelector('[data-testid=auth-toggle]').onclick=()=>{reg=!reg;d.querySelector('[data-testid=auth-submit]').textContent=reg?'Create account':'Sign in'};d.querySelector('form').onsubmit=async e=>{e.preventDefault();let b=Object.fromEntries(new FormData(e.target));try{let x=await api(reg?'/auth/register':'/auth/login',{method:'POST',body:b});token=x.token;localStorage.setItem('huddle.token',token);load()}catch(x){d.querySelector('#err').textContent=x.message}}}
async function load(){try{me=(await api('/auth/me')).user;channels=(await api('/workspaces')).workspaces;render()}catch(e){auth()}}
async function render(){let w=channels[0];if(!w){document.body.innerHTML='<div class="modal"><h2 data-testid="empty-state-create-workspace">Create a workspace</h2><form data-testid="workspace-create-form"><input name="slug" placeholder="workspace-slug"><input name="name" placeholder="Workspace name"><button data-testid="workspace-general-submit" data-button-role="primary">Create</button></form></div>';document.querySelector('form').onsubmit=async e=>{e.preventDefault();await api('/workspaces',{method:'POST',body:Object.fromEntries(new FormData(e.target))});load()};return}let d=await api('/workspaces/'+w.slug);document.body.innerHTML='<div class="layout"><aside class="side"><h2 data-testid="workspace-header">'+w.name+'</h2><div data-testid="current-user">'+me.display_name+'</div><button data-testid="logout-btn" data-button-role="danger">Logout</button><h3>Channels</h3><div data-testid="channel-list"></div><button data-testid="new-channel-btn" data-button-role="secondary">New channel</button></aside><main class="main"><h2 data-testid="channel-title">#general</h2><div data-testid="channel-topic"></div><div data-testid="message-list"></div><form><input data-testid="message-input" name="body" placeholder="Message"><button data-testid="send-btn" data-button-role="primary">Send</button></form></main></div>';let list=document.querySelector('[data-testid=channel-list]');d.channels.forEach(c=>{let b=el('button',{'data-testid':'channel-entry','data-channel-id':c.id,'data-channel-name':c.name});b.textContent='# '+c.name;b.onclick=()=>show(c);list.append(b)});document.querySelector('[data-testid=logout-btn]').onclick=()=>{localStorage.clear();auth()};document.querySelector('form').onsubmit=async e=>{e.preventDefault();let b=Object.fromEntries(new FormData(e.target));if(cur)await api('/channels/'+cur.id+'/messages',{method:'POST',body:b});e.target.reset();if(cur)show(cur)};if(d.channels[0])show(d.channels[0])}
async function show(c){cur=c;document.querySelector('[data-testid=channel-title]').textContent='#'+c.name;let x=await api('/channels/'+c.id+'/messages');document.querySelector('[data-testid=message-list]').innerHTML=x.messages.reverse().map(m=>'<div class="msg" data-testid="message"><b>'+m.author.display_name+'</b> <small>'+m.created_at+'</small><div data-testid="message-body">'+m.body+(m.edited_at?' <i>(edited)</i>':'')+'</div><button data-testid="open-thread-btn">Reply '+m.reply_count+'</button></div>').join('')}
load();</script></body></html>'''
if __name__=='__main__':
 ThreadingHTTPServer(('127.0.0.1',PORT),H).serve_forever()
