import socket,threading,sqlite3,datetime,re,secrets
DB='/app/huddle.db'
def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00','Z')
def q(sql,args=(),one=False):
 c=sqlite3.connect(DB); c.row_factory=sqlite3.Row; x=c.execute(sql,args); c.commit(); r=x.fetchone() if one else x.fetchall(); c.close(); return r
def send(c,line): c.sendall((line+'\r\n').encode())
def client(c,addr):
 token=None; nick=None; uid=None; joined={}
 def prefix(): return (nick or '*')+'!user@localhost'
 try:
  buf=b''
  while True:
   z=c.recv(4096)
   if not z: break
   buf+=z
   while b'\n' in buf:
    raw,buf=buf.split(b'\n',1); line=raw.decode(errors='replace').strip('\r')
    a=line.split(' '); cmd=a[0].upper() if a else ''
    if cmd=='PASS': token=a[1] if len(a)>1 else ''
    elif cmd=='NICK':
     nn=a[1] if len(a)>1 else ''
     if q('SELECT t.token FROM tokens t WHERE t.token=?',(token,),True):
      if q('SELECT id FROM users WHERE username=?',(nn,),True) and nick!=nn: send(c,':irc 433 * '+nn+' :Nickname in use'); continue
      nick=nn; u=q('SELECT u.* FROM users u JOIN tokens t ON t.user_id=u.id WHERE t.token=?',(token,),True); uid=u['id']
    elif cmd=='USER':
     if uid and nick:
      for n,text in [('001','Welcome to Huddle IRC'),('002','Your host is huddle'),('003','This server was created today'),('004','huddle 1.0 o o'),('005','CHANTYPES=#'),('422','MOTD File is missing')]: send(c,':irc '+n+' '+nick+' :'+text)
    elif cmd=='PING': send(c,':irc PONG '+(a[1] if len(a)>1 else 'irc'))
    elif cmd=='PONG': pass
    elif cmd=='JOIN':
     if not uid or len(a)<2: continue
     name=a[1]; m=re.match(r'^#([^/]+)/(.+)$',name)
     if not m:send(c,':irc 403 '+(nick or '*')+' '+name+' :No such channel');continue
     ch=q('SELECT c.* FROM channels c JOIN workspaces w ON w.id=c.workspace_id WHERE w.slug=? AND c.name=?',(m.group(1),m.group(2)),True)
     if not ch:send(c,':irc 403 '+nick+' '+name+' :No such channel');continue
     joined[ch['id']]=name; send(c,':'+prefix()+' JOIN '+name)
     send(c,':irc 353 '+nick+' = '+name+' :'+nick); send(c,':irc 366 '+nick+' '+name+' :End of NAMES list')
    elif cmd=='NAMES' and len(a)>1: send(c,':irc 353 '+nick+' = '+a[1]+' :'+nick); send(c,':irc 366 '+nick+' '+a[1]+' :End of NAMES list')
    elif cmd=='PRIVMSG' and len(a)>=3:
     target=a[1]; body=' '.join(a[2:]); body=body[1:] if body.startswith(':') else body
     ch_id=next((x for x,n in joined.items() if n==target),None)
     if ch_id:
      n=q('SELECT COALESCE(MAX(event_id),0)+1 n FROM events WHERE channel_id=?',(ch_id,),True)['n']; m=q('INSERT INTO messages(channel_id,author_id,body,created_at,event_id) VALUES(?,?,?,?,?)',(ch_id,uid,body,now(),n)); q('INSERT INTO events VALUES(?,?,?,?,?)',(ch_id,n,'message.created',m.lastrowid,now()))
    elif cmd=='QUIT': break
    elif cmd in ('WHO','LIST','TOPIC','MODE'): pass
    elif cmd: send(c,':irc 421 '+(nick or '*')+' '+cmd+' :Unknown command')
 finally:
  try:c.close()
  except:pass
def main():
 s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1); s.bind(('0.0.0.0',6667)); s.listen(100)
 while True:
  c,a=s.accept(); threading.Thread(target=client,args=(c,a),daemon=True).start()
if __name__=='__main__':main()
