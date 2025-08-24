from __future__ import annotations
import argparse, csv, html, json, os, re, sys, time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
import tldextract
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

ANCR="Enter Yours Here"
RAD=7500; LIM=500; OUT="berlin_places.csv"; SL=2.0; TMO=10
TYPES="home_goods_store,furniture_store,hardware_store,bakery,restaurant,store"
QUERIES=("Küchengeschäft, Kochgeschirr, Haushaltswaren, Geschenkartikelladen, "
         "Feinkost, Delikatessen, Metzgerei, Holzwerkstatt, Möbelschreinerei, "
         "Restaurantbedarf, Kochschule, Einrichtungshaus")
MAXP=5
GGC="https://maps.googleapis.com/maps/api/geocode/json"
GTX="https://maps.googleapis.com/maps/api/place/textsearch/json"
GNB="https://maps.googleapis.com/maps/api/place/nearbysearch/json"
GDT="https://maps.googleapis.com/maps/api/place/details/json"
PAGES=["","contact","kontakt","impressum","about","ueber-uns"]
PFX=("info@","kontakt@","hello@","sales@","bestellung@","shop@","service@","office@","mail@")
UA="JCboards Outreach Research Bot (contact: your-email@example.com)"
EMR=re.compile(r"(?:mailto:)?([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})",re.IGNORECASE)

@dataclass
class Rec:
    place_id:str
    name:Optional[str]=None
    formatted_address:Optional[str]=None
    international_phone_number:Optional[str]=None
    formatted_phone_number:Optional[str]=None
    website:Optional[str]=None
    public_email:Optional[str]=None
    lat:Optional[float]=None
    lng:Optional[float]=None
    types:List[str]=field(default_factory=list)
    rating:Optional[float]=None
    user_ratings_total:Optional[int]=None
    google_maps_url:Optional[str]=None
    source_query:Set[str]=field(default_factory=set)
    robots_respected:Optional[bool]=None
    email_source_page:Optional[str]=None
    scrape_notes:Optional[str]=None
    def mg(self,o:"Rec")->None:
        for f in["name","formatted_address","international_phone_number","formatted_phone_number","website","public_email","google_maps_url","rating","user_ratings_total","email_source_page","scrape_notes"]:
            if getattr(self,f) in (None,"",[]):
                v=getattr(o,f)
                if v not in (None,"",[]): setattr(self,f,v)
        if not self.lat and o.lat: self.lat=o.lat
        if not self.lng and o.lng: self.lng=o.lng
        if not self.types and o.types: self.types=list(dict.fromkeys(o.types))
        elif o.types: self.types=list(dict.fromkeys(list(self.types)+list(o.types)))
        self.source_query|=o.source_query
        if self.robots_respected is None: self.robots_respected=o.robots_respected

def d(msg:str)->None: print(msg,flush=True)
def slp(s:float)->None: time.sleep(max(0.0,s))

def norm_dom(u:str)->Optional[str]:
    if not u: return None
    try:
        p=urlparse(u);
        if not p.scheme: p=urlparse("http://"+u)
        ext=tldextract.extract(p.netloc)
        rd=ext.top_domain_under_public_suffix
        if not rd: return None
        return rd.lower()
    except: return None

def jn(base:str,path:str)->str:
    if not base.endswith("/"): base+="/"
    return urljoin(base,path)

def pick_email(es:List[str])->Optional[str]:
    if not es: return None
    seen=set(); u=[]
    for e in es:
        x=e.strip().lower().strip(".,;:()<>[]{}\"'")
        if x and x not in seen: seen.add(x); u.append(x)
    for pf in PFX:
        for e in u:
            if e.startswith(pf): return e
    return u[0] if u else None

class TH(Exception): pass
def _chk(r:requests.Response)->None:
    if 500<=r.status_code<600: raise TH(f"5xx {r.status_code}")
    if r.status_code==429: raise TH("429")
    if not (200<=r.status_code<300): raise requests.HTTPError(f"{r.status_code}: {r.text[:200]}")

@retry(reraise=True,stop=stop_after_attempt(3),wait=wait_exponential(multiplier=0.8,min=0.8,max=6),
       retry=retry_if_exception_type((TH,requests.ConnectionError,requests.Timeout)))
def GET(url:str,*,hdrs:Optional[Dict[str,str]]=None,params:Optional[Dict[str,str]]=None,timeout:int=TMO)->requests.Response:
    r=requests.get(url,headers=hdrs,params=params,timeout=timeout); _chk(r); return r

@dataclass
class Robots:
    allows:List[str]=field(default_factory=list)
    disallows:List[str]=field(default_factory=list)
    fetched:bool=False
    def ok(self,path:str)->bool:
        if not path.startswith("/"): path="/"+path
        a=""; b=""
        for r in self.allows:
            if path.startswith(r) and len(r)>len(a): a=r
        for r in self.disallows:
            if path.startswith(r) and len(r)>len(b): b=r
        if a or b: return len(a)>=len(b)
        return True

def robots(base:str,timeout:int)->Robots:
    rb=Robots(False)
    try:
        p=urlparse(base);
        if not p.scheme: p=urlparse("http://"+base)
        u=f"{p.scheme}://{p.netloc}/robots.txt"
        r=GET(u,hdrs={"User-Agent":UA},timeout=timeout)
        rb.fetched=True
        act=False; A=[]; D=[]
        for line in r.text.splitlines():
            s=line.strip()
            if not s or s.startswith("#"): continue
            k,sep,v=s.partition(":")
            if not sep: continue
            k=k.strip().lower(); v=v.strip()
            if k=="user-agent":
                act=(v=="*"); continue
            if not act: continue
            if k=="allow":
                p=v if v.startswith("/") else "/"+v; A.append(p)
            elif k=="disallow":
                if v=="": continue
                p=v if v.startswith("/") else "/"+v; D.append(p)
        rb.allows=A; rb.disallows=D
        return rb
    except: return rb

def geocode(key:str,addr:str,sl:float,tm:int)->Tuple[float,float]:
    r=GET(GGC,params={"key":key,"address":addr},timeout=tm).json()
    if r.get("status")!="OK": raise RuntimeError(f"Geocode failed: {r.get('status')} - {r.get('error_message')}")
    loc=r["results"][0]["geometry"]["location"]; slp(sl); return loc["lat"],loc["lng"]

def txtsearch(key:str,q:str,loc:Tuple[float,float],rad:int,sl:float,tm:int,lim:int)->List[Dict]:
    lat,lng=loc; P={"key":key,"query":q,"location":f"{lat},{lng}","radius":rad}
    res=[]; nxt=None
    while True:
        if nxt: P["pagetoken"]=nxt; slp(2.1)
        j=GET(GTX,params=P,timeout=tm).json()
        st=j.get("status")
        if st not in("OK","ZERO_RESULTS"):
            d(f"[WARN] TextSearch status={st} for '{q}'"); break
        r=j.get("results",[]); res.extend(r); d(f"  TextSearch '{q}' -> +{len(r)} (total {len(res)})")
        if len(res)>=lim: res=res[:lim]; break
        nxt=j.get("next_page_token");
        if not nxt: break
        slp(sl)
    slp(sl); return res

def nearby(key:str,t:str,loc:Tuple[float,float],rad:int,sl:float,tm:int,lim:int)->List[Dict]:
    lat,lng=loc; P={"key":key,"type":t,"location":f"{lat},{lng}","radius":rad}
    res=[]; nxt=None
    while True:
        if nxt: P["pagetoken"]=nxt; slp(2.1)
        j=GET(GNB,params=P,timeout=tm).json()
        st=j.get("status")
        if st not in("OK","ZERO_RESULTS"):
            d(f"[WARN] Nearby status={st} for '{t}'"); break
        r=j.get("results",[]); res.extend(r); d(f"  Nearby '{t}' -> +{len(r)} (total {len(res)})")
        if len(res)>=lim: res=res[:lim]; break
        nxt=j.get("next_page_token")
        if not nxt: break
        slp(sl)
    slp(sl); return res
    

def details(key:str,pid:str,sl:float,tm:int)->Optional[Dict]:
    F=["name","formatted_address","international_phone_number","formatted_phone_number","website","url","geometry/location","types","rating","user_ratings_total"]
    j=GET(GDT,params={"key":key,"place_id":pid,"fields":",".join(F)},timeout=tm).json()
    if j.get("status")!="OK": d(f"[WARN] Details failed for {pid}: {j.get('status')}"); return None
    slp(sl); return j.get("result")

def base_of(website:str)->Optional[str]:
    if not website: return None
    try:
        p=urlparse(website)
        if not p.scheme: p=urlparse("http://"+website)
        return f"{p.scheme}://{p.netloc}"
    except: return None

def build_pages(base:str)->List[str]:
    u=[]; u.append(base if base.endswith("/") else base+"/")
    for p in PAGES[1:]: u.append(jn(base,p))
    s=set(); o=[]
    for x in u:
        if x not in s: s.add(x); o.append(x)
    return o

def emails_from_html(t:str)->List[str]:
    if not t: return []
    s=BeautifulSoup(t,"html.parser"); es=set()
    for a in s.find_all("a",href=True):
        for m in EMR.findall(a.get("href","")): es.add(m.lower())
    for m in EMR.findall(s.get_text(" ",strip=True)): es.add(m.lower())
    return sorted(es)

def fetch(url:str,tm:int)->Optional[str]:
    try:
        r=GET(url,hdrs={"User-Agent":UA},timeout=tm)
        ct=r.headers.get("Content-Type","")
        if ("text/html" not in ct and "text/" not in ct and ct): return None
        return r.text
    except: return None

def discover(website:str,maxp:int,tm:int,per:float=1.0)->Tuple[Optional[str],Optional[str],bool,str]:
    if not website: return (None,None,True,"no website")
    base=base_of(website)
    if not base: return (None,None,True,"invalid website URL")
    rb=robots(base,timeout=tm)
    if rb.fetched and not rb.ok("/"): return (None,None,False,"robots disallow /")
    dom=norm_dom(base)
    c=build_pages(base)[:maxp]
    for u in c:
        path=urlparse(u).path or "/"
        if rb.fetched and not rb.ok(path): slp(per); continue
        h=fetch(u,tm=tm)
        if not h: slp(per); continue
        txt=html.unescape(h or "")
        es=emails_from_html(txt)
        if dom:
            es=[e for e in es if norm_dom(e.split("@",1)[1])==dom]
        if es:
            e=pick_email(es)
            if e: return (e,u,True,f"email(s) found: {', '.join(es)}")
        slp(per)
    return (None,None,True,"no email found")

def rec_from_det(d:Dict,src:str)->Rec:
    return Rec(
        place_id=d.get("place_id") or d.get("id") or "",
        name=d.get("name"),
        formatted_address=d.get("formatted_address"),
        international_phone_number=d.get("international_phone_number"),
        formatted_phone_number=d.get("formatted_phone_number"),
        website=d.get("website"),
        google_maps_url=d.get("url"),
        lat=(d.get("geometry",{}).get("location",{}).get("lat") if d.get("geometry") else None),
        lng=(d.get("geometry",{}).get("location",{}).get("lng") if d.get("geometry") else None),
        types=d.get("types",[]) or [],
        rating=d.get("rating"),
        user_ratings_total=d.get("user_ratings_total"),
        source_query=set([src]) if src else set()
    )

def to_row(r:Rec)->Dict[str,object]:
    return {
        "name":r.name or "",
        "formatted_address":r.formatted_address or "",
        "international_phone_number (or formatted_phone_number)": r.international_phone_number or r.formatted_phone_number or "",
        "website":r.website or "",
        "public_email":r.public_email or "",
        "place_id":r.place_id or "",
        "lat":r.lat if r.lat is not None else "",
        "lng":r.lng if r.lng is not None else "",
        "types":";".join(r.types) if r.types else "",
        "rating":r.rating if r.rating is not None else "",
        "user_ratings_total":r.user_ratings_total if r.user_ratings_total is not None else "",
        "google_maps_url":r.google_maps_url or "",
        "source_query":";".join(sorted(r.source_query)) if r.source_query else "",
        "robots_respected": "" if r.robots_respected is None else ("true" if r.robots_respected else "false"),
        "email_source_page": r.email_source_page or "",
        "scrape_notes": r.scrape_notes or "",
    }

def run(key:str,addr:str,rad:int,queries:List[str],types:List[str],lim:int,out_csv:str,gs:float,crawl:bool,maxp:int,tm:int)->None:
    d(f"[1/6] Geocoding anchor address: {addr}")
    loc=geocode(key,addr,gs,tm)
    d(f"      → lat,lng = {loc[0]}, {loc[1]}")
    allres=[]; rem=lim
    d(f"[2/6] Text Search queries (radius={rad}m):")
    for q in [x.strip() for x in queries if x.strip()]:
        if rem<=0: break
        its=txtsearch(key,q,loc,rad,gs,tm,rem)
        for it in its: allres.append((f"query:{q}",it))
        rem=max(0,lim-len({r[1].get('place_id') for r in allres}))
    d(f"[3/6] Nearby Search types (radius={rad}m):")
    for t in [x.strip() for x in types if x.strip()]:
        if rem<=0: break
        its=nearby(key,t,loc,rad,gs,tm,rem)
        for it in its: allres.append((f"type:{t}",it))
        rem=max(0,lim-len({r[1].get('place_id') for r in allres}))
    d(f"[4/6] Fetching Place Details and de-duplicating…")
    by:Dict[str,Rec]={}
    for src,it in allres:
        pid=it.get("place_id") or it.get("id")
        if not pid: continue
        det=details(key,pid,gs,tm)
        if det is None: det={"place_id":pid,"name":it.get("name"),"geometry":it.get("geometry")}
        r=rec_from_det(det,src)
        if pid in by: by[pid].mg(r)
        else: by[pid]=r
    dom_ix:Dict[str,str]={}
    for pid,r in list(by.items()):
        dm=norm_dom(r.website) if r.website else None
        if not dm: continue
        if dm in dom_ix:
            keep=dom_ix[dm]; by[keep].mg(r); del by[pid]
        else: dom_ix[dm]=pid
    ws=[r for r in by.values() if r.website]
    d(f"[5/6] Email discovery (crawl={'ON' if crawl else 'OFF'}) — websites to check: {len(ws)}")
    crawled=0; found=0
    for r in ws:
        if not crawl:
            r.robots_respected=True; r.scrape_notes="crawl disabled"; continue
        try:
            e,src,ok,nt=discover(r.website,maxp,tm,per=1.0)
            r.robots_respected=ok; r.scrape_notes=nt; r.public_email=e or r.public_email; r.email_source_page=src or r.email_source_page
            crawled+=1
            if r.public_email: found+=1
        except Exception as ex:
            r.robots_respected=True; r.scrape_notes=f"error: {type(ex).__name__}: {ex}"
    d(f"[6/6] Writing CSV → {out_csv}")
    rows=[to_row(r) for r in by.values()]
    df=pd.DataFrame(rows,columns=[
        "name","formatted_address","international_phone_number (or formatted_phone_number)","website","public_email",
        "place_id","lat","lng","types","rating","user_ratings_total","google_maps_url","source_query",
        "robots_respected","email_source_page","scrape_notes"
    ])
    df.to_csv(out_csv,index=False,encoding="utf-8-sig")
    tot=len(allres); uniq=len(by); wsum=sum(1 for r in by.values() if r.website)
    d(""); d("Summary"); d("-------")
    d(f"Total places fetched (raw): {tot}")
    d(f"Unique places after dedupe: {uniq}")
    d(f"Websites discovered:        {wsum}")
    d(f"Sites crawled:              {crawled}")
    d(f"Emails found:               {found}")
    d(f"CSV path:                   {os.path.abspath(out_csv)}")

def args()->argparse.Namespace:
    load_dotenv()
    p=argparse.ArgumentParser(description="Google Places → Emails → CSV (Berlin).")
    p.add_argument("--api-key",default=os.getenv("GOOGLE_API_KEY"))
    p.add_argument("--address",default=ANCR)
    p.add_argument("--radius",type=int,default=int(os.getenv("DEFAULT_RADIUS",RAD)))
    p.add_argument("--queries",default=os.getenv("DEFAULT_QUERIES",QUERIES))
    p.add_argument("--types",default=os.getenv("DEFAULT_TYPES",TYPES))
    p.add_argument("--limit",type=int,default=int(os.getenv("DEFAULT_LIMIT",LIM)))
    p.add_argument("--out",default=OUT)
    p.add_argument("--sleep",type=float,default=SL)
    p.add_argument("--crawl",dest="crawl",action=argparse.BooleanOptionalAction,default=True)
    p.add_argument("--max-pages-per-site",type=int,default=MAXP)
    p.add_argument("--timeout",type=int,default=TMO)
    a=p.parse_args()
    if not a.api_key: p.error("Missing Google API key. Provide --api-key or set GOOGLE_API_KEY in .env")
    a.queries=[x.strip() for x in (a.queries.split(",") if a.queries else []) if x.strip()]
    a.types=[x.strip() for x in (a.types.split(",") if a.types else []) if x.strip()]
    return a

def main()->None:
    a=args()
    d("Deps installer (if needed):")
    d("  pip install requests pandas python-dotenv beautifulsoup4 tldextract tenacity")
    d("")
    try:
        run(a.api_key,a.address,a.radius,a.queries,a.types,a.limit,a.out,a.sleep,a.crawl,a.max_pages_per_site,a.timeout)
    except KeyboardInterrupt:
        d("\nInterrupted by user."); sys.exit(1)
    except Exception as e:
        d(f"\n[ERROR] {type(e).__name__}: {e}"); sys.exit(2)

if __name__=="__main__": main()

