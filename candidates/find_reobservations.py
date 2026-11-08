
#!/usr/bin/env python3
"""
Find re-observations of candidate sky positions.

For each candidate in candidate_list.txt:
  - Read candidate RA/DEC.
  - Scan BeamData/*.ahdr from observations >= START_OBS_PREFIX.
  - Compare candidate to every BM-Idx in every header.
  - Report all beam centres within MAX_SEP_ARCSEC.

Edit START_OBS_PREFIX or START_DATE_FILTER as needed.
"""

import os, re, glob, math

DATA_ROOT="/lustre_data/spotlight/data"
CANDIDATE_FILE="candidate_list.txt"

# Scan observations whose names start with 50_ (roughly June onward in your tree).
OBS_PREFIXES=("50_",)
MAX_SEP_ARCSEC=300.0   # 5 arcmin


def hms_to_deg(h):
    H,M,S=map(float,h.split(":"))
    return (H+M/60+S/3600)*15

def dms_to_deg(d):
    sign=-1 if d.startswith("-") else 1
    d=d.replace("+","").replace("-","")
    D,M,S=map(float,d.split(":"))
    return sign*(D+M/60+S/3600)

def sep_arcsec(ra1,dec1,ra2,dec2):
    ra1,dec1,ra2,dec2=map(math.radians,[ra1,dec1,ra2,dec2])
    c=(math.sin(dec1)*math.sin(dec2)+
       math.cos(dec1)*math.cos(dec2)*math.cos(ra1-ra2))
    c=max(-1,min(1,c))
    return math.degrees(math.acos(c))*3600

def parse_candidates(fname):
    txt=open(fname).read()
    blocks=[b.strip() for b in txt.split("-"*60) if b.strip()]
    out=[]
    for b in blocks:
        def g(k):
            m=re.search(rf"^{re.escape(k)}\s*:\s*(.+)$",b,re.M)
            return m.group(1).strip() if m else None
        ra=g("RA"); dec=g("DEC")
        if not ra or not dec or ra=="NA" or dec=="NA":
            continue
        out.append({
            "obs":g("OBS DIR"),
            "target":g("TARGET"),
            "beam":int(g("BEAM NO")),
            "ra_deg":hms_to_deg(ra),
            "dec_deg":dms_to_deg(dec),
            "ra":ra,
            "dec":dec
        })
    return out

def wanted_obs(obs):
    return any(obs.startswith(p) for p in OBS_PREFIXES)

def scan_headers():
    idx=[]
    for obs in sorted(os.listdir(DATA_ROOT)):
        if not wanted_obs(obs):
            continue
        bdir=os.path.join(DATA_ROOT,obs,"BeamData")
        if not os.path.isdir(bdir):
            continue
        for hdr in glob.glob(os.path.join(bdir,"*.ahdr")):
            with open(hdr) as f:
                for line in f:
                    cols=line.split()
                    if len(cols)!=4:
                        continue
                    try:
                        ra=float(cols[0]); dec=float(cols[1]); bm=int(cols[2])
                    except:
                        continue
                    idx.append({
                        "obs":obs,
                        "hdr":os.path.basename(hdr),
                        "beam":bm,
                        "ra_deg":math.degrees(ra),
                        "dec_deg":math.degrees(dec)
                    })
    return idx

cands=parse_candidates(CANDIDATE_FILE)
headers=scan_headers()

print(f"Indexed {len(headers)} beam centres\n")

for c in cands:
    print("="*100)
    print(f"Candidate : {c['target']}")
    print(f"Original  : {c['obs']}  Beam {c['beam']}")
    found=0
    for h in headers:
        # skip same observation if desired
        if h["obs"]==c["obs"]:
            continue
        s=sep_arcsec(c["ra_deg"],c["dec_deg"],h["ra_deg"],h["dec_deg"])
        if s<=MAX_SEP_ARCSEC:
            found+=1
            print(f"{h['obs']:24s}  Beam {h['beam']:4d}  {s:8.2f}\"  {h['hdr']}")
    if found==0:
        print("No re-observation found within %.1f arcsec." % MAX_SEP_ARCSEC)

