
import re, math, requests, tabula, pandas as pd
from pathlib import Path
import truststore; truststore.inject_into_ssl()

PDF_URLS = {
    2024: "https://www.ibo.org/globalassets/new-structure/about-the-ib/pdfs/dp-final-statistical-bulletin-may-2024_en.pdf",
    2023: "https://www.ibo.org/globalassets/new-structure/about-the-ib/pdfs/dp-cp-provisional-statistical-bulletin-may-2023.pdf",
}
PAGES = {2024: "20-28", 2023: "20-27"}
WORK = Path("ib_stats"); WORK.mkdir(exist_ok=True)
RAW_XLSX   = WORK / "IB_tables_2023_2024.xlsx"
REPORT_XLS = WORK / "subject_growth_full_canon.xlsx"

def download_pdfs():
    for yr, url in PDF_URLS.items():
        fn = WORK / f"ib_{yr}.pdf"
        if fn.exists(): print(f"{fn.name} already exists"); continue
        print(f"Downloading {yr}")
        fn.write_bytes(requests.get(url, timeout=30).content)

def extract_tables(pdf: Path, pages: str):
    return tabula.read_pdf(pdf.as_posix(), pages=pages, lattice=True, multiple_tables=True, force_subprocess=True)

def pdfs_to_excel():
    with pd.ExcelWriter(RAW_XLSX, engine="xlsxwriter") as w:
        for yr in PDF_URLS:
            tbls = extract_tables(WORK/f"ib_{yr}.pdf", PAGES[yr])
            
            for i, df in enumerate(tbls, 1):
                df.to_excel(w, sheet_name=f"{yr}_tbl{i}"[:31], index=False)
            print(f"{yr}: {len(tbls)} tables saved")

TOKEN = {"LIT":"LITERATURE","LAL":"LANGUAGE AND LITERATURE", "SC":"SCIENCE","SCI":"SCIENCE","POL":"POLITICS", "MOD":"MODERN","GR":"GREEK"}

def token_expand(t:str)->str:
    t=re.sub(r"([A-Z]+)A\b", r"\1 A", t)
    t=t.replace("GLOB. POL","GLOBAL POLITICS")\
       .replace("SOC.CUL.ANTH.","SOCIAL AND CULTURAL ANTHROPOLOGY")\
       .replace("CL.GK.ROM.ST.","CLASSICAL GREEK AND ROMAN STUDIES")\
       .replace("VISUALARTS","VISUAL ARTS")
    return t

def changesigns(raw:str)->str:
    s=raw.upper().replace("_X000D_"," ").replace("_"," ")
    m=re.match(r"^(.*?\b(?:HL|SL))\b", s)
    
    if not m: return ""
    
    s=token_expand(m.group(1))
    s=re.sub(r"[:.\-]"," ", s)
    s=" ".join(TOKEN.get(tok,tok) for tok in s.split()).replace("&","AND")
    
    return re.sub(r"\s+"," ", s).strip()

def counts_for_year(xls:pd.ExcelFile, yr:int):
    cnt, rep = {}, {}
    for sh in [s for s in xls.sheet_names if s.startswith(str(yr))]:
        
        df=pd.read_excel(xls, sheet_name=sh, header=None)
        start=df.iloc[:,0].astype(str).str.strip().eq("Subject")
        
        if not start.any(): continue
        
        for _, row in df.iloc[start.idxmax()+1:].iterrows():
            
            raw=str(row[0]).strip()
            if raw.upper() in ("","SUBJECT") or raw.upper().startswith("FINAL"): break
            key=changesigns(raw)
            if not key or key in cnt:  
                continue

            val=None
            for cell in row[1:]:
                if isinstance(cell,(int,float)) and not math.isnan(cell):
                    val=int(cell); break
                    
                if isinstance(cell,str):
                    d=re.search(r"\d{1,6}", cell.replace(",",""))
                    if d: val=int(d.group()); break
                    
            if val is None: continue

            rep[key]=raw
            cnt[key]=val

    return pd.DataFrame({"Canonical":cnt.keys(), "Subject":[rep[c] for c in cnt], f"Count_{yr}":cnt.values()})

def build_report():
    xls=pd.ExcelFile(RAW_XLSX)
    df23, df24 = counts_for_year(xls,2023), counts_for_year(xls,2024)

    merged=pd.merge(df24, df23, on=["Canonical","Subject"], how="outer")
    
    merged[["Count_2023","Count_2024"]] = merged[["Count_2023","Count_2024"]].fillna(0).astype(int)
    merged["Diff"]= merged["Count_2024"]-merged["Count_2023"]
    merged["Pct"]= merged.apply(lambda r: round(r["Diff"]/r["Count_2023"]*100,1) if r["Count_2023"] else None, axis = 1)

    merged.sort_values("Subject").drop(columns = "Canonical").to_excel(REPORT_XLS,index=False)
    print(f"Saved data to {REPORT_XLS}")


     
    getPrint = merged.sort_values("Pct", ascending = False)\
                    [["Subject","Count_2023","Count_2024","Diff","Pct"]].head(250)
    print("TOP 20")
    print(getPrint.to_string(index=False))
    

if __name__=="__main__":
    download_pdfs()
    pdfs_to_excel()
    build_report()
