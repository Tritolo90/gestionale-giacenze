
import streamlit as st
import pandas as pd
import numpy as np
import os
import glob
import re
from datetime import datetime

st.set_page_config(layout="wide")
st.title("Gestionale Giacenze 📦")

# --- PERCORSI RELATIVI ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FOLDER_DIGIGEM = os.path.join(BASE_DIR, "Digigem")
FOLDER_SAP = os.path.join(BASE_DIR, "SAP")
FILE_NAV = os.path.join(BASE_DIR, "NAV.xlsx")
FILE_ANAGRAFICA = os.path.join(BASE_DIR, "anagrafica_fornitori.csv")

@st.cache_data(ttl=3600, show_spinner="Elaborazione dati in corso...")
def process_all_data():
    def rinomina_file_sap_in_txt(folder_path):
        files = glob.glob(os.path.join(folder_path, "*.xls*"))
        for path in files:
            base, _ = os.path.splitext(path)
            txt = base + ".txt"
            if not os.path.exists(txt):
                os.rename(path, txt)
        return glob.glob(os.path.join(folder_path, "*.txt"))

    def parse_sap_file_manually(path):
        with open(path, 'r', encoding='utf-16') as f:
            lines = [l.strip() for l in f]
        rows = []
        current = "N/D"
        for line in lines:
            parts = re.split(r'\t+', line)
            if not parts or not parts[0]:
                continue
            if parts[0].strip() == "IMSU" and len(parts) > 1:
                current = parts[1].strip()
                continue
            if parts[0].strip().isdigit() and len(parts) > 6:
                rows.append({
                    'Materiale': parts[0].strip(),
                    'mag': current,
                    'Descrizione': parts[1].strip(),
                    'Qtà Disponibile': parts[6].strip()
                })
        return pd.DataFrame(rows)

    def carica_nav(path):
        if not os.path.exists(path):
            return pd.DataFrame(columns=['Materiale','mag','Giacenza','Descrizione_NAV'])
        df = pd.read_excel(path, sheet_name="Foglio1")
        df.rename(columns={"Quantità":"Giacenza","Nr. Articolo":"Materiale","Cod. Ubicazione":"mag"}, inplace=True)
        rules = {'Giacenza':'sum'}
        if 'Descrizione Articolo D' in df.columns:
            rules['Descrizione Articolo D'] = 'first'
        nav = df.groupby(['Materiale','mag']).agg(rules).reset_index()
        if 'Descrizione Articolo D' in nav.columns:
            nav.rename(columns={'Descrizione Articolo D':'Descrizione_NAV'}, inplace=True)
        return nav

    # Fase 1
    csvs = glob.glob(os.path.join(FOLDER_DIGIGEM,"*.csv"))
    df_csv = pd.concat([pd.read_csv(f, sep=',', encoding='latin1', low_memory=False) for f in csvs], ignore_index=True)
    df_csv.rename(columns={'cod_nmu':'NMU'}, inplace=True)
    df_csv.columns = df_csv.columns.str.strip()

    df_nav_raw = pd.read_excel(FILE_NAV, sheet_name="Foglio1")
    drops = ["Nr. Articolo","Nr. Seriale 2","Nr. Lotto","Tipo di Documento","Nr. Documento","Nr. Riga Documento",
             "Tipo Origine Custom","Quantità","Cod. Ubicazione","Cod. Progetto","Aperto","Nr. Ordine Bar Code","Nr. Movimento Articolo",
             "Tipo origine","Nr. Origine"]
    df_nav_raw.drop(columns=drops, inplace=True, errors='ignore')
    df_nav_raw['Data di Registrazione'] = pd.to_datetime(df_nav_raw['Data di Registrazione'], errors='coerce')
    df_nav_raw.sort_values(["Data di Registrazione","Nr. Movimento"], ascending=[False,False], inplace=True)
    df_nav_raw.drop_duplicates(subset=["Nr. Seriale"], keep='first', inplace=True)
    df_nav_raw.rename(columns={"Nr. Seriale":"serial_number_tim"}, inplace=True)
    df_nav_raw.columns = df_nav_raw.columns.str.strip()

    df_merge = pd.merge(df_csv, df_nav_raw, on='serial_number_tim', how='left')
    validi = ["Reso Carico","Carico","Cambio Progetto","Trasf. in Ingresso","Rett. Positiva","Trasf. in Uscita","Rett. Negativa"]
    conds = [
        df_merge['Tipo Movimento']=="Rientro",
        df_merge['Subappaltatore'].notna() & df_merge['Subappaltatore']!="",
        df_merge['Cod. Risorsa Caposquadra'].notna() & df_merge['Cod. Risorsa Caposquadra']!="",
        df_merge['Tipo Movimento'].isin(validi),
        pd.to_datetime(df_merge['createdAt'], errors='coerce').dt.year<=2023
    ]
    choices = ["Carico", df_merge['Subappaltatore'], df_merge['Cod. Risorsa Caposquadra'], df_merge['Tipo Movimento'], "ANTE 2023"]
    df_merge['Stato_Originale'] = np.select(conds, choices, default='NON IN NAV')

    if os.path.exists(FILE_ANAGRAFICA):
        df_ana = pd.read_csv(FILE_ANAGRAFICA, sep=';', dtype=str)
        df_ana['CodiceJoin'] = df_ana['Codice'].str.extract(r'(\d+)').fillna('0')
        df_merge['CodiceJoin'] = df_merge['Stato_Originale'].astype(str).str.extract(r'(\d+)').fillna('0')
        df_merge = pd.merge(df_merge, df_ana, left_on='CodiceJoin', right_on='Codice', how='left')
        df_merge['Stato'] = df_merge['Nome'].fillna(df_merge['Stato_Originale'])
        df_merge.drop(columns=['CodiceJoin','Codice','Nome','Stato_Originale'], inplace=True, errors='ignore')
    else:
        df_merge.rename(columns={'Stato_Originale':'Stato'}, inplace=True)

    detail_cols = ['NMU','desc_nmu','Stato','serial_number_tim','serial_number_forn','status','cod_terr_sap','status_regman','Data di Registrazione']
    df_detail = df_merge[[c for c in detail_cols if c in df_merge.columns]]

    # Fase 2
    txts = rinomina_file_sap_in_txt(FOLDER_SAP)
    if not txts:
        df_sap = pd.DataFrame(columns=['Materiale','mag','Descrizione','Qtà Disponibile'])
    else:
        dfs = [parse_sap_file_manually(t) for t in txts]
        df_sap = pd.concat(dfs, ignore_index=True)
        for col in ['Qtà Disponibile','Materiale']:
            df_sap[col] = pd.to_numeric(df_sap[col].str.replace(',','.'), errors='coerce').fillna(0)
        df_sap = df_sap.groupby(['Materiale','mag']).agg({'Descrizione':'first','Qtà Disponibile':'sum'}).reset_index()

    df_csv.rename(columns={'NMU':'Materiale','cod_terr_sap':'mag'}, inplace=True)
    df_digi = df_csv.groupby(['Materiale','mag']).size().reset_index(name='Conteggio')
    df_navagg = carica_nav(FILE_NAV)

    # Fase 3
    mappa = {'S014':'CT','S016':'SR','S017':'RG','S230':'ME'}
    df_sap['mag'] = df_sap['mag'].replace(mappa)
    df_digi['mag'] = df_digi['mag'].replace(mappa)
    df_navagg['mag'] = df_navagg['mag'].replace(mappa)

    merged = pd.merge(df_sap, df_digi, on=['Materiale','mag'], how='outer')
    merged = pd.merge(merged, df_navagg, on=['Materiale','mag'], how='outer')
    merged.rename(columns={'Conteggio':'Qtà Digigem','Giacenza':'NAV.Giacenza','Qtà Disponibile':'Qtà Disponibile(SAP)'}, inplace=True)

    if 'desc_nmu' in df_csv.columns:
        ana2 = df_csv[['Materiale','desc_nmu']].dropna().drop_duplicates('Materiale')
        merged = pd.merge(merged, ana2, on='Materiale', how='left')
        merged['Descrizione'] = merged['Descrizione'].fillna(merged['desc_nmu'])
        merged.drop(columns=['desc_nmu'], inplace=True, errors='ignore')

    if 'Descrizione_NAV' in merged.columns:
        merged['Descrizione'] = merged['Descrizione'].fillna(merged['Descrizione_NAV'])
        merged.drop(columns=['Descrizione_NAV'], inplace=True, errors='ignore')

    for c in ['Qtà Disponibile(SAP)','Qtà Digigem','NAV.Giacenza']:
        if c in merged.columns: merged[c] = merged[c].fillna(0)
    for c in ['Qtà Disponibile(SAP)','Qtà Digigem','NAV.Giacenza','Materiale']:
        merged[c] = pd.to_numeric(merged[c], errors='coerce').fillna(0).astype(int)

    merged['Delta(Digigem - SAP)'] = merged['Qtà Digigem'] - merged['Qtà Disponibile(SAP)']
    merged['VIAGGIANTE (NAV - SAP)'] = merged['NAV.Giacenza'] - merged['Qtà Disponibile(SAP)']
    merged.rename(columns={'mag':'Provincia'}, inplace=True)

    cols_final = ['Materiale','Provincia','Descrizione','Qtà Disponibile(SAP)','Qtà Digigem','Delta(Digigem - SAP)','NAV.Giacenza','VIAGGIANTE (NAV - SAP)']
    df_summary = merged[[c for c in cols_final if c in merged.columns]]
    df_summary.rename(columns={'Materiale':'NMU'}, inplace=True)

    return df_detail, df_summary

# UI
if 'loaded' not in st.session_state:
    st.session_state.loaded = False
    st.session_state.last_update = "Mai"

c1, c2, _ = st.columns([2,2,6])
if c1.button("🔄 Carica Dati"):
    st.session_state.df_detail, st.session_state.df_summary = process_all_data()
    st.session_state.loaded = True
    st.session_state.last_update = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

if c2.button("🧹 Pulisci Cache"):
    st.cache_data.clear()
    st.session_state.loaded = False
    st.success("Cache pulita.")

st.caption(f"Ultimo aggiornamento: {st.session_state.last_update}")
st.markdown("---")

if st.session_state.loaded:
    tab1, tab2 = st.tabs(["Dettaglio Seriali","Riepilogo Magazzini"])
    with tab1:
        st.dataframe(st.session_state.df_detail, use_container_width=True)
    with tab2:
        st.dataframe(st.session_state.df_summary, use_container_width=True)
