# app.py (versione definitiva All-in-One per il deploy)
import streamlit as st
import pandas as pd
import numpy as np
import os
import glob
import re
from datetime import datetime

st.set_page_config(layout="wide")
st.title("Gestionale Giacenze 📦")

# --- DEFINIZIONE DEI PERCORSI (relativi alla root del progetto) ---
FOLDER_DIGIGEM = "Digigem"
FOLDER_SAP = "SAP"
FILE_NAV = "NAV.xlsx"

# ==============================================================================
# === TUTTA LA LOGICA DI ELABORAZIONE DATI È ORA IN QUESTA FUNZIONE            ===
# ==============================================================================

@st.cache_data(show_spinner=False) # Mette in cache il risultato finale
def process_all_data():
    """
    Esegue l'intera pipeline di elaborazione dati e restituisce i due DataFrame finali.
    """
    # --- Funzioni di supporto interne ---
    def rinomina_file_sap_in_txt(folder_path):
        files_xls = glob.glob(os.path.join(folder_path, "*.xls*"))
        if files_xls:
            for file_path in files_xls:
                root, _ = os.path.splitext(file_path)
                new_path = root + ".txt"
                if not os.path.exists(new_path):
                    os.rename(file_path, new_path)
        return glob.glob(os.path.join(folder_path, "*.txt"))

    def parse_sap_file_manually(file_path):
        with open(file_path, 'r', encoding='utf-16') as f:
            lines = [line.strip() for line in f.readlines()]
        clean_data_rows = []; current_mag = "N/D"
        for line in lines:
            fields = re.split(r'\t+', line)
            if not fields or not fields[0]: continue
            if fields[0].strip() == "IMSU" and len(fields) > 1:
                current_mag = fields[1].strip(); continue
            if fields[0].strip().isdigit() and len(fields) > 7:
                try:
                    clean_data_rows.append({'Materiale': fields[0].strip(),'mag': current_mag,'Descrizione': fields[1].strip(),'Qtà Disponibile': fields[7].strip()})
                except IndexError: continue
        return pd.DataFrame(clean_data_rows)

    def carica_totale_csv_aggregato(df_dettaglio):
        if 'cod_terr_sap' not in df_dettaglio.columns or 'NMU' not in df_dettaglio.columns: return pd.DataFrame(columns=['Materiale', 'mag', 'Conteggio'])
        df_agg = df_dettaglio.groupby(['cod_terr_sap', 'NMU']).size().reset_index(name='Conteggio')
        df_agg.rename(columns={'NMU': 'Materiale', 'cod_terr_sap': 'mag'}, inplace=True)
        return df_agg

    def carica_giacenza_nav_semplice(file_path):
        if not os.path.exists(file_path): return None
        df = pd.read_excel(file_path, sheet_name="Foglio1")
        df.rename(columns={"Quantità": "Giacenza", "Nr. Articolo": "Materiale", "Cod. Ubicazione": "mag"}, inplace=True)
        aggregation_rules = {'Giacenza': 'sum'}
        if 'Descrizione Articolo D' in df.columns:
            aggregation_rules['Descrizione Articolo D'] = 'first'
        giacenze_nav = df.groupby(['Materiale', 'mag']).agg(aggregation_rules).reset_index()
        if 'Descrizione Articolo D' in giacenze_nav.columns:
            giacenze_nav.rename(columns={'Descrizione Articolo D': 'Descrizione_NAV'}, inplace=True)
        return giacenze_nav

    # --- FASE 1: Creazione Dati di Dettaglio ---
    csv_files = glob.glob(os.path.join(FOLDER_DIGIGEM, "*.csv"))
    df_totale_csv = pd.concat([pd.read_csv(f, sep=',', encoding='latin1', low_memory=False) for f in csv_files], ignore_index=True)
    df_totale_csv.rename(columns={'cod_nmu': 'NMU'}, inplace=True)
    df_totale_csv.columns = df_totale_csv.columns.str.strip()
    
    df_nav = pd.read_excel(FILE_NAV, sheet_name="Foglio1")
    cols_to_drop_nav = ["Nr. Articolo", "Nr. Seriale 2", "Nr. Lotto", "Tipo di Documento", "Nr. Documento", "Nr. Riga Documento", "Tipo Origine Custom", "Quantità", "Cod. Ubicazione", "Cod. Progetto", "Aperto", "Nr. Ordine Bar Code", "Nr. Movimento Articolo", "Tipo origine", "Nr. Origine"]
    df_nav.drop(columns=cols_to_drop_nav, inplace=True, errors='ignore')
    df_nav['Data di Registrazione'] = pd.to_datetime(df_nav['Data di Registrazione'], errors='coerce')
    df_nav.sort_values(by=["Data di Registrazione", "Nr. Movimento"], ascending=[False, False], inplace=True)
    df_nav.drop_duplicates(subset=["Nr. Seriale"], keep='first', inplace=True)
    df_nav.rename(columns={"Nr. Seriale": "serial_number_tim"}, inplace=True)
    df_nav.columns = df_nav.columns.str.strip()
    
    df_merged_detail = pd.merge(df_totale_csv, df_nav, on='serial_number_tim', how='left')
    
    validi = ["Reso Carico", "Carico", "Cambio Progetto", "Trasf. in Ingresso", "Rett. Positiva", "Trasf. in Uscita", "Rett. Negativa"]
    conditions = [ df_merged_detail['Tipo Movimento'].eq("Rientro"), df_merged_detail['Subappaltatore'].notna() & df_merged_detail['Subappaltatore'].ne(""), df_merged_detail['Cod. Risorsa Caposquadra'].notna() & df_merged_detail['Cod. Risorsa Caposquadra'].ne(""), df_merged_detail['Tipo Movimento'].isin(validi), (pd.to_datetime(df_merged_detail['createdAt'], errors='coerce').dt.year <= 2023) ]
    choices = [ "Carico", df_merged_detail['Subappaltatore'], df_merged_detail['Cod. Risorsa Caposquadra'], df_merged_detail['Tipo Movimento'], "ANTE 2023" ]
    df_merged_detail['Stato'] = np.select(conditions, choices, default='NON IN NAV')
    
    final_cols = ['NMU', 'desc_nmu', 'Stato', 'serial_number_tim', 'serial_number_forn', 'status', 'cod_terr_sap', 'status_regman', 'Data di Registrazione']
    df_dettaglio = df_merged_detail[[c for c in final_cols if c in df_merged_detail.columns]]

    # --- FASE 2: Creazione Dati Aggregati per Magazzino ---
    sap_txt_files = rinomina_file_sap_in_txt(FOLDER_SAP)
    if not sap_txt_files:
        giacenze_sap = pd.DataFrame(columns=['Materiale', 'mag', 'Descrizione', 'Qtà Disponibile'])
    else:
        lista_df_sap = [parse_sap_file_manually(f) for f in sap_txt_files]
        df_sap = pd.concat(lista_df_sap, ignore_index=True)
        for col in ["Qtà Disponibile", "Materiale"]:
            df_sap[col] = pd.to_numeric(df_sap[col].str.replace(',', '.'), errors='coerce').fillna(0)
        sap_agg_rules = {'Descrizione': 'first', 'Qtà Disponibile': 'sum'}
        giacenze_sap = df_sap.groupby(['Materiale', 'mag']).agg(sap_agg_rules).reset_index()
    
    giacenze_digigem = carica_totale_csv_aggregato(df_totale_csv)
    giacenze_nav = carica_giacenza_nav_semplice(FILE_NAV)
    
    df_summary = pd.merge(giacenze_sap, giacenze_digigem, on=['Materiale', 'mag'], how='outer')
    df_summary = pd.merge(df_summary, giacenze_nav, on=['Materiale', 'mag'], how='outer')
    
    df_summary.rename(columns={'Conteggio': 'Qtà Digigem', 'Giacenza': 'NAV.Giacenza', 'Qtà Disponibile': 'Qtà Disponibile(SAP)'}, inplace=True)
    
    if 'desc_nmu' in df_totale_csv.columns:
        anagrafica_digigem = df_totale_csv[['NMU', 'desc_nmu']].dropna(subset=['NMU', 'desc_nmu']).drop_duplicates(subset=['NMU'])
        anagrafica_digigem.rename(columns={'NMU': 'Materiale'}, inplace=True)
        df_summary = pd.merge(df_summary, anagrafica_digigem, on='Materiale', how='left')
        df_summary['Descrizione'] = df_summary['Descrizione'].fillna(df_summary['desc_nmu'])
        df_summary.drop(columns=['desc_nmu'], inplace=True, errors='ignore')

    if 'Descrizione_NAV' in df_summary.columns:
        df_summary['Descrizione'] = df_summary['Descrizione'].fillna(df_summary['Descrizione_NAV'])
        df_summary.drop(columns=['Descrizione_NAV'], inplace=True, errors='ignore')

    df_summary['Descrizione'] = df_summary['Descrizione'].fillna('')
    colonne_qta = ['Qtà Disponibile(SAP)', 'Qtà Digigem', 'NAV.Giacenza']
    for col in colonne_qta:
        if col in df_summary.columns: df_summary[col] = df_summary[col].fillna(0)
    
    for col in ['Qtà Disponibile(SAP)', 'Qtà Digigem', 'NAV.Giacenza', 'Materiale']:
        df_summary[col] = pd.to_numeric(df_summary[col], errors='coerce').fillna(0).astype('int64')
        
    df_summary['Delta(Digigem - SAP)'] = df_summary['Qtà Digigem'] - df_summary['Qtà Disponibile(SAP)']
    df_summary['VIAGGIANTE (NAV - SAP)'] = df_summary['NAV.Giacenza'] - df_summary['Qtà Disponibile(SAP)']
    
    mappa_province = {'S014': 'CT', 'S016': 'SR', 'S017': 'RG', 'S230': 'ME'}
    df_summary['mag'] = df_summary['mag'].map(mappa_province).fillna(df_summary['mag'])
    df_summary.rename(columns={'mag': 'Provincia'}, inplace=True)
    
    final_cols = ['Materiale', 'Provincia', 'Descrizione', 'Qtà Disponibile(SAP)', 'Qtà Digigem', 'Delta(Digigem - SAP)', 'NAV.Giacenza', 'VIAGGIANTE (NAV - SAP)']
    df_summary = df_summary[[c for c in final_cols if c in df_summary.columns]]
    df_summary.rename(columns={'Materiale': 'NMU'}, inplace=True)
    
    return df_dettaglio, df_summary


# --- INTERFACCIA UTENTE ---

# Inizializza lo stato della sessione
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

# Pulsante per avviare l'aggiornamento
if st.button("🔄 Carica / Aggiorna Dati"):
    with st.spinner("Elaborazione in corso... Questo potrebbe richiedere alcuni minuti."):
        # Esegui la pipeline e salva i dati nello stato della sessione
        st.session_state.df_dettaglio, st.session_state.df_riepilogo = process_all_data()
        st.session_state.last_update = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        st.session_state.data_loaded = True
    st.success("Dati elaborati con successo!")


# Mostra il timestamp se i dati sono stati caricati
if st.session_state.data_loaded:
    st.caption(f"Dati aggiornati il: {st.session_state.last_update}")

st.markdown("---")

# Mostra le schede solo se i dati sono stati caricati
if st.session_state.data_loaded:
    df_dettaglio = st.session_state.df_dettaglio
    df_riepilogo_magazzino = st.session_state.df_riepilogo

    tab1, tab2, tab3 = st.tabs(["Ricerca Seriale Dettagliata", "Riepilogo per Magazzino", "🔎 Ricerca Libera"])
    
    # ... (Il codice delle 3 schede rimane identico) ...
    
else:
    st.info("Clicca su 'Carica / Aggiorna Dati' per avviare l'analisi.")