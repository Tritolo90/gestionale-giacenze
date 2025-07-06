# app.py (versione definitiva e completa con tutte le logiche corrette)
import streamlit as st
import pandas as pd
import numpy as np
import os
import glob
import re
from datetime import datetime

st.set_page_config(layout="wide")
st.title("Gestionale Giacenze 📦")

# --- DEFINIZIONE DEI PERCORSI ---
FOLDER_DIGIGEM = "Digigem"
FOLDER_SAP = "SAP"
FILE_NAV = "NAV.xlsx"
FILE_ANAGRAFICA = 'anagrafica_fornitori.csv'

# ==============================================================================
# === FUNZIONE DI ELABORAZIONE DATI CON CACHE
# ==============================================================================

@st.cache_data(ttl=3600, show_spinner="Elaborazione dati in corso... Questo potrebbe richiedere alcuni minuti.")
def process_all_data():
    """
    Esegue l'intera pipeline di elaborazione dati e RESTITUISCE i due DataFrame finali.
    """
    
    # --- Funzioni di supporto interne ---
    def rinomina_file_sap_in_txt(folder_path):
        files_xls = glob.glob(os.path.join(folder_path, "*.xls*"))
        if files_xls:
            for file_path in files_xls:
                root, _ = os.path.splitext(file_path)
                new_path = root + ".txt"
                if not os.path.exists(new_path): os.rename(file_path, new_path)
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
            if fields[0].strip().isdigit() and len(fields) > 6:
                try:
                    clean_data_rows.append({'Materiale': fields[0].strip(),'mag': current_mag,'Descrizione': fields[1].strip(),'Qtà Disponibile': fields[6].strip()})
                except IndexError: continue
        return pd.DataFrame(clean_data_rows)

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

    # --- INIZIO PIPELINE ---
    st.write("FASE 1/3: Elaborazione dati di dettaglio (Digigem, NAV, Anagrafica Fornitori)...")
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
    df_merged_detail['Stato_Originale'] = np.select(conditions, choices, default='NON IN NAV')

    if os.path.exists(FILE_ANAGRAFICA):
        df_anagrafica = pd.read_csv(FILE_ANAGRAFICA, sep=';', dtype=str).dropna()
        df_anagrafica['CodiceJoin'] = df_anagrafica['Codice'].str.extract(r'(\d+)').fillna('0')
        df_merged_detail['CodiceJoin'] = df_merged_detail['Stato_Originale'].astype(str).str.extract(r'(\d+)').fillna('0')
        df_merged_detail = pd.merge(df_merged_detail, df_anagrafica, on='CodiceJoin', how='left')
        df_merged_detail['Stato'] = df_merged_detail['Nome'].fillna(df_merged_detail['Stato_Originale'])
        df_merged_detail.drop(columns=['CodiceJoin', 'Codice', 'Nome', 'Stato_Originale'], inplace=True, errors='ignore')
    else:
        st.warning(f"File anagrafica '{FILE_ANAGRAFICA}' non trovato.")
        df_merged_detail.rename(columns={'Stato_Originale': 'Stato'}, inplace=True)
    
    final_cols_detail = ['NMU', 'desc_nmu', 'Stato', 'serial_number_tim', 'serial_number_forn', 'status', 'cod_terr_sap', 'status_regman', 'Data di Registrazione']
    df_dettaglio = df_merged_detail[[c for c in final_cols_detail if c in df_merged_detail.columns]]

    # FASE 2: Dati Aggregati
    st.write("FASE 2/3: Elaborazione dati aggregati...")
    sap_txt_files = rinomina_file_sap_in_txt(FOLDER_SAP)
    if not sap_txt_files:
        giacenze_sap = pd.DataFrame(columns=['Materiale', 'mag', 'Descrizione', 'Qtà Disponibile'])
    else:
        lista_df_sap = [parse_sap_file_manually(f) for f in sap_txt_files]
        df_sap = pd.concat(lista_df_sap, ignore_index=True)
        for col in ["Qtà Disponibile", "Materiale"]: df_sap[col] = pd.to_numeric(df_sap[col].str.replace(',', '.'), errors='coerce').fillna(0)
        sap_agg_rules = {'Descrizione': 'first', 'Qtà Disponibile': 'sum'}
        giacenze_sap = df_sap.groupby(['Materiale', 'mag']).agg(sap_agg_rules).reset_index()
    
    df_totale_csv.rename(columns={'NMU': 'Materiale', 'cod_terr_sap': 'mag'}, inplace=True)
    giacenze_digigem = df_totale_csv.groupby(['Materiale', 'mag']).size().reset_index(name='Conteggio')
    
    giacenze_nav = carica_giacenza_nav_semplice(FILE_NAV)

    st.write("FASE 3/3: Unione e calcoli finali...")
    mappa_province = { 'IMSUS014': 'CT', 'CT01': 'CT', 'S014': 'CT', 'IMSUS016': 'SR', 'SR01': 'SR', 'S016': 'SR', 'IMSUS017': 'RG', 'RG01': 'RG', 'S017': 'RG', 'IMSUS230': 'ME', 'CL104025ME': 'ME', 'S230': 'ME' }
    
    # Standardizza i codici magazzino PRIMA di aggregare e unire
    giacenze_sap['mag'] = giacenze_sap['mag'].replace(mappa_province)
    giacenze_digigem['mag'] = giacenze_digigem['mag'].replace(mappa_province)
    giacenze_nav['mag'] = giacenze_nav['mag'].replace(mappa_province)
    
    # Ri-aggrega dopo la standardizzazione per consolidare le righe
    giacenze_sap = giacenze_sap.groupby(['Materiale', 'mag', 'Descrizione'])['Qtà Disponibile'].sum().reset_index()
    giacenze_digigem = giacenze_digigem.groupby(['Materiale', 'mag'])['Conteggio'].sum().reset_index()
    nav_agg_rules = {'Giacenza': 'sum'}
    if 'Descrizione_NAV' in giacenze_nav.columns: nav_agg_rules['Descrizione_NAV'] = 'first'
    giacenze_nav = giacenze_nav.groupby(['Materiale', 'mag']).agg(nav_agg_rules).reset_index()
    
    df_summary = pd.merge(giacenze_sap, giacenze_digigem, on=['Materiale', 'mag'], how='outer')
    df_summary = pd.merge(df_summary, giacenze_nav, on=['Materiale', 'mag'], how='outer')
    
    df_summary.rename(columns={'Conteggio': 'Qtà Digigem', 'Giacenza': 'NAV.Giacenza', 'Qtà Disponibile': 'Qtà Disponibile(SAP)'}, inplace=True)
    
    df_totale_csv.rename(columns={'Materiale': 'NMU'}, inplace=True)
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
    
    df_summary.rename(columns={'mag': 'Provincia'}, inplace=True)
    final_cols = ['Materiale', 'Provincia', 'Descrizione', 'Qtà Disponibile(SAP)', 'Qtà Digigem', 'Delta(Digigem - SAP)', 'NAV.Giacenza', 'VIAGGIANTE (NAV - SAP)']
    df_riepilogo = df_summary[[c for c in final_cols if c in df_summary.columns]]
    df_riepilogo.rename(columns={'Materiale': 'NMU'}, inplace=True)
    
    return df_dettaglio, df_riepilogo

# --- INTERFACCIA UTENTE ---

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
    st.session_state.last_update = "Mai eseguito"

col1, col2 = st.columns([2, 5])
with col1:
    if st.button("🔄 Carica e Processa Dati", type="primary"):
        df_d, df_r = process_all_data()
        st.session_state.df_dettaglio = df_d
        st.session_state.df_riepilogo = df_r
        st.session_state.data_loaded = True
        st.session_state.last_update = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        st.rerun()

with col2:
    st.caption(f"Dati in memoria aggiornati il: {st.session_state.last_update}")

st.markdown("---")

if st.session_state.data_loaded:
    df_dettaglio = st.session_state.df_dettaglio
    df_riepilogo_magazzino = st.session_state.df_riepilogo
    
    for col in ['Stato', 'NMU', 'serial_number_tim', 'serial_number_forn']:
        if col in df_dettaglio.columns:
            df_dettaglio[col] = df_dettaglio[col].astype(str)
    if 'NMU' in df_riepilogo_magazzino.columns:
        df_riepilogo_magazzino['NMU'] = df_riepilogo_magazzino['NMU'].astype(str)

    tab1, tab2, tab3 = st.tabs(["Ricerca Seriale Dettagliata", "Riepilogo per Magazzino", "🔎 Ricerca Libera"])
    
    with tab1:
        st.header("Ricerca Guidata per Fornitore/Stato e NMU")
        df_dettaglio_tab1 = df_dettaglio.copy()
        df_dettaglio_tab1['Stato'] = df_dettaglio_tab1['Stato'].fillna('')
        df_dettaglio_tab1.dropna(subset=['NMU'], inplace=True)
        stati_fissi = [ "Carico", "ANTE 2023", "NON IN NAV", "Reso Carico", "Cambio Progetto", "Trasf. in Ingresso", "Rett. Positiva", "Trasf. in Uscita", "Rett. Negativa", "A MAGAZZINO", "INSTALLATO", "IN TRANSITO", "GUASTO" ]
        tutti_stati = df_dettaglio_tab1['Stato'].unique()
        lista_fornitori = ["Seleziona..."] + sorted([s for s in tutti_stati if s not in stati_fissi and s != '' and s != 'nan'])
        fornitore_selezionato = st.selectbox("1. Scegli il Fornitore o Stato", lista_fornitori, key="forn_dettaglio")
        if fornitore_selezionato != "Seleziona...":
            df_per_fornitore = df_dettaglio_tab1[df_dettaglio_tab1['Stato'] == fornitore_selezionato].copy()
            df_per_fornitore['NMU_con_desc'] = df_per_fornitore['NMU'].astype(str) + " - " + df_per_fornitore['desc_nmu'].fillna('')
            lista_nmu = ["Seleziona un NMU..."] + sorted(df_per_fornitore['NMU_con_desc'].unique().tolist())
            nmu_selezionato_display = st.selectbox("2. Scegli l'NMU", lista_nmu, key="nmu_dettaglio")
            if nmu_selezionato_display != "Seleziona un NMU...":
                nmu_reale = nmu_selezionato_display.split(" - ")[0]
                df_finale = df_per_fornitore[df_per_fornitore['NMU'] == nmu_reale]
                colonne_da_visualizzare = ['serial_number_tim', 'serial_number_forn', 'status', 'cod_terr_sap', 'status_regman', 'desc_nmu', 'Data di Registrazione']
                colonne_esistenti = [col for col in colonne_da_visualizzare if col in df_finale.columns]
                st.markdown(f"#### Dettaglio per NMU: **{nmu_reale}**")
                st.write(f"**{len(df_finale)}** seriali trovati per lo stato/fornitore: **{fornitore_selezionato}**")
                st.dataframe(df_finale[colonne_esistenti], use_container_width=True, hide_index=True)

    with tab2:
        st.header("Riepilogo Giacenze per Magazzino")
        df_riepilogo_magazzino['NMU'] = df_riepilogo_magazzino['NMU'].astype(str)
        col1_tab2, col2_tab2 = st.columns(2)
        with col1_tab2:
            df_riepilogo_magazzino['Provincia'] = df_riepilogo_magazzino['Provincia'].astype(str).fillna('')
            province_disponibili = ["Tutte"] + sorted(df_riepilogo_magazzino['Provincia'].unique().tolist())
            provincia_selezionata = st.selectbox("Filtra per Provincia:", province_disponibili)
        with col2_tab2:
            nmu_da_cercare = st.text_input("Filtra per NMU:", key="nmu_riepilogo")
        df_visualizzato = df_riepilogo_magazzino
        if provincia_selezionata != "Tutte":
            df_visualizzato = df_visualizzato[df_visualizzato['Provincia'] == provincia_selezionata]
        if nmu_da_cercare:
            df_visualizzato = df_visualizzato[df_visualizzato['NMU'].str.startswith(nmu_da_cercare)]
        st.dataframe(df_visualizzato, use_container_width=True, hide_index=True)

    with tab3:
        st.header("Ricerca Libera per Seriale o NMU")
        df_dettaglio_tab3 = df_dettaglio.copy()
        for col in ['NMU', 'serial_number_tim', 'serial_number_forn', 'Stato']:
            if col in df_dettaglio_tab3.columns:
                df_dettaglio_tab3[col] = df_dettaglio_tab3[col].astype(str)
        
        campo_di_ricerca = st.radio("Cerca per:",('NMU', 'Seriale TIM', 'Seriale Fornitore'), horizontal=True, key="campo_ricerca")
        valore_ricerca = st.text_input("Inserisci un valore di ricerca parziale:", key="valore_ricerca")
        if valore_ricerca:
            if campo_di_ricerca == 'NMU':
                risultati = df_dettaglio_tab3[df_dettaglio_tab3['NMU'].str.contains(valore_ricerca, case=False, na=False)]
            elif campo_di_ricerca == 'Seriale TIM':
                risultati = df_dettaglio_tab3[df_dettaglio_tab3['serial_number_tim'].str.contains(valore_ricerca, case=False, na=False)]
            else:
                risultati = df_dettaglio_tab3[df_dettaglio_tab3['serial_number_forn'].str.contains(valore_ricerca, case=False, na=False)]
            
            st.write(f"Trovati **{len(risultati)}** risultati.")
            if not risultati.empty:
                colonne_da_mostrare = ['Stato', 'NMU', 'desc_nmu', 'serial_number_tim', 'serial_number_forn']
                colonne_esistenti = [col for col in colonne_da_mostrare if col in risultati.columns]
                st.dataframe(risultati[colonne_esistenti], use_container_width=True, hide_index=True)
else:
    st.info("Benvenuto! Clicca su 'Carica / Processa Dati' per iniziare l'analisi.")