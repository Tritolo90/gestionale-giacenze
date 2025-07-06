# app.py (versione definitiva con correzione UFuncTypeError)
import streamlit as st
import pandas as pd
import numpy as np
import os
import glob
import re
from datetime import datetime

st.set_page_config(layout="wide")
st.title("Gestionale Giacenze 📦")

# --- DEFINIZIONE DEI PERCORSI (relativi alla posizione di app.py) ---
FOLDER_DIGIGEM = "Digigem"
FOLDER_SAP = "SAP"
FILE_NAV = "NAV.xlsx"

# ==============================================================================
# === FUNZIONE DI ELABORAZIONE DATI CON CACHE
# ==============================================================================

@st.cache_data(ttl=3600, show_spinner="Elaborazione dati in corso...")
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

    # --- FASE 1: Dati di Dettaglio ---
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
    
    final_cols_detail = ['NMU', 'desc_nmu', 'Stato', 'serial_number_tim', 'serial_number_forn', 'status', 'cod_terr_sap', 'status_regman', 'Data di Registrazione']
    df_dettaglio = df_merged_detail[[c for c in final_cols_detail if c in df_merged_detail.columns]]

    # --- FASE 2: Dati Aggregati ---
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
    
    df_nav_raw = pd.read_excel(FILE_NAV, sheet_name="Foglio1")
    df_nav_raw.rename(columns={"Quantità": "Giacenza", "Nr. Articolo": "Materiale", "Cod. Ubicazione": "mag"}, inplace=True)
    giacenze_nav = df_nav_raw.groupby(['Materiale', 'mag'])['Giacenza'].sum().reset_index()

    df_summary = pd.merge(giacenze_sap, giacenze_digigem, on=['Materiale', 'mag'], how='outer')
    df_summary = pd.merge(df_summary, giacenze_nav, on=['Materiale', 'mag'], how='outer')
    df_summary.rename(columns={'Conteggio': 'Qtà Digigem', 'Giacenza': 'NAV.Giacenza', 'Qtà Disponibile': 'Qtà Disponibile(SAP)'}, inplace=True)
    
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
    df_riepilogo = df_summary[[c for c in final_cols if c in df_summary.columns]]
    df_riepilogo.rename(columns={'Materiale': 'NMU'}, inplace=True)
    
    st.session_state.last_update = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    return df_dettaglio, df_riepilogo

# --- INTERFACCIA UTENTE ---

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

if st.button("🔄 Carica / Aggiorna Dati", type="primary"):
    df_d, df_r = process_all_data()
    st.session_state.df_dettaglio = df_d
    st.session_state.df_riepilogo = df_r
    st.session_state.data_loaded = True
    st.success("Dati elaborati!")

if st.session_state.data_loaded:
    if 'last_update' in st.session_state:
        st.caption(f"Dati aggiornati il: {st.session_state.last_update}")

    df_dettaglio = st.session_state.df_dettaglio
    df_riepilogo_magazzino = st.session_state.df_riepilogo
    
    if 'Stato' in df_dettaglio.columns:
        df_dettaglio['Stato'] = df_dettaglio['Stato'].astype(str)
    if 'NMU' in df_dettaglio.columns:
        df_dettaglio['NMU'] = df_dettaglio['NMU'].astype(str)
    if 'NMU' in df_riepilogo_magazzino.columns:
        df_riepilogo_magazzino['NMU'] = df_riepilogo_magazzino['NMU'].astype(str)

    tab1, tab2, tab3 = st.tabs(["Ricerca Seriale Dettagliata", "Riepilogo per Magazzino", "🔎 Ricerca Libera"])
    
    with tab1:
        st.header("Ricerca Guidata per Fornitore e NMU")
        df_dettaglio_tab1 = df_dettaglio.copy()
        df_dettaglio_tab1['Stato'] = df_dettaglio_tab1['Stato'].fillna('')
        df_dettaglio_tab1.dropna(subset=['NMU'], inplace=True)
        stati_fissi = [ "Carico", "ANTE 2023", "NON IN NAV", "Reso Carico", "Cambio Progetto", "Trasf. in Ingresso", "Rett. Positiva", "Trasf. in Uscita", "Rett. Negativa", "A MAGAZZINO", "INSTALLATO", "IN TRANSITO", "GUASTO" ]
        tutti_stati = df_dettaglio_tab1['Stato'].unique()
        lista_fornitori = ["Seleziona un fornitore..."] + sorted([s for s in tutti_stati if s not in stati_fissi and s != '' and s != 'nan'])
        fornitore_selezionato = st.selectbox("1. Scegli il Fornitore o Stato", lista_fornitori, key="forn_dettaglio")
        
        if fornitore_selezionato != "Seleziona un fornitore...":
            df_per_fornitore = df_dettaglio_tab1[df_dettaglio_tab1['Stato'] == fornitore_selezionato].copy()
            
            # ===========================================================================
            # === LA MODIFICA CHIAVE È QUI: Convertiamo NMU in testo PRIMA di sommarlo ===
            # ===========================================================================
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
    st.info("Benvenuto! Clicca su 'Carica / Aggiorna Dati' per iniziare.")