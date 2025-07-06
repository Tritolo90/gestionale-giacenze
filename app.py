import streamlit as st
import pandas as pd
import numpy as np
import os
import glob
import re
from datetime import datetime

st.set_page_config(layout="wide")
st.title("Gestionale Giacenze 📦")

# --- PERCORSI DEI FILE E DELLE CARTELLE ---
FOLDER_DIGIGEM = "Digigem"
FOLDER_SAP = "SAP"
FILE_NAV = "NAV.xlsx"
FILE_ANAGRAFICA = 'anagrafica_fornitori.csv'
FILE_LAST_UPDATE = 'last_update.txt'
FILE_DETTAGLIO_OUTPUT = "inventario_dettagliato_finale.csv"
FILE_RIEPILOGO_OUTPUT = "riepilogo_per_magazzino.csv"


# ==============================================================================
# === LA LOGICA DI ELABORAZIONE DATI È ORA IN UNA FUNZIONE CON CACHE            ===
# ==============================================================================

@st.cache_data(ttl=3600) # Mette in cache i dati per 1 ora
def process_all_data():
    """
    Esegue l'intera pipeline di elaborazione dati e restituisce i due DataFrame finali.
    Questa funzione viene messa in cache per velocità.
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
            if fields[0].strip().isdigit() and len(fields) > 6:
                try:
                    clean_data_rows.append({'Materiale': fields[0].strip(),'mag': current_mag,'Descrizione': fields[1].strip(),'Qtà Disponibile': fields[6].strip()})
                except IndexError: continue
        return pd.DataFrame(clean_data_rows)
    
    # ... (le altre funzioni di supporto rimangono le stesse)

    # --- INIZIO PIPELINE ---
    
    # FASE 1
    csv_files = glob.glob(os.path.join(FOLDER_DIGIGEM, "*.csv"))
    df_totale_csv = pd.concat([pd.read_csv(f, sep=',', encoding='latin1', low_memory=False) for f in csv_files], ignore_index=True)
    df_totale_csv.rename(columns={'cod_nmu': 'NMU', 'descr_impresa_utilizzo': 'Fornitore'}, inplace=True)
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

    df_final_detail = df_merged_detail # Continuiamo a lavorare su questo per aggiungere i nomi fornitori
    
    if os.path.exists(FILE_ANAGRAFICA):
        df_anagrafica = pd.read_csv(FILE_ANAGRAFICA, sep=';', dtype=str)
        df_anagrafica['Codice'] = df_anagrafica['Codice'].str.strip()
        df_final_detail['CodiceJoin'] = df_final_detail['Stato'].astype(str).str.extract(r'(\d+)').fillna('0')
        df_final_detail = pd.merge(df_final_detail, df_anagrafica, left_on='CodiceJoin', right_on='Codice', how='left')
        df_final_detail['Fornitore/Stato'] = df_final_detail['Nome'].fillna(df_final_detail['Stato'])
        df_final_detail.drop(columns=['CodiceJoin', 'Codice', 'Nome'], inplace=True, errors='ignore')
    else:
        df_final_detail.rename(columns={'Stato': 'Fornitore/Stato'}, inplace=True)

    final_cols_detail = ['NMU', 'desc_nmu', 'Fornitore/Stato', 'serial_number_tim', 'serial_number_forn', 'status', 'cod_terr_sap', 'status_regman', 'Data di Registrazione']
    df_final_detail = df_final_detail[[c for c in final_cols_detail if c in df_final_detail.columns]]
    df_final_detail.to_csv(FILE_DETTAGLIO_OUTPUT, index=False)
    
    # FASE 2
    sap_txt_files = rinomina_file_sap_in_txt(FOLDER_SAP)
    if not sap_txt_files:
        df_final_summary = pd.DataFrame() # Crea un dataframe vuoto se non ci sono file SAP
    else:
        lista_df_sap = [parse_sap_file_manually(f) for f in sap_txt_files]
        df_sap = pd.concat(lista_df_sap, ignore_index=True)
        for col in ["Qtà Disponibile", "Materiale"]: df_sap[col] = pd.to_numeric(df_sap[col].str.replace(',', '.'), errors='coerce').fillna(0)
        sap_agg_rules = {'Descrizione': 'first', 'Qtà Disponibile': 'sum'}
        giacenze_sap = df_sap.groupby(['Materiale', 'mag']).agg(sap_agg_rules).reset_index()
        # ... resto della logica FASE 2 ...
        df_final_summary = giacenze_sap # Semplificazione per l'esempio

    df_final_summary.to_csv(FILE_RIEPILOGO_OUTPUT, index=False)

    # Scrive il timestamp e restituisce i dati
    with open(FILE_LAST_UPDATE, "w") as f:
        f.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    
    return df_final_detail, df_final_summary


# --- INTERFACCIA UTENTE ---

# Funzione per mostrare l'ultimo aggiornamento
def show_last_update():
    if os.path.exists(FILE_LAST_UPDATE):
        with open(FILE_LAST_UPDATE, "r") as f:
            st.caption(f"Dati aggiornati il: {f.read()}")
    else:
        st.caption("Dati non ancora generati.")

# Pulsante di aggiornamento e timestamp
col1, col2 = st.columns([1, 3])
with col1:
    if st.button("🔄 Aggiorna Dati"):
        # Pulisce la cache per forzare il ricalcolo
        st.cache_data.clear()
        # Mostra un messaggio di stato generico
        with st.spinner("Elaborazione in corso... Questo potrebbe richiedere alcuni minuti."):
            process_all_data() # Chiama la funzione che ora è cachable
        st.success("Elaborazione completata!")
        # Non serve st.rerun() perché Streamlit lo fa già dopo un'azione sul bottone

with col2:
    show_last_update()

st.markdown("---")

# Se i file di output non esistono, chiedi all'utente di aggiornare
if not os.path.exists(FILE_DETTAGLIO_OUTPUT) or not os.path.exists(FILE_RIEPILOGO_OUTPUT):
    st.warning("I file di dati non sono stati ancora generati. Clicca sul pulsante 'Aggiorna Dati' per avviare la prima elaborazione.")
else:
    # Creazione delle schede
    tab1, tab2, tab3 = st.tabs(["Ricerca Seriale Dettagliata", "Riepilogo per Magazzino", "🔎 Ricerca Libera"])
    
    # ... (Il resto del codice delle schede rimane identico a prima) ...
    with tab1:
        st.header("Ricerca Guidata per Fornitore e NMU")
        # ...

    with tab2:
        st.header("Riepilogo Giacenze per Magazzino")
        # ...

    with tab3:
        st.header("Ricerca Libera per Seriale o NMU")
        # ...