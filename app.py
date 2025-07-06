# app.py (versione con correzione nome colonna 'Stato')
import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide")
st.title("Gestionale Giacenze 📦")

# --- FUNZIONE PER CARICARE I DATI ---
@st.cache_data
def load_data(file_path):
    if not os.path.exists(file_path):
        return None
    try:
        dtype_map = {
            'NMU': str, 'serial_number_tim': str, 'Fornitore/Stato': str, 
            'serial_number_forn': str, 'status': str,
            'cod_terr_sap': str, 'status_regman': str, 'Stato': str
        }
        return pd.read_csv(file_path, dtype=dtype_map)
    except Exception as e:
        st.error(f"Errore nel caricamento del file {file_path}: {e}")
        return None

# Caricamento dei dataset
df_dettaglio = load_data("inventario_dettagliato_finale.csv")
df_riepilogo_magazzino = load_data("riepilogo_per_magazzino.csv")

# ===========================================================================
# === LA MODIFICA CHIAVE È QUI: Rendiamo il nome della colonna coerente     ===
# ===========================================================================
if df_dettaglio is not None and 'Fornitore/Stato' in df_dettaglio.columns:
    df_dettaglio.rename(columns={'Fornitore/Stato': 'Stato'}, inplace=True)


# Creazione delle schede
tab1, tab2, tab3 = st.tabs(["Ricerca Seriale Dettagliata", "Riepilogo per Magazzino", "🔎 Ricerca Libera"])

# --- Scheda 1: Ricerca Seriale per Fornitore/NMU ---
with tab1:
    st.header("Ricerca Guidata per Fornitore e NMU")
    if df_dettaglio is None:
        st.error("File dati di dettaglio ('inventario_dettagliato_finale.csv') non trovato.")
    else:
        df_dettaglio_tab1 = df_dettaglio.copy()
        df_dettaglio_tab1['Stato'] = df_dettaglio_tab1['Stato'].fillna('')
        df_dettaglio_tab1 = df_dettaglio_tab1.dropna(subset=['NMU'])
        
        stati_fissi = [ "Carico", "ANTE 2023", "NON IN NAV", "Reso Carico", "Cambio Progetto", "Trasf. in Ingresso", "Rett. Positiva", "Trasf. in Uscita", "Rett. Negativa", "A MAGAZZINO", "INSTALLATO", "IN TRANSITO", "GUASTO" ]
        tutti_stati = df_dettaglio_tab1['Stato'].unique()
        lista_fornitori = ["Seleziona un fornitore..."] + sorted([s for s in tutti_stati if s not in stati_fissi and s != ''])
        
        fornitore_selezionato = st.selectbox("1. Scegli il Fornitore o Stato", lista_fornitori, key="forn_dettaglio")
        
        if fornitore_selezionato != "Seleziona un fornitore...":
            df_per_fornitore = df_dettaglio_tab1[df_dettaglio_tab1['Stato'] == fornitore_selezionato].copy()
            
            df_per_fornitore['NMU_con_desc'] = df_per_fornitore['NMU'] + " - " + df_per_fornitore['desc_nmu'].fillna('')
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

# --- Scheda 2: Riepilogo per Magazzino ---
with tab2:
    st.header("Riepilogo Giacenze per Magazzino")
    if df_riepilogo_magazzino is None:
        st.error("File di riepilogo ('riepilogo_per_magazzino.csv') non trovato.")
    else:
        df_riepilogo_magazzino['NMU'] = df_riepilogo_magazzino['NMU'].astype(str)
        col1, col2 = st.columns(2)
        with col1:
            province_disponibili = ["Tutte"] + sorted(df_riepilogo_magazzino['Provincia'].unique().tolist())
            provincia_selezionata = st.selectbox("Filtra per Provincia:", province_disponibili)
        with col2:
            nmu_da_cercare = st.text_input("Filtra per NMU:", key="nmu_riepilogo")
        
        df_visualizzato = df_riepilogo_magazzino
        if provincia_selezionata != "Tutte":
            df_visualizzato = df_visualizzato[df_visualizzato['Provincia'] == provincia_selezionata]
        if nmu_da_cercare:
            df_visualizzato = df_visualizzato[df_visualizzato['NMU'].str.startswith(nmu_da_cercare)]
        st.dataframe(df_visualizzato, use_container_width=True, hide_index=True)


# --- Scheda 3: Ricerca Libera ---
with tab3:
    st.header("Ricerca Libera per Seriale o NMU")
    if df_dettaglio is None:
        st.error("File dati di dettaglio ('inventario_dettagliato_finale.csv') non trovato.")
    else:
        df_dettaglio_tab3 = df_dettaglio.copy()
        campo_di_ricerca = st.radio(
            "Cerca per:",
            ('NMU', 'Seriale TIM', 'Seriale Fornitore'),
            horizontal=True,
            key="campo_ricerca"
        )
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
            st.info("Inserisci un valore nella casella di ricerca per visualizzare i risultati.")