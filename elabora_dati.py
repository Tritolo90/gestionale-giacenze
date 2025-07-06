# elabora_dati.py (versione con join robusto su anagrafica)
import pandas as pd
import numpy as np
import os
import glob
import re

# --- FUNZIONI DI SUPPORTO (invariate) ---
def rinomina_file_sap_in_txt(folder_path):
    print("... Fase di controllo/rinomina file SAP in .txt ...")
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

def carica_totale_csv_aggregato(df_dettaglio):
    if 'cod_terr_sap' not in df_dettaglio.columns or 'NMU' not in df_dettaglio.columns: return None
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


# --- PIPELINE DATI PRINCIPALE ---
def run_data_pipeline():
    print(">>> AVVIO PIPELINE DATI COMPLETA <<<")
    
    # FASE 1: Creazione Dati di Dettaglio
    print("\n--- INIZIO FASE 1: Creazione Dati di Dettaglio ---")
    folder_path_digigem = r"C:\Users\a.cremona\OneDrive - SIELTE S.p.A\MAGAZZINO PROGRAMMA\Digigem"
    csv_files = glob.glob(os.path.join(folder_path_digigem, "*.csv"))
    df_totale_csv = pd.concat([pd.read_csv(f, sep=',', encoding='latin1', low_memory=False) for f in csv_files], ignore_index=True)
    df_totale_csv.rename(columns={'cod_nmu': 'NMU'}, inplace=True)
    df_totale_csv.columns = df_totale_csv.columns.str.strip()
    
    nav_file_path = r"C:\Users\a.cremona\OneDrive - SIELTE S.p.A\MAGAZZINO PROGRAMMA\NAV.xlsx"
    df_nav = pd.read_excel(nav_file_path, sheet_name="Foglio1")
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

    # =================================================================================
    # === LA MODIFICA CHIAVE È QUI: Logica di join con chiave numerica pulita       ===
    # =================================================================================
    print("Arricchimento nomi fornitori...")
    file_anagrafica = 'anagrafica_fornitori.csv'
    if os.path.exists(file_anagrafica):
        df_anagrafica = pd.read_csv(file_anagrafica, sep=';', dtype=str)
        # Pulisce la chiave di join dell'anagrafica estraendo solo i numeri
        df_anagrafica['CodiceJoin'] = df_anagrafica['Codice'].str.extract(r'(\d+)').fillna('0')

        # Pulisce la chiave di join dei dati principali estraendo solo i numeri
        df_merged_detail['CodiceJoin'] = df_merged_detail['Stato_Originale'].astype(str).str.extract(r'(\d+)').fillna('0')
        
        # Unisce usando la chiave numerica pulita
        df_merged_detail = pd.merge(df_merged_detail, df_anagrafica, on='CodiceJoin', how='left')
        
        # Se trova il nome, lo usa, altrimenti mantiene lo stato originale
        df_merged_detail['Fornitore/Stato'] = df_merged_detail['Nome'].fillna(df_merged_detail['Stato_Originale'])
        
        df_merged_detail.drop(columns=['CodiceJoin', 'Codice', 'Nome', 'Stato_Originale'], inplace=True, errors='ignore')
    else:
        print(f"ATTENZIONE: File anagrafica '{file_anagrafica}' non trovato.")
        df_merged_detail.rename(columns={'Stato_Originale': 'Fornitore/Stato'}, inplace=True)

    final_cols = ['NMU', 'desc_nmu', 'Fornitore/Stato', 'serial_number_tim', 'serial_number_forn', 'status', 'cod_terr_sap', 'status_regman', 'Data di Registrazione']
    df_final_detail = df_merged_detail[[c for c in final_cols if c in df_merged_detail.columns]]
    df_final_detail.to_csv("inventario_dettagliato_finale.csv", index=False)
    print("--- FINE FASE 1: File 'inventario_dettagliato_finale.csv' creato. ---")

    # FASE 2 (invariata)
    print("\n--- INIZIO FASE 2: Creazione Riepilogo per Magazzino ---")
    # ... (Il resto dello script rimane invariato) ...
    folder_path_sap = r"C:\Users\a.cremona\OneDrive - SIELTE S.p.A\MAGAZZINO PROGRAMMA\SAP"
    sap_txt_files = rinomina_file_sap_in_txt(folder_path_sap)
    if not sap_txt_files: return
    lista_df_sap = [parse_sap_file_manually(f) for f in sap_txt_files]
    df_sap = pd.concat(lista_df_sap, ignore_index=True)
    for col in ["Qtà Disponibile", "Materiale"]: df_sap[col] = pd.to_numeric(df_sap[col].str.replace(',', '.'), errors='coerce').fillna(0)
    sap_agg_rules = {'Descrizione': 'first', 'Qtà Disponibile': 'sum'}
    giacenze_sap = df_sap.groupby(['Materiale', 'mag']).agg(sap_agg_rules).reset_index()
    giacenze_digigem = carica_totale_csv_aggregato(df_totale_csv)
    giacenze_nav = carica_giacenza_nav_semplice(nav_file_path)
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
    mappa_province_completa = { 'IMSUS014': 'CT', 'CT01': 'CT', 'S014': 'CT', 'IMSUS016': 'SR', 'SR01': 'SR', 'S016': 'SR', 'IMSUS017': 'RG', 'RG01': 'RG', 'S017': 'RG', 'IMSUS230': 'ME', 'CL104025ME': 'ME', 'S230': 'ME' }
    df_summary['mag'] = df_summary['mag'].replace(mappa_province_completa)
    df_summary.rename(columns={'mag': 'Provincia'}, inplace=True)
    final_cols = ['Materiale', 'Provincia', 'Descrizione', 'Qtà Disponibile(SAP)', 'Qtà Digigem', 'Delta(Digigem - SAP)', 'NAV.Giacenza', 'VIAGGIANTE (NAV - SAP)']
    df_summary = df_summary[[c for c in final_cols if c in df_summary.columns]]
    df_summary.rename(columns={'Materiale': 'NMU'}, inplace=True)
    df_summary.to_csv("riepilogo_per_magazzino.csv", index=False)
    print("--- FINE FASE 2: File 'riepilogo_per_magazzino.csv' creato. ---")
    print("\n>>> PIPELINE COMPLETATA. Entrambi i file di dati sono pronti. <<<")


if __name__ == "__main__":
    run_data_pipeline()