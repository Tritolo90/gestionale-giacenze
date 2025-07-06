# elabora_dati.py
import pandas as pd
import numpy as np
import os
import glob

def rinomina_file_sap_in_txt(folder_path):
    """
    Cerca file .xls* nella cartella SAP, li rinomina in .txt se non già presenti,
    e restituisce la lista di tutti i file .txt.
    """
    print("... Fase di controllo/rinomina file SAP in .txt ...")
    files_xls = glob.glob(os.path.join(folder_path, "*.xls*"))
    if files_xls:
        for file_path in files_xls:
            root, _ = os.path.splitext(file_path)
            new_path = root + ".txt"
            if not os.path.exists(new_path):
                os.rename(file_path, new_path)
    return glob.glob(os.path.join(folder_path, "*.txt"))

def carica_totale_csv_aggregato(df_dettaglio):
    """Crea un aggregato dai dati Digigem per il merge con SAP."""
    if 'cod_terr_sap' not in df_dettaglio.columns or 'NMU' not in df_dettaglio.columns: 
        return pd.DataFrame(columns=['Materiale', 'mag', 'Conteggio'])
    df_agg = df_dettaglio.groupby(['cod_terr_sap', 'NMU']).size().reset_index(name='Conteggio')
    df_agg.rename(columns={'NMU': 'Materiale', 'cod_terr_sap': 'mag'}, inplace=True)
    return df_agg

def carica_giacenza_nav_semplice(file_path):
    """Carica e aggrega i dati grezzi da NAV."""
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

def run_data_pipeline():
    """Esegue l'intera pipeline di elaborazione dati."""
    print(">>> AVVIO PIPELINE DATI COMPLETA <<<")
    
    # FASE 1: Creazione Dati di Dettaglio
    print("\n--- INIZIO FASE 1: Creazione Dati di Dettaglio ---")
    folder_path_digigem = r"Digigem"
    nav_file_path = r"NAV.xlsx"
    file_anagrafica = 'anagrafica_fornitori.csv'
    
    csv_files = glob.glob(os.path.join(folder_path_digigem, "*.csv"))
    df_totale_csv = pd.concat([pd.read_csv(f, sep=',', encoding='latin1', low_memory=False) for f in csv_files], ignore_index=True)
    df_totale_csv.rename(columns={'cod_nmu': 'NMU', 'descr_impresa_utilizzo': 'Fornitore'}, inplace=True)
    df_totale_csv.columns = df_totale_csv.columns.str.strip()

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
    df_merged_detail['Stato'] = np.select(conditions, choices, default='NON IN NAV')

    print("--- FINE FASE 1: File 'inventario_dettagliato_finale.csv' creato. ---")

    # FASE 2: Creazione Riepilogo per Magazzino
    print("\n--- INIZIO FASE 2: Creazione Riepilogo per Magazzino ---")
    folder_path_sap = r"SAP"
    sap_txt_files = rinomina_file_sap_in_txt(folder_path_sap)
    if not sap_txt_files: return

    all_rows = []
    for file_path in sap_txt_files:
        with open(file_path, 'r', encoding='utf-16') as f:
            for line in f:
                all_rows.append(line.strip().split('\t'))
    
    df_sap_raw = pd.DataFrame(all_rows)
    print(f"Lette {len(all_rows)} righe grezze da {len(sap_txt_files)} file SAP.")

    cols_to_drop_initial = [col for col in [0, 4, 5, 7, 8, 10] if col in df_sap_raw.columns]
    df_sap = df_sap_raw.drop(columns=cols_to_drop_initial)
    df_sap = df_sap.iloc[5:].reset_index(drop=True)
    df_sap.columns = ['Materiale', 'mag', 'Descrizione', 'Qtà Fisica', 'Qtà Disponibile']
    df_sap = df_sap.iloc[5:].reset_index(drop=True)
    df_sap.dropna(how='all', inplace=True)
    df_sap['mag'] = df_sap['mag'].replace(r'^\s*$', np.nan, regex=True).ffill()
    for col in ["Qtà Fisica", "Qtà Disponibile", "Materiale"]:
        df_sap[col] = pd.to_numeric(df_sap[col], errors='coerce').fillna(0)
    df_sap = df_sap[~df_sap['Materiale'].astype(str).isin(["Div.", "IMSU", "Materiale"])]
    df_sap['Materiale'] = df_sap['Materiale'].astype('int64')
    df_sap.sort_values(by='mag', inplace=True)
    
    giacenze_sap = df_sap.groupby(['Materiale', 'mag', 'Descrizione'])['Qtà Disponibile'].sum().reset_index()

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
        if col in df_summary.columns:
            df_summary[col] = df_summary[col].fillna(0)
    
    for col in ['Qtà Disponibile(SAP)', 'Qtà Digigem', 'NAV.Giacenza', 'Materiale']:
        df_summary[col] = pd.to_numeric(df_summary[col], errors='coerce').fillna(0).astype('int64')
        
    df_summary['Delta(Digigem - SAP)'] = df_summary['Qtà Digigem'] - df_summary['Qtà Disponibile(SAP)']
    df_summary['VIAGGIANTE (NAV - SAP)'] = df_summary['NAV.Giacenza'] - df_summary['Qtà Disponibile(SAP)']
    
    mappa_province = {'S014': 'CT', 'S016': 'SR', 'S017': 'RG', 'S230': 'ME'}
    df_summary['Provincia'] = df_summary['mag'].map(mappa_province).fillna(df_summary['mag'])
    
    final_cols = ['Materiale', 'Provincia', 'Descrizione', 'Qtà Disponibile(SAP)', 'Qtà Digigem', 'Delta(Digigem - SAP)', 'NAV.Giacenza', 'VIAGGIANTE (NAV - SAP)']
    df_summary = df_summary[[c for c in final_cols if c in df_summary.columns]]
    df_summary.rename(columns={'Materiale': 'NMU'}, inplace=True)
    
    df_summary.to_csv("riepilogo_per_magazzino.csv", index=False)
    print("--- FINE FASE 2: File 'riepilogo_per_magazzino.csv' creato. ---")
    
    print("\n>>> PIPELINE COMPLETATA. Entrambi i file di dati sono pronti. <<<")

if __name__ == "__main__":
    run_data_pipeline()