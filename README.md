# SportSet - Esercizio DBT

## Descrizione del progetto
**SportSet** è un progetto di esercitazione realizzato per imparare e praticare l'uso di **dbt (Data Build Tool)**. L’obiettivo principale del progetto è prendere dati grezzi da pagine web e trasformarli in modelli puliti e strutturati tramite dbt, applicando best practice di progettazione di data pipeline.

Il progetto non è legato a dati reali di produzione, ma serve come **sandbox didattica** per esercitarsi con:  
- creazione di **sources** e **staging models**  
- trasformazioni SQL di base e avanzate  
- gestione dei dati **incremental** e pulizia dei valori (es. rimozione di simboli `$` o `%`)  
- utilizzo di timestamp per tracciare aggiornamenti  
- progettazione di un flusso logico: `raw → staging → intermediate → marts`

## Dati
I dati provengono da pagine web scaricate o da CSV simulati. Sono organizzati in tabelle raw come:  
- `customers_raw`  
- `orders_raw`  
- `order_details_raw`  
- `products_raw`  
- `reseller_raw`  
- `sales_territory_raw`  
- `dates_raw`  

Ogni tabella viene poi trasformata in uno **staging model** con colonne pulite, tipi corretti e gestione di aggiornamenti incrementali tramite chiavi univoche.

## Funzionalità principali
1. **Staging dei dati**  
   - Pulizia valori numerici e formattazioni (`$`, `,`, `%`)  
   - Creazione di colonne di audit come `last_update_timestamp`  
   - Modelli incrementali basati sulle chiavi univoche

2. **Incremental loading**  
   - Evita di ricaricare l’intera tabella ad ogni run  
   - Aggiorna solo i nuovi record (append-only)

3. **Organizzazione modulare**  
   - Separazione chiara tra dati raw, staging e modelli aggregati  
   - Facilità di test e manutenzione



