# Project plan

**OBIETTIVO**:  Confronto metodologie per record linkage
**Metodi**  =   [Machine_Learning, ?]

### TASK 1: Studio

> Studio e rielaborazione di articoli/interventi/libri riguardo il topic record linkage e data fusion.


### TASK 2: Sperimentazione

Ottenere un dataset contenente più informazioni possibili.

> **Input**:      8 dataset (identificabili per data) contenenti dati provenienti da 12(?) diverse fonti

> **Output**:     Dataset contenente (idealmente) un record per ogni ristorante, associato a più informazioni possibili 
                (data quality?)

#### STEP:

1.  Raggruppamento dei dati per fonte (o per data?) e allineamento dello schema.

> for metodo in Metodi :

2.  Record linkage su singola fonte per rimuovere duplicati.
    -   Quale dato tengo?
    -   Come tratto le informazioni della singola entità?

3.  Record linkage tra risultati dello step 2.