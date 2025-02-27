# Traffic Accident Data Analysis

This is a study project for the *Computational Tools Applied to Civil Engineering* course at UFSC. The goal of this code is to analyze traffic accident data and identify high-concentration locations.

## Features
- Imports and processes accident data from official sources.
- Loads data into a PostgreSQL/PostGIS database.
- Performs geospatial operations to enhance accident data.
- Analyzes and visualizes accident distributions.
- Compares estimated and real traffic volumes (VMDa).

## Technologies Used
- Python
- PostgreSQL + PostGIS
- Pandas
- GeoPandas
- SQLAlchemy
- Matplotlib

## Data Sources
The input data is sourced from public government datasets:
- **Federal Road Network Data:** [DNIT - PNV/ESNV](https://www.gov.br/dnit/pt-br/assuntos/atlas-e-mapas/pnv-esnv)
- **Traffic Accident Records:** [PRF Open Data](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf)
- **Estimated VMDa:** [DNIT Modelagem](http://servicos.dnit.gov.br/dadospnct/Modelagem)
- **Municipal Boundaries (SC):** [IBGE Maps Portal](https://portaldemapas.ibge.gov.br/)
- **Population Data:** [IBGE Census 2022](https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Populacao_e_domicilios_Primeiros_resultados/Resultados_da_2a_apuracao_20231027/)

## Installation and Setup
1. Install PostgreSQL with PostGIS extension.
2. Install required Python libraries:
   ```bash
   pip install psycopg2 pandas geopandas sqlalchemy matplotlib
   ```
3. Configure database connection parameters in the script.
4. Run the script to process and analyze the data.

## Notes
- This project is for educational purposes only.
- The data and results should not be used for official decision-making.
- Ensure database access and permissions before running the script.

## Author
João Vitor Ferreira Pedro Engineering Student at UFSC. GitHub: https://github.com/jvfpedro

