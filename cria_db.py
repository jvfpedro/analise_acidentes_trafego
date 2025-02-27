import psycopg2
import time
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
import geoalchemy2
import matplotlib.pyplot as plt

#====================================================================================
arquivo_acidentes = r"C:\Users\Administrador\Desktop\Ferramentas Computacionais Aplicadas a Engenharia Civil\T2\dados\alterados\dados_acidentes.csv"
arquivo_vmda = r"C:\Users\Public\VMDa2022_SNV_202301B_edit.csv"
arquivo_populacao = r"C:\Users\Public\CD2022_Populacao_Coletada_Imputada_e_Total_Municipio_e_UF_20231222.csv"
arquivo_malha_rod = r"C:\Users\Administrador\Desktop\Ferramentas Computacionais Aplicadas a Engenharia Civil\T2\dados\originais\SNV_202404A.shp"
arquivo_malha_mun = r"C:\Users\Administrador\Desktop\Ferramentas Computacionais Aplicadas a Engenharia Civil\T2\dados\originais\SC_Municipios_2022.shp"
#====================================================================================

importa_acidentes = False
importa_vmda = True
importa_populacao = False
importa_malha_rod = False
importa_malha_mun = False
start = time.time()

# Estabelecer a conexão com o banco de dados
connection = psycopg2.connect(
    dbname="ecv2812",  # Substitua "nome_do_banco" pelo nome do seu banco de dados
    user="aluno",  # Substitua "nome_de_usuario" pelo seu nome de usuário do PostgreSQL
    password="aluno",    # Substitua "sua_senha" pela senha do seu usuário do PostgreSQL
    host="localhost",         # Se o banco estiver em outro host, substitua por seu endereço IP ou hostname
    port="5432"
)

with connection:
    cursor = connection.cursor()
    if importa_acidentes:
        # Criar um cursor para executar comandos SQL
        cursor.execute("DROP TABLE IF EXISTS acidentes")
        cursor.execute('CREATE TABLE acidentes(uf char(2), br int, km real, classe varchar(100), lat real, lon real)')

        conta = 0
        for line in open(arquivo_acidentes, "r").readlines():
            if conta > 0:
                line = line.replace("\n","")
                print(line)
                partes = line.split(";")

                if partes[1] != "NA":
                    cursor.execute("INSERT INTO acidentes (uf, br, km, classe, lat, lon) VALUES ('%s', %d, %f, '%s', %f, %f)" %(partes[0], int(partes[1]), float(partes[2]), partes[3], float(partes[4]), float(partes[5])))
            conta +=1
        cursor.execute("DELETE FROM acidentes WHERE uf != 'SC'")
        cursor.execute("ALTER TABLE acidentes ADD COLUMN geom_coord geometry(Point, 4326);")
        cursor.execute("UPDATE acidentes SET geom_coord = ST_SetSRID(ST_MakePoint(lon, lat), 4326);")



        '''
        cursor.execute("ALTER TABLE acidentes ADD COLUMN geom_srl geometry(Point, 4326);")
        sql_join = "JOIN acidentes AS a ON r.vl_br::INT=a.br::INT AND r.vl_km_inic<a.km AND r.vl_km_fina > a.km"
        sql_1 = "SELECT r.geom FROM rodovias AS r %s" % sql_join
        sql_2 = "SELECT (a.km - r.vl_mk_inic)/(r.vl_km_fina - r.vl_km_inic) FROM acidentes AS a, rodovias AS r %s" %sql_join
        cursor.execute("UPDATE acidentes SET geom_srk = ST_LineInterpolatePoint((%s), (%s))" % (sql_1, sql_2))
        '''
    
    if importa_vmda:
        cursor.execute("DROP TABLE IF EXISTS vmda")
        cursor.execute("CREATE TABLE vmda (cod_snv char(10), vmda_c int, vmda_d int)")
        cursor.execute("COPY vmda FROM '%s' WITH DELIMITER ';' NULL AS ''" % arquivo_vmda)

    if importa_populacao:
        cursor.execute("DROP TABLE IF EXISTS populacao")
        cursor.execute("CREATE TABLE populacao (uf char(2), cod_mun int, nome_mun char(50), pop_total int)")
        df = pd.read_csv(arquivo_populacao, delimiter=';', encoding='latin1')
        temp_file = r"C:\Users\Public\temp_populacao.csv"
        df.to_csv(temp_file, sep=';', index=False, encoding='utf-8')
        cursor.execute("COPY populacao FROM '%s' WITH DELIMITER ';'" % temp_file)

    engine = create_engine(f"postgresql://aluno:aluno@localhost:5432/ecv2812")

    if importa_malha_rod:
        gdf = gpd.read_file(arquivo_malha_rod)
        gdf.to_postgis('rod', engine, if_exists='replace', index=False)
        cursor.execute("DELETE FROM rod WHERE sg_uf != 'SC'")

    if importa_malha_mun:
        gdf = gpd.read_file(arquivo_malha_mun)
        gdf.to_postgis('municipios', engine, if_exists='replace', index=False)

    cursor.execute("""SELECT classe, COUNT(*) FROM acidentes GROUP BY classe""")
    resultados_acidentes = cursor.fetchall()
    classes = [row[0] for row in resultados_acidentes]
    counts = [row[1] for row in resultados_acidentes]
    plt.figure(figsize=(10, 6))
    plt.bar(classes, counts, color=['blue', 'green', 'red'])
    plt.xlabel('Classe')
    plt.ylabel('Quantidade')
    plt.title('Quantidades de Acidentes por Classe')
    plt.show()

    cursor.execute("""SELECT vmda_c, vmda_d FROM vmda WHERE vmda_c IS NOT NULL AND vmda_d IS NOT NULL""")
    resultados_vmda = cursor.fetchall()
    diferenca = [row[0] - row[1] for row in resultados_vmda]

    # Criar um histograma das diferenças
    plt.figure(figsize=(10, 6))
    plt.hist(diferenca, bins=500, edgecolor='black')
    plt.xlabel('Diferença de Volume (vmda_c - vmda_d)')
    plt.ylabel('Quantidade')
    plt.title('Histograma das Diferenças de Volume')
    plt.xlim(-5000, 5000)
    plt.show()


    print(f"Tempo de processamento = {time.time() - start}")