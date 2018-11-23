
# coding: utf-8

# In[152]:


from cassandra.cluster import Cluster
cluster = Cluster(["172.17.0.2"])
session = cluster.connect("tfbda")
query = 'select ano, count(mes), qtd_obt,sum(vlr_bnf), cod_mun FROM tfbda.bsafm_obt GROUP BY ano, cod_mun limit 24'
# qtd_obt jah eh a mesma por conta do dado ser anual
# rows = session.execute('SELECT ano, mes, count(*) FROM bsa_fam GROUP BY ano, mes'210055)
# rows = session.execute('SELECT * FROM bsa_fam obt_inf where bsa_fam.cod_mun = obt_inf.cod_mun ALLOW FILTERING')
# rows = session.execute('SELECT * FROM obt_inf  where cod_mun = 210055 ALLOW FILTERING')
rows = session.execute(query)
rows[:]


# In[153]:


# verificando a soma dos valores bnf 
query = """select ano, mes, qtd_obt,vlr_bnf, cod_mun FROM tfbda.bsafm_obt
where cod_mun = 210005 and ano = 2014 GROUP BY ano, cod_mun, mes limit 24 ALLOW FILTERING"""
rows = session.execute(query)
rows[:]
a = 0

for item in rows:
    a += item.vlr_bnf
#     print(item.vlr_bnf)
    
print(a)


# In[91]:


# criando o arquivo merged file, que é a juncao do bolsa familia e obitos
import pandas as p

df1 = pd.read_csv('obitos_infantis_MA.csv', sep = ';')
# df = pd.read_csv('novos_dados.csv',encoding ='latin1', error_bad_lines=False, sep='\t', header=0)
df2 = pd.read_csv('bolsa_familia_Brasil.csv', encoding ='latin1', error_bad_lines=False)
# df2.to_csv('bolsa_familia_Brasil.csv', encoding='utf-8')
# df2 = pd.read_csv('bolsa_familia_Brasil.csv')
print(df1.columns)
print(df2.columns)
df3 = pd.merge(df1, df2, on=['cod_mun', 'ano'])
df3 = df3.drop(df3.columns[df3.columns.str.contains('unnamed',case = False)],axis = 1)
df3.to_csv('merged_data.csv')
#select ano, mes, qtd_obt,sum(qtd_fam), cod_mun FROM tfbda.bsafm_obt GROUP BY ano, mes, cod_mun limit 24;

