import pandas as pd
import numpy as np
import pymysql
import psycopg2
import time
import os
from itertools import combinations

##selecionar diretorio com os arquivos a serem lidos
os.chdir('C:/Users/danilo.chaves/Documents/Python/Gerenciamento Banco AVA')
oferta = pd.read_csv('Input/oferta_201.csv', sep=';', encoding='latin-1')
de_para_banco = pd.read_csv('Input/De_Para_Banco.csv', sep= ';', encoding='latin-1')
est_disciplina = pd.read_csv('Input/Estrutura_Disciplinas.csv', sep= ';', encoding='latin-1')

# Criar conexão com o banco em Prod do AVA MOODLE
database_ = 'prod_kls'
user_ = 'pgoncalves_aval'
password_ = 'ava2020'
host_ = 'cm-kls-202001.cluster-cu0eljf5y2ht.us-east-1.rds.amazonaws.com'
port_ = '3306'

conn = pymysql.connect(host=host_, user=user_, passwd=password_, db=database_)

sql = """SELECT  DISTINCT
-- qc5.name as Categoria_Principal,
                  qc4.name as Categoria_Principal,
                  qc3.name as Categoria_Disciplina,
                  qc2.name as Categoria_Unidade,
                  'Seção'  as Categoria_Seção,
                  qc1.name as Categoria_QUIZ,
                  q1.id as ID_QUESTAO,
                  q1.name as NOME_QUESTAO,
                  q1.qtype as TIPO,
                  CASE
                  WHEN q1.canceled = 1 THEN
                  'CANCELADO'
                  WHEN q1.canceled = 2 THEN
                  'CANCELADO E REGRADE'
                  ELSE
                  'OK'
                  END AS STATUS
                  FROM prod_kls.mdl_question q1
                  JOIN prod_kls.mdl_question_categories qc1 ON qc1.id = q1.category
                  LEFT JOIN prod_kls.mdl_question_categories qc2 ON qc2.id = qc1.parent
                  LEFT JOIN prod_kls.mdl_question_categories qc3 ON qc3.id = qc2.parent
                  LEFT JOIN prod_kls.mdl_question_categories qc4 ON qc4.id = qc3.parent
                  WHERE  q1.qtype <>'random' and
                  qc4.name in ('KLS 1.0','Graduação','ED','ED_17.2')
                  /*,'ED','ED_17.2','Graduação')*/


                  UNION
                  /************KLS 2.0 e KLS 1.0************/

                  SELECT  DISTINCT
                  qc5.name as Categoria_Principal,
                  qc4.name as Categoria_Disciplina,
                  qc3.name as Categoria_Unidade,
                  qc2.name as Categoria_Seção,
                  qc1.name as Categoria_QUIZ,
                  q1.id as ID_QUESTAO,
                  q1.name as NOME_QUESTAO,
                  q1.qtype as TIPO,
                  CASE
                  WHEN q1.canceled = 1 THEN
                  'CANCELADO'
                  WHEN q1.canceled = 2 THEN
                  'CANCELADO E REGRADE'
                  ELSE
                  'OK'
                  END AS STATUS
                  FROM prod_kls.mdl_question q1
                  JOIN prod_kls.mdl_question_categories qc1 ON qc1.id = q1.category
                  LEFT JOIN prod_kls.mdl_question_categories qc2 ON qc2.id = qc1.parent
                  LEFT JOIN prod_kls.mdl_question_categories qc3 ON qc3.id = qc2.parent
                  LEFT JOIN prod_kls.mdl_question_categories qc4 ON qc4.id = qc3.parent
                  LEFT JOIN prod_kls.mdl_question_categories qc5 ON qc5.id = qc4.parent
                  WHERE q1.qtype <>'random' and
                  qc5.name in ( 'KLS 2.0','KLS 1.0')

                  UNION

                  SELECT  DISTINCT
                  qc3.name as Categoria_Principal,
                  qc1.name as Categoria_Disciplina,
                  'Unidade' as Categoria_unidade,
                  'Seção'  as Categoria_Seção,
                  qc2.name as Categoria_QUIZ,
                  q1.id as ID_QUESTAO,
                  q1.name as NOME_QUESTAO,
                  q1.qtype as TIPO,
                  CASE
                  WHEN q1.canceled = 1 THEN
                  'CANCELADO'
                  WHEN q1.canceled = 2 THEN
                  'CANCELADO E REGRADE'
                  ELSE
                  'OK'
                  END AS STATUS
                  FROM prod_kls.mdl_question q1
                  JOIN prod_kls.mdl_question_categories qc1 ON qc1.id = q1.category
                  LEFT JOIN prod_kls.mdl_question_categories qc2 ON qc2.id = qc1.parent
                  LEFT JOIN prod_kls.mdl_question_categories qc3 ON qc3.id = qc2.parent
                  -- LEFT JOIN prod_kls.mdl_question_categories qc4 ON qc4.id = qc3.parent
                  WHERE  q1.qtype <>'random' and
                  qc3.name in ('ED ADAPTATIVO');
                  """

df = pd.read_sql(sql, conn)
df.head()
oferta.head()
est_disciplina.dtypes

# ajustar coluna seções de numero inteiro(int64) para tipo Objeto (string)
est_disciplina['DE_PARA'] = est_disciplina['DE_PARA'].astype(str)
oferta['seções'] = oferta['seções'].astype(str)


#Realiza um "PROCV"(MERGE) entre as tabelas OFERTA e ESTRUTURA DISCIPLINA informando as colunas que são iguais
base_oferta = pd.merge(oferta,
                        est_disciplina,
                        left_on=['Categoria_Principal', 'seções'],
                        right_on=['Categoria_Principal', 'DE_PARA'],
                        how='left')

len(base_oferta) ###Verifica o tamanho da tabela
base_oferta.head() ###trás os 5 primeiros

# CRIA TABELA AUXILIAR PARA REMOVER DUPLICADOS das colunas abaixo na tabela "de_para_banco"
aux_quiz = de_para_banco[['QUIZ_HARMONIZADO', 'QTD_MIN_QUESTOES']].drop_duplicates()

###Realiza um "PROCV"(MERGE) entre as tabelas BASE_OFERTA e AUX_QUIZ
base_oferta = pd.merge(base_oferta, aux_quiz,
                        left_on=['Categoria_QUIZ'],
                        right_on=['QUIZ_HARMONIZADO'],
                        how='left')


len(oferta)
len(base_oferta)
base_oferta.head()

######## Fim Estrutura Oferta disciplina

######## Higienizar parâmetros de Unidade e Quiz do Banco
###Realiza um "PROCV"(MERGE) entre as tabelas "df e de_para_banco", informando as colunas que possuem dados idênticos
base_oferta.head()
banco_questoes_higienizado = pd.merge(df,
                                      de_para_banco,
                                      on=['Categoria_Principal', 'Categoria_Unidade', 'Categoria_QUIZ'],
                                      how='left')

######## Fim henização parâmetros de Unidade e Quiz do Banco

####Tranforma todos os dados das colunas em tipo string(str)
banco_questoes_higienizado = banco_questoes_higienizado.astype(str)

######## Criar indicador de quantidades de questões
din_bq = banco_questoes_higienizado.groupby(['Categoria_Principal',
                                            'Categoria_Disciplina',
                                            'UNIDADE_HARMONIZADO',
                                            'Categoria_Seção',
                                            'QUIZ_HARMONIZADO']).size().reset_index(name = ('qtd_questoes'))

din_bq.head()
#CRIA UM NOVA TABELA PARA REMOVER DUPLICADOS DE ID DE QUESTÃO DA TABELA "banco_questoes_higienizado"
bqaux = banco_questoes_higienizado.drop('ID_QUESTAO', axis=1)
bqaux = bqaux.drop_duplicates()
bqaux.shape

######## Criar dinâmica de quantidades de questões distintas
bqaux.head()
aux_bq = bqaux.groupby(['Categoria_Principal',
                         'Categoria_Disciplina',
                         'UNIDADE_HARMONIZADO',
                         'Categoria_Seção',
                         'QUIZ_HARMONIZADO']).size().reset_index(name = ('qtd_questoes_distintas'))

######## Criar dinâmica de quantidades de questões vs quantidade de questões distintas
din_bq = pd.merge(din_bq,
                aux_bq,
                on=['Categoria_Principal',
                'Categoria_Disciplina',
                'UNIDADE_HARMONIZADO',
                'Categoria_Seção',
                'QUIZ_HARMONIZADO'],
                how='left')
din_bq.head()
din_bq.dtypes
##tranforma Ctegoria Principal em tipo objeto
din_bq['Categoria_Principal'] = din_bq['Categoria_Principal'].astype(str)
##tranforma Ctegoria Disciplina em letras maiusculas
din_bq['Categoria_Disciplina'] = din_bq['Categoria_Disciplina'].str.upper()
din_bq['UNIDADE_HARMONIZADO'] = din_bq['UNIDADE_HARMONIZADO'].astype(str)
din_bq['Categoria_Seção'] = din_bq['Categoria_Seção'].astype(str)
din_bq['QUIZ_HARMONIZADO'] = din_bq['QUIZ_HARMONIZADO'].astype(str)
base_oferta['Categoria_Principal'] = base_oferta['Categoria_Principal'].astype(str)
base_oferta['Categoria_Disciplina'] = base_oferta['Categoria_Disciplina'].str.upper()
base_oferta['Categoria_Unidade'] = base_oferta['Categoria_Unidade'].astype(str)
base_oferta['Categoria_Seção'] = base_oferta['Categoria_Seção'].astype(str)
base_oferta['Categoria_QUIZ'] = base_oferta['Categoria_QUIZ'].astype(str)

##Inicio da base final das necessidade das questões que estão falntando no AVA com base na lista de Oferta(P.O)
verifica_banco = pd.merge(base_oferta,
                        din_bq,
                        left_on=['Categoria_Principal','Categoria_Disciplina','Categoria_Unidade','Categoria_Seção','Categoria_QUIZ'],
                        right_on=['Categoria_Principal','Categoria_Disciplina','UNIDADE_HARMONIZADO','Categoria_Seção','QUIZ_HARMONIZADO'],
                        how='left')
##COMANDO SHAPE monstra a quantide de Linha e Colunas
verifica_banco.shape

## SELECIONA AS COLUNAS QUE DESEJA EXIBIR NA NOVA TABELA
verifica_banco = verifica_banco [['Categoria_Principal', 'Categoria_Disciplina', 'seções', 'Data_Material',
                                    'Categoria_Unidade', 'Categoria_Seção','Categoria_QUIZ', 'QTD_MIN_QUESTOES', 'qtd_questoes',
                                    'qtd_questoes_distintas']]

##TRANSFORMA DADOS "NAN" (NAN É  = NULO)  EM 0
verifica_banco['qtd_questoes'] = verifica_banco['qtd_questoes'].fillna(0)
verifica_banco['qtd_questoes_distintas'] = verifica_banco['qtd_questoes_distintas'].fillna(0)

##TRANSFORMA DADOS que estão com casas decimais em números inteiros, por exempro ( 5.0 em 5)
verifica_banco['qtd_questoes'] = verifica_banco['qtd_questoes'].astype('int64')
verifica_banco['qtd_questoes_distintas'] = verifica_banco['qtd_questoes_distintas'].astype('int64')


####### IRÁ INSERIR UMA COLUNA NO RESULTADO COM O STATUS DE 'OK' PARA OS QUESTIONÁRIOS LOCALIZADOS
#NA TABELA verifica_banco, ONDE O NÚMERO DE QUESTÕES QUE FOR MAIOR QUE 0, OS RESULTADOS QUE FOREM IGUAIS A 0 SERÃO
#EXIBIDOS O STATUS 'VERIFICAR'
verifica_banco['status'] = np.where((verifica_banco['qtd_questoes'] == verifica_banco['qtd_questoes_distintas'])
                                    & (verifica_banco['qtd_questoes'] != 0),
                                    'OK',
                                    'VERIFICAR')
verifica_banco.head(100)
verifica_banco.to_excel('verifica_banco.xlsx', encoding = 'latin-1')
