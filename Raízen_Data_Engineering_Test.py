import pandas as pd
from datetime import datetime
#Foram gerados 2 arquivos a serem lidos (um sobre dados de Derivados de Petróleo e outro sobre Diesel) para geração das tabelas solicitadas.
files = 2 #Número de arquivos a serem lidos para geração das tabelas.
for p in range(files):
    #Faz a leitura de quantidade de abas para iteração das extrações e valida os dados do arquivo de origem para início do processamento.
    file_name = 'vendas-combustiveis-m3-{}.xls'.format(p)
    interact_columns = "A,B,D:Q" #colunas que serão utilizadas
    columns_name_parameter = ['COMBUSTÍVEL' , 'ANO', 'ESTADO', 'UNIDADE', 'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
    change_col_name = {'Jan': '01-', 'Fev': '02-', 'Mar': '03-', 'Abr': '04-', 'Mai': '05-', 'Jun': '06-', 'Jul': '07-', 'Ago': '08-', 'Set': '09-', 'Out': '10-', 'Nov': '11-', 'Dez': '12-'}
    change_col_name_trf = {'01-': 'Jan', '02-': 'Fev','03-': 'Mar', '04-': 'Abr',  '05-': 'Mai', '06-': 'Jun',  '07-': 'Jul',  '08-': 'Ago', '09-': 'Set',  '10-': 'Out',  '11-': 'Nov',  '12-': 'Dez'}
    change_col_name_ods = {'Year_Month': 'MES_ANO', 'UF': 'ESTADO', 'Product': 'COMBUSTÍVEL', 'Unit': 'UNIDADE', 'Volume': 'VALOR', 'Created_at': 'DT_Criacao'}
    change_col_name_final = {'MES_ANO': 'Year_Month', 'ESTADO': 'UF', 'COMBUSTÍVEL': 'Product', 'UNIDADE': 'Unit', 'VALOR': 'Volume', 'DT_Criacao': 'Created_at'}
    xls = pd.ExcelFile(file_name)
    res = len(xls.sheet_names)
    m=0 #Se ao final da etapa de validação for igual ao valor do número de abas, vão passar para a etapa de extração.
    vl_nm_col = []
    stop = 0
    for h in range(res):
        valida_arquivo = pd.read_excel(file_name, sheet_name=h, usecols = interact_columns)
        for col_name_xls in valida_arquivo.columns:
            vl_nm_col.append(col_name_xls)
        if vl_nm_col == columns_name_parameter:
            vl_nm_col = []
            m +=1
        else:
            vl_nm_col = []
            print('Falha na aba {}'.format(m+1))
            stop = 1
            break #Para o loop

    if stop == 0:       
        print('Validação do arquivo {} finalizada, iniciando ETL'.format(file_name))
                
        for i in range(res):
            #EXTRACTION
            arquivo_xls = pd.read_excel(file_name, sheet_name=i, usecols = interact_columns)
            DF_TRF = arquivo_xls
            if i == 0 and p ==0:
                print('Iteração {} de {}'.format(i+1, res))
                DF_EXT = DF_TRF
                #Transformation
                DF_TRF.rename(columns=change_col_name, inplace = True)
                DF_TRF = DF_TRF.melt(id_vars = ['COMBUSTÍVEL', 'ANO', 'ESTADO', 'UNIDADE'], value_vars = ['01-','02-', '03-', '04-', '05-', '06-', '07-', '08-', '09-', '10-', '11-', '12-'], var_name='MES', value_name='VALOR')
                DF_TRF['COMBUSTÍVEL'] = DF_TRF['COMBUSTÍVEL'].str.replace('m3', '', regex=True)
                DF_TRF['COMBUSTÍVEL'] = DF_TRF['COMBUSTÍVEL'].str.replace('(','', regex=True)
                DF_TRF['COMBUSTÍVEL'] = DF_TRF['COMBUSTÍVEL'].str.replace(')','', regex=True)
                DF_TRF['MES_ANO'] = DF_TRF['MES'].map(str)+ DF_TRF['ANO'].map(str)
                DF_TRF = DF_TRF.fillna(0)
                dt = datetime.now()
                DF_TRF['DT_Criacao'] = dt.strftime("%d-%m-%Y %H:%M:%S")
                DF_TRF = DF_TRF[['MES_ANO','ESTADO','COMBUSTÍVEL', 'UNIDADE', 'VALOR', 'DT_Criacao']]
                DF_ODS = DF_TRF
                #LOAD
                TABELA_FINAL_1 = DF_TRF
                DF_TRF = pd.DataFrame(columns=DF_TRF.columns)
                DF_EXT.rename(columns=change_col_name_trf, inplace = True)
                DF_ODS.rename(columns=change_col_name_ods, inplace = True)
                TABELA_FINAL_1.rename(columns=change_col_name_final, inplace = True)
                
            elif i != 0 and p==0:
                print('Iteração {} de {}'.format(i+1, res))
                DF_EXT = DF_EXT.append(DF_TRF, ignore_index=True)
                #Transformation
                DF_TRF.rename(columns=change_col_name, inplace = True)
                DF_TRF = DF_TRF.melt(id_vars = ['COMBUSTÍVEL', 'ANO', 'ESTADO', 'UNIDADE'], value_vars = ['01-','02-', '03-', '04-', '05-', '06-', '07-', '08-', '09-', '10-', '11-', '12-'], var_name='MES', value_name='VALOR')
                DF_TRF['COMBUSTÍVEL'] = DF_TRF['COMBUSTÍVEL'].str.replace('m3', '', regex=True)
                DF_TRF['COMBUSTÍVEL'] = DF_TRF['COMBUSTÍVEL'].str.replace('(','', regex=True)
                DF_TRF['COMBUSTÍVEL'] = DF_TRF['COMBUSTÍVEL'].str.replace(')','', regex=True)
                DF_TRF['MES_ANO'] = DF_TRF['MES'].map(str)+ DF_TRF['ANO'].map(str)
                DF_TRF = DF_TRF.fillna(0)
                dt = datetime.now()
                DF_TRF['DT_Criacao'] = dt.strftime("%d-%m-%Y %H:%M:%S")
                DF_TRF = DF_TRF[['MES_ANO','ESTADO','COMBUSTÍVEL', 'UNIDADE', 'VALOR', 'DT_Criacao']]
                DF_ODS = DF_ODS.append(DF_TRF, ignore_index=True)
                #LOAD
                DF_TRF.rename(columns=change_col_name_final, inplace = True)
                TABELA_FINAL_1 = TABELA_FINAL_1.append(DF_TRF, ignore_index=True)
                DF_TRF = pd.DataFrame(columns=DF_TRF.columns)
                
            if i == 0 and p==1:
                print('Iteração {} de {}'.format(i+1, res))
                DF_EXT_2 = DF_TRF
                #Transformation
                DF_TRF.rename(columns=change_col_name, inplace = True)
                DF_TRF = DF_TRF.melt(id_vars = ['COMBUSTÍVEL', 'ANO', 'ESTADO', 'UNIDADE'], value_vars = ['01-','02-', '03-', '04-', '05-', '06-', '07-', '08-', '09-', '10-', '11-', '12-'], var_name='MES', value_name='VALOR')
                DF_TRF['COMBUSTÍVEL'] = DF_TRF['COMBUSTÍVEL'].str.replace('m3', '', regex=True)
                DF_TRF['COMBUSTÍVEL'] = DF_TRF['COMBUSTÍVEL'].str.replace('(','', regex=True)
                DF_TRF['COMBUSTÍVEL'] = DF_TRF['COMBUSTÍVEL'].str.replace(')','', regex=True)
                DF_TRF['MES_ANO'] = DF_TRF['MES'].map(str)+ DF_TRF['ANO'].map(str)
                DF_TRF = DF_TRF.fillna(0)
                dt = datetime.now()
                DF_TRF['DT_Criacao'] = dt.strftime("%d-%m-%Y %H:%M:%S")
                DF_TRF = DF_TRF[['MES_ANO','ESTADO','COMBUSTÍVEL', 'UNIDADE', 'VALOR', 'DT_Criacao']]
                DF_ODS_2 = DF_TRF
                #LOAD
                TABELA_FINAL_2 = DF_TRF
                DF_TRF = pd.DataFrame(columns=DF_TRF.columns)
                DF_EXT_2.rename(columns=change_col_name_trf, inplace = True)
                DF_ODS_2.rename(columns=change_col_name_ods, inplace = True)
                TABELA_FINAL_2.rename(columns=change_col_name_final, inplace = True)
                
            elif i !=0 and p==1:
                print('Iteração {} de {}'.format(i+1, res))
                DF_EXT_2 = DF_EXT_2.append(DF_TRF, ignore_index=True)
                #Transformation
                DF_TRF.rename(columns=change_col_name, inplace = True)
                DF_TRF = DF_TRF.melt(id_vars = ['COMBUSTÍVEL', 'ANO', 'ESTADO', 'UNIDADE'], value_vars = ['01-','02-', '03-', '04-', '05-', '06-', '07-', '08-', '09-', '10-', '11-', '12-'], var_name='MES', value_name='VALOR')
                DF_TRF['COMBUSTÍVEL'] = DF_TRF['COMBUSTÍVEL'].str.replace('m3', '', regex=True)
                DF_TRF['COMBUSTÍVEL'] = DF_TRF['COMBUSTÍVEL'].str.replace('(','', regex=True)
                DF_TRF['COMBUSTÍVEL'] = DF_TRF['COMBUSTÍVEL'].str.replace(')','', regex=True)
                DF_TRF['MES_ANO'] = DF_TRF['MES'].map(str)+ DF_TRF['ANO'].map(str)
                DF_TRF = DF_TRF.fillna(0)
                dt = datetime.now()
                DF_TRF['DT_Criacao'] = dt.strftime("%d-%m-%Y %H:%M:%S")
                DF_TRF = DF_TRF[['MES_ANO','ESTADO','COMBUSTÍVEL', 'UNIDADE', 'VALOR', 'DT_Criacao']]
                DF_ODS_2 = DF_ODS_2.append(DF_TRF, ignore_index=True)
                #LOAD
                DF_TRF.rename(columns=change_col_name_final, inplace = True)
                TABELA_FINAL_2 = TABELA_FINAL_2.append(DF_TRF, ignore_index=True)
                DF_TRF = pd.DataFrame(columns=DF_TRF.columns)
                
    
    else:
        if p ==0:
            print('Revisar a {}ª aba do arquivo origem de Vendas de Derivados de Petróleo'.format(m+1))
        else:   
            print('Revisar a {}ª aba do arquivo origem de Vendas de Diesel'.format(m+1))
    
    if stop == 0 and p==0:
        TABELA_FINAL_1.to_csv('Sales_Oil_Derivative_Fuels', sep ='|')
        Total_Final = TABELA_FINAL_1['Volume'].sum()
        DF_EXT.to_csv('Extracoes_Petroleo', sep ='|')
        col_list_sum = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
        Total_raw = DF_EXT[col_list_sum].sum(axis=1).sum()
        DF_ODS.to_csv('Intermediarios_Petroleo', sep ='|')
        display(TABELA_FINAL_1)
        Diff_vol_oil = Total_Final - Total_raw
        print('Há uma diferença de {} m³ entre a carga e os dados de origem'.format(Diff_vol_oil))
        print('------------------------------------------------------------------------------------')
    elif stop == 0 and p==1:
        TABELA_FINAL_2.to_csv('Sales_of_Diesel', sep ='|')
        Total_Final_2 = TABELA_FINAL_2['Volume'].sum()
        DF_EXT_2.to_csv('Extracoes_Diesel', sep ='|')
        col_list_sum = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
        Total_raw_2 = DF_EXT_2[col_list_sum].sum(axis=1).sum()
        DF_ODS_2.to_csv('Intermediarios_Diesel', sep ='|')
        display(TABELA_FINAL_2)
        Diff_vol_diesel = Total_Final_2 - Total_raw_2
        print('Há uma diferença de {} m³ entre a carga e os dados de origem'.format(Diff_vol_diesel))
    else:
        None 