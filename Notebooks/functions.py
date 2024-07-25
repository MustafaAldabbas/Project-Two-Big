import pandas as pd

def read_data_files(demo_file, web_data1_file, web_data2_file, experiment_clients_file):
    """
    Reads multiple CSV files into pandas DataFrames.
    
    Parameters:
    demo_file (str): '../Data/Raw data/df_final_demo (1).txt'.
    web_data1_file (str): ../Data/Raw data/df_final_web_data_pt_1.txt'.
    web_data2_file (str): '../Data/Raw data/df_final_web_data_pt_2.txt'.
    experiment_clients_file (str):'../Data/Raw data/df_final_experiment_clients.txt'.
    
    Returns:
    tuple: A tuple containing four pandas DataFrames.
    """
    df_demo = pd.read_csv(demo_file, delimiter=',', header=0)
    df_web_data1 = pd.read_csv(web_data1_file, delimiter=',', header=0)
    df_web_data2 = pd.read_csv(web_data2_file, delimiter=',', header=0)
    df_experiment_clients = pd.read_csv(experiment_clients_file, delimiter=',', header=0)
    
    return df_demo, df_web_data1, df_web_data2, df_experiment_clients


demo_file = '../Data/Raw data/df_final_demo (1).txt'
web_data1_file = '../Data/Raw data/df_final_web_data_pt_1.txt'
web_data2_file = '../Data/Raw data/df_final_web_data_pt_2.txt'
experiment_clients_file = '../Data/Raw data/df_final_experiment_clients.txt'

df_demo, df_web_data1, df_web_data2, df_experiment_clients = read_data_files(
    demo_file, web_data1_file, web_data2_file, experiment_clients_file)
