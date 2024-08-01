# Load the data
Import Dan_functions as fn

demo_path = '../Data/Raw/df_final_demo (1).txt'
client_path = '../Data/Raw/df_final_experiment_clients.txt'
web_path1 = '../Data/Raw/df_final_web_data_pt_1.txt'
web_path2 = '../Data/Raw/df_final_web_data_pt_2.txt'


final_df = load_merge_date(demo_path, client_path, web_path1, web_path2)
clean_data(final_df)