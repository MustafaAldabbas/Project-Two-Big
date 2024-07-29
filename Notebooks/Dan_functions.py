
import pandas as pd

def load_merge_date(demo_path, client_path, web_path1, web_path2) -> pd.DataFrame:
    """
    Load and merge data from multiple sources
    """
    # load data
    demo_df= pd.read_csv(demo_path)
    client_df= pd.read_csv(client_path)
    web_df1= pd.read_csv(web_path1)
    web_df2= pd.read_csv(web_path2)

    # concat web data
    concat_web_df= pd.concat([web_df1, web_df2])

    # merge data
    
    merged_web_client_df= pd.merge(concat_web_df,client_df, on='client_id', how='inner')
    final_df= pd.merge(merged_web_client_df, demo_df, on='client_id', how='left')

    return final_df

def clean_data(final_df) -> pd.DataFrame:
    """
    drop duplicates and missing values
    """
    # drop duplicates
    final_df.drop_duplicates(inplace=True)

    # drop missing values
    final_df.dropna(inplace=True)

    # change column names to lower case
    final_df.columns = final_df.columns.str.lower()

    # drop columns gender = x
    final_df = final_df[final_df['gendr'] != 'X']

    return final_df

def save_dataframe_to_csv(dataframe, file_path):
    dataframe.to_csv(file_path, index=False)
    return 

def create_pairplot(dataframe):
    sns.pairplot(dataframe, vars=['clnt_age', 'num_accts', 'clnt_tenure_yr', 'bal', 'calls_6_mnth', 'logons_6_mnth'], 
             diag_kind='kde', plot_kws={'color':'skyblue', 'alpha':0.5}, 
             diag_kws={'color':'skyblue', 'alpha':0.5})
    plt.show()

# create a function to create a plot for num_accounts
def generate_account_plot(dataframe):
    account_plot = dataframe['num_accts'].value_counts() \
        .head(10) \
        .plot(kind='bar', figsize=(10, 6), color='skyblue', fontsize=13, rot=45, title='Top 10 Number of Accounts')
    account_plot.set_xlabel('Top 10 Number of Accounts')
    account_plot.set_ylabel('Count')
    return account_plot


