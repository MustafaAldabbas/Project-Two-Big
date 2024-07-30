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






def merge_web_dataframes(df_web_data1, df_web_data2, ignore_index=True):
    """
    Merge two dataframes into one.

    Parameters:
    df1 (pd.DataFrame): The first dataframe.
    df2 (pd.DataFrame): The second dataframe.
    ignore_index (bool): Whether to ignore the index or not. Default is True.

    Returns:
    pd.DataFrame: The merged dataframe.
    """
    web_data = pd.concat([df_web_data1,df_web_data2], ignore_index=ignore_index)
    return web_data





import pandas as pd

def check_missing_values_and_data_types(dfs, names):
    """
    Check for missing values and data types in multiple datasets.

    Parameters:
    dfs (list of pd.DataFrame): List of dataframes to check.
    names (list of str): List of names corresponding to the dataframes.

    Returns:
    dict: Dictionary containing missing values and data types for each dataframe.
    """
    result = {}
    for df, name in zip(dfs, names):
        missing_values = df.isnull().sum()
        data_types = df.dtypes
        result[name] = {
            'missing_values': missing_values,
            'data_types': data_types
        }
    return result





import pandas as pd

def clean_and_verify_data(df_demo, df_experiment_clients, df_web_data):
    """
    Clean the dataframes and verify the cleaning process.

    Parameters:
    df_demo (pd.DataFrame): Client demographical profiles dataframe.
    df_experiment_clients (pd.DataFrame): Experiment roster dataframe.
    df_web_data (pd.DataFrame): Web data dataframe.

    Returns:
    dict: Dictionary containing cleaned dataframes and verification results.
    """
    # Drop rows with missing values in Client demographical Profiles
    df_demo_cleaned = df_demo.dropna()

    # Drop rows with missing Variation in Experiment Roster
    df_experiment_clients_cleaned = df_experiment_clients.dropna(subset=['Variation'])

    # Convert date_time to datetime format in Web Data
    df_web_data['date_time'] = pd.to_datetime(df_web_data['date_time'])

    # Verify the cleaning process
    missing_values_demo_cleaned = df_demo_cleaned.isnull().sum()
    missing_values_experiment_cleaned = df_experiment_clients_cleaned.isnull().sum()
    data_types_web_cleaned = df_web_data.dtypes

    verification_results = {
        'missing_values_demo_cleaned': missing_values_demo_cleaned,
        'missing_values_experiment_cleaned': missing_values_experiment_cleaned,
        'data_types_web_cleaned': data_types_web_cleaned
    }

    cleaned_dataframes = {
        'df_demo_cleaned': df_demo_cleaned,
        'df_experiment_clients_cleaned': df_experiment_clients_cleaned,
        'df_web_data': df_web_data
    }

    return cleaned_dataframes, verification_results






import pandas as pd

def merge_client_data(df_demo, df_experiment_clients, on='client_id'):
    """
    Merge the cleaned client demographic data with the experiment client data.

    Parameters:
    df_demo_cleaned (pd.DataFrame): Cleaned client demographical profiles dataframe.
    df_experiment_clients_cleaned (pd.DataFrame): Cleaned experiment roster dataframe.
    on (str): Column name to merge on. Default is 'client_id'.

    Returns:
    pd.DataFrame: The merged dataframe.
    """
    df_merged = pd.merge(df_demo, df_experiment_clients, on=on)
    return df_merged




def merge_and_display_head(df_merged, df_web_data, on='client_id', n=5):
    """
    Merge df_merged with df_web_data on a specified column and display the first few rows.

    Parameters:
    df_merged (pd.DataFrame): The first dataframe.
    df_web_data (pd.DataFrame): The second dataframe.
    on (str): The column name to merge on. Default is 'client_id'.
    n (int): The number of rows to display from the merged dataframe. Default is 5.

    Returns:
    pd.DataFrame: The merged dataframe.
    """
    df = pd.merge(df_merged, df_web_data, on=on)
    print(df.head(n))
    return df




def clean_and_transform_dataframe(df):
    """
    Standardize column names, split date_time column, remove rows with NA values in 'variation',
    and change data types of specific columns in the dataframe.

    Parameters:
    df (pd.DataFrame): The input dataframe to be cleaned and transformed.

    Returns:
    pd.DataFrame: The cleaned and transformed dataframe.
    """
    # Dictionary to rename columns
    rename_dict = {
        'client_id': 'client_id',
        'clnt_tenure_yr': 'client_tenure_years',
        'clnt_tenure_mnth': 'client_tenure_months',
        'clnt_age': 'client_age',
        'gendr': 'gender',
        'num_accts': 'number_of_accounts',
        'bal': 'balance',
        'calls_6_mnth': 'calls_last_6_months',
        'logons_6_mnth': 'logins_last_6_months',
        'Variation': 'variation',
        'visitor_id': 'visitor_id',
        'visit_id': 'visit_id',
        'process_step': 'process_step',
        'date_time': 'date_time'
    }

    # Renaming columns
    df.rename(columns=rename_dict, inplace=True)

    # Splitting the date_time column into date and time columns
    df['date'] = pd.to_datetime(df['date_time']).dt.date
    df['time'] = pd.to_datetime(df['date_time']).dt.time
    df.drop(columns=['date_time'], inplace=True)

    # Remove rows with NA values in the 'variation' column
    df.dropna(subset=['variation'], inplace=True)

    # Change the data types of client_id, date, and time columns
    df['client_id'] = df['client_id'].astype(str)
    df['date'] = pd.to_datetime(df['date'])
    df['time'] = pd.to_datetime(df['time'].astype(str), format='%H:%M:%S').dt.time

    return df




import pandas as pd

def process_and_merge_data(df_web_data1, df_web_data2, df_demo, df_experiment_clients, on='client_id', n=5):
    """
    Merge web dataframes, client dataframes, and finally merge the result with the web data.

    Parameters:
    df_web_data1 (pd.DataFrame): The first web data dataframe.
    df_web_data2 (pd.DataFrame): The second web data dataframe.
    df_demo (pd.DataFrame): The client demographical profiles dataframe.
    df_experiment_clients (pd.DataFrame): The experiment roster dataframe.
    on (str): The column name to merge on. Default is 'client_id'.
    n (int): The number of rows to display from the merged dataframe. Default is 5.

    Returns:
    pd.DataFrame: The final merged dataframe.
    """
    # Merge web dataframes
    df_web_data = pd.concat([df_web_data1, df_web_data2], ignore_index=True)

    # Merge client demographical profiles with experiment roster
    df_merged = pd.merge(df_demo, df_experiment_clients, on=on)

    # Merge the result with web data
    df = pd.merge(df_merged, df_web_data, on=on)


    return df





import pandas as pd

def merge_clean_transform_data(df_web_data1, df_web_data2, df_demo, df_experiment_clients, on='client_id', n=5):
    """
    Merge web dataframes, clean client data, merge client data with web data, and transform the resulting dataframe.

    Parameters:
    df_web_data1 (pd.DataFrame): The first web data dataframe.
    df_web_data2 (pd.DataFrame): The second web data dataframe.
    df_demo (pd.DataFrame): The client demographical profiles dataframe.
    df_experiment_clients (pd.DataFrame): The experiment roster dataframe.
    on (str): The column name to merge on. Default is 'client_id'.
    n (int): The number of rows to display from the merged dataframe. Default is 5.

    Returns:
    pd.DataFrame: The final cleaned and transformed dataframe.
    """
    # Merge web dataframes
    df_web_data = pd.concat([df_web_data1, df_web_data2], ignore_index=True)

    # Clean client data
    def clean_and_verify_data(df_demo, df_experiment_clients, df_web_data):
        # Drop rows with missing values in Client demographical Profiles
        df_demo_cleaned = df_demo.dropna()

        # Drop rows with missing Variation in Experiment Roster
        df_experiment_clients_cleaned = df_experiment_clients.dropna(subset=['Variation'])

        # Convert date_time to datetime format in Web Data
        df_web_data['date_time'] = pd.to_datetime(df_web_data['date_time'])

        cleaned_dataframes = {
            'df_demo_cleaned': df_demo_cleaned,
            'df_experiment_clients_cleaned': df_experiment_clients_cleaned,
            'df_web_data': df_web_data
        }

        return cleaned_dataframes

    cleaned_data_frames = clean_and_verify_data(df_demo, df_experiment_clients, df_web_data)

    # Merge client demographical profiles with experiment roster
    df_merged = pd.merge(cleaned_data_frames['df_demo_cleaned'], cleaned_data_frames['df_experiment_clients_cleaned'], on=on)

    # Merge the result with web data
    df = pd.merge(df_merged, cleaned_data_frames['df_web_data'], on=on)

    # Transform the resulting dataframe
    def clean_and_transform_dataframe(df):
        rename_dict = {
            'client_id': 'client_id',
            'clnt_tenure_yr': 'client_tenure_years',
            'clnt_tenure_mnth': 'client_tenure_months',
            'clnt_age': 'client_age',
            'gendr': 'gender',
            'num_accts': 'number_of_accounts',
            'bal': 'balance',
            'calls_6_mnth': 'calls_last_6_months',
            'logons_6_mnth': 'logins_last_6_months',
            'Variation': 'variation',
            'visitor_id': 'visitor_id',
            'visit_id': 'visit_id',
            'process_step': 'process_step',
            'date_time': 'date_time'
        }

        # Renaming columns
        df.rename(columns=rename_dict, inplace=True)

        # Splitting the date_time column into date and time columns
        df['date'] = pd.to_datetime(df['date_time']).dt.date
        df['time'] = pd.to_datetime(df['date_time']).dt.time
        df.drop(columns=['date_time'], inplace=True)

        # Remove rows with NA values in the 'variation' column
        df.dropna(subset=['variation'], inplace=True)

        # Change the data types of client_id, date, and time columns
        df['client_id'] = df['client_id'].astype(str)
        df['date'] = pd.to_datetime(df['date'])
        df['time'] = pd.to_datetime(df['time'].astype(str), format='%H:%M:%S').dt.time

        return df

    df = clean_and_transform_dataframe(df)

    # Display the first few rows of the merged dataframe
    print(df.head(n))

    return df

# Example usage:
# Assuming df_web_data1, df_web_data2, df_demo, and df_experiment_clients are predefined dataframes
# df_final = merge_clean_transform_data(df_web_data1, df_web_data2, df_demo, df_experiment_clients)







import pandas as pd

def merge_clean_transform_data(df_web_data1, df_web_data2, df_demo, df_experiment_clients, on='client_id', n=5):
    """
    Merge web dataframes, clean client data, merge client data with web data, and transform the resulting dataframe.

    Parameters:
    df_web_data1 (pd.DataFrame): The first web data dataframe.
    df_web_data2 (pd.DataFrame): The second web data dataframe.
    df_demo (pd.DataFrame): The client demographical profiles dataframe.
    df_experiment_clients (pd.DataFrame): The experiment roster dataframe.
    on (str): The column name to merge on. Default is 'client_id'.
    n (int): The number of rows to display from the merged dataframe. Default is 5.

    Returns:
    pd.DataFrame: The final cleaned and transformed dataframe.
    """
    # Merge web dataframes
    df_web_data = pd.concat([df_web_data1, df_web_data2], ignore_index=True)

    # Clean client data
    def clean_and_verify_data(df_demo, df_experiment_clients, df_web_data):
        # Drop rows with missing values in Client demographical Profiles
        df_demo_cleaned = df_demo.dropna()

        # Drop rows with missing Variation in Experiment Roster
        df_experiment_clients_cleaned = df_experiment_clients.dropna(subset=['Variation'])

        # Convert date_time to datetime format in Web Data
        df_web_data['date_time'] = pd.to_datetime(df_web_data['date_time'])

        cleaned_dataframes = {
            'df_demo_cleaned': df_demo_cleaned,
            'df_experiment_clients_cleaned': df_experiment_clients_cleaned,
            'df_web_data': df_web_data
        }

        return cleaned_dataframes

    cleaned_data_frames = clean_and_verify_data(df_demo, df_experiment_clients, df_web_data)

    # Merge client demographical profiles with experiment roster
    df_merged = pd.merge(cleaned_data_frames['df_demo_cleaned'], cleaned_data_frames['df_experiment_clients_cleaned'], on=on)

    # Merge the result with web data
    df = pd.merge(df_merged, cleaned_data_frames['df_web_data'], on=on)

    # Transform the resulting dataframe
    def clean_and_transform_dataframe(df):
        rename_dict = {
            'client_id': 'client_id',
            'clnt_tenure_yr': 'client_tenure_years',
            'clnt_tenure_mnth': 'client_tenure_months',
            'clnt_age': 'client_age',
            'gendr': 'gender',
            'num_accts': 'number_of_accounts',
            'bal': 'balance',
            'calls_6_mnth': 'calls_last_6_months',
            'logons_6_mnth': 'logins_last_6_months',
            'Variation': 'variation',
            'visitor_id': 'visitor_id',
            'visit_id': 'visit_id',
            'process_step': 'process_step',
            'date_time': 'date_time'
        }

        # Renaming columns
        df.rename(columns=rename_dict, inplace=True)

        # Splitting the date_time column into date and time columns
        df['date'] = pd.to_datetime(df['date_time']).dt.date
        df['time'] = pd.to_datetime(df['date_time']).dt.time
        df.drop(columns=['date_time'], inplace=True)

        # Create a new column combining date and time
        df['date_time'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))

        # Drop the separate date and time columns
        df.drop(columns=['date', 'time'], inplace=True)

        # Remove rows with NA values in the 'variation' column
        df.dropna(subset=['variation'], inplace=True)

        # Change the data types of client_id
        df['client_id'] = df['client_id'].astype(str)

        return df

    df = clean_and_transform_dataframe(df)

    # Display the first few rows of the merged dataframe
    print(df.head(n))

    return df

# Example usage:
# Assuming df_web_data1, df_web_data2, df_demo, and df_experiment_clients are predefined dataframes
# df_final = merge_clean_transform_data(df_web_data1, df_web_data2, df_demo, df_experiment_clients)






import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def UnivariateAnalysis(df):
    """
    Perform univariate analysis on the given dataframe and display relevant statistics and visualizations.

    Parameters:
    df (pd.DataFrame): The dataframe to analyze.

    Returns:
    dict: A dictionary containing descriptive statistics for age, tenure, balance, logons, and calls.
    """
    results = {}

    required_columns = ['client_age', 'client_tenure_years', 'balance', 'logins_last_6_months', 'calls_last_6_months']
    for col in required_columns:
        if col not in df.columns:
            print(f"Column '{col}' not found in the dataframe. Available columns: {df.columns}")
            return results

    # Age distribution
    age_distribution = df['client_age'].describe()
    results['age_distribution'] = age_distribution

    # Tenure distribution
    tenure_distribution = df['client_tenure_years'].describe()
    results['tenure_distribution'] = tenure_distribution

    # Balance distribution
    balance_distribution = df['balance'].describe()
    results['balance_distribution'] = balance_distribution

    # Logons distribution
    logons_distribution = df['logins_last_6_months'].describe()
    results['logons_distribution'] = logons_distribution

    # Calls distribution
    calls_distribution = df['calls_last_6_months'].describe()
    results['calls_distribution'] = calls_distribution

    # Plotting
    fig, axes = plt.subplots(3, 2, figsize=(12, 12))
    fig.suptitle('Univariate Analysis', fontsize=16)

    sns.histplot(df['client_age'], color='orchid', bins=30, kde=True, ax=axes[0, 0])
    axes[0, 0].set_title('The Distribution of The Clients Age')
    axes[0, 0].set_xlabel('Age')
    axes[0, 0].set_ylabel('Frequency')

    sns.histplot(df['client_tenure_years'], color='red', bins=55, kde=True, ax=axes[0, 1])
    axes[0, 1].set_title('Distribution of Client Tenure (Years)')
    axes[0, 1].set_xlabel('Tenure (Years)')
    axes[0, 1].set_ylabel('Frequency')

    sns.histplot(df['balance'], bins=200, kde=True, ax=axes[1, 0])
    axes[1, 0].set_title('Distribution of Client Balance')
    axes[1, 0].set_xlabel('Balance')
    axes[1, 0].set_ylabel('Frequency')

    sns.histplot(df['logins_last_6_months'], color='olivedrab', bins=7, kde=True, ax=axes[1, 1])
    axes[1, 1].set_title('The Distribution of Clients Logins in the Last 6 Months')
    axes[1, 1].set_xlabel('Logins in 6 Months')
    axes[1, 1].set_ylabel('Frequency')

    sns.histplot(df['calls_last_6_months'], color='lightcoral', bins=7, kde=True, ax=axes[2, 0])
    axes[2, 0].set_title('Distribution of Calls in the Last 6 Months')
    axes[2, 0].set_xlabel('Calls in 6 Months')
    axes[2, 0].set_ylabel('Frequency')

    # Hide the empty subplot (axes[2, 1])
    fig.delaxes(axes[2, 1])

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.show()

    return results

# Example usage:
# Assuming df_merged is the dataframe to analyze
# analysis_results = UnivariateAnalysis(df_merged)





import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def BivariateAnalysis(df):
    """
    Perform bivariate analysis on the given dataframe and display relevant statistics and visualizations.

    Parameters:
    df (pd.DataFrame): The dataframe to analyze.

    Returns:
    dict: A dictionary containing correlation values for age vs. logins and number of accounts vs. balance.
    """
    results = {}

    # Correlation between age and logins
    age_logons_corr = df[['client_age', 'logins_last_6_months']].corr().iloc[0, 1]
    results['age_logons_corr'] = age_logons_corr

    # Correlation between number of accounts and balance
    accts_balance_corr = df[['number_of_accounts', 'balance']].corr().iloc[0, 1]
    results['accts_balance_corr'] = accts_balance_corr

    # Plotting
    fig, axes = plt.subplots(3, 2, figsize=(14, 12))
    fig.suptitle('Bivariate Analysis', fontsize=16)

    # Age vs. Logins Scatter Plot
    sns.scatterplot(x='client_age', y='logins_last_6_months', data=df, ax=axes[0, 0])
    axes[0, 0].set_title('Age vs. Logins in Last 6 Months')
    axes[0, 0].set_xlabel('Age')
    axes[0, 0].set_ylabel('Logins in Last 6 Months')

    # Number of Accounts vs. Balance Bar Plot
    sns.barplot(x='number_of_accounts', y='balance', data=df, ax=axes[0, 1])
    axes[0, 1].set_title('Number of Accounts vs. Balance')
    axes[0, 1].set_xlabel('Number of Accounts')
    axes[0, 1].set_ylabel('Balance')

    # Gender vs. Balance Box Plot
    sns.boxplot(x='gender', y='balance', data=df, ax=axes[1, 0])
    axes[1, 0].set_title('Gender vs. Balance')
    axes[1, 0].set_xlabel('Gender')
    axes[1, 0].set_ylabel('Balance')

    # Gender vs. Calls in Last 6 Months Bar Plot
    gender_calls_avg = df.groupby('gender')['calls_last_6_months'].mean().reset_index()
    sns.barplot(x='gender', y='calls_last_6_months', data=gender_calls_avg, ax=axes[1, 1])
    axes[1, 1].set_title('Average Calls in Last 6 Months by Gender')
    axes[1, 1].set_xlabel('Gender')
    axes[1, 1].set_ylabel('Average Calls in Last 6 Months')

    # Hiding the last empty plot in the grid
    fig.delaxes(axes[2, 0])
    fig.delaxes(axes[2, 1])

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.show()

    return results






import pandas as pd
import matplotlib.pyplot as plt

def ClientExperimentGroupsAnalysis(df):
    """
    Analyze the number of clients in the experiment for both test and control groups, and visualize the results.

    Parameters:
    df (pd.DataFrame): The dataframe to analyze.

    Returns:
    tuple: A tuple containing the number of clients in the test group and the control group.
    """
    # Ensure the 'process_step' column is present
    if 'process_step' not in df.columns:
        print(f"'process_step' column not found in the dataframe. Available columns: {df.columns}")
        return None

    # Calculate the number of unique clients
    unique_clients_count = df['client_id'].nunique()

    # Calculate the number of unique clients in the test group
    test_group_count = df[df['variation'] == 'Test']['client_id'].nunique()

    # Calculate the number of unique clients in the control group
    control_group_count = df[df['variation'] == 'Control']['client_id'].nunique()

    # Calculate the percentages
    test_group_percentage = (test_group_count / unique_clients_count) * 100
    control_group_percentage = (control_group_count / unique_clients_count) * 100

    # Display the percentages
    print(f"Percentage of clients in the test group: {test_group_percentage:.2f}%")
    print(f"Percentage of clients in the control group: {control_group_percentage:.2f}%")

    # Visualize the Percentage of the Test and the control group 
    labels = [f'Test Group: {test_group_count} Clients', f'Control Group: {control_group_count} Clients']
    sizes = [test_group_percentage, control_group_percentage]
    colors = ['#ff9999','#66b3ff']
    explode = (0.1, 0)  # explode the 1st slice

    # Plotting the pie chart
    plt.figure(figsize=(4, 4))
    plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
            shadow=True, startangle=140)
    plt.title('Percentage of Clients in Test and Control Groups')
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.show()

    return test_group_count, control_group_count





import pandas as pd
import matplotlib.pyplot as plt

def AgeDistributionInExperimentGroup(df):
    """
    Analyze and display the age distribution for clients in the test and control groups, and plot the distributions.

    Parameters:
    df (pd.DataFrame): The dataframe to analyze.

    Returns:
    tuple: A tuple containing the age distributions for the test group and the control group.
    """
    # Check if the required columns are present
    required_columns = ['variation', 'client_age']
    for col in required_columns:
        if col not in df.columns:
            print(f"Column '{col}' not found in the dataframe. Available columns: {df.columns}")
            return None

    # Age distribution for the test group
    age_distribution_test_group = df[df['variation'] == 'Test']['client_age'].describe()

    # Age distribution for the control group
    age_distribution_control_group = df[df['variation'] == 'Control']['client_age'].describe()

    # Display the age distributions
    print("Test Group Age Distribution:")
    print(age_distribution_test_group)
    print("\nControl Group Age Distribution:")
    print(age_distribution_control_group)

    # Plotting the age distribution for the test group
    plt.figure(figsize=(6, 3))

    plt.subplot(1, 2, 1)
    plt.hist(df[df['variation'] == 'Test']['client_age'], bins=30, color='blue', alpha=0.7)
    plt.title('Age Distribution - Test Group')
    plt.xlabel('Age')
    plt.ylabel('Frequency')

    # Plotting the age distribution for the control group
    plt.subplot(1, 2, 2)
    plt.hist(df[df['variation'] == 'Control']['client_age'], bins=30, color='green', alpha=0.7)
    plt.title('Age Distribution - Control Group')
    plt.xlabel('Age')
    plt.ylabel('Frequency')

    plt.tight_layout()
    plt.show()

    return age_distribution_test_group, age_distribution_control_group











import pandas as pd
import numpy as np
from statsmodels.stats.proportion import proportions_ztest
import matplotlib.pyplot as plt

def analyze_experiment_completion_rates(df):
    """
    Analyze the completion rates for test and control groups during the experiment timeframe,
    perform statistical tests, and visualize the results.

    Parameters:
    df (pd.DataFrame): The dataframe to analyze.

    Returns:
    dict: A dictionary containing the completion rates and statistical test results.
    """
    # Convert date_time to datetime format
    df['date_time'] = pd.to_datetime(df['date_time'])

    # Filter data for the experiment timeframe
    experiment_start = pd.Timestamp('2017-03-15')
    experiment_end = pd.Timestamp('2017-06-20')
    df_experiment = df[(df['date_time'] >= experiment_start) & (df['date_time'] <= experiment_end)]

    # Hypothesis: The new website drives more confirmations
    # Extract unique clients for completion rates
    completion_df = df_experiment[df_experiment['process_step'] == 'confirm'].drop_duplicates(subset='client_id')

    # Number of completions in each group
    completions_control = completion_df[completion_df['variation'] == 'Control']['client_id'].nunique()
    completions_test = completion_df[completion_df['variation'] == 'Test']['client_id'].nunique()

    # Number of clients in each group
    clients_control = df_experiment[df_experiment['variation'] == 'Control']['client_id'].nunique()
    clients_test = df_experiment[df_experiment['variation'] == 'Test']['client_id'].nunique()

    # Proportions
    completion_rate_control = completions_control / clients_control
    completion_rate_test = completions_test / clients_test

    # Perform two-proportion z-test
    count = np.array([completions_test, completions_control])
    nobs = np.array([clients_test, clients_control])
    stat, pval = proportions_ztest(count, nobs)

    # Display the results for Hypothesis
    completion_results = {
        'completion_rate_test': completion_rate_test,
        'completion_rate_control': completion_rate_control,
        'z_statistic': stat,
        'p_value': pval
    }

    # Completion Rates Pie Chart for Test Group
    fig, ax = plt.subplots(1, 2, figsize=(14, 7))

    ax[0].pie([completion_rate_test, 1 - completion_rate_test], labels=['Completed', 'Not Completed'], autopct='%1.1f%%', startangle=90, colors=['#ff9999', '#66b3ff'])
    ax[0].set_title('Completion Rates - Test Group')

    ax[1].pie([completion_rate_control, 1 - completion_rate_control], labels=['Completed', 'Not Completed'], autopct='%1.1f%%', startangle=90, colors=['#ff9999', '#66b3ff'])
    ax[1].set_title('Completion Rates - Control Group')

    plt.tight_layout()
    plt.show()

    # Print completion rates, z-statistic, and p-value
    print(f"Completion Rates:\nTest Group: {completion_rate_test:.2%}\nControl Group: {completion_rate_control:.2%}")
    print(f"Z-statistic: {stat:.2f}")
    print(f"P-value: {pval:.2e}")

    return completion_results










import pandas as pd
import numpy as np
from statsmodels.stats.proportion import proportions_ztest
import matplotlib.pyplot as plt

def analyze_experiment_error_rates(df):
    """
    Analyze the error rates for test and control groups during the experiment timeframe,
    perform statistical tests, and visualize the results.

    Parameters:
    df (pd.DataFrame): The dataframe to analyze.

    Returns:
    dict: A dictionary containing the error rates and statistical test results.
    """
    # Convert date_time to datetime format
    df['date_time'] = pd.to_datetime(df['date_time'])

    # Filter data for the experiment timeframe
    experiment_start = pd.Timestamp('2017-03-15')
    experiment_end = pd.Timestamp('2017-06-20')
    df_experiment = df[(df['date_time'] >= experiment_start) & (df['date_time'] <= experiment_end)]

    # Extract unique clients for completion rates
    completion_df = df_experiment[df_experiment['process_step'] == 'confirm'].drop_duplicates(subset='client_id')

    # Number of clients in each group
    clients_control = df_experiment[df_experiment['variation'] == 'Control']['client_id'].nunique()
    clients_test = df_experiment[df_experiment['variation'] == 'Test']['client_id'].nunique()

    # Define error as clients jumping between steps without confirming
    clients_with_errors = df_experiment[df_experiment.duplicated(subset=['client_id', 'process_step'], keep=False)]
    error_clients = clients_with_errors[~clients_with_errors['client_id'].isin(completion_df['client_id'])]

    # Number of errors in each group
    errors_control = error_clients[error_clients['variation'] == 'Control']['client_id'].nunique()
    errors_test = error_clients[error_clients['variation'] == 'Test']['client_id'].nunique()

    # Proportions
    error_rate_control = errors_control / clients_control
    error_rate_test = errors_test / clients_test

    # Perform two-proportion z-test
    count_errors = np.array([errors_test, errors_control])
    nobs_errors = np.array([clients_test, clients_control])
    stat_errors, pval_errors = proportions_ztest(count_errors, nobs_errors)

    # Save the results for Hypothesis 2
    error_results = {
        'error_rate_test': error_rate_test,
        'error_rate_control': error_rate_control,
        'z_statistic': stat_errors,
        'p_value': pval_errors
    }

    # Error Rates Pie Chart for Test Group
    fig, ax = plt.subplots(1, 2, figsize=(14, 7))

    ax[0].pie([error_rate_test, 1 - error_rate_test], labels=['Errors', 'No Errors'], autopct='%1.1f%%', startangle=90, colors=['#ff9999', '#66b3ff'])
    ax[0].set_title('Error Rates - Test Group')

    ax[1].pie([error_rate_control, 1 - error_rate_control], labels=['Errors', 'No Errors'], autopct='%1.1f%%', startangle=90, colors=['#ff9999', '#66b3ff'])
    ax[1].set_title('Error Rates - Control Group')

    plt.tight_layout()
    plt.show()

    # Print error rates, z-statistic, and p-value
    print(f"Error Rates:\nTest Group: {error_rate_test:.2%}\nControl Group: {error_rate_control:.2%}")
    print(f"Z-statistic: {stat_errors:.2f}")
    print(f"P-value: {pval_errors:.2e}")

    return error_results






import pandas as pd
from scipy.stats import ttest_ind
import matplotlib.pyplot as plt

def analyze_logins_between_groups(df):
    """
    Analyze the mean logins between test and control groups during the experiment timeframe,
    perform statistical tests, and visualize the results.

    Parameters:
    df (pd.DataFrame): The dataframe to analyze.

    Returns:
    pd.DataFrame: A dataframe containing the mean logins and statistical test results.
    """
    # Convert date_time to datetime format
    df['date_time'] = pd.to_datetime(df['date_time'])

    # Filter data for the experiment timeframe
    experiment_start = pd.Timestamp('2017-03-15')
    experiment_end = pd.Timestamp('2017-06-20')
    df_experiment = df[(df['date_time'] >= experiment_start) & (df['date_time'] <= experiment_end)]

    # Extract logins data for both groups
    logins_control = df_experiment[df_experiment['variation'] == 'Control']['logins_last_6_months']
    logins_test = df_experiment[df_experiment['variation'] == 'Test']['logins_last_6_months']

    # Perform t-test to compare means
    t_stat, p_val = ttest_ind(logins_test, logins_control, equal_var=False)

    # Calculate mean logins for both groups
    mean_logins_test = logins_test.mean()
    mean_logins_control = logins_control.mean()

    # Save the results in a new dataframe
    results = {
        'Metric': ['Mean Logins', 'T-Statistic', 'P-Value'],
        'Test Group': [mean_logins_test, t_stat, p_val],
        'Control Group': [mean_logins_control, t_stat, p_val]
    }

    login_testing_results = pd.DataFrame(results)

    # Visualize the results with a bar chart
    fig, ax = plt.subplots()
    groups = ['Test Group', 'Control Group']
    means = [mean_logins_test, mean_logins_control]
    ax.bar(groups, means, color=['blue', 'orange'])
    ax.set_title('Mean Logins in the Last 6 Months')
    ax.set_ylabel('Mean Number of Logins')

    # Print mean logins, t-statistic, and p-value
    print(f"Mean Logins:\nTest Group: {mean_logins_test:.2f}\nControl Group: {mean_logins_control:.2f}")
    print(f"T-statistic: {t_stat:.2f}")
    print(f"P-value: {p_val:.2e}")

    plt.show()

    return login_testing_results






import pandas as pd
import numpy as np
from scipy.stats import ttest_ind
import matplotlib.pyplot as plt

def analyze_time_per_step_between_groups(df):
    """
    Analyze the average time spent on each step between test and control groups during the experiment timeframe,
    perform statistical tests, and visualize the results.

    Parameters:
    df (pd.DataFrame): The dataframe to analyze.

    Returns:
    tuple: A tuple containing two dataframes - average time per step and t-test results.
    """
    # Convert date_time to datetime format
    df['date_time'] = pd.to_datetime(df['date_time'])

    # Filter data for the experiment timeframe
    experiment_start = pd.Timestamp('2017-03-15')
    experiment_end = pd.Timestamp('2017-06-20')
    df_experiment = df[(df['date_time'] >= experiment_start) & (df['date_time'] <= experiment_end)]

    # Sort the data by visit_id and date_time to calculate the time difference between steps
    df_experiment = df_experiment.sort_values(by=['visit_id', 'date_time'])

    # Calculate the time difference between consecutive steps
    df_experiment['time_diff'] = df_experiment.groupby('visit_id')['date_time'].diff()

    # Convert time_diff to seconds
    df_experiment['time_diff_seconds'] = df_experiment['time_diff'].dt.total_seconds()

    # Calculate the average time spent on each step based on visit_id and variation (control or test)
    avg_time_per_step = df_experiment.groupby(['variation', 'process_step'])['time_diff_seconds'].mean().reset_index()

    # Separate the test and control groups
    test_group = df_experiment[df_experiment['variation'] == 'Test']
    control_group = df_experiment[df_experiment['variation'] == 'Control']

    # Perform t-test to compare times for each step
    steps = avg_time_per_step['process_step'].unique()
    t_test_results = []

    for step in steps:
        test_times = test_group[test_group['process_step'] == step]['time_diff_seconds'].dropna()
        control_times = control_group[control_group['process_step'] == step]['time_diff_seconds'].dropna()
        t_stat, p_val = ttest_ind(test_times, control_times, equal_var=False)
        t_test_results.append({'process_step': step, 't_statistic': t_stat, 'p_value': p_val})

    # Save the t-test results in a dataframe
    t_test_results_df = pd.DataFrame(t_test_results)

    # Save the average time per step results to a CSV file
    avg_time_per_step.to_csv('avg_time_per_step.csv', index=False)
    t_test_results_df.to_csv('t_test_results.csv', index=False)

    # Visualize the average time spent on each step
    fig, ax = plt.subplots(figsize=(14, 8))
    for label, df_group in avg_time_per_step.groupby('variation'):
        ax.plot(df_group['process_step'], df_group['time_diff_seconds'], marker='o', label=label)

    ax.set_title('Average Time Spent on Each Step by Group')
    ax.set_xlabel('Process Step')
    ax.set_ylabel('Average Time Spent (Seconds)')
    ax.legend(title='Group')
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Print average time per step and t-test results
    print("Average Time Spent on Each Step:")
    print(avg_time_per_step)
    print("\nT-test Results for Each Step:")
    print(t_test_results_df)

    plt.show()

    return avg_time_per_step, t_test_results_df


